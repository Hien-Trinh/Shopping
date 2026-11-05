"""The ingest query on real local Delta tables, with Landing logs from the API's own delta-rs
(docs/specs/step-a2.md)."""

import json
import subprocess
import uuid
from pathlib import Path

from delta.tables import DeltaTable
from support import delete, listing, poison, up

from ingestion import ingest, replay
from ingestion.envelope import Change
from ingestion.status import Outcome as O

ROOT = Path(__file__).parents[2]
LAND = Path(__file__).with_name("land.py")
HOUR = "2023-11-14T22"  # the hour of every submission() below


def submission(n: int) -> str:
    """A UUIDv7 at 2023-11-14 22:13:20 UTC, as tables.hour reads them."""
    return str(uuid.UUID(f"{1_700_000_000_000:012x}7000{0x8000:04x}{n:012x}"))


def land(data: Path, *steps: list) -> list:
    args = ["uv", "run", "--frozen", "python", str(LAND), str(data)]
    done = subprocess.run(args, input=json.dumps(steps), capture_output=True, text=True, cwd=ROOT)
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


def commit(n: int, changes: list[Change]) -> list:
    return ["commit", [
        [submission(n), i, c.merchant_id, c.merchant_product_id, c.source_version, c.op,
         None if c.listing is None else dict(c.listing)]
        for i, c in enumerate(changes)
    ]]  # fmt: skip


def cached(spark) -> bool:
    return not spark._jsparkSession.sharedState().cacheManager().isEmpty()


def run(spark, data: Path):
    query = ingest.start(spark, str(data), trigger={"availableNow": True})
    query.awaitTermination()
    assert not cached(spark)  # each batch released its plan
    return query


def fingerprints(spark, data: Path) -> dict:
    rows = spark.read.format("delta").load(f"{data}/listing_store").collect()
    return {(r.merchant_id, r.merchant_product_id): replay.fingerprint(r.asDict()) for r in rows}


def outcomes(spark, data: Path) -> list[tuple]:  # a list: a duplicate row would show
    return sorted(tuple(r) for r in spark.read.format("delta").load(f"{data}/outcomes").collect())


def store_rows(spark, data: Path) -> list[tuple]:
    rows = spark.read.format("delta").load(f"{data}/listing_store").drop("updated_at").collect()
    return sorted(tuple(r) for r in rows)  # a key is unique, so sorting stops before any null


def batches(query) -> int:
    return sum(p.numInputRows > 0 for p in query.recentProgress)


def version(spark, table: Path) -> int:
    return DeltaTable.forPath(spark, str(table)).history(1).first().version


# Each Change with the Outcome it gets. "tie" holds a higher seq in the earlier commit, and at a
# tiny cap the last commit reads "gone"'s stored Tombstone.
BACKLOG = [
    [
        (up("plain", 1), O.WRITTEN),
        (up("tie", 5, listing(title="first")), O.WRITTEN),
        (up("newer", 3), O.WRITTEN),
        (up("again", 1), O.WRITTEN),
        (up("gone", 1), O.WRITTEN),
    ],
    [
        (up("tie", 5, listing(title="second")), O.CONFLICT),
        (up("newer", 2), O.STALE),
        (up("again", 1), O.ALREADY_APPLIED),
        (delete("gone", 2), O.WRITTEN),
        (up("spoiled", 1), O.WRITTEN),
        (poison("spoiled", 2), O.FAILED),
        (up("spoiled", 3), O.WRITTEN),
    ],
    [
        (up("gone", 1), O.STALE),
        (up("gone", 3), O.WRITTEN),
    ],
]


BACKLOG_OUTCOMES = sorted(
    (HOUR, submission(n), i, outcome.value)
    for n, changes in enumerate(BACKLOG)
    for i, (_, outcome) in enumerate(changes)
)


def land_backlog(data: Path) -> None:
    commits = [commit(n, [c for c, _ in changes]) for n, changes in enumerate(BACKLOG)]
    land(data, ["init"], *commits)


def test_a_backlog_lands_as_the_store_oracle_says_with_one_outcome_per_change(spark, tmp_path):
    land_backlog(tmp_path)
    run(spark, tmp_path)
    landed = [pair for changes in BACKLOG for pair in changes]
    failed = {i for i, (_, outcome) in enumerate(landed) if outcome is O.FAILED}
    oracle = replay.expected_store([c for c, _ in landed], failed)
    assert replay.diff(oracle, fingerprints(spark, tmp_path)) == []
    assert outcomes(spark, tmp_path) == BACKLOG_OUTCOMES


def test_the_same_log_at_any_cap_gives_the_same_store_and_outcomes(spark, tmp_path, monkeypatch):
    large, tiny = tmp_path / "large", tmp_path / "tiny"
    land_backlog(large)
    assert batches(run(spark, large)) == 1
    land_backlog(tiny)
    monkeypatch.setattr(ingest, "MAX_BYTES", 1)
    assert batches(run(spark, tiny)) > 1
    assert store_rows(spark, tiny) == store_rows(spark, large)
    assert outcomes(spark, tiny) == outcomes(spark, large)


def test_a_price_past_a_floats_precision_stays_exact_and_replays_as_already_applied(
    spark, tmp_path
):
    big = up("big", 1, listing(price=2**53 + 1))
    land(tmp_path, ["init"], commit(0, [big]))
    run(spark, tmp_path)
    land(tmp_path, commit(1, [big]))
    run(spark, tmp_path)
    assert outcomes(spark, tmp_path) == [
        (HOUR, submission(0), 0, "written"),
        (HOUR, submission(1), 0, "already_applied"),
    ]
    store = spark.read.format("delta").load(f"{tmp_path}/listing_store")
    assert store.first().price_micros == 2**53 + 1


def test_a_compaction_and_a_retention_delete_add_no_outcome_and_no_commit(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("plain", 1)]), commit(1, [up("other", 1)]))
    run(spark, tmp_path)
    written = [tmp_path / "outcomes", tmp_path / "listing_store"]
    before = outcomes(spark, tmp_path), [version(spark, t) for t in written]
    # each did its work: two files compacted, two rows deleted
    assert land(tmp_path, ["compact"], ["delete", "received_at < '2100-01-01T00:00:00+00:00'"]) == [
        2,
        2,
    ]
    assert batches(run(spark, tmp_path)) == 1  # the query did read the DELETE's rows
    assert (outcomes(spark, tmp_path), [version(spark, t) for t in written]) == before


def test_the_store_has_its_layout(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("plain", 1)]))
    run(spark, tmp_path)
    table = DeltaTable.forPath(spark, str(tmp_path / "listing_store"))
    detail = table.detail().first()
    assert detail.clusteringColumns == ["merchant_id", "merchant_product_id"]
    assert detail.properties["delta.enableDeletionVectors"] == "true"
    assert detail.properties["delta.enableChangeDataFeed"] == "true"
    assert not table.toDF().schema["source_version"].nullable  # Delta enforces it at the MERGE


def test_outcomes_stay_readable_by_delta_rs_past_sparks_checkpoint(spark, tmp_path):
    land(tmp_path, ["init"])
    columns = "hour string, submission_id string, change_index int, outcome string"
    for n in range(12):
        row = spark.createDataFrame([(HOUR, submission(n), 0, "written")], columns)
        ingest.append_outcomes(str(tmp_path / "outcomes"), row, "query", n)
    assert (tmp_path / "outcomes/_delta_log/00000000000000000010.checkpoint.parquet").exists()
    assert land(tmp_path, ["outcomes"]) == [[12, 1, 2, None, None]]


def test_a_plan_recomputed_after_its_merge_gives_the_same_outcomes(spark, tmp_path, monkeypatch):
    merge = ingest.merge

    def merge_then_drop_the_persist(path, writes):
        merge(path, writes)
        assert cached(spark)
        spark.catalog.clearCache()

    monkeypatch.setattr(ingest, "merge", merge_then_drop_the_persist)
    land_backlog(tmp_path)
    run(spark, tmp_path)
    assert outcomes(spark, tmp_path) == BACKLOG_OUTCOMES


def test_a_kill_before_the_first_offset_still_reads_commit_by_commit(spark, tmp_path):
    land_backlog(tmp_path)
    checkpoint = (
        tmp_path / "checkpoints/ingest"
    )  # as Spark leaves it from the start to the first batch
    (checkpoint / "offsets").mkdir(parents=True)
    (checkpoint / "metadata").write_text(f'{{"id":"{uuid.uuid4()}"}}\n')
    run(spark, tmp_path)
    assert outcomes(spark, tmp_path) == BACKLOG_OUTCOMES


def test_a_batch_writes_one_file_per_table(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up(f"p{n}", 1) for n in range(20)]))
    run(spark, tmp_path)
    assert len(list(tmp_path.glob("outcomes/*/*.parquet"))) == 1
    assert len(list(tmp_path.glob("listing_store/*.parquet"))) == 1
