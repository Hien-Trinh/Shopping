"""The ingest query on real local Delta tables, with Landing logs from the API's own delta-rs
(docs/specs/step-a2.md)."""

import json
import re
import shutil
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from delta.exceptions import (
    ConcurrentAppendException,
    ConcurrentDeleteDeleteException,
    ConcurrentDeleteReadException,
)
from delta.tables import DeltaMergeBuilder, DeltaTable
from pyspark.errors import AnalysisException, StreamingQueryException
from pyspark.sql import functions as F
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


# What a replay makes of BACKLOG once its MERGE has landed: rule 3 plans it against the head.
BACKLOG_REPLAYED = sorted(
    (HOUR, submission(n), i, outcome.value)
    for n, changes in enumerate(
        [
            [O.ALREADY_APPLIED, O.ALREADY_APPLIED, O.ALREADY_APPLIED, O.ALREADY_APPLIED, O.STALE],
            [O.CONFLICT, O.STALE, O.ALREADY_APPLIED, O.STALE, O.STALE, O.FAILED, O.ALREADY_APPLIED],
            [O.STALE, O.ALREADY_APPLIED],
        ]
    )
    for i, outcome in enumerate(changes)
)


BACKLOG_COMMITS = [commit(n, [c for c, _ in changes]) for n, changes in enumerate(BACKLOG)]


def land_backlog(data: Path) -> None:
    land(data, ["init"], *BACKLOG_COMMITS)


def assert_the_store_oracle_holds(spark, data: Path) -> None:
    landed = [pair for changes in BACKLOG for pair in changes]
    failed = {i for i, (_, outcome) in enumerate(landed) if outcome is O.FAILED}
    oracle = replay.expected_store([c for c, _ in landed], failed)
    assert replay.diff(oracle, fingerprints(spark, data)) == []


def crash_around(spark, data: Path, name: str, *, after: bool) -> None:
    """A real query that raises around `ingest.<name>`: before it runs, or after it returns."""
    real = getattr(ingest, name)

    def crashing(*args):
        if after:
            real(*args)
        raise RuntimeError("the crash")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ingest, name, crashing)
        with pytest.raises(StreamingQueryException, match="the crash"):
            run(spark, data)


def test_a_backlog_lands_as_the_store_oracle_says_with_one_outcome_per_change(spark, tmp_path):
    land_backlog(tmp_path)
    run(spark, tmp_path)
    assert_the_store_oracle_holds(spark, tmp_path)
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


@pytest.fixture(params=["batch 0", "a later batch"])
def reported(request, spark, tmp_path) -> list[tuple]:
    """Lands the backlog for a query to crash on, in batch 0 or after one more run has reported
    commit 0, since only batch 0's replay has no start offset. Returns the Outcomes so far."""
    if request.param == "batch 0":
        land_backlog(tmp_path)
        return []
    land(tmp_path, ["init"], BACKLOG_COMMITS[0])
    run(spark, tmp_path)
    land(tmp_path, *BACKLOG_COMMITS[1:])
    return BACKLOG_OUTCOMES[:5]


def test_a_crash_before_the_merge_replays_as_if_nothing_happened(spark, tmp_path, reported):
    crash_around(spark, tmp_path, "merge", after=False)
    assert outcomes(spark, tmp_path) == reported
    run(spark, tmp_path)
    assert_the_store_oracle_holds(spark, tmp_path)
    assert outcomes(spark, tmp_path) == BACKLOG_OUTCOMES


def test_a_crash_after_the_merge_replays_with_no_written_outcome(spark, tmp_path, reported):
    crash_around(spark, tmp_path, "merge", after=True)
    assert outcomes(spark, tmp_path) == reported
    run(spark, tmp_path)
    assert_the_store_oracle_holds(spark, tmp_path)
    assert outcomes(spark, tmp_path) == reported + BACKLOG_REPLAYED[len(reported) :]


def test_a_crash_after_the_append_adds_no_duplicate_outcome(spark, tmp_path, reported):
    crash_around(spark, tmp_path, "append_outcomes", after=True)
    written = [tmp_path / "outcomes", tmp_path / "listing_store"]
    before = [version(spark, t) for t in written]
    assert outcomes(spark, tmp_path) == BACKLOG_OUTCOMES
    run(spark, tmp_path)
    assert_the_store_oracle_holds(spark, tmp_path)
    assert outcomes(spark, tmp_path) == BACKLOG_OUTCOMES
    assert [version(spark, t) for t in written] == before


def start_through_main(data: Path, monkeypatch) -> None:
    # A start the guard should have refused then ends, instead of waiting on the 60 s trigger.
    monkeypatch.setattr(ingest, "TRIGGER", {"availableNow": True})
    ingest.main(["--data", str(data)])


def test_no_recorded_batch_over_non_empty_outcomes_is_refused(spark, tmp_path, monkeypatch):
    land(tmp_path, ["init"], commit(0, [up("plain", 1)]))
    run(spark, tmp_path)
    before = outcomes(spark, tmp_path)
    checkpoint = tmp_path / "checkpoints/ingest"

    def assert_refused():
        with pytest.raises(RuntimeError, match="clean start wipes") as refused:
            start_through_main(tmp_path, monkeypatch)
        assert str(tmp_path / "outcomes") in str(refused.value)
        assert str(checkpoint) in str(refused.value)

    for folder in filter(Path.is_dir, checkpoint.iterdir()):
        shutil.rmtree(folder)  # metadata alone, as a kill before the first offset leaves it
    assert_refused()
    shutil.rmtree(checkpoint)
    assert_refused()
    assert outcomes(spark, tmp_path) == before


def test_a_checkpoint_with_a_lost_table_is_refused(spark, tmp_path, monkeypatch):
    land(tmp_path, ["init"], commit(0, [up("plain", 1)]))
    run(spark, tmp_path)
    shutil.rmtree(tmp_path / "listing_store")
    with pytest.raises(RuntimeError, match=re.escape(str(tmp_path / "listing_store"))):
        start_through_main(tmp_path, monkeypatch)
    assert not (tmp_path / "listing_store").exists()
    shutil.rmtree(tmp_path / "outcomes")  # an append would create it, without its properties
    with pytest.raises(AnalysisException, match=re.escape(str(tmp_path / "outcomes"))):
        start_through_main(tmp_path, monkeypatch)
    assert not (tmp_path / "outcomes").exists()


def test_a_missing_init_names_the_landing_log_and_creates_nothing(spark, tmp_path, monkeypatch):
    with pytest.raises(AnalysisException, match=re.escape(str(tmp_path / "landing_log"))):
        start_through_main(tmp_path, monkeypatch)
    assert list(tmp_path.iterdir()) == []


def test_no_checkpoint_over_empty_outcomes_starts(spark, tmp_path, monkeypatch):
    land(tmp_path, ["init"], commit(0, [up("plain", 1)]))
    start_through_main(tmp_path, monkeypatch)
    assert outcomes(spark, tmp_path) == [(HOUR, submission(0), 0, "written")]


def test_a_recreated_landing_log_is_refused(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("plain", 1)]))
    run(spark, tmp_path)
    shutil.rmtree(tmp_path / "landing_log")
    land(tmp_path, ["init"], commit(1, [up("other", 1)]))
    refusal = "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE"
    with pytest.raises(StreamingQueryException, match=refusal):
        run(spark, tmp_path)


def seeded_store(spark, data: Path) -> str:
    land(data, ["init"], commit(0, [up("a", 1), up("b", 1), up("c", 1)]))
    run(spark, data)
    return str(data / "listing_store")


def source_versions(spark, store: str) -> dict:
    rows = spark.read.format("delta").load(store).collect()
    return {r.merchant_product_id: r.source_version for r in rows}


def test_a_row_the_merges_condition_refuses_raises(spark, tmp_path):
    store = seeded_store(spark, tmp_path)
    rows = spark.read.format("delta").load(store).drop("updated_at")
    rows = rows.filter("merchant_product_id IN ('a', 'b')")
    writes = rows.withColumn(  # a is behind the store and b ahead of it
        "source_version", F.expr("IF(merchant_product_id = 'a', 0L, 2L)")
    )
    with pytest.raises(RuntimeError, match="applied 1 of 2 rows"):
        ingest.merge(store, writes)
    assert source_versions(spark, store) == {"a": 1, "b": 2, "c": 1}


def count_attempts(monkeypatch, first_raises=None) -> list:
    attempts = []
    execute = DeltaMergeBuilder.execute

    def counted(builder):
        attempts.append(1)
        if first_raises and len(attempts) == 1:
            raise first_raises("raised by hand")
        return execute(builder)

    monkeypatch.setattr(DeltaMergeBuilder, "execute", counted)
    return attempts


def merge_at_a_gate(spark, store: str, gates: Path, during) -> None:
    """`ingest.merge` of the store's rows at their next version, doing `during(n)` while the
    source's nth evaluation waits: once per attempt, then once for the check's count. The source
    has one partition: two gated ones would hold both of local[2]'s slots, and an OPTIMIZE would
    hang."""
    gates.mkdir()

    def wait(batches):
        n = len(list(gates.glob("started-*"))) + 1
        (gates / f"started-{n}").touch()
        deadline = time.monotonic() + 60
        while not (gates / f"go-{n}").exists():
            if time.monotonic() > deadline:
                raise TimeoutError(f"no one opened gate {n}")
            time.sleep(0.01)
        yield from batches

    rows = spark.read.format("delta").load(store).drop("updated_at")
    writes = rows.withColumn("source_version", F.col("source_version") + 1)
    writes = writes.localCheckpoint(eager=True)
    with ThreadPoolExecutor(1) as pool:
        gated = writes.coalesce(1).mapInArrow(wait, writes.schema)
        merging = pool.submit(ingest.merge, store, gated)
        n = 1
        while not merging.done():
            if (gates / f"started-{n}").exists():
                during(n)
                (gates / f"go-{n}").touch()
                n += 1
            time.sleep(0.01)
    merging.result()


def test_an_optimize_during_the_merge_fails_one_attempt_and_the_retry_lands(
    spark, tmp_path, monkeypatch
):
    store = seeded_store(spark, tmp_path)
    attempts = count_attempts(monkeypatch)

    def optimize_once(n):
        if n == 1:
            spark.sql(f"OPTIMIZE delta.`{store}`").collect()

    merge_at_a_gate(spark, store, tmp_path / "gates", optimize_once)
    assert len(attempts) == 2
    history = DeltaTable.forPath(spark, store).history(2).collect()
    assert [h.operation for h in history] == ["MERGE", "OPTIMIZE"]
    assert source_versions(spark, store) == {"a": 2, "b": 2, "c": 2}


def test_a_conflict_on_every_attempt_raises_after_three(spark, tmp_path, monkeypatch):
    store = seeded_store(spark, tmp_path)
    attempts = count_attempts(monkeypatch)

    def new_small_file_and_optimize(n):  # after the first, OPTIMIZE alone finds nothing to do
        rows = spark.read.format("delta").load(store).limit(1)
        rows = rows.withColumn("merchant_product_id", F.lit(f"new-{n}"))
        rows.write.format("delta").mode("append").save(store)
        spark.sql(f"OPTIMIZE delta.`{store}`").collect()

    with pytest.raises(ConcurrentAppendException):
        merge_at_a_gate(spark, store, tmp_path / "gates", new_small_file_and_optimize)
    assert len(attempts) == 3
    seeded = {"a": 1, "b": 1, "c": 1}
    assert source_versions(spark, store) == seeded | {"new-1": 1, "new-2": 1, "new-3": 1}


# A real OPTIMIZE or DELETE raises ConcurrentAppendException on this layout (probed), so by hand.
@pytest.mark.parametrize(
    "conflict", [ConcurrentDeleteReadException, ConcurrentDeleteDeleteException]
)
def test_the_conflicts_nothing_here_provokes_are_retried_as_well(
    spark, tmp_path, monkeypatch, conflict
):
    store = seeded_store(spark, tmp_path)
    attempts = count_attempts(monkeypatch, first_raises=conflict)
    ingest.merge(store, spark.read.format("delta").load(store).drop("updated_at"))
    assert len(attempts) == 2
