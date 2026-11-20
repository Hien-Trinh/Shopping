"""Change Export on real Spark: the export query over a Listing Store that the ingest query
writes (docs/specs/step-a7.md)."""

import re
import shutil
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.errors import StreamingQueryException
from support import delete, up
from test_ingest import commit, fingerprints, land, run, version

from ingestion import change_export, classify_udf, replay
from ingestion.collapse import collapse


def export(spark, data: Path) -> None:
    query = change_export.start(spark, str(data), trigger={"availableNow": True})
    query.awaitTermination()


def names(data: Path) -> list[str]:
    return sorted(p.name for p in (data / "export").glob("*.parquet"))


def exported(spark, data: Path) -> list[list[dict]]:
    """Each file's rows, in name order, without the time a MERGE wrote them at."""
    files = change_export.files(spark, str(data))
    return [[{k: v for k, v in row.items() if k != "updated_at"} for row in rows] for rows in files]


def assert_the_export_oracle_holds(spark, data: Path) -> None:
    live = replay.live(fingerprints(spark, data))
    assert replay.diff(live, replay.replay_exports(change_export.files(spark, str(data)))) == []


def crash_after_the_write(spark, data: Path) -> None:
    real = change_export.apply_batch

    def crashing(*args):
        real(*args)
        raise RuntimeError("the crash")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(change_export, "apply_batch", crashing)
        with pytest.raises(StreamingQueryException, match="the crash"):
            export(spark, data)


def test_the_files_replay_to_the_live_store_and_each_is_its_ranges_collapse(spark, tmp_path):
    store = str(tmp_path / "listing_store")
    land(tmp_path, ["init"])
    run(spark, tmp_path)  # creates the store, empty
    export(spark, tmp_path)
    assert names(tmp_path) == []  # an empty store: no file
    land(tmp_path, commit(0, [up("a", 1), up("b", 1), up("c", 1), up("d", 1)]))
    run(spark, tmp_path, classify_udf.DOWN)  # every Listing flagged
    export(spark, tmp_path)
    land(tmp_path, commit(1, [up("a", 2), delete("b", 2)]))
    run(spark, tmp_path)  # and a Backfill round: "c" and "d" are reclassified
    DeltaTable.forPath(spark, store).optimize().executeCompaction()
    DeltaTable.forPath(spark, store).delete("merchant_product_id = 'd'")  # a physical delete
    land(tmp_path, commit(2, [up("e", 1)]))
    run(spark, tmp_path)
    export(spark, tmp_path)

    assert_the_export_oracle_holds(spark, tmp_path)
    files = change_export.files(spark, str(tmp_path))
    assert len(files) == 2
    by_key = {row["merchant_product_id"]: row for row in files[1]}
    assert {key: row["op"] for key, row in by_key.items()} == {
        "a": "upsert", "b": "delete", "c": "upsert", "d": "delete", "e": "upsert"
    }  # fmt: skip
    assert by_key["c"]["needs_reclassify"] is False  # the round's row, not the flagged one
    for name, rows in zip(names(tmp_path), files, strict=True):
        # Twelve digits each, so name order is version order past version 9.
        first, last = map(int, re.fullmatch(r"(\d{12})-(\d{12})\.parquet", name).groups())
        feed = spark.read.format("delta").option("readChangeFeed", "true")
        feed = feed.option("startingVersion", first).option("endingVersion", last).load(store)
        assert rows == collapse(row.asDict(recursive=True) for row in feed.collect())
    assert last == version(spark, tmp_path / "listing_store")


def test_an_idle_store_and_a_compaction_add_no_file(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    run(spark, tmp_path)
    land(tmp_path, commit(1, [up("b", 1)]))
    run(spark, tmp_path)
    export(spark, tmp_path)
    before = names(tmp_path)
    assert len(before) == 1
    export(spark, tmp_path)  # idle
    compacted = DeltaTable.forPath(spark, str(tmp_path / "listing_store")).optimize()
    assert compacted.executeCompaction().first().metrics.numFilesRemoved > 0
    export(spark, tmp_path)
    assert names(tmp_path) == before


def test_a_crash_after_the_write_replays_to_the_files_of_an_uninterrupted_run(spark, tmp_path):
    crashed, plain = tmp_path / "crashed", tmp_path / "plain"

    def both(n: int, changes: list) -> None:
        for data in (crashed, plain):
            land(data, *([["init"]] if n == 0 else []), commit(n, changes))
            run(spark, data)

    both(0, [up("a", 1), up("b", 1)])
    crash_after_the_write(spark, crashed)  # the first batch: the store's state, not its feed
    export(spark, plain)
    assert names(crashed) == names(plain) != []  # the crash left its file
    both(1, [up("a", 2), delete("b", 2)])
    export(spark, crashed)  # the replay, then the new commit
    export(spark, plain)
    both(2, [up("c", 1)])
    crash_after_the_write(spark, crashed)  # a later batch
    export(spark, plain)
    both(3, [up("c", 2)])
    for data in (crashed, plain):
        export(spark, data)
    assert len(names(plain)) == 4
    assert names(crashed) == names(plain)
    assert exported(spark, crashed) == exported(spark, plain)
    assert_the_export_oracle_holds(spark, crashed)


def test_a_replay_replaces_a_partial_directory(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1)]))
    run(spark, tmp_path)
    crash_after_the_write(spark, tmp_path)
    (directory,) = (tmp_path / "export").iterdir()
    (directory / "_SUCCESS").unlink()
    # What a crash during the write can leave on Databricks (fact 7): a second data file.
    (part,) = directory.glob("part-*.parquet")
    shutil.copy(part, directory / "part-00001-partial.snappy.parquet")
    export(spark, tmp_path)
    assert len(list(directory.glob("part-*.parquet"))) == 1
    assert (directory / "_SUCCESS").exists()
    assert_the_export_oracle_holds(spark, tmp_path)


def test_a_lost_checkpoint_exports_the_stores_state_as_one_file(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1)]))
    run(spark, tmp_path)
    export(spark, tmp_path)
    land(tmp_path, commit(1, [up("a", 2), delete("b", 2), up("c", 1)]))
    run(spark, tmp_path)
    export(spark, tmp_path)
    shutil.rmtree(tmp_path / "checkpoints/export")  # the runbook's recovery
    export(spark, tmp_path)
    files = change_export.files(spark, str(tmp_path))
    head = version(spark, tmp_path / "listing_store")
    # The state at the head takes the name of the head's own file, and holds all its rows.
    assert names(tmp_path)[-1] == f"{head:012}-{head:012}.parquet"
    ops = {row["merchant_product_id"]: row["op"] for row in files[-1]}
    assert ops == {"a": "upsert", "b": "delete", "c": "upsert"}
    live = replay.live(fingerprints(spark, tmp_path))
    assert replay.diff(live, replay.replay_exports(files[-1:])) == []  # from this file alone
    assert_the_export_oracle_holds(spark, tmp_path)
