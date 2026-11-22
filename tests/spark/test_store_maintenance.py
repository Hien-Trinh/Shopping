"""The Listing Store's maintenance on real Spark: what is due, and each action on a store that the
ingest query writes (docs/specs/step-a8.md)."""

import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from delta.exceptions import ConcurrentAppendException
from delta.tables import DeltaTable
from support import delete, up
from test_change_export import assert_the_export_oracle_holds, export
from test_ingest import commit, fingerprints, land, run, store_rows, version
from test_liveness import wait_until

from ingestion import replay
from ingestion import store_maintenance as sm

T = datetime(2026, 1, 1, tzinfo=UTC)
H, S = timedelta(hours=1), timedelta(seconds=1)
EVERY = 6 * H


def due(now, last, snapshots, head=9):
    return sm.due(now, last, snapshots, head, every=EVERY)


def tick(spark, data: Path, schema: str, now: datetime, **last) -> None:
    """One tick with nothing due by the clock, but for the actions whose last run `last` moves."""
    ran = {"optimize": now, "vacuum": now} | last
    sm.tick(spark, str(data), schema, ran, now, every=EVERY, conflicts=(ConcurrentAppendException,))


def snapshots(spark, schema: str) -> list[str]:
    rows = spark.sql(f"SHOW TABLES IN {schema} LIKE 'snapshot_v*'").collect()
    return sorted(row.tableName for row in rows)


def lines(capsys) -> list[dict]:
    printed = map(json.loads, capsys.readouterr().out.splitlines())
    return [line for line in printed if line["type"] == "maintenance"]


def files(spark, data: Path) -> int:
    return DeltaTable.forPath(spark, f"{data}/listing_store").detail().first().numFiles


def test_due_runs_each_action_at_its_cadence_in_one_order():
    assert due(T, {"optimize": T}, []) == [("snapshot", 9), ("vacuum", 9)]  # the thread's start
    ran, taken = {"optimize": T, "vacuum": T}, [(9, T)]
    assert due(T + H - S, ran, taken) == []
    assert due(T + H, ran, taken) == [("optimize", 9)]
    assert due(T + 24 * H - S, {"optimize": T + 24 * H, "vacuum": T}, taken) == []
    assert due(T + 24 * H, ran, taken) == [("optimize", 9), ("vacuum", 9)]
    late = {"optimize": T + 9 * 24 * H, "vacuum": T + 9 * 24 * H}
    old = [(3, T), (5, T + H), (8, T + 9 * 24 * H - EVERY)]
    assert due(T + 9 * 24 * H, {"optimize": T}, old) == [
        ("snapshot", 9), ("drop", 3), ("drop", 5), ("optimize", 9), ("vacuum", 9)
    ]  # fmt: skip
    assert due(T + 9 * 24 * H, late, old) == [("snapshot", 9), ("drop", 3), ("drop", 5)]


def test_due_takes_a_snapshot_when_the_newest_is_older_than_the_cadence_and_the_head_moved():
    ran = {"optimize": T + EVERY, "vacuum": T + EVERY}
    assert due(T + EVERY - S, ran, [(8, T)]) == []
    assert due(T + EVERY, ran, [(8, T)]) == [("snapshot", 9)]
    assert due(T + EVERY, ran, [(8, T), (7, T - EVERY)]) == [("snapshot", 9)]
    assert due(T + EVERY, ran, [(7, T - EVERY), (8, T + S)]) == []  # the newest by time, not last
    assert due(T + EVERY, ran, [(9, T)]) == []  # an idle store: one snapshot per version


def test_due_drops_what_is_older_than_keep_but_never_the_newest_snapshot():
    now = T + sm.KEEP
    ran = {"optimize": now, "vacuum": now}
    assert due(now, ran, [(3, T), (9, now)]) == []  # at KEEP exactly: kept
    assert due(now + S, ran, [(3, T), (9, now)]) == [("drop", 3)]
    assert due(now + S, ran, [(3, T), (9, T)]) == [("drop", 3)]  # an idle store keeps its head's
    assert due(now + S, ran, [(8, T), (3, T)]) == [("snapshot", 9), ("drop", 3), ("drop", 8)]
    assert due(now + S, ran, [(8, T)], head=8) == []


def test_keep_is_at_most_deltas_default_retention():
    # VACUUM runs on the default, 7 days, and a snapshot must be dropped before its files are.
    assert sm.KEEP <= timedelta(days=7)
    assert sm.OPTIMIZE_EVERY == H and sm.VACUUM_EVERY == 24 * H


def test_a_snapshot_reads_as_the_store_at_its_pin_after_the_store_moves_on(
    spark, tmp_path, schema, capsys
):
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1), up("c", 1)]))
    run(spark, tmp_path)
    pin, pinned = version(spark, tmp_path / "listing_store"), store_rows(spark, tmp_path)
    now = datetime.now(UTC)
    tick(spark, tmp_path, schema, now)
    assert lines(capsys) == [{"type": "maintenance", "op": "snapshot", "version": pin}]
    land(tmp_path, commit(1, [up("a", 2), delete("b", 2)]))
    run(spark, tmp_path)
    land(tmp_path, commit(2, [up("d", 1)]))
    run(spark, tmp_path)
    before, head = files(spark, tmp_path), version(spark, tmp_path / "listing_store")
    tick(spark, tmp_path, schema, now, optimize=now - H, vacuum=now - 24 * H)
    assert lines(capsys) == [  # the head when the tick began
        {"type": "maintenance", "op": "optimize", "version": head},
        {"type": "maintenance", "op": "vacuum", "version": head},
    ]
    assert files(spark, tmp_path) < before
    name = f"snapshot_v{pin}"
    assert snapshots(spark, schema) == [name]
    snapshot = spark.table(f"{schema}.{name}")
    assert sorted(tuple(r) for r in snapshot.drop("updated_at").collect()) == pinned
    assert store_rows(spark, tmp_path) != pinned
    location = spark.sql(f"DESCRIBE DETAIL {schema}.{name}").first().location
    assert location.endswith(f"{tmp_path}/snapshots/{name}")


def test_a_second_tick_at_the_same_head_adds_no_snapshot(spark, tmp_path, schema, capsys):
    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    run(spark, tmp_path)
    now = datetime.now(UTC)
    tick(spark, tmp_path, schema, now)
    taken = snapshots(spark, schema)
    assert len(lines(capsys)) == 1
    tick(spark, tmp_path, schema, now + EVERY + H)
    assert snapshots(spark, schema) == taken and lines(capsys) == []


def test_eight_days_on_the_old_snapshots_are_dropped_and_the_newest_stays(
    spark, tmp_path, schema, capsys
):
    now = datetime.now(UTC)
    pins = []
    for n in range(3):
        land(tmp_path, *([["init"]] if n == 0 else []), commit(n, [up("a", n + 1)]))
        run(spark, tmp_path)
        pins.append(version(spark, tmp_path / "listing_store"))
        tick(spark, tmp_path, schema, now + n * (EVERY + H))
    names = [f"snapshot_v{pin}" for pin in pins]
    assert snapshots(spark, schema) == names
    lines(capsys)
    tick(spark, tmp_path, schema, now + timedelta(days=8))
    assert snapshots(spark, schema) == names[-1:]
    assert lines(capsys) == [{"type": "maintenance", "op": "drop", "version": p} for p in pins[:2]]
    # The job deletes no clone directory (decision 9).
    assert sorted(p.name for p in (tmp_path / "snapshots").iterdir()) == names


def test_both_oracles_hold_when_every_action_runs_between_batches(spark, tmp_path, schema):
    changes = [up("a", 1), up("b", 1), up("c", 1)], [up("a", 2), delete("b", 2)], [up("d", 1)]
    now = datetime.now(UTC)
    for n, batch in enumerate(changes):
        land(tmp_path, *([["init"]] if n == 0 else []), commit(n, batch))
        run(spark, tmp_path)
        export(spark, tmp_path)
        before = store_rows(spark, tmp_path)
        late = now + n * (EVERY + H)  # a snapshot's age is its real CLONE commit's
        tick(spark, tmp_path, schema, late, optimize=now - 24 * H, vacuum=now - 24 * H)
        assert store_rows(spark, tmp_path) == before
    export(spark, tmp_path)  # the OPTIMIZE and VACUUM commits: no new file, and no failure
    assert len(snapshots(spark, schema)) == 3
    landed = [change for batch in changes for change in batch]
    assert replay.diff(replay.expected_store(landed), fingerprints(spark, tmp_path)) == []
    assert_the_export_oracle_holds(spark, tmp_path)


def test_an_optimize_that_loses_a_conflict_is_skipped_until_the_next_hour(
    spark, tmp_path, schema, capsys, monkeypatch
):
    def losing(*args):
        raise ConcurrentAppendException("a MERGE won")

    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    run(spark, tmp_path)
    head = version(spark, tmp_path / "listing_store")
    now = datetime.now(UTC)
    last = {"optimize": now - H, "vacuum": now}
    conflicts = (ConcurrentAppendException,)
    monkeypatch.setattr(sm, "optimize", losing)
    sm.tick(spark, str(tmp_path), schema, last, now, every=EVERY, conflicts=conflicts)
    assert lines(capsys)[-1] == {
        "type": "maintenance", "op": "optimize", "version": head, "skipped": "conflict"
    }  # fmt: skip
    assert last["optimize"] == now
    monkeypatch.setattr(sm, "vacuum", losing)  # any other action's conflict is a failure
    with pytest.raises(ConcurrentAppendException):
        sm.tick(
            spark, str(tmp_path), schema, {"optimize": now}, now, every=EVERY, conflicts=conflicts
        )


def test_the_thread_ticks_in_its_own_pool_and_stops(spark, tmp_path, monkeypatch):
    pools, failures = [], []

    def recording(session, data, schema, last, now, *, every, conflicts):
        pools.append(session.sparkContext.getLocalProperty("spark.scheduler.pool"))
        assert (data, schema, every, conflicts) == (str(tmp_path), "default", EVERY, (KeyError,))
        assert list(last) == ["optimize"] and now.tzinfo is UTC  # a VACUUM at the start

    monkeypatch.setattr(sm, "tick", recording)
    monkeypatch.setattr(sm, "TICK", 0.05)
    stop = sm.start(
        spark, str(tmp_path), schema="default", every=EVERY, conflicts=(KeyError,),
        failed=failures.append,
    )  # fmt: skip
    wait_until(lambda: len(pools) >= 2)
    stop.set()
    (thread,) = (t for t in threading.enumerate() if t.name == "maintenance")
    thread.join(10)
    assert not thread.is_alive() and thread.daemon
    assert set(pools) == {"maintenance"} and failures == []
    assert spark.sparkContext.getLocalProperty("spark.scheduler.pool") is None  # the main thread's


def test_a_tick_that_raises_ends_the_thread_and_reports_the_error(spark, tmp_path, monkeypatch):
    failures, error = [], RuntimeError("the vacuum failed")

    def failing(*args, **kwargs):
        raise error

    monkeypatch.setattr(sm, "tick", failing)
    sm.start(
        spark, str(tmp_path), schema="default", every=EVERY, conflicts=(), failed=failures.append
    )
    wait_until(lambda: failures)
    assert failures == [error]
    wait_until(lambda: not [t for t in threading.enumerate() if t.name == "maintenance"])
