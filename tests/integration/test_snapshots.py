"""Catalog Snapshots (docs/specs/step-5c.md): pinned Listing Store copies, pruned at 7 days."""

import os
import threading
from datetime import UTC, datetime, timedelta

import pytest
from deltalake import DeltaTable
from support import classified, listing, product_in
from test_supervisor import wait_for

from catalog import events, snapshots, state, store
from catalog.events import EventLog
from catalog.plan import Write
from catalog.replay import diff

NOW = datetime(2026, 10, 3, 12, tzinfo=UTC)
A, B, C = (("m_1", product_in(p)) for p in (5, 9, 40))


def write(key, sv, content=None, cls=None):
    return Write(key, sv, content, (cls or classified()) if content else None, False)


class Env:
    def __init__(self, tmp):
        self.tmp = tmp
        self.dt = store.ensure(str(tmp / "data" / "listing_store"))
        self.out = tmp / "data" / "snapshots"
        self.events = EventLog(tmp / "data" / "events", "snapshots")

    def merge(self, *writes):
        return store.merge(self.dt, list(writes), NOW).version

    def rows(self, dt):
        return sorted(
            dt.to_pyarrow_dataset().to_table().to_pylist(),
            key=lambda r: (r["merchant_id"], r["merchant_product_id"]),
        )

    def oracle(self, path):
        """The snapshot equals the Listing Store at its pinned version, row for row."""
        version, copy = snapshots.pinned(path), DeltaTable(str(path))
        assert diff(store.fingerprints(self.dt, version), store.fingerprints(copy)) == []
        return self.rows(copy) == self.rows(DeltaTable(self.dt.table_uri, version=version))


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def test_a_snapshot_equals_the_store_at_its_pinned_version(env):
    env.merge(write(A, 1, listing(title="a")), write(B, 1, listing()))
    env.merge(write(A, 2, listing(title="b")), write(C, 1, listing()))
    env.merge(write(B, 2))  # a Tombstone, kept in the snapshot
    env.merge(write(C, 1, listing(), classified("Home > Mugs")))  # a reclassify
    store.compact(env.dt, range(64))
    head = env.dt.version()
    path = snapshots.take(env.dt, env.out, NOW)
    assert path == env.out / "20261003T120000Z"
    assert snapshots.pinned(path) == head
    assert env.oracle(path)
    env.merge(write(A, 3, listing(title="later")))  # the store moving on doesn't change it
    assert snapshots.pinned(path) == head
    assert env.oracle(path)
    assert len(env.rows(DeltaTable(str(path)))) == 3


def test_an_empty_store_snapshots_to_an_empty_table(env):
    path = snapshots.take(env.dt, env.out, NOW)
    assert snapshots.pinned(path) == env.dt.version()
    assert env.oracle(path)
    assert env.rows(DeltaTable(str(path))) == []


def test_a_failed_copy_leaves_no_folder(env, monkeypatch):
    env.merge(write(A, 1, listing()))

    def full_disk(table_uri, *args, **kwargs):  # dies after its first file
        os.makedirs(table_uri)
        open(os.path.join(table_uri, "part-0.parquet"), "wb").close()
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(snapshots, "write_deltalake", full_disk)
    with pytest.raises(OSError):
        snapshots.take(env.dt, env.out, NOW)
    assert os.listdir(env.out) == []


H = timedelta(hours=1)


def tick(env, now):
    return snapshots.tick(env.dt, env.out, env.events, now, every=6 * H, keep=7 * 24 * H)


def names(env):
    return [p.name for p in snapshots.existing(env.out)]


def test_the_next_snapshot_is_due_six_hours_after_the_newest(env):
    assert tick(env, NOW) == 6 * H  # none yet: at once
    assert tick(env, NOW + 2 * H) == 4 * H  # a restart doesn't add one
    snapshots.take(env.dt, env.out, NOW - 8 * 24 * H)
    assert tick(env, NOW + 2 * H) == 4 * H  # an old one is pruned even when none is due
    assert tick(env, NOW + 6 * H) == 6 * H
    assert names(env) == ["20261003T120000Z", "20261003T180000Z"]  # the 8-day one is gone
    (event, _) = [e for e in events.read(env.tmp / "data" / "events") if e["type"] == "snapshot"]
    assert (event["name"], event["version"], event["rows"]) == ("20261003T120000Z", 0, 0)


def test_a_long_outage_takes_one_snapshot_not_a_burst(env):
    tick(env, NOW)
    assert tick(env, NOW + 48 * H) == 6 * H
    assert names(env) == ["20261003T120000Z", "20261005T120000Z"]


def test_snapshots_older_than_seven_days_are_pruned(env):
    day = 24 * H
    for age in (8, 6, 1):
        snapshots.take(env.dt, env.out, NOW - age * day)
    tick(env, NOW)
    assert names(env) == ["20260927T120000Z", "20261002T120000Z", "20261003T120000Z"]
    pruned = [
        e["name"]
        for e in events.read(env.tmp / "data" / "events")
        if e["type"] == "snapshot_pruned"
    ]
    assert pruned == ["20260925T120000Z"]


def test_the_newest_snapshot_is_kept_however_old(env):
    for age in (9, 8):
        snapshots.take(env.dt, env.out, NOW - age * 24 * H)
    snapshots.prune(env.out, NOW, keep=7 * 24 * H, events=env.events)
    assert names(env) == ["20260925T120000Z"]


def test_hidden_leftovers_of_a_crash_are_deleted_and_never_listed(env):
    snapshots.take(env.dt, env.out, NOW)
    for leftover in (".20261003T180000Z.ab.tmp", ".20260920T120000Z.cd.deleting"):
        (env.out / leftover / "_delta_log").mkdir(parents=True)
    assert names(env) == ["20261003T120000Z"]
    snapshots.prune(env.out, NOW, keep=7 * 24 * H, events=env.events)
    assert sorted(os.listdir(env.out)) == ["20261003T120000Z"]


def test_run_snapshots_until_stopped(env):
    stop = threading.Event()
    runner = threading.Thread(
        target=snapshots.run, args=(env.tmp / "data", env.tmp / "state"), kwargs={"stop": stop}
    )
    runner.start()
    wait_for(lambda: len(names(env)) == 1)
    stop.set()
    runner.join(timeout=10)
    assert not runner.is_alive()  # the 6 h wait ends at once


def test_a_second_snapshotter_is_refused_before_touching_anything(env):
    snapshots.take(env.dt, env.out, NOW - 30 * 24 * H)
    (env.out / ".20261003T180000Z.ab.tmp").mkdir()  # another snapshotter's copy in progress
    before = sorted(os.listdir(env.out))
    with (
        state.claim_snapshots(env.tmp / "state"),
        pytest.raises(state.PartitionTaken, match="Catalog Snapshots"),
    ):
        snapshots.run(env.tmp / "data", env.tmp / "state", stop=threading.Event())
    assert sorted(os.listdir(env.out)) == before
