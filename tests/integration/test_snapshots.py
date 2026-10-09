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
        self.state = tmp / "state"
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
    path = snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
    assert path == env.out / "20261003T120000Z"
    assert snapshots.pinned(path) == head
    assert env.oracle(path)
    copy = DeltaTable(str(path))
    assert copy.metadata().partition_columns == ["partition"]
    assert copy.history(1)[0]["table"] == env.dt.metadata().id
    env.merge(write(A, 3, listing(title="later")))  # the store moving on doesn't change it
    assert snapshots.pinned(path) == head
    assert env.oracle(path)
    assert len(env.rows(DeltaTable(str(path)))) == 3


def test_an_empty_store_snapshots_to_an_empty_table(env):
    path = snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
    assert snapshots.pinned(path) == env.dt.version()
    assert env.oracle(path)
    assert env.rows(DeltaTable(str(path))) == []


def test_a_failed_copy_leaves_no_folder(env):
    env.merge(write(A, 1, listing()), write(B, 1, listing()))
    lost = next(p for p in (env.tmp / "data" / "listing_store").rglob("*.parquet"))
    lost.unlink()  # the copy's read fails inside delta-rs, after it has started writing
    with pytest.raises(Exception, match="not found|No such file"):
        snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
    assert os.listdir(env.out) == []


def test_a_second_snapshot_in_the_same_second_is_refused_and_leaves_nothing(env):
    snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
    with pytest.raises(OSError):
        snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
    assert os.listdir(env.out) == ["20261003T120000Z"]


# The pin (step 7f): the store's vacuum keeps the version a pass is copying while it exists.


def test_a_pass_pins_the_version_it_copies_and_unpins_after(env, monkeypatch):
    env.merge(write(A, 1, listing()))
    seen = []
    real = snapshots.write_deltalake
    monkeypatch.setattr(
        snapshots,
        "write_deltalake",
        lambda *a, **k: (seen.append(state.load_pin(env.state)), real(*a, **k))[1],
    )
    snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
    (pin,) = seen  # written before the copy, naming the version being copied
    assert pin[0] == env.dt.version() and pin[1] == NOW
    assert state.load_pin(env.state) is None  # removed after it


def test_a_pass_through_tick_pins_as_well(env, monkeypatch):
    env.merge(write(A, 1, listing()))
    seen = []
    real = snapshots.write_deltalake
    monkeypatch.setattr(
        snapshots,
        "write_deltalake",
        lambda *a, **k: (seen.append(state.load_pin(env.state)), real(*a, **k))[1],
    )
    tick(env, NOW)
    assert seen == [(env.dt.version(), NOW)]


def test_a_failure_before_the_copy_unpins_too(env, monkeypatch):
    env.merge(write(A, 1, listing()))

    def broken(*a, **k):
        raise RuntimeError("no table today")

    monkeypatch.setattr(snapshots, "DeltaTable", broken)
    with pytest.raises(RuntimeError):
        snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
    assert state.load_pin(env.state) is None


def test_a_failed_copy_unpins_too(env):
    env.merge(write(A, 1, listing()), write(B, 1, listing()))
    next(p for p in (env.tmp / "data" / "listing_store").rglob("*.parquet")).unlink()
    with pytest.raises(Exception, match="not found|No such file"):
        snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
    assert state.load_pin(env.state) is None


def test_a_pin_left_by_a_killed_pass_is_removed_at_start(env):
    snapshots.take(
        env.dt, env.out, datetime.now(UTC), state_dir=env.state
    )  # fresh: the run takes none
    state.save_pin(env.state, 0, NOW)
    stop = threading.Event()
    runner = threading.Thread(
        target=snapshots.run, args=(env.tmp / "data", env.state), kwargs={"stop": stop}
    )
    runner.start()
    wait_for(lambda: state.load_pin(env.state) is None)
    stop.set()
    runner.join(timeout=10)
    assert names(env) == [snapshots.existing(env.out)[0].name]  # still the one snapshot


H = timedelta(hours=1)


def tick(env, now):
    return snapshots.tick(
        env.dt, env.out, env.events, now, every=6 * H, keep=7 * 24 * H, state_dir=env.state
    )


def names(env):
    return [p.name for p in snapshots.existing(env.out)]


def test_the_next_snapshot_is_due_six_hours_after_the_newest(env):
    assert tick(env, NOW) == 6 * H  # none yet: at once
    assert tick(env, NOW + 2 * H) == 4 * H  # a restart doesn't add one
    snapshots.take(env.dt, env.out, NOW - 8 * 24 * H, state_dir=env.state)
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
        snapshots.take(env.dt, env.out, NOW - age * day, state_dir=env.state)
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
        snapshots.take(env.dt, env.out, NOW - age * 24 * H, state_dir=env.state)
    snapshots.prune(env.out, NOW, keep=7 * 24 * H, events=env.events)
    assert names(env) == ["20260925T120000Z"]


def test_hidden_leftovers_of_a_crash_are_deleted_and_never_listed(env):
    snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
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
    snapshots.take(env.dt, env.out, NOW - 30 * 24 * H, state_dir=env.state)
    (env.out / ".20261003T180000Z.ab.tmp").mkdir()  # another snapshotter's copy in progress
    before = sorted(os.listdir(env.out))
    with (
        state.claim_snapshots(env.tmp / "state"),
        pytest.raises(state.PartitionTaken, match="Catalog Snapshots"),
    ):
        snapshots.run(env.tmp / "data", env.tmp / "state", stop=threading.Event())
    assert sorted(os.listdir(env.out)) == before


def test_a_snapshot_exactly_seven_days_old_is_kept(env):
    snapshots.take(env.dt, env.out, NOW - 7 * 24 * H - timedelta(seconds=1), state_dir=env.state)
    snapshots.take(env.dt, env.out, NOW - 7 * 24 * H, state_dir=env.state)
    snapshots.take(env.dt, env.out, NOW, state_dir=env.state)
    snapshots.prune(env.out, NOW, keep=7 * 24 * H, events=env.events)
    assert names(env) == ["20260926T120000Z", "20261003T120000Z"]


def test_stray_entries_in_the_snapshot_folder_are_ignored(env):
    env.out.mkdir(parents=True)
    (env.out / ".DS_Store").write_bytes(b"x")
    (env.out / "README").write_text("not a snapshot")
    (env.out / "backup").mkdir()
    assert tick(env, NOW) == 6 * H
    assert tick(env, NOW + 8 * 24 * H) == 6 * H
    assert names(env) == ["20261011T120000Z"]
    assert sorted(os.listdir(env.out)) == [".DS_Store", "20261011T120000Z", "README", "backup"]


def test_an_interval_under_a_second_is_refused():
    with pytest.raises(SystemExit):
        snapshots.main(["--every", "0.5"])


def test_an_error_that_ends_the_run_is_in_the_events_too(env, monkeypatch):  # 5c review
    def broken(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(snapshots, "tick", broken)
    with pytest.raises(OSError):
        snapshots.run(env.tmp / "data", env.tmp / "state", stop=threading.Event())
    [stop] = [e for e in events.read(env.tmp / "data" / "events") if e["type"] == "snapshots_stop"]
    assert stop["error"] == "OSError(28, 'No space left on device')"
