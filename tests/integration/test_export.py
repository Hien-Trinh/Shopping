"""Change Export (docs/specs/step-5a.md): change feed since the watermark → export files."""

import shutil
import threading
from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest
from support import classified, listing, product_in
from test_supervisor import wait_for

from catalog import delta, events, export, state, store
from catalog.events import EventLog
from catalog.plan import Write
from catalog.replay import diff, live, replay_exports

NOW = datetime(2026, 10, 3, 12, tzinfo=UTC)
A, B, C = (("m_1", product_in(p)) for p in (5, 9, 40))


def write(key, sv, content=None, cls=None):
    return Write(key, sv, content, (cls or classified()) if content else None, False)


class Env:
    def __init__(self, tmp):
        self.tmp = tmp
        self.dt = store.ensure(str(tmp / "data" / "listing_store"))
        self.out, self.state = tmp / "data" / "export", tmp / "state"
        self.events = EventLog(tmp / "data" / "events", "export")

    def merge(self, *writes):
        return store.merge(self.dt, list(writes), NOW).version

    def tick(self, max_versions=1000):
        return export.tick(self.dt, self.out, self.state, self.events, max_versions=max_versions)

    def watermark(self):
        return state.load_watermark(self.state, self.dt.metadata().id)

    def names(self):
        return [p.name for p in export.files(self.out)]

    def oracle(self):
        """Replaying the files equals the live Listing Store at the watermark."""
        version = self.watermark()
        files = [pq.read_table(p).to_pylist() for p in export.files(self.out)]
        return diff(live(store.fingerprints(self.dt, version)), replay_exports(files))


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def name(v1, v2):
    return f"{v1:012}-{v2:012}.parquet"


def test_upserts_deletes_and_reclassifies_replay_to_the_live_store(env):
    env.merge(write(A, 1, listing(title="a")), write(B, 1, listing()))
    env.merge(write(A, 2, listing(title="b")), write(C, 1, listing()))
    env.merge(write(B, 2))  # a Tombstone
    env.merge(write(C, 1, listing(), classified("Home > Mugs")))  # a reclassify
    head = env.dt.version()
    assert env.tick() is False
    assert env.names() == [name(0, head)]
    assert env.watermark() == head
    assert env.oracle() == []
    rows = pq.read_table(export.files(env.out)[0]).to_pylist()
    got = {r["merchant_product_id"]: (r["source_version"], r["op"]) for r in rows}
    assert got == {A[1]: (2, "upsert"), B[1]: (2, "delete"), C[1]: (1, "upsert")}
    assert next(r for r in rows if r["merchant_product_id"] == C[1])["primary_category"] == (
        "Home > Mugs"
    )
    (event,) = [e for e in events.read(env.tmp / "data" / "events") if e["type"] == "export"]
    assert (event["from"], event["to"], event["rows"], event["deletes"]) == (0, head, 3, 1)


def test_an_idle_store_writes_nothing(env):
    env.tick()
    before = (env.names(), env.watermark())
    assert env.tick() is False
    assert (env.names(), env.watermark()) == before


def test_compaction_alone_writes_no_file_but_moves_the_watermark(env):
    env.merge(write(A, 1, listing()))
    env.merge(write(("m_1", product_in(5, prefix="alt")), 1, listing()))  # a 2nd file in A's
    env.tick()
    before, v = env.names(), env.dt.version()
    store.compact(env.dt, [5])
    assert env.dt.version() == v + 1
    assert env.tick() is False
    assert env.names() == before
    assert env.watermark() == env.dt.version()
    assert [e["to"] for e in events.read(env.tmp / "data" / "events") if e["type"] == "export"] == [
        v
    ]  # no event for the compaction
    assert env.oracle() == []


def test_a_far_behind_export_splits_into_capped_ranges_in_version_order(env):
    for sv in range(1, 13):  # versions 1-12, all one Listing: replaying out of order shows
        env.merge(write(A, sv, listing(title=f"t{sv}")))
    assert [env.tick(max_versions=4) for _ in range(4)] == [True, True, True, False]
    assert env.names() == [name(0, 3), name(4, 7), name(8, 11), name(12, 12)]
    assert env.oracle() == []


def test_a_crash_before_the_watermark_adopts_the_file_rather_than_overlap_it(env):
    env.merge(write(A, 1, listing()))
    env.tick()
    first = env.dt.version()
    state.save_watermark(env.state, -1, env.dt.metadata().id)  # as if the crash came first
    env.merge(write(A, 2, listing(title="b")), write(B, 1, listing()))
    env.tick()
    assert env.names() == [name(0, first), name(first + 1, env.dt.version())]
    assert env.oracle() == []


def crash(*_, **__):
    raise OSError("disk full")


def test_a_failed_write_leaves_no_file_and_no_temp_file(env, monkeypatch):
    env.merge(write(A, 1, listing()))
    monkeypatch.setattr(export.os, "replace", crash)
    with pytest.raises(OSError, match="disk full"):
        env.tick()
    monkeypatch.undo()
    assert (list(env.out.iterdir()), env.watermark()) == ([], -1)
    env.tick()
    assert env.names() == [name(0, 1)]


def test_an_adopted_file_saves_its_watermark_and_logs_its_export(env, monkeypatch):
    env.merge(write(A, 1, listing()), write(B, 2))  # one live Listing, one Tombstone
    monkeypatch.setattr(env.events, "emit", crash)  # the crash: after the file, before the rest
    with pytest.raises(OSError):
        env.tick()
    monkeypatch.undo()
    assert (env.names(), env.watermark()) == ([name(0, 1)], -1)
    assert env.tick() is False  # nothing new to export: only the adoption moves the watermark
    assert env.watermark() == 1
    found = [e for e in events.read(env.tmp / "data" / "events") if e["type"] == "export"]
    assert [(e["from"], e["to"], e["rows"], e["deletes"]) for e in found] == [(0, 1, 2, 1)]


def test_a_watermark_from_a_recreated_store_is_refused(env):
    env.merge(write(A, 1, listing()))
    env.tick()
    shutil.rmtree(env.tmp / "data" / "listing_store")
    env.dt = store.ensure(str(env.tmp / "data" / "listing_store"))
    with pytest.raises(state.OffsetsMismatch, match="export watermark"):
        env.tick()


def test_a_torn_watermark_is_corrupt_state(env):
    env.state.mkdir()
    (env.state / "export_watermark.json").write_text('{"table": ')
    with pytest.raises(state.CorruptState):
        env.tick()


def test_run_exports_until_stopped(env):
    env.merge(write(A, 1, listing()))
    stop = threading.Event()
    runner = threading.Thread(
        target=export.run, args=(env.tmp / "data", env.state), kwargs={"stop": stop}
    )
    runner.start()
    wait_for(lambda: env.names() == [name(0, 1)])
    stop.set()
    runner.join(timeout=10)
    assert not runner.is_alive()  # the minute's wait ends at once


def test_a_second_exporter_is_refused_before_reading_anything(env):
    env.merge(write(A, 1, listing()))
    with state.claim_export(env.state), pytest.raises(state.PartitionTaken, match="Change Export"):
        export.run(env.tmp / "data", env.state, stop=threading.Event())
    assert env.names() == []


# Gap recovery (docs/specs/step-5b.md): cleanup removed history the change feed still needs.


def drop_change_data(dt):
    """What 5d's retention will do: delta-rs VACUUM leaves the change feed's files in place."""
    dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    for f in delta.local(dt.table_uri).glob("_change_data/**/*.parquet"):
        f.unlink()


def clean_log(dt):
    dt.alter.set_table_properties({"delta.logRetentionDuration": "interval 0 seconds"})
    dt.create_checkpoint()
    dt.cleanup_metadata()


def behind_a_gap(env):
    """Export once, then change every Listing so the files the change feed needs get replaced."""
    env.merge(write(A, 1, listing()), write(B, 1, listing()))
    env.tick()
    env.merge(write(C, 1, listing()))  # an insert: the change feed reads it from its data file
    env.merge(write(C, 2, listing(title="c2")), write(B, 2))  # replaces that file; B's Tombstone
    env.merge(write(A, 2, listing(title="a2")))
    return env.watermark() + 1


def gap_events(env):
    return [e for e in events.read(env.tmp / "data" / "events") if e["type"] == "gap_recovered"]


@pytest.mark.parametrize("cleanup", [drop_change_data, clean_log])
def test_a_gap_exports_the_whole_store_once(env, cleanup):
    first = behind_a_gap(env)
    cleanup(env.dt)
    head = env.dt.version()
    assert env.tick() is False
    assert env.names() == [name(0, first - 1), name(first, head)]
    assert env.watermark() == head
    assert env.oracle() == []
    rows = pq.read_table(export.files(env.out)[1]).to_pylist()
    got = {r["merchant_product_id"]: (r["source_version"], r["op"]) for r in rows}
    assert got == {A[1]: (2, "upsert"), B[1]: (2, "delete"), C[1]: (2, "upsert")}
    (event,) = gap_events(env)
    assert (event["from"], event["to"], event["rows"], event["deletes"]) == (first, head, 3, 1)
    assert event["head"] == head


def test_a_gap_longer_than_max_versions_is_still_one_file(env):
    first = behind_a_gap(env)
    drop_change_data(env.dt)
    assert env.tick(max_versions=1) is False
    assert env.names()[1:] == [name(first, env.dt.version())]
    assert env.oracle() == []


def test_a_crash_after_the_gap_file_adopts_it(env, monkeypatch):
    first = behind_a_gap(env)
    drop_change_data(env.dt)
    monkeypatch.setattr(env.events, "emit", crash)
    with pytest.raises(OSError):
        env.tick()
    monkeypatch.undo()
    assert env.watermark() == first - 1
    assert env.tick() is False
    assert env.names()[1:] == [name(first, env.dt.version())]
    assert env.watermark() == env.dt.version()
    assert env.oracle() == []


def test_a_read_error_that_is_not_a_gap_propagates(env):
    env.merge(write(A, 1, listing()))
    (feed,) = delta.local(env.dt.table_uri).glob("_change_data/**/*.parquet")
    feed.chmod(0)  # a real object-store error on a file that exists, not a gap
    try:
        with pytest.raises(Exception, match="Permission denied"):
            env.tick()
    finally:
        feed.chmod(0o644)
    assert (env.names(), env.watermark()) == ([], -1)


def test_an_error_that_ends_the_run_is_in_the_events_too(env, monkeypatch):  # 5c review
    def broken(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(export, "tick", broken)
    with pytest.raises(OSError):
        export.run(env.tmp / "data", env.state, stop=threading.Event())
    [stop] = [e for e in events.read(env.tmp / "data" / "events") if e["type"] == "export_stop"]
    assert stop["error"] == "OSError(28, 'No space left on device')"
