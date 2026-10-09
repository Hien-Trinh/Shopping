"""Maintenance (docs/specs/step-5d.md): retention and cleanup, never past the slowest reader."""

import os
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from support import classified, listing, product_in, up
from test_supervisor import wait_for

from catalog import events, export, landing, maintenance, state, store
from catalog.events import EventLog
from catalog.keys import PARTITIONS
from catalog.plan import Write

A, B = product_in(3), product_in(40)  # two partitions
LATER = timedelta(days=8)  # past the 7-day Landing log retention
LANDING_RETENTION = timedelta(days=7)  # plan-v1 A13, independent of the module's constant
EVENTS_RETENTION = timedelta(days=3)  # plan-v1 A13, for events and export files
HALF_HOUR = timedelta(minutes=30)


class Env:
    def __init__(self, tmp):
        self.tmp = tmp
        self.log = landing.ensure(str(tmp / "data" / "landing_log"))
        self.store = store.ensure(str(tmp / "data" / "listing_store"))
        self.state = tmp / "state"
        self.events = EventLog(tmp / "data" / "events", "maintenance")

    def add(self, *changes):
        now = datetime.now(UTC)
        return landing.append(self.log, [("s1", i, c, now) for i, c in enumerate(changes)])

    def offsets(self, **behind):
        """Every partition caught up to the head, except `p<N>=position` ones."""
        self.log.update_incremental()
        caught_up = {p: (self.log.version() + 1, 0) for p in range(PARTITIONS)}
        moved = caught_up | {int(k[1:]): v for k, v in behind.items()}
        state.save_offsets(self.state, moved, landing.table_id(self.log))

    def merge(self, sv):
        """A MERGE replaces the file holding the Listing, as each worker batch does."""
        write = Write(("m_1", A), sv, listing(), classified(), False)
        return store.merge(self.store, [write], datetime.now(UTC)).version

    def change_data(self):
        return sorted((self.tmp / "data" / "listing_store" / "_change_data").rglob("*.parquet"))

    def watermark(self, version):
        state.save_watermark(self.state, version, self.store.metadata().id)

    def tick(self, later=timedelta(), floor=timedelta(hours=1)):
        now = datetime.now(UTC) + later
        data = self.tmp / "data"
        maintenance.tick(self.log, self.store, data, self.state, self.events, now, floor=floor)

    def rows(self):
        return landing.ensure(self.log.table_uri).to_pyarrow_table().num_rows

    def vacuum_store(self, later=timedelta()):
        now = datetime.now(UTC) + later
        return maintenance.vacuum_store(self.store, self.state, self.events, now)

    def store_files(self):
        return sorted((self.tmp / "data" / "listing_store").rglob("part-*.parquet"))

    def store_events(self):
        return [e for e in events.read(self.tmp / "data" / "events") if e["type"] == "store_vacuum"]

    def reports(self):
        return [e for e in events.read(self.tmp / "data" / "events") if e["type"] == "maintenance"]


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def test_rows_past_the_retention_are_deleted_once_every_partition_read_them(env):
    env.add(up(A, 1), up(B, 1))
    env.offsets()
    env.tick(later=LATER)
    assert env.rows() == 0
    assert env.reports()[-1]["deleted_rows"] == 2


def test_rows_within_the_hour_past_the_retention_stay(env):
    """received_at is stamped before the commit: the hour keeps a slow commit's rows."""
    env.add(up(A, 1))
    env.offsets()
    env.tick(later=LANDING_RETENTION + timedelta(minutes=30))
    assert env.rows() == 1


def test_rows_stay_while_a_partition_has_not_read_them(env):
    first = env.add(up(A, 1))
    env.add(up(B, 1))
    env.offsets(p3=(first, 0))
    env.tick(later=LATER)
    assert env.rows() == 2
    (report,) = env.reports()
    assert report["deleted_rows"] == 0
    assert report["landing_log"]["next"] == first
    assert datetime.fromisoformat(report["landing_log"]["unread"]) <= datetime.now(UTC)


def test_a_vacuum_keeps_what_a_partition_behind_still_reads(env):
    first = env.add(up(A, 1))
    for sv in range(2, 5):
        env.add(up(A, sv))
    env.offsets(p3=(first, 0))
    env.tick(floor=timedelta())  # compacts, replacing every file the change feed reads
    env.tick(floor=timedelta())  # vacuums
    assert env.reports()[-1]["landing_log"]["vacuumed"] == 0
    batch = landing.read(env.log, {3: (first, 0)}, limit=10)
    assert [c.change.source_version for c in batch.changes] == [1, 2, 3, 4]


def test_without_the_guard_that_vacuum_breaks_the_read(env):
    """The control for the test above: the files it keeps are the ones the read needs."""
    first = env.add(up(A, 1))
    for sv in range(2, 5):
        env.add(up(A, sv))
    env.log.optimize.compact()
    env.log.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
    with pytest.raises(Exception, match="not found"):
        landing.read(env.log, {3: (first, 0)}, limit=10)


def test_a_vacuum_removes_replaced_files_once_everything_is_read(env):
    for sv in range(1, 5):
        env.add(up(A, sv))
    env.offsets()
    env.tick(floor=timedelta())
    env.offsets()  # workers pass the compaction commit too
    env.tick(floor=timedelta())
    assert env.reports()[-1]["landing_log"]["vacuumed"] > 0
    assert env.rows() == 4


def test_the_listing_stores_vacuum_keeps_what_the_exporter_still_reads(env):
    exported = env.merge(1)
    for sv in range(2, 5):
        env.merge(sv)
    env.watermark(exported)
    env.offsets()
    env.tick(floor=timedelta())
    assert env.reports()[-1]["listing_store"]["vacuumed"] == 0
    feed = pa.table(env.store.load_cdf(starting_version=exported + 1).read_all())
    assert feed.num_rows > 0


def test_the_listing_stores_vacuum_removes_replaced_files_once_exported(env):
    for sv in range(1, 5):
        head = env.merge(sv)
    env.watermark(head)
    env.offsets()
    env.tick(floor=timedelta())
    assert env.reports()[-1]["listing_store"]["vacuumed"] > 0
    assert env.reports()[-1]["listing_store"]["vacuum_commit_failed"] is False


# The Listing Store's own vacuum (step 7f): every minute, dead files older than the grace go.


def test_the_store_vacuum_removes_a_file_replaced_before_the_grace(env):
    env.merge(1)
    head = env.merge(2)  # replaces the file holding the Listing
    env.watermark(head)
    before = env.store_files()
    env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    assert len(env.store_files()) < len(before)


def test_the_store_vacuum_keeps_the_files_of_a_version_committed_inside_the_grace(env):
    env.merge(1)
    head = env.merge(2)
    env.watermark(head)
    before = env.store_files()
    report = env.vacuum_store()  # both versions are seconds old
    assert env.store_files() == before
    assert report["removed"] == 0
    assert env.store_events() == []  # nothing removed, nothing said


def test_the_store_vacuum_keeps_what_change_export_has_not_read(env):
    exported = env.merge(1)
    for sv in range(2, 5):
        env.merge(sv)  # versions 2 to 4 unread by Change Export; each replaces the last's file
    env.watermark(exported)
    env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    feed = pa.table(env.store.load_cdf(starting_version=exported + 1).read_all())
    assert feed.num_rows > 0  # the feed from the first unread version still reads
    assert len(env.change_data()) == 4  # its files are the feed's, never a sweep's business


def test_the_store_vacuum_never_removes_an_untracked_file(env):
    env.merge(1)
    head = env.merge(2)
    env.watermark(head)
    stray = env.tmp / "data" / "listing_store" / "partition=3" / "part-inflight.parquet"
    stray.write_bytes(b"a MERGE still writing its output")
    env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    assert stray.exists()


def test_the_store_vacuum_says_what_it_removed(env):
    env.merge(1)
    head = env.merge(2)
    env.watermark(head)
    env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    (event,) = env.store_events()
    assert event["removed"] >= 1 and event["bytes"] > 0 and event["ms"] >= 0


def test_the_store_vacuum_keeps_the_version_a_snapshots_pass_has_pinned(env):
    pinned = env.merge(1)
    head = env.merge(2)
    env.watermark(head)
    state.save_pin(env.state, pinned, datetime.now(UTC))  # a Snapshots pass copying version 1
    before = env.store_files()
    report = env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    assert env.store_files() == before
    assert report["pin_stale"] is False
    state.clear_pin(env.state)  # the pass finished
    env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    assert len(env.store_files()) < len(before)


def test_the_store_vacuum_ignores_a_pin_older_than_an_hour(env):
    pinned = env.merge(1)
    head = env.merge(2)
    env.watermark(head)
    state.save_pin(env.state, pinned, datetime.now(UTC) - timedelta(hours=1, seconds=1))
    before = env.store_files()
    report = env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    assert len(env.store_files()) < len(before)
    assert report["pin_stale"] is True


def test_a_reader_inside_the_grace_still_reads_every_row_across_a_vacuum(env):
    env.merge(1)
    reader = DeltaTable(env.store.table_uri)  # loaded now, at this version
    before = reader.to_pyarrow_table().num_rows
    head = env.merge(2)  # replaces the file the reader's version holds
    env.watermark(head)
    env.vacuum_store()  # the reader's version is seconds old
    assert reader.to_pyarrow_table().num_rows == before


def test_a_pinned_reader_still_reads_every_row_across_a_vacuum_after_the_grace(env):
    pinned = env.merge(1)
    reader = DeltaTable(env.store.table_uri, version=pinned)
    before = reader.to_pyarrow_table().num_rows
    head = env.merge(2)
    env.watermark(head)
    state.save_pin(env.state, pinned, datetime.now(UTC))
    env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    assert reader.to_pyarrow_table().num_rows == before


def test_without_the_grace_that_reader_loses_its_file(env):
    env.merge(1)
    reader = DeltaTable(env.store.table_uri)
    head = env.merge(2)
    env.watermark(head)
    env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    with pytest.raises(Exception, match="not found|No such file"):
        reader.to_pyarrow_table()


def test_the_grace_runs_from_the_removal_not_the_commit(env):
    # An idle store: version 1 is old, but its file was only just replaced, and a reader that
    # loaded version 1 up to a grace ago may still hold it.
    env.merge(1)
    time.sleep(0.3)
    head = env.merge(2)
    env.watermark(head)
    before = env.store_files()
    maintenance.vacuum_store(
        env.store, env.state, env.events, datetime.now(UTC), grace=timedelta(seconds=0.15)
    )
    assert env.store_files() == before  # removed 0.0 s ago, inside a 0.15 s grace
    time.sleep(0.2)
    maintenance.vacuum_store(
        env.store, env.state, env.events, datetime.now(UTC), grace=timedelta(seconds=0.15)
    )
    assert len(env.store_files()) < len(before)


def test_a_malformed_or_impossible_pin_keeps_nothing_and_kills_nothing(env):
    env.merge(1)
    head = env.merge(2)
    env.watermark(head)
    before = env.store_files()
    (env.state / "snapshot_pin.json").write_text("{not json")
    report = env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    assert len(env.store_files()) < len(before) and report["pin_stale"] is False
    state.save_pin(env.state, head + 50, datetime.now(UTC))  # a version the log doesn't have
    report = env.vacuum_store(later=maintenance.GRACE + timedelta(seconds=1))
    assert report["errors"] == 0


def test_a_file_that_cannot_be_removed_is_reported_and_retried(env, monkeypatch):
    env.merge(1)
    head = env.merge(2)
    env.watermark(head)
    real = Path.unlink
    calls = []

    def flaky(self, *a, **k):
        calls.append(self)
        if len(calls) == 1:
            raise PermissionError("busy")
        return real(self, *a, **k)

    monkeypatch.setattr(Path, "unlink", flaky)
    seen = {}
    now = datetime.now(UTC) + maintenance.GRACE + timedelta(seconds=1)
    first = maintenance.vacuum_store(env.store, env.state, env.events, now, seen=seen)
    assert first["errors"] == 1 and first["removed"] == 0 and first["pending"] == 1
    second = maintenance.vacuum_store(env.store, env.state, env.events, now, seen=seen)
    assert second["errors"] == 0 and second["removed"] == 1 and second["pending"] == 0
    assert [e["errors"] for e in env.store_events()] == [1, 0]


def test_the_sweep_remembers_where_it_read_the_log_to(env):
    env.merge(1)
    head = env.merge(2)
    env.watermark(head)
    seen = {}
    later = datetime.now(UTC) + maintenance.GRACE + timedelta(seconds=1)
    maintenance.vacuum_store(env.store, env.state, env.events, later, seen=seen)
    assert seen["version"] == head and seen["pending"] == {}
    head = env.merge(3)
    maintenance.vacuum_store(env.store, env.state, env.events, later, seen=seen)
    assert seen["version"] == head  # only the new commit was read


def test_the_hourly_vacuums_commit_conflict_is_reported_not_fatal(env, monkeypatch):
    """delta-rs's vacuum commits a VACUUM END after deleting; a MERGE landing meanwhile makes
    that commit fail after the files are gone (7f.2's run: 165 maintenance restarts).

    A deliberate exception to the plan's "no mocks of delta-rs": the race cannot be forced from
    Python, so the real vacuum runs and the real exception class is raised after it, which the
    review's race script confirmed is what delta-rs raises (round 1 of #111).
    """
    from deltalake.exceptions import CommitFailedError

    env.merge(1)
    head = env.merge(2)  # version 1's file is due for the vacuum
    env.watermark(head)
    env.offsets()
    due = env.store.vacuum(retention_hours=0, enforce_retention_duration=False)
    assert len(due) == 1
    real = type(env.store).vacuum

    def conflicting(self, *a, **k):
        out = real(self, *a, **k)
        if not k.get("dry_run", True):
            raise CommitFailedError("a concurrent transaction deleted data this operation read")
        return out

    monkeypatch.setattr(type(env.store), "vacuum", conflicting)
    env.tick(later=timedelta(hours=2), floor=timedelta())  # completes: the pass carried on
    report = env.reports()[-1]["listing_store"]
    assert report["vacuum_commit_failed"] is True
    assert report["vacuumed"] == 1  # what the vacuum deleted before its commit failed
    assert not (env.tmp / "data" / "listing_store" / due[0]).exists()
    assert report["logs_cleaned"] is True and "change_data" in report  # the rest of the pass ran


def test_change_data_files_go_once_exported(env):
    for sv in range(1, 5):
        head = env.merge(sv)
    assert env.change_data()  # the MERGEs' updates; vacuum never removes these
    env.watermark(head)
    env.offsets()
    env.tick(later=maintenance.FEED_RETENTION + timedelta(hours=2), floor=timedelta())
    assert env.change_data() == []
    assert env.reports()[-1]["listing_store"]["change_data"] > 0


def test_the_stores_feed_stays_as_long_as_the_snapshots_so_old_versions_still_rebuild(env):
    # The oracles rebuild an old version from the feed (step 7f); a snapshot is kept 7 days, so
    # the feed is too, however long ago Change Export read it.
    env.merge(1)
    want = store.fingerprints(env.store, 1)  # read from version 1's own files, which exist now
    head = env.merge(2)
    files = env.change_data()
    env.watermark(head)
    env.offsets()
    env.tick(later=timedelta(days=6, hours=23), floor=timedelta())  # version 1's files go
    assert env.change_data() == files  # the feed stays
    assert store.fingerprints(env.store, 1) == want  # so an old version still rebuilds
    assert timedelta(days=7) == maintenance.FEED_RETENTION  # the snapshots' lifetime


def test_change_data_files_stay_an_hour_past_the_retention(env):
    """A file is written before its commit: the hour keeps one whose commit is still landing."""
    for sv in range(1, 3):
        head = env.merge(sv)
    files = env.change_data()
    env.watermark(head)
    env.offsets()
    env.tick(later=timedelta(minutes=30), floor=timedelta())
    assert env.change_data() == files


def test_change_data_files_stay_while_the_exporter_has_not_read_them(env):
    exported = env.merge(1)
    for sv in range(2, 5):
        env.merge(sv)
    files = env.change_data()
    env.watermark(exported)
    env.offsets()
    env.tick(floor=timedelta())
    assert env.change_data() == files
    feed = pa.table(env.store.load_cdf(starting_version=exported + 1).read_all())
    assert "update_postimage" in feed["_change_type"].to_pylist()


def _old_logs_expire_at_once(dt):
    dt.alter.set_table_properties({"delta.logRetentionDuration": "interval 0 seconds"})
    dt.create_checkpoint()


def test_old_log_files_go_once_every_partition_read_them(env):
    first = env.add(up(A, 1))
    env.add(up(A, 2))
    _old_logs_expire_at_once(env.log)
    env.offsets()
    env.tick(floor=timedelta())
    assert env.reports()[-1]["landing_log"]["logs_cleaned"] is True
    with pytest.raises(Exception, match="Invalid table version"):
        landing.read(env.log, {3: (first, 0)}, limit=10)


def test_log_files_stay_while_a_partition_has_not_read_them(env):
    first = env.add(up(A, 1))
    env.add(up(A, 2))
    _old_logs_expire_at_once(env.log)  # with Delta's own cleanup on, this alone removes them
    env.offsets(p3=(first, 0))
    env.tick(floor=timedelta())
    assert env.reports()[-1]["landing_log"]["logs_cleaned"] is False
    batch = landing.read(env.log, {3: (first, 0)}, limit=10)
    assert [c.change.source_version for c in batch.changes] == [1, 2]


def test_a_reader_whose_history_is_gone_holds_everything_back(env):
    """State reset against old data: the partition's next version has no log left (5e's case)."""
    first = env.add(up(A, 1))
    env.add(up(A, 2))
    _old_logs_expire_at_once(env.log)
    env.offsets()
    env.tick(floor=timedelta())  # the log of `first` goes
    env.offsets(p3=(first, 0))
    env.tick(later=LATER, floor=timedelta())
    report = env.reports()[-1]["landing_log"]
    assert (report["history_gone"], report["slowest"], report["logs_cleaned"]) == (True, 3, False)
    assert env.reports()[-1]["deleted_rows"] == 0
    assert env.rows() == 2


def test_run_cleans_until_stopped(env):
    env.offsets()
    stop = threading.Event()
    runner = threading.Thread(
        target=maintenance.run, args=(env.tmp / "data", env.state), kwargs={"stop": stop}
    )
    runner.start()
    wait_for(lambda: len(env.reports()) == 1)
    stop.set()
    runner.join(timeout=10)
    assert not runner.is_alive()  # the wait for the next pass ends at once


def test_run_vacuums_the_store_on_its_own_faster_clock(env, monkeypatch):
    env.offsets()
    passes = []
    swept = {"removed": 2, "bytes": 10, "errors": 0}
    monkeypatch.setattr(maintenance, "vacuum_store", lambda *a, **k: passes.append(a[3]) or swept)
    stop = threading.Event()
    runner = threading.Thread(
        target=maintenance.run,
        args=(env.tmp / "data", env.state),
        kwargs={"stop": stop, "interval": 0.4, "store_vacuum": 0.05},
    )
    runner.start()
    wait_for(lambda: len(env.reports()) >= 2, timeout=10)
    stop.set()
    runner.join(timeout=10)
    assert not runner.is_alive()
    assert len(passes) >= 3  # several vacuums inside one pass's interval
    totals = env.reports()[1]["store_vacuum"]  # the pass reports the sweeps since the last one
    assert totals["passes"] >= 2 and totals["removed"] == 2 * totals["passes"]


def test_a_second_maintenance_is_refused_before_touching_anything(env):
    env.add(up(A, 1))
    env.offsets()
    version = env.log.version()
    with state.claim_maintenance(env.state), pytest.raises(state.PartitionTaken, match="Maint"):
        maintenance.run(env.tmp / "data", env.state, stop=threading.Event())
    assert landing.ensure(env.log.table_uri).version() == version
    assert env.reports() == []


def test_new_tables_leave_log_cleanup_to_maintenance(env):
    for dt in (env.log, env.store):
        config = dt.metadata().configuration
        assert config["delta.enableExpiredLogCleanup"] == "false"
        assert config["delta.logRetentionDuration"] == "interval 1 hours"  # the tick's floor


def _emitted_at(env, when):
    log = EventLog(env.tmp / "data" / "events", "api", clock=lambda: when.timestamp())
    log.emit([{"type": "probe", "at": when.isoformat()}])


def test_event_hours_past_the_retention_go(env):
    real = datetime.now(UTC)
    now = real.replace(minute=10)  # early in the hour, so `kept` shares the horizon's hour
    edge = now - EVENTS_RETENTION - timedelta(hours=1)  # its hour ends at or before the horizon
    kept = now - EVENTS_RETENTION + HALF_HOUR
    for when in (now - EVENTS_RETENTION - timedelta(hours=2), edge, kept):
        _emitted_at(env, when)
    env.offsets()
    env.tick(later=now - real)
    found = events.read(env.tmp / "data" / "events")
    assert [e["at"] for e in found if e["type"] == "probe"] == [kept.isoformat()]
    assert env.reports()[-1]["event_hours"] == 2


def _export_file(env, first, last, age):
    path = env.tmp / "data" / "export" / f"{first:012}-{last:012}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    written = (datetime.now(UTC) - age).timestamp()
    os.utime(path, (written, written))
    return path.name


def test_export_files_past_the_retention_go_once_the_watermark_passed_them(env):
    old = EVENTS_RETENTION + HALF_HOUR
    exported = _export_file(env, 0, 4, old)
    _export_file(env, 5, 9, old)  # a crash before its watermark: the exporter adopts it next
    _export_file(env, 0, 2, EVENTS_RETENTION - HALF_HOUR)
    stray = env.tmp / "data" / "export" / "backup.parquet"  # not the exporter's: left alone
    stray.write_bytes(b"")
    env.watermark(4)
    env.offsets()
    env.tick()
    names = [p.name for p in export.files(env.tmp / "data" / "export")]
    assert exported not in names
    assert names == [f"{0:012}-{2:012}.parquet", f"{5:012}-{9:012}.parquet", stray.name]
    assert env.reports()[-1]["export_files"] == 1


def test_an_error_that_ends_the_run_is_in_the_events_too(env, monkeypatch):  # 5c review
    def broken(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(maintenance, "tick", broken)
    with pytest.raises(OSError):
        maintenance.run(env.tmp / "data", env.state, stop=threading.Event())
    [stop] = [
        e for e in events.read(env.tmp / "data" / "events") if e["type"] == "maintenance_stop"
    ]
    assert stop["error"] == "OSError(28, 'No space left on device')"
