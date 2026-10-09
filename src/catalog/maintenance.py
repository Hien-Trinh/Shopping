"""Maintenance: retention and cleanup of the Delta tables, never past the slowest reader
(docs/specs/step-5d.md).

A table's slowest reader is the first version it has not read: the lowest worker offset for
the Landing log, the export watermark + 1 for the Listing Store. History older than a time may
go only if that version is past the head or was committed at or after that time.
"""

import argparse
import json
import math
import os
import threading
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from deltalake import DeltaTable

from catalog import delta, entry, export, landing, state, store, worker
from catalog import events as event_files
from catalog.events import EventLog, stopping
from catalog.keys import PARTITIONS

INTERVAL = 600.0  # seconds between passes
LANDING_RETENTION = timedelta(days=7)  # plan-v1 A13
# ponytail: received_at is stamped before the commit, so a commit stuck longer than this could
# lose rows to the DELETE (B1's hang was 5 minutes). Upgrade path: delete by commit version.
RECEIVE_LAG = timedelta(hours=1)
HOUR = timedelta(hours=1)
GONE = datetime.min.replace(tzinfo=UTC)  # the first unread version's log was cleaned (5e)
# The Listing Store's own vacuum (step 7f): a MERGE leaves every file it rewrote on disk, about
# one 1,000-row file per Change, so an hour of them at 50/s is tens of GB. Every STORE_VACUUM
# seconds the files the log marks removed go, except those a reader can still need: a version
# committed within GRACE (a reader loads the current version and finishes in seconds), the
# versions Change Export has not read, and a version Catalog Snapshots has pinned.
STORE_VACUUM = 60.0  # seconds between the Listing Store's vacuums
GRACE = timedelta(minutes=2)
PIN_TTL = HOUR  # a Snapshots pin older than this was left by a crashed pass


def run(
    data: Path,
    state_dir: Path,
    *,
    stop: threading.Event,
    interval: float = INTERVAL,
    store_vacuum: float = STORE_VACUUM,
    grace: timedelta = GRACE,
) -> None:
    """A pass every `interval` seconds and a store vacuum every `store_vacuum` seconds until
    `stop` is set; any error propagates, and the supervisor's restart is safe: each step is one
    commit or only deletes (spec decision 8)."""
    with state.claim_maintenance(state_dir):  # first: a second one dies before touching anything
        landing_dt = landing.ensure(str(data / "landing_log"))
        store_dt = store.ensure(str(data / "listing_store"))
        events = EventLog(data / "events", "maintenance")
        events.emit([{"type": "maintenance_start", "pid": os.getpid()}])
        with stopping(events, stop):
            next_pass = next_vacuum = time.monotonic()
            seen: dict = {}  # the sweep's memory; totals go into the pass's report
            totals = {"passes": 0, "removed": 0, "bytes": 0, "errors": 0}
            while not stop.is_set():
                now = time.monotonic()
                if now >= next_pass:
                    tick(landing_dt, store_dt, data, state_dir, events, datetime.now(UTC),
                         store_vacuum=dict(totals))  # fmt: skip
                    totals = dict.fromkeys(totals, 0)
                    next_pass = now + interval
                if now >= next_vacuum:
                    swept = vacuum_store(
                        store_dt, state_dir, events, datetime.now(UTC), grace=grace, seen=seen
                    )
                    totals["passes"] += 1
                    for k in ("removed", "bytes", "errors"):
                        totals[k] += swept[k]
                    next_vacuum = now + store_vacuum
                stop.wait(max(0.0, min(next_pass, next_vacuum) - time.monotonic()))


def tick(
    landing_dt: DeltaTable,
    store_dt: DeltaTable,
    data: Path,
    state_dir: Path,
    events: EventLog,
    now: datetime,
    *,
    floor: timedelta = timedelta(hours=delta.LOG_RETENTION_HOURS),
    store_vacuum: dict | None = None,
) -> None:
    """One full pass; `store_vacuum` is the store sweeps' totals since the last pass, so the
    10-minute report shows the vacuum ran even when its passes had nothing to say."""
    started = time.monotonic()
    landing_dt.update_incremental()
    store_dt.update_incremental()
    offsets = state.load_offsets(state_dir, range(PARTITIONS), landing.table_id(landing_dt))
    watermark = state.load_watermark(state_dir, store_dt.metadata().id)
    readers = {
        "landing_log": (landing_dt, min(v for v, _ in offsets.values())),
        "listing_store": (store_dt, watermark + 1),
    }
    report: dict = {
        "type": "maintenance",
        "store_vacuum": store_vacuum or {"passes": 0, "removed": 0, "bytes": 0, "errors": 0},
        "event_hours": event_files.prune(data / "events", now),
        "export_files": export.prune(data / "export", now, watermark),
    }
    unread = {}
    for name, (dt, next_version) in readers.items():  # only deletes, so a full disk still frees
        unread[name] = _unread_since(dt, next_version)
        when = None if unread[name] is None else unread[name].isoformat()  # how far behind
        report[name] = {"next": next_version, "unread": when} | _clean(dt, unread[name], now, floor)
    report["landing_log"]["slowest"] = min(offsets, key=offsets.__getitem__)
    landing_dt.optimize.compact()  # beside the API's appends (spike B2)
    report["deleted_rows"] = 0
    if _read_before(unread["landing_log"], now - LANDING_RETENTION):
        cutoff = now - LANDING_RETENTION - RECEIVE_LAG
        report["deleted_rows"] = landing_dt.delete(f"received_at < '{cutoff.isoformat()}'")[
            "num_deleted_rows"
        ]
    events.emit([report | {"ms": round((time.monotonic() - started) * 1000)}])


def vacuum_store(
    store_dt: DeltaTable,
    state_dir: Path,
    events: EventLog,
    now: datetime,
    *,
    grace: timedelta = GRACE,
    seen: dict | None = None,
) -> dict:
    """Remove the Listing Store's dead files no reader can still need; the pass's report, which
    is also emitted as a `store_vacuum` event when anything was removed or went wrong.

    A sweep over the log's own `remove` actions, not delta-rs's vacuum: a file goes once its
    removal is older than `grace` (a reader that loaded a version while the file was live did so
    before the removal, so it has had the whole grace) and no Catalog Snapshots pass has pinned
    the version that holds it. `seen` is the sweep's memory across passes (the last log version
    read and the removals still pending), owned by the caller; None starts from the oldest log.
    """
    started = time.monotonic()
    store_dt.update_incremental()
    root = delta.local(store_dt.table_uri)
    memory = seen if seen is not None else {}
    pending: dict[str, tuple[int, int]] = memory.setdefault("pending", {})  # path: (ms, size)
    _read_removes(root, memory, pending)
    cutoff = (now - grace).timestamp() * 1000
    pin, pin_stale = state.load_pin(state_dir), False
    pinned: set[str] = set()
    if pin is not None:  # a Catalog Snapshots pass is copying that version: keep its files
        pinned_version, since = pin
        pin_stale = now - since > PIN_TTL  # left by a killed pass
        if not pin_stale:
            pinned = _files_of(store_dt, pinned_version)
    removed = size = errors = 0
    for path, (ms, bytes_) in list(pending.items()):
        if ms >= cutoff or path in pinned:
            continue
        try:
            (root / path).unlink()
            removed, size = removed + 1, size + bytes_
        except FileNotFoundError:  # delta-rs's own vacuum (the hourly pass) got there first
            pass
        except OSError:  # kept pending: retried next pass
            errors += 1
            continue
        del pending[path]
    report = {
        "type": "store_vacuum",
        "removed": removed,
        "bytes": size,
        "errors": errors,
        "pending": len(pending),
        "pin_stale": pin_stale,
        "ms": round((time.monotonic() - started) * 1000),
    }
    if removed or errors or pin_stale:
        events.emit([report])
    return report


def _read_removes(root: Path, memory: dict, pending: dict[str, tuple[int, int]]) -> None:
    """Add the `remove` actions of the log files past `memory["version"]` to `pending`."""
    # ponytail: one JSON file per commit since the last pass (about 155 a minute at 50/s); a
    # restart rereads what the hourly log cleanup left, and removes older than that are the
    # hourly delta-rs vacuum's. Upgrade path: the checkpoint's remove actions.
    last = memory.get("version", -1)
    for log in sorted((root / "_delta_log").glob("*.json")):
        version = int(log.stem)
        if version <= last:
            continue
        with log.open() as f:
            for action in map(json.loads, f):
                if "remove" in action:
                    r = action["remove"]
                    pending[r["path"]] = (r.get("deletionTimestamp") or 0, r.get("size") or 0)
        memory["version"] = version


def _files_of(dt: DeltaTable, version: int) -> set[str]:
    """The data files a version holds, relative to the table root; empty if the log can't give
    that version (a pin past the head or past the cleaned history: nothing to protect)."""
    try:
        at = DeltaTable(dt.table_uri, version=version)
    except Exception:  # delta-rs raises its own DeltaError for an unknown version
        return set()
    root = delta.local(dt.table_uri)
    return {str(delta.local(u).relative_to(root)) for u in at.file_uris()}


def _clean(dt: DeltaTable, unread: datetime | None, now: datetime, floor: timedelta) -> dict:
    """Remove the history no unread version needs."""
    if unread == GONE:  # how far back it reads is unknown: nothing is safe to remove
        return {"history_gone": True, "vacuumed": 0, "change_data": 0, "logs_cleaned": False}
    lag = timedelta() if unread is None else now - unread
    # Whole hours (delta-rs), at least the floor: vacuum also removes untracked files older than
    # this, which could be a commit still being written.
    hours = max(math.ceil(floor / HOUR), math.ceil(lag / HOUR))
    removed = dt.vacuum(retention_hours=hours, dry_run=False, enforce_retention_duration=False)
    # Vacuum skips paths starting with "_", so the change feed's own files are ours to remove.
    # One is written before its commit: an hour more keeps one whose commit is still landing.
    cutoff = (now - (hours + 1) * HOUR).timestamp()
    change_data = delta.local(dt.table_uri) / "_change_data"
    old = [f for f in change_data.rglob("*.parquet") if f.stat().st_mtime < cutoff]
    for f in old:
        f.unlink()
    # Log files older than the table's log retention (delta.LOG_RETENTION_HOURS, the floor).
    logs_cleaned = _read_before(unread, now - floor)
    if logs_cleaned:
        dt.cleanup_metadata()
    return {
        "history_gone": False,
        "vacuumed": len(removed),
        "change_data": len(old),
        "logs_cleaned": logs_cleaned,
    }


def _unread_since(dt: DeltaTable, version: int) -> datetime | None:
    """When the first unread version was committed; None if nothing is unread."""
    if version > dt.version():
        return None
    # The commit's own record: DeltaTable(version=n).history() ignores n and lists the newest.
    log = delta.local(dt.table_uri) / "_delta_log" / f"{version:020}.json"
    try:
        with log.open() as f:
            ts = next(a["commitInfo"]["timestamp"] for a in map(json.loads, f) if "commitInfo" in a)
    except FileNotFoundError:  # log cleaned past it: treat it as unread forever (fail closed)
        return GONE
    return datetime.fromtimestamp(ts / 1000, UTC)


def _read_before(unread: datetime | None, t: datetime) -> bool:
    """Whether every version committed before `t` has been read."""
    return unread is None or unread >= t


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.maintenance")
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--state", type=Path, default=state.STATE)
    args.add_argument("--interval", type=float, default=INTERVAL)
    args.add_argument("--store-vacuum", type=float, default=STORE_VACUUM)
    args.add_argument("--grace", type=float, default=GRACE.total_seconds(), help="seconds")
    a = args.parse_args(argv)
    try:
        supervisor = worker.supervisor_pid()
    except ValueError as e:
        args.error(str(e))
    stop = worker.stop_on_signals()
    # so a kill -9ed supervisor leaves none behind
    worker.watch_supervisor(supervisor, stop, a.data / "events", "maintenance")
    run(
        a.data, a.state, stop=stop, interval=a.interval, store_vacuum=a.store_vacuum,
        grace=timedelta(seconds=a.grace),
    )  # fmt: skip


if __name__ == "__main__":
    entry.exit_with(main)  # any error is 1: the supervisor restarts it
