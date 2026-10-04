"""Catalog Snapshots: a copy of the Listing Store at a pinned version every 6 h, kept 7 days
(design doc, Catalog Snapshots; docs/specs/step-5c.md).

A snapshot is a Delta table at snapshots/<ts>/, written to a hidden folder and renamed into place,
so it is complete or invisible. Its commit records the Listing Store version it copied.
"""

import argparse
import os
import shutil
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from deltalake import CommitProperties, DeltaTable, write_deltalake

from catalog import entry, state, store, worker
from catalog.events import EventLog

NAME = "%Y%m%dT%H%M%SZ"  # UTC; name order is time order
EVERY, KEEP = timedelta(hours=6), timedelta(days=7)  # design doc: every 6 h, keep 7 days


def run(
    data: Path,
    state_dir: Path,
    *,
    stop: threading.Event,
    every: timedelta = EVERY,
    keep: timedelta = KEEP,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> None:
    """Snapshot when due until `stop` is set. Any error propagates: the supervisor restarts it."""
    with state.claim_snapshots(state_dir):  # first: a second one dies before deleting anything
        dt = store.ensure(str(data / "listing_store"))
        events = EventLog(data / "events", "snapshots")
        events.emit([{"type": "snapshots_start", "pid": os.getpid()}])
        while not stop.is_set():
            wait = tick(dt, data / "snapshots", events, clock(), every=every, keep=keep)
            stop.wait(wait.total_seconds())


def tick(
    dt: DeltaTable,
    snapshot_dir: Path,
    events: EventLog,
    now: datetime,
    *,
    every: timedelta,
    keep: timedelta,
) -> timedelta:
    """Take a snapshot if the newest is `every` old (or none exists); the wait until the next.

    Due times come from snapshot names, not the process start: restarts add no snapshot, and a
    long outage takes one, not a burst.
    """
    started = time.monotonic()
    newest = max((_taken(p) for p in existing(snapshot_dir)), default=None)
    if newest is None or now - newest >= every:
        path = take(dt, snapshot_dir, now)
        rows = DeltaTable(str(path)).to_pyarrow_dataset().count_rows()
        ms = round((time.monotonic() - started) * 1000)
        event = {"type": "snapshot", "name": path.name, "version": pinned(path), "rows": rows}
        events.emit([event | {"ms": ms}])
        newest = now
    prune(snapshot_dir, now, keep=keep, events=events)
    return newest + every - now


def existing(snapshot_dir: Path) -> list[Path]:
    """The complete snapshots, oldest first: hidden folders are being written or deleted."""
    if not snapshot_dir.exists():
        return []
    return sorted(p for p in snapshot_dir.iterdir() if not p.name.startswith("."))


def take(dt: DeltaTable, snapshot_dir: Path, now: datetime) -> Path:
    """Copy the Listing Store's current version to snapshot_dir/<now>; the new folder."""
    dt.update_incremental()
    version = dt.version()
    pinned = DeltaTable(dt.table_uri, version=version)
    path = snapshot_dir / now.astimezone(UTC).strftime(NAME)
    tmp = snapshot_dir / f".{path.name}.{uuid.uuid4().hex}.tmp"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    try:
        write_deltalake(
            str(tmp),
            pinned.to_pyarrow_dataset().scanner().to_reader(),  # streamed: no table in memory
            partition_by=["partition"],
            commit_properties=CommitProperties(
                custom_metadata={"pinned_version": str(version), "table": dt.metadata().id}
            ),
        )
        os.rename(tmp, path)  # fails if a snapshot of the same second exists
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)  # else every retry on a full disk leaves one more
        raise
    return path


def prune(snapshot_dir: Path, now: datetime, *, keep: timedelta, events: EventLog) -> None:
    """Delete snapshots older than `keep`, never the newest, and any hidden leftover of a crash.

    Safe only under the one snapshotter's lock: a hidden folder is never another's copy.
    """
    complete = existing(snapshot_dir)
    for path in complete[:-1]:
        if now - _taken(path) > keep:
            # Hidden first: a crash mid-delete must not leave a half snapshot that looks whole.
            os.rename(path, path.with_name(f".{path.name}.{uuid.uuid4().hex}.deleting"))
            events.emit([{"type": "snapshot_pruned", "name": path.name}])
    for path in snapshot_dir.glob(".*"):
        shutil.rmtree(path)


def pinned(path: Path) -> int:
    """The Listing Store version the snapshot at `path` copied."""
    return int(DeltaTable(str(path)).history(1)[0]["pinned_version"])


def _taken(path: Path) -> datetime:
    return datetime.strptime(path.name, NAME).replace(tzinfo=UTC)


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.snapshots")
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--state", type=Path, default=state.STATE)
    args.add_argument("--every", type=float, default=EVERY.total_seconds(), help="seconds")
    a = args.parse_args(argv)
    try:
        supervisor = worker.supervisor_pid()
    except ValueError as e:
        args.error(str(e))
    stop = worker.stop_on_signals()
    worker.watch_supervisor(supervisor, stop)  # so a kill -9ed supervisor leaves no snapshotter
    run(a.data, a.state, stop=stop, every=timedelta(seconds=a.every))


if __name__ == "__main__":
    entry.exit_with(main)  # any error is 1: the supervisor restarts it
