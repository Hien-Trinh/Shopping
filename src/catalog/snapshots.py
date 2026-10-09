"""Catalog Snapshots: a copy of the Listing Store at a pinned version every 6 h, kept 7 days
(design doc, Catalog Snapshots; docs/specs/step-5c.md).

A snapshot is a Delta table at snapshots/<ts>/, written to a hidden folder and renamed into place,
so it is complete or invisible. Its commit records the Listing Store version it copied.
"""

import argparse
import os
import re
import shutil
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from deltalake import CommitProperties, DeltaTable, write_deltalake

from catalog import entry, state, store, worker
from catalog.events import EventLog, stopping

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
        state.clear_pin(state_dir)  # a pin left by a killed pass pins nothing
        dt = store.ensure(str(data / "listing_store"))
        events = EventLog(data / "events", "snapshots")
        events.emit([{"type": "snapshots_start", "pid": os.getpid()}])
        with stopping(events, stop):
            while not stop.is_set():
                wait = tick(
                    dt, data / "snapshots", events, clock(), every=every, keep=keep,
                    state_dir=state_dir,
                )  # fmt: skip
                stop.wait(wait.total_seconds())


def tick(
    dt: DeltaTable,
    snapshot_dir: Path,
    events: EventLog,
    now: datetime,
    *,
    every: timedelta,
    keep: timedelta,
    state_dir: Path,
) -> timedelta:
    """Take a snapshot if the newest is `every` old (or none exists); the wait until the next.

    Due times come from snapshot names, not the process start: restarts add no snapshot, and a
    long outage takes one, not a burst.
    """
    started = time.monotonic()
    newest = max((_taken(p) for p in existing(snapshot_dir)), default=None)
    if newest is None or now - newest >= every:
        path = take(dt, snapshot_dir, now, state_dir=state_dir)
        rows = DeltaTable(str(path)).to_pyarrow_dataset().count_rows()
        ms = round((time.monotonic() - started) * 1000)
        event = {"type": "snapshot", "name": path.name, "version": pinned(path), "rows": rows}
        events.emit([event | {"ms": ms}])
        newest = now
    prune(snapshot_dir, now, keep=keep, events=events)
    return newest + every - now


def existing(snapshot_dir: Path) -> list[Path]:
    """The complete snapshots, oldest first. Hidden folders are being written or deleted, and
    anything else not named like a snapshot is ignored rather than crash-looping the process."""
    return sorted(p for p in snapshot_dir.glob("*") if re.fullmatch(r"\d{8}T\d{6}Z", p.name))


def take(dt: DeltaTable, snapshot_dir: Path, now: datetime, *, state_dir: Path) -> Path:
    """Copy the Listing Store's current version to snapshot_dir/<now>; the new folder.

    The version is pinned in `state_dir` for the copy, so the store's vacuum keeps its files
    however long the copy takes (step 7f); the pin goes when the copy ends, either way.
    """
    dt.update_incremental()
    version = dt.version()
    path = snapshot_dir / now.astimezone(UTC).strftime(NAME)
    tmp = snapshot_dir / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        state.save_pin(state_dir, version, now)
        pinned = DeltaTable(dt.table_uri, version=version)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
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
    finally:
        state.clear_pin(state_dir)
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
    for path in [*snapshot_dir.glob(".*.tmp"), *snapshot_dir.glob(".*.deleting")]:
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
    if a.every < 1:  # names have a second's resolution: two in one second would collide
        args.error("--every must be at least 1 second")
    try:
        supervisor = worker.supervisor_pid()
    except ValueError as e:
        args.error(str(e))
    stop = worker.stop_on_signals()
    # so a kill -9ed supervisor leaves no snapshotter
    worker.watch_supervisor(supervisor, stop, a.data / "events", "snapshots")
    run(a.data, a.state, stop=stop, every=timedelta(seconds=a.every))


if __name__ == "__main__":
    entry.exit_with(main)  # any error is 1: the supervisor restarts it
