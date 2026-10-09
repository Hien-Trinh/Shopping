"""Small durable state on local disk: offsets, watermarks, partition locks, heartbeats.

Layout under the state directory:
  offsets/pNN.json   next Landing log position per partition, and which Landing log (A9)
  export_watermark.json   last exported Listing Store version, and which Listing Store (A9)
  locks/pNN.lock     flock held by the partition's owner (A8)
  locks/export.lock  flock held by the one Change Export
  locks/maintenance.lock  flock held by the one maintenance process
  locks/snapshots.lock  flock held by the one snapshotter
  locks/backfill.lock  flock held by the one Backfill
  backfill.json      the Backfill's last Landing log commit, and which Landing log (step 6c)
  heartbeat/<worker>.json   last sign of life, for the supervisor (B1)
  supervisor.lock    flock held by the one supervisor running against this directory
"""

import fcntl
import json
import os
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO

from catalog.landing import START, Position

# Shared by every worker and the supervisor, so they can't disagree:
CLOCK = time.monotonic  # heartbeats: system-wide, and laptop sleep or NTP never jumps it (B7)
DATA, STATE = Path("data"), Path("state")  # default directories of every process
WORKER = "worker-"  # a worker's name prefix: its Procfile name, heartbeat file and event log


class CorruptState(RuntimeError):
    pass


class OffsetsMismatch(RuntimeError):
    pass


def load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default
    except ValueError as e:
        raise CorruptState(f"{path} is unreadable ({e}); it was not written by save()") from None


def save(path: Path, value: Any) -> None:
    """Atomic: after a process crash, readers see the old value or the new one, never a torn file.

    ponytail: process-crash safe only. Power loss can still lose or tear the write (no directory
    fsync, no F_FULLFSYNC on macOS), as it can delta-rs commits, which are not fsynced either.
    Upgrade path: fsync new Delta log and data files after each commit, then F_FULLFSYNC here.
    """
    with atomic(path) as f:
        f.write(json.dumps(value).encode())


@contextmanager
def atomic(path: Path) -> Iterator[BinaryIO]:
    """A binary file that replaces `path` whole on a clean exit, durable as save() is.

    On any exception the temp file is removed, else every retry on a full disk leaves one more.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")  # unique per call, not per pid
    try:
        with open(tmp, "wb") as f:
            yield f
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _offset_file(state: Path, p: int) -> Path:
    return state / "offsets" / f"p{p:02}.json"


def load_offsets(state: Path, partitions: Iterable[int], table: str) -> dict[int, Position]:
    """Next positions in the Landing log identified by `table` (landing.table_id).

    Refuses offsets saved against another Landing log (one recreated since): trusting them
    would silently skip every row below them.
    """
    offsets = {}
    for p in partitions:
        saved = load(_offset_file(state, p), None)
        if saved is not None and saved["table"] != table:
            raise OffsetsMismatch(
                f"partition {p} offset belongs to Landing log {saved['table']}, not {table}:"
                " reset the state and data directories together"
            )
        offsets[p] = START if saved is None else tuple(saved["next"])
    return offsets


def save_offsets(state: Path, offsets: Mapping[int, Position], table: str) -> None:
    for p, position in offsets.items():
        save(_offset_file(state, p), {"table": table, "next": list(position)})


def load_watermark(state: Path, table: str) -> int:
    """The last Listing Store version Change Export wrote out; -1 if none. Refuses a watermark
    saved against another Listing Store, as load_offsets does."""
    saved = load(state / "export_watermark.json", None)
    if saved is None:
        return -1
    if saved["table"] != table:
        raise OffsetsMismatch(
            f"export watermark belongs to Listing Store {saved['table']}, not {table}:"
            " reset the state and data directories together"
        )
    return saved["version"]


def save_watermark(state: Path, version: int, table: str) -> None:
    save(state / "export_watermark.json", {"table": table, "version": version})


def save_pin(state: Path, version: int, started: datetime) -> None:
    """The Listing Store version a Catalog Snapshots pass is copying, and since when: the
    store's vacuum keeps that version's files while the file exists (step 7f)."""
    save(state / "snapshot_pin.json", {"version": version, "started": started.isoformat()})


def load_pin(state: Path) -> tuple[int, datetime] | None:
    """The pin, or None when there is none or it is malformed (the vacuum then keeps nothing
    for it, as for a stale one, rather than dying on a file only Snapshots writes)."""
    try:
        saved = load(state / "snapshot_pin.json", None)
        since = datetime.fromisoformat(saved["started"])
        if since.utcoffset() is None:
            return None
        return int(saved["version"]), since
    except CorruptState, TypeError, KeyError, ValueError:
        return None


def clear_pin(state: Path) -> None:
    (state / "snapshot_pin.json").unlink(missing_ok=True)


def load_backfill(state: Path, table: str) -> int:
    """The Landing log version of the Backfill's last commit; -1 if none. Refuses one saved
    against another Landing log, as load_offsets does."""
    saved = load(state / "backfill.json", None)
    if saved is None:
        return -1
    if saved["table"] != table:
        raise OffsetsMismatch(
            f"backfill state belongs to Landing log {saved['table']}, not {table}:"
            " reset the state and data directories together"
        )
    return saved["version"]


def save_backfill(state: Path, version: int, table: str) -> None:
    save(state / "backfill.json", {"table": table, "version": version})


class PartitionTaken(RuntimeError):
    pass


@contextmanager
def claim(state: Path, partitions: Iterable[int]) -> Iterator[None]:
    """Hold an exclusive lock on each partition for the duration; fail fast if one is taken.

    The lock lives as long as any process holds its open file, so never fork (rather than
    spawn) while claiming: a forked child would keep the partitions locked after a kill -9.
    """
    with _hold(state, {f"p{p:02}": f"partition {p}" for p in partitions}):
        yield


@contextmanager
def claim_export(state: Path) -> Iterator[None]:
    """One Change Export per state directory: two would interleave overlapping files."""
    with _hold(state, {"export": "Change Export"}):
        yield


@contextmanager
def claim_snapshots(state: Path) -> Iterator[None]:
    """One snapshotter per state directory: its prune deletes hidden folders as crash leftovers."""
    with _hold(state, {"snapshots": "Catalog Snapshots"}):
        yield


@contextmanager
def claim_maintenance(state: Path) -> Iterator[None]:
    """One maintenance per state directory: two would race their DELETEs and vacuums."""
    with _hold(state, {"maintenance": "Maintenance"}):
        yield


@contextmanager
def claim_backfill(state: Path) -> Iterator[None]:
    """One Backfill per state directory: two would append the same rows twice."""
    with _hold(state, {"backfill": "Backfill"}):
        yield


@contextmanager
def _hold(state: Path, locks: Mapping[str, str]) -> Iterator[None]:
    """flock locks/<name>.lock for each name; PartitionTaken names the first one held."""
    (state / "locks").mkdir(parents=True, exist_ok=True)
    fds = []
    try:
        for name, what in locks.items():
            fds.append(os.open(state / "locks" / f"{name}.lock", os.O_CREAT | os.O_RDWR))
            try:
                fcntl.flock(fds[-1], fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise PartitionTaken(f"{what} is owned by another process") from None
        yield
    finally:
        for fd in fds:
            os.close(fd)


def _heartbeat_file(state: Path, worker: str) -> Path:
    return state / "heartbeat" / f"{worker}.json"


def beat(state: Path, worker: str, now: float) -> None:
    save(_heartbeat_file(state, worker), {"ts": now})


def last_beat(state: Path, worker: str) -> float | None:
    """The clock reading of the worker's last beat; None if it never beat or the file is bad.

    A heartbeat is disposable (the next beat rewrites it), so a torn or malformed one counts as
    none rather than stopping the supervisor that reads it.
    """
    try:
        ts = load(_heartbeat_file(state, worker), {})["ts"]
    except CorruptState, KeyError, TypeError:
        return None
    return ts if isinstance(ts, int | float) else None
