"""Small durable state on local disk: offsets, watermarks, partition locks, heartbeats.

Layout under the state directory:
  offsets/pNN.json   next Landing log position per partition, and which Landing log (A9)
  locks/pNN.lock     flock held by the partition's owner (A8)
  heartbeat/<worker>.json   last sign of life, for the supervisor (B1)
"""

import fcntl
import json
import os
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from catalog.landing import START, Position


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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w") as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


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


class PartitionTaken(RuntimeError):
    pass


@contextmanager
def claim(state: Path, partitions: Iterable[int]) -> Iterator[None]:
    """Hold an exclusive lock on each partition for the duration; fail fast if one is taken.

    The lock lives as long as any process holds its open file, so never fork (rather than
    spawn) while claiming: a forked child would keep the partitions locked after a kill -9.
    """
    locks = state / "locks"
    locks.mkdir(parents=True, exist_ok=True)
    fds = []
    try:
        for p in partitions:
            fds.append(os.open(locks / f"p{p:02}.lock", os.O_CREAT | os.O_RDWR))
            try:
                fcntl.flock(fds[-1], fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise PartitionTaken(f"partition {p} is owned by another process") from None
        yield
    finally:
        for fd in fds:
            os.close(fd)


def _heartbeat_file(state: Path, worker: str) -> Path:
    return state / "heartbeat" / f"{worker}.json"


def beat(state: Path, worker: str, now: float) -> None:
    save(_heartbeat_file(state, worker), {"ts": now})


def heartbeat_age(state: Path, worker: str, now: float) -> float | None:
    """Seconds since the worker's last beat; None if it never beat."""
    last = load(_heartbeat_file(state, worker), None)
    return None if last is None else now - last["ts"]
