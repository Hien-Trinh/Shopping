"""Small durable state on local disk: offsets, watermarks, partition locks, heartbeats.

Layout under the state directory:
  offsets/pNN.json   last processed Landing log position per partition (A9)
  locks/pNN.lock     flock held by the partition's owner (A8); released when the process dies
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


def load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default


def save(path: Path, value: Any) -> None:
    """Atomic: readers see the old value or the new one, never a torn file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w") as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_offsets(state: Path, partitions: Iterable[int]) -> dict[int, Position]:
    return {p: tuple(load(state / "offsets" / f"p{p:02}.json", START)) for p in partitions}


def save_offsets(state: Path, offsets: Mapping[int, Position]) -> None:
    for p, position in offsets.items():
        save(state / "offsets" / f"p{p:02}.json", list(position))


class PartitionTaken(RuntimeError):
    pass


@contextmanager
def claim(state: Path, partitions: Iterable[int]) -> Iterator[None]:
    """Hold an exclusive lock on each partition for the duration; fail fast if one is taken."""
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


def beat(state: Path, worker: str, now: float) -> None:
    save(state / "heartbeat" / f"{worker}.json", {"ts": now})


def heartbeat_age(state: Path, worker: str, now: float) -> float | None:
    """Seconds since the worker's last beat; None if it never beat."""
    last = load(state / "heartbeat" / f"{worker}.json", None)
    return None if last is None else now - last["ts"]
