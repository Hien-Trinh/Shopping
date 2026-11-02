"""Events: one JSON line per stage per Change, one file per process run per hour (A11).

Writers never share a file, so any number of processes log without locks, and a restarted
process never appends after the torn last line its predecessor may have left.
"""

import contextlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

_HOUR = "%Y-%m-%dT%H"
RETENTION = timedelta(days=3)  # plan-v1 A13; the API answers 404 for an id older than this


class EventLog:
    def __init__(self, root: Path, process: str, clock: Callable[[], float] = time.time):
        self.root, self.process, self.clock = root, process, clock
        self._new_file()

    def _new_file(self) -> None:
        # Never append after a line that may be torn: a fresh nonce per process run (a restart
        # can reuse the pid, e.g. pid 1 in a container) and after any failed write.
        self.name = f"{self.process}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

    def emit(self, events: Iterable[Mapping]) -> None:
        """Append events with a shared timestamp (ms) that the events can't override."""
        now = self.clock()
        stamp = {"ts": int(now * 1000)}
        lines = "".join(json.dumps(dict(e) | stamp, separators=(",", ":")) + "\n" for e in events)
        if not lines:
            return
        hour = self.root / datetime.fromtimestamp(now, UTC).strftime(_HOUR)
        hour.mkdir(parents=True, exist_ok=True)
        try:
            with open(hour / f"{self.name}.jsonl", "a") as f:
                f.write(lines)
        except OSError:  # e.g. a full disk mid-write: the next emit starts a new file
            self._new_file()
            raise


@contextlib.contextmanager
def stopping(log: EventLog, stop):
    """Log `<process>_stop` as the block ends: with the error that ended it (re-raised), and why
    the watch stopped it (entry's watch sets `stop.reason`). Best effort: the error matters more."""
    event = {"type": f"{log.process}_stop"}
    try:
        yield
    except Exception as e:
        event["error"] = repr(e)[:500]  # as worker_stop's
        raise
    finally:
        if reason := getattr(stop, "reason", None):
            event["reason"] = reason
        with contextlib.suppress(OSError):
            log.emit([event])


def prepared(root: Path, process: str, event: Mapping, at: float) -> Callable[[], None]:
    """A write of one event stamped `at`, built now, for a hard exit to run later: raw os calls on
    its own file, every OSError swallowed. An EventLog or a print could block there on a lock or
    a full pipe that the stuck thread holds."""
    hour = root / datetime.fromtimestamp(at, UTC).strftime(_HOUR)
    path = hour / f"{process}-{os.getpid()}-{uuid.uuid4().hex[:8]}.jsonl"
    line = json.dumps(dict(event) | {"ts": int(at * 1000)}, separators=(",", ":")) + "\n"
    data = line.encode()

    def write() -> None:
        try:
            os.makedirs(hour, exist_ok=True)
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NONBLOCK, 0o644)
            try:
                os.write(fd, data)
            finally:
                os.close(fd)
        except OSError:
            pass

    return write


def prune(root: Path, now: datetime) -> int:
    """Delete the hour directories whose last second is past RETENTION; returns how many."""
    last = (now - RETENTION - timedelta(hours=1)).astimezone(UTC).strftime(_HOUR)
    old = [p for p in root.glob("*") if p.is_dir() and p.name <= last]
    for hour in old:
        shutil.rmtree(hour)
    return len(old)


def read(root: Path, since: datetime | None = None, submission_id: str | None = None) -> list[dict]:
    """Events from `since`'s hour onward, oldest first.

    Trusts only complete lines: an unterminated last line is still being written, or was torn
    by a crash. (DuckDB's read_json with ignore_errors would return it as a partial event.)
    """
    if since is not None and since.utcoffset() is None:
        raise ValueError("since must be timezone-aware")
    floor = since.astimezone(UTC).strftime(_HOUR) if since else ""
    # The writer's fixed separators make the bytes of a submission_id field predictable, so most
    # lines are skipped without parsing them.
    needle = (
        None if submission_id is None else b'"submission_id":' + json.dumps(submission_id).encode()
    )
    found = []
    for hour in sorted(p for p in root.glob("*") if p.is_dir() and p.name >= floor):
        for file in sorted(hour.glob("*.jsonl")):
            try:
                f = file.open("rb")
            except FileNotFoundError:  # retention removed it mid-scan
                continue
            with f:
                for line in f:
                    if not line.endswith(b"\n") or (needle is not None and needle not in line):
                        continue
                    try:
                        event = json.loads(line)
                    except ValueError:  # corrupted on disk; one bad line must not fail every read
                        continue
                    if submission_id is None or event.get("submission_id") == submission_id:
                        found.append(event)
    return sorted(found, key=lambda e: e["ts"])
