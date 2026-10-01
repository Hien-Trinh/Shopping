"""Events: one JSON line per stage per Change, one file per process run per hour (A11).

Writers never share a file, so any number of processes log without locks, and a restarted
process never appends after the torn last line its predecessor may have left.
"""

import json
import os
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path

_HOUR = "%Y-%m-%dT%H"


class EventLog:
    def __init__(self, root: Path, process: str, clock: Callable[[], float] = time.time):
        self.root, self.name, self.clock = root, f"{process}-{os.getpid()}", clock

    def emit(self, events: Iterable[Mapping]) -> None:
        """Append events with a shared timestamp (ms) that the events can't override."""
        now = self.clock()
        stamp = {"ts": int(now * 1000)}
        lines = "".join(json.dumps(dict(e) | stamp, separators=(",", ":")) + "\n" for e in events)
        if not lines:
            return
        hour = self.root / datetime.fromtimestamp(now, UTC).strftime(_HOUR)
        hour.mkdir(parents=True, exist_ok=True)
        with open(hour / f"{self.name}.jsonl", "a") as f:
            f.write(lines)


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
