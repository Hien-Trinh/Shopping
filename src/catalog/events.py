"""Events: one JSON line per stage per Change, one file per process per hour (A11).

Writers never share a file, so any number of processes can log without locks.
"""

import json
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path

_HOUR = "%Y-%m-%dT%H"


class EventLog:
    def __init__(self, root: Path, process: str, clock: Callable[[], float] = time.time):
        self.root, self.process, self.clock = root, process, clock

    def emit(self, events: Iterable[Mapping]) -> None:
        """Append events with a shared timestamp (ms), in one write."""
        now = self.clock()
        stamp = {"ts": int(now * 1000)}
        lines = "".join(json.dumps(stamp | dict(e), separators=(",", ":")) + "\n" for e in events)
        if not lines:
            return
        hour = self.root / datetime.fromtimestamp(now, UTC).strftime(_HOUR)
        hour.mkdir(parents=True, exist_ok=True)
        with open(hour / f"{self.process}.jsonl", "ab+") as f:
            # Only this process writes this file, so a torn last line is our own crash: end it,
            # or the next event would be glued onto it and lost too.
            if f.seek(0, 2) and (f.seek(-1, 2), f.read(1))[1] != b"\n":
                lines = "\n" + lines
            f.write(lines.encode())


def read(root: Path, since: datetime | None = None, submission_id: str | None = None) -> list[dict]:
    """Events from `since`'s hour onward, oldest first.

    Skips a line still being written and a line torn by a crash (DuckDB's read_json with
    ignore_errors would instead return a partial event, which could look real).
    """
    floor = since.astimezone(UTC).strftime(_HOUR) if since else ""
    found = []
    for hour in sorted(p for p in root.glob("*") if p.is_dir() and p.name >= floor):
        for file in sorted(hour.glob("*.jsonl")):
            for line in file.read_bytes().split(b"\n")[:-1]:  # the last piece is unterminated
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if submission_id is None or event.get("submission_id") == submission_id:
                    found.append(event)
    return sorted(found, key=lambda e: e["ts"])
