"""Change Export: the Listing Store's change feed since the watermark, as one Parquet file per
version range (design doc, lifecycle step 9; docs/specs/step-5a.md).

A file is named by its version range, zero-padded so name order is version order, and appears
complete or not at all. The watermark moves only after its file is written.
"""

import argparse
import os
import threading
import time
import uuid
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable

from catalog import delta, entry, state, store, worker
from catalog.collapse import collapse
from catalog.events import EventLog

INTERVAL = 60.0  # seconds between ticks once caught up (design doc: every minute)
# ponytail: caps versions, not rows, and collapse runs in Python: a 1M initial load still puts
# ~1M rows in memory per file. Measure in Phase 7; cap by rows or collapse in Arrow if it matters.
MAX_VERSIONS = 1000
SCHEMA = store.SCHEMA.append(pa.field("op", pa.string(), nullable=False))


def run(
    data: Path,
    state_dir: Path,
    *,
    stop: threading.Event,
    interval: float = INTERVAL,
    max_versions: int = MAX_VERSIONS,
) -> None:
    """Export every `interval` seconds, or at once while behind, until `stop` is set.

    Any error propagates: the watermark never moved past a file not written, so the supervisor's
    restart is always safe (spec decision 5).
    """
    with state.claim_export(state_dir):  # first: a second exporter dies before reading anything
        dt = store.ensure(str(data / "listing_store"))
        events = EventLog(data / "events", "export")
        events.emit([{"type": "export_start", "pid": os.getpid()}])
        while not stop.is_set():
            if not tick(dt, data / "export", state_dir, events, max_versions=max_versions):
                stop.wait(interval)


def files(export_dir: Path) -> list[Path]:
    """The export files in version order; a consumer replays them in this order."""
    return sorted(export_dir.glob("*.parquet"))


def tick(
    dt: DeltaTable, export_dir: Path, state_dir: Path, events: EventLog, *, max_versions: int
) -> bool:
    """Export the next range of at most `max_versions` versions; True if more are waiting."""
    started = time.monotonic()
    table = dt.metadata().id
    first = state.load_watermark(state_dir, table) + 1
    # A crash between a file and its watermark: adopt the file. Exporting again from the old
    # watermark would write a second file over the same versions, ending at a newer head.
    while written := sorted(export_dir.glob(f"{first:012}-*.parquet")):
        first = int(written[-1].stem.split("-")[1]) + 1
        state.save_watermark(state_dir, first - 1, table)
    dt.update_incremental()
    head = dt.version()
    last = min(head, first + max_versions - 1)
    if first > last:
        return False
    feed = delta.plain(
        pa.table(dt.load_cdf(starting_version=first, ending_version=last).read_all())
    )
    rows = collapse(feed.to_pylist())
    if rows:  # compaction commits carry no feed rows: pass them without an empty file
        _write(export_dir / f"{first:012}-{last:012}.parquet", rows)
        events.emit(
            [
                {
                    "type": "export",
                    "from": first,
                    "to": last,
                    "rows": len(rows),
                    "deletes": sum(r["op"] == "delete" for r in rows),
                    "ms": round((time.monotonic() - started) * 1000),
                    "head": head,
                }
            ]
        )
    state.save_watermark(state_dir, last, table)
    return last < head


def _write(path: Path, rows: list[dict]) -> None:
    """Atomic, as state.save: a crash leaves only a hidden temp file, which files() skips."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), tmp)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.export")
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--state", type=Path, default=state.STATE)
    args.add_argument("--interval", type=float, default=INTERVAL)
    a = args.parse_args(argv)
    try:
        supervisor = worker.supervisor_pid()
    except ValueError as e:
        args.error(str(e))
    stop = worker.stop_on_signals()
    worker.watch_supervisor(supervisor, stop)  # so a kill -9ed supervisor leaves no exporter
    run(a.data, a.state, stop=stop, interval=a.interval)


if __name__ == "__main__":
    entry.exit_with(main)  # any error is 1: the supervisor restarts it
