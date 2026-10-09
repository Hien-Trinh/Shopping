"""Change Export: the Listing Store's change feed since the watermark, as one Parquet file per
version range (design doc, lifecycle step 9; docs/specs/step-5a.md).

A file is named by its version range, zero-padded so name order is version order, and appears
complete or not at all. The watermark moves only after its file is written.
If cleanup removed history the change feed needs, the next file is the whole store instead
(A18, docs/specs/step-5b.md).
"""

import argparse
import os
import re
import threading
import time
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from deltalake import DeltaTable

from catalog import delta, entry, state, store
from catalog.collapse import collapse
from catalog.events import EventLog, stopping

INTERVAL = 60.0  # seconds between ticks once caught up (design doc: every minute)
# ponytail: caps versions, not rows, and collapse runs in Python: a 1M initial load still puts
# ~1M rows in memory per file. Measure in Phase 7; cap by rows or collapse in Arrow if it matters.
MAX_VERSIONS = 1000
RETENTION = timedelta(days=3)  # plan-v1 A13
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
        with stopping(events, stop):
            while not stop.is_set():
                if not tick(dt, data / "export", state_dir, events, max_versions=max_versions):
                    stop.wait(interval)


def files(export_dir: Path) -> list[Path]:
    """The export files in version order; a consumer replays them in this order."""
    return sorted(export_dir.glob("*.parquet"))


def prune(export_dir: Path, now: datetime, watermark: int) -> int:
    """Delete the files older than RETENTION that end at or below the watermark; returns how
    many. One past it is a crash's file that the exporter adopts on its next tick."""
    cutoff = (now - RETENTION).timestamp()
    old = []
    for p in files(export_dir):
        if not (name := re.fullmatch(r"\d+-(\d+)", p.stem)):
            continue  # not the exporter's: one stray file must not stop every pass
        if int(name[1]) <= watermark and p.stat().st_mtime < cutoff:
            old.append(p)
    for p in old:
        p.unlink()
    return len(old)


def tick(
    dt: DeltaTable, export_dir: Path, state_dir: Path, events: EventLog, *, max_versions: int
) -> bool:
    """Export the next range of at most `max_versions` versions; True if more are waiting."""
    started = time.monotonic()
    table = dt.metadata().id
    first = state.load_watermark(state_dir, table) + 1
    # A crash between a file and its watermark: adopt the file. Exporting again from the old
    # watermark would write a second file over the same versions, ending at a newer head.
    # Its event may be lost too, so it is logged again: a repeated event is harmless.
    while written := sorted(export_dir.glob(f"{first:012}-*.parquet")):
        end = int(written[-1].stem.split("-")[1])
        ops = pq.read_table(written[-1], columns=["op"])["op"]
        events.emit([_event("export", first, end, ops, started, None) | {"adopted": True}])
        state.save_watermark(state_dir, end, table)
        first = end + 1
    dt.update_incremental()
    head = dt.version()
    last = min(head, first + max_versions - 1)
    if first > last:
        return False
    try:
        feed = delta.plain(
            pa.table(dt.load_cdf(starting_version=first, ending_version=last).read_all())
        )
    except Exception as e:
        if not delta.history_gone(e):
            raise
        # A gap (A18): export the whole store at head instead. Tombstones are kept forever (A17),
        # so upserting the live rows and deleting the Tombstones leaves a consumer exactly right.
        kind, last, out = "gap_recovered", head, _whole(dt)
    else:
        kind, out = "export", pa.Table.from_pylist(collapse(feed.to_pylist()), schema=SCHEMA)
    if out.num_rows:  # compaction commits carry no feed rows: pass them without an empty file
        # a kill -9 leaves only a hidden temp file, which files() skips
        with state.atomic(export_dir / f"{first:012}-{last:012}.parquet") as f:
            pq.write_table(out, f)
        events.emit([_event(kind, first, last, out["op"], started, head)])
    state.save_watermark(state_dir, last, table)
    return last < head


def _whole(dt: DeltaTable) -> pa.Table:
    """Every row of the Listing Store at the loaded version, as export rows, never in Python."""
    rows = delta.plain(dt.to_pyarrow_dataset().to_table())
    op = pc.if_else(rows["is_tombstone"], "delete", "upsert")
    return rows.append_column("op", op).select(SCHEMA.names).cast(SCHEMA)


def _event(kind: str, first: int, last: int, ops, started: float, head: int | None) -> dict:
    return {
        "type": kind,
        "from": first,
        "to": last,
        "rows": len(ops),
        "deletes": pc.sum(pc.equal(ops, "delete")).as_py() or 0,
        "ms": round((time.monotonic() - started) * 1000),
        "head": head,  # None for an adopted file: the head it was cut at is gone
    }


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.export")
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--state", type=Path, default=state.STATE)
    args.add_argument("--interval", type=float, default=INTERVAL)
    a = args.parse_args(argv)
    supervisor = entry.supervisor_pid(args)
    stop = entry.watch_supervisor(supervisor, a.data / "events", "export")
    run(a.data, a.state, stop=stop, interval=a.interval)


if __name__ == "__main__":
    entry.exit_with(main)  # any error is 1: the supervisor restarts it
