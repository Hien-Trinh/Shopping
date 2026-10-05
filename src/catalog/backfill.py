"""Backfill: op=reclassify Changes for Listings flagged `needs_reclassify` or classified under
another taxonomy version (docs/specs/step-6c.md).

It reads the Listing Store and appends to the Landing log, so each partition's worker stays the
only writer of its Listings (ADR-0001). One round is in flight at a time: it appends again only
once every worker offset has passed its last commit, so no row is appended twice while unread and
no more than `limit` reclassifies ever sit ahead of Merchant Changes.
"""

import argparse
import contextlib
import os
import shutil
import threading
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.compute as pc
from deltalake import DeltaTable

from catalog import api, classify, delta, entry, landing, state, store, worker
from catalog.envelope import Change, Key
from catalog.events import EventLog, stopping
from catalog.keys import PARTITIONS

INTERVAL = 10.0  # seconds between ticks
LIMIT = 1000  # reclassifies per round: about one worker batch, spread over the workers
_COLUMNS = [
    "merchant_id",
    "merchant_product_id",
    "source_version",
    "is_tombstone",
    "needs_reclassify",
    "taxonomy_version",
    "updated_at",
]


@dataclass(frozen=True)
class Pending:
    rows: list[tuple[Key, int]]  # (key, stored source_version), oldest updated_at first
    flagged: int  # live rows with needs_reclassify, in all
    outdated: int  # live unflagged rows on another taxonomy version, in all


def pending(dt: DeltaTable, version: str, limit: int) -> Pending:
    """Up to `limit` live Listings to reclassify against `version`, and how many there are.

    Oldest first: a row the classifier fails again gets a new updated_at and goes to the back.
    """
    dt.update_incremental()
    # ponytail: scans every live row each round; Phase 7 measures it at 1M Listings.
    # Filtered in Arrow, not at scan time: delta-rs reports string_view columns (see store.read).
    rows = delta.plain(dt.to_pyarrow_dataset().to_table(columns=_COLUMNS))
    live = rows.filter(pc.invert(rows["is_tombstone"]))
    flagged = live["needs_reclassify"]
    outdated = pc.and_(
        pc.invert(flagged), pc.not_equal(live["taxonomy_version"], version).fill_null(True)
    )
    todo = live.filter(pc.or_(flagged, outdated)).sort_by(
        [("updated_at", "ascending"), ("merchant_id", "ascending"),
         ("merchant_product_id", "ascending")]
    )  # fmt: skip
    picked = [
        ((r["merchant_id"], r["merchant_product_id"]), r["source_version"])
        for r in todo.slice(0, limit).to_pylist()
    ]
    return Pending(picked, pc.sum(flagged).as_py() or 0, pc.sum(outdated).as_py() or 0)


def tick(
    landing_dt: DeltaTable,
    store_dt: DeltaTable,
    state_dir: Path,
    events: EventLog,
    version: str,
    now: datetime,
    *,
    limit: int = LIMIT,
    min_free: int = api.MIN_FREE,
) -> int | None:
    """Append one round of reclassifies if the last one was read; the commit's version, or None."""
    started = time.monotonic()
    landing_dt.update_incremental()
    table = landing.table_id(landing_dt)
    last = state.load_backfill(state_dir, table)
    offsets = state.load_offsets(state_dir, range(PARTITIONS), table)
    if min(v for v, _ in offsets.values()) <= last:  # a worker hasn't read the last round yet
        return None
    if shutil.disk_usage(delta.local(landing_dt.table_uri)).free < min_free:  # as the API refuses
        with contextlib.suppress(OSError):  # best effort: the disk is nearly full
            events.emit([{"type": "backfill_skipped", "reason": "low_disk"}])
        return None
    found = pending(store_dt, version, limit)
    if not found.rows:
        return None
    submission = f"backfill-{uuid.uuid7()}"  # not a UUIDv7, so GET /submissions 404s it
    entries = [
        (submission, i, Change(*key, source_version, "reclassify"), now)
        for i, (key, source_version) in enumerate(found.rows)
    ]
    # ponytail: a crash between the append and the save appends the same rows once more next
    # round, which only costs classifying them again.
    appended = landing.append(landing_dt, entries)
    state.save_backfill(state_dir, appended, table)
    event = {
        "type": "backfill",
        "submission_id": submission,
        "appended": len(entries),
        "flagged": found.flagged,
        "outdated": found.outdated,
        "version": version,
        "landing_version": appended,
        "ms": round((time.monotonic() - started) * 1000),
    }
    events.emit([event])
    return appended


def run(
    data: Path,
    state_dir: Path,
    version: str,
    *,
    stop: threading.Event,
    interval: float = INTERVAL,
    limit: int = LIMIT,
) -> None:
    """A tick every `interval` seconds until `stop` is set. Any error propagates: the supervisor
    restarts it, and a restart is safe (at worst one round appended twice)."""
    with state.claim_backfill(state_dir):  # first: a second one dies before appending anything
        landing_dt = landing.ensure(str(data / "landing_log"))
        store_dt = store.ensure(str(data / "listing_store"))
        events = EventLog(data / "events", "backfill")
        events.emit([{"type": "backfill_start", "pid": os.getpid(), "version": version}])
        with stopping(events, stop):
            while not stop.is_set():
                tick(
                    landing_dt, store_dt, state_dir, events, version, datetime.now(UTC), limit=limit
                )
                stop.wait(interval)


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.backfill")
    # The workers' flag: their version is the one rows should be on (step-6c.md, decision 5).
    args.add_argument("--classifier", choices=classify.KINDS, required=True)
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--state", type=Path, default=state.STATE)
    args.add_argument("--interval", type=float, default=INTERVAL)
    args.add_argument("--limit", type=int, default=LIMIT)
    a = args.parse_args(argv)
    if a.limit < 1:
        args.error("--limit must be at least 1")
    if not a.interval > 0:  # 0 or nan would rescan the Listing Store in a tight loop
        args.error("--interval must be more than 0")
    try:
        supervisor = worker.supervisor_pid()
    except ValueError as e:
        args.error(str(e))
    stop = worker.stop_on_signals()
    # so a kill -9ed supervisor leaves none behind
    worker.watch_supervisor(supervisor, stop, a.data / "events", "backfill")
    version = classify.taxonomy_version(a.classifier)
    run(a.data, a.state, version, stop=stop, interval=a.interval, limit=a.limit)


if __name__ == "__main__":
    entry.exit_with(main)  # any error is 1: the supervisor restarts it
