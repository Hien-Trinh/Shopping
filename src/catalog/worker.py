"""Ingestion worker: batches from the Landing log to the Listing Store (design doc, lifecycle).

A batch: read -> plan -> classify -> one conditional MERGE -> events -> offsets. Offsets are saved
last, so a crash anywhere earlier replays the batch, which plan's rules make safe (at least once).
`run` loops batches over the partitions one worker owns; `python -m catalog.worker` starts it.
"""

import argparse
import os
import signal
import sys
import threading
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from deltalake import DeltaTable

from catalog import landing, state, store
from catalog.classify import UNCATEGORIZED, FakeClassifier
from catalog.events import EventLog
from catalog.keys import owned, partition
from catalog.landing import Batch, Landed, Position
from catalog.plan import Classification, Outcome, Write, plan
from catalog.status import FAILED


def process_batch(
    landing_dt: DeltaTable,
    store_dt: DeltaTable,
    classifier,
    events: EventLog,
    state_dir: Path,
    offsets: Mapping[int, Position],
    *,
    limit: int = 1000,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Batch:
    """Process up to `limit` Changes past `offsets`; returns the batch, with the new offsets.

    The caller must hold `state.claim()` on every partition in `offsets` (ADR-0001): two workers
    on one partition would redo each other's work and could pick the wrong conflict winner.
    """
    batch = landing.read(landing_dt, offsets, limit)
    # A Change whose data can't be stored fails alone, before planning, so the rest plan without it
    # (design doc, lifecycle step 8). Any other error propagates: storage or a bug, never the data.
    errors = store.unstorable([x.change.listing for x in batch.changes])
    good = [x for x, error in zip(batch.changes, errors, strict=True) if error is None]
    merged = iter(_merge(store_dt, good, classifier, events, now))
    results = [{"type": FAILED, "error": error} if error else next(merged) for error in errors]
    events.emit(
        {
            "submission_id": landed.submission_id,
            "change_index": landed.change_index,
            "merchant_id": landed.change.merchant_id,
            "merchant_product_id": landed.change.merchant_product_id,
            "partition": landed.partition,
        }
        | result
        for landed, result in zip(batch.changes, results, strict=True)
    )
    moved = {p: pos for p, pos in batch.offsets.items() if pos != offsets[p]}
    state.save_offsets(state_dir, moved, landing.table_id(landing_dt))
    return batch


POLL = 0.2  # seconds between reads when caught up: the design doc's "1,000 changes or 200 ms"
ATTEMPTS = 5  # failed ticks in a row before crashing; backoff 1+2+4+8 s stays under 3d's 60 s
COMPACT_EVERY = 100  # batches with changes between compactions (ponytail: untuned until Phase 7)


def run(
    data: Path,
    state_dir: Path,
    index: int,
    workers: int,
    classifier,
    *,
    stop: threading.Event,
    limit: int = 1000,
    compact_every: int = COMPACT_EVERY,
    clock: Callable[[], float] = time.time,
) -> None:
    """Process the partitions worker `index` of `workers` owns until `stop` is set.

    Any error is retried with backoff and never advances an offset (lifecycle step 8); after
    ATTEMPTS failed ticks in a row it propagates, so the worker crashes and 3d restarts it.
    """
    name, mine = f"worker-{index}", owned(index, workers)
    with state.claim(state_dir, mine):  # first: a duplicate worker dies before writing anything
        landing_dt = landing.ensure(str(data / "landing_log"))
        store_dt = store.ensure(str(data / "listing_store"))
        events = EventLog(data / "events", name)
        offsets = state.load_offsets(state_dir, mine, landing.table_id(landing_dt))
        state.beat(state_dir, name, clock())  # before the first batch
        failures = busy = 0
        while not stop.is_set():
            started = clock()
            try:
                batch = process_batch(
                    landing_dt, store_dt, classifier, events, state_dir, offsets, limit=limit
                )
                moved = {p: pos for p, pos in batch.offsets.items() if pos != offsets[p]}
                offsets = batch.offsets  # saved: never re-read, even if what follows fails
                busy += bool(batch.changes)
                if busy >= compact_every:  # owner-only (ADR-0001); a failure retries next tick
                    store.compact(store_dt, mine)
                    busy = 0
                if moved:  # for lag per partition and utilization; quiet ticks log nothing
                    tick = {
                        "type": "batch",
                        "worker": name,
                        "changes": len(batch.changes),
                        "ms": round((clock() - started) * 1000),
                        "head": landing_dt.version(),  # lag of p = head + 1 - next[p][0]
                        "next": moved,
                    }
                    events.emit([tick])
                # Only this thread beats, after a whole tick: a hung MERGE stops the beats (B1).
                state.beat(state_dir, name, clock())
            except Exception as e:
                failures += 1
                if failures >= ATTEMPTS:
                    raise
                print(f"{name}: tick failed ({failures}/{ATTEMPTS}): {e!r}", file=sys.stderr)
                stop.wait(2 ** (failures - 1))
                continue
            failures = 0
            if len(batch.changes) < limit:  # a full batch means a backlog: read again at once
                stop.wait(POLL)


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.worker")
    args.add_argument("--index", type=int, required=True)
    args.add_argument("--workers", type=int, required=True)
    args.add_argument("--data", type=Path, default=Path("data"))
    args.add_argument("--state", type=Path, default=Path("state"))
    a = args.parse_args(argv)
    stop = threading.Event()

    def on_signal(*_):  # Event.set takes a lock the interrupted thread may hold: set it elsewhere
        threading.Thread(target=stop.set).start()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    # ponytail: FakeClassifier until the real one (step 6b) is chosen by a flag.
    run(a.data, a.state, a.index, a.workers, FakeClassifier(), stop=stop)


_WRITES = {Outcome.WRITTEN, Outcome.RECLASSIFIED}


class Overtaken(RuntimeError):
    """The MERGE applied fewer rows than planned: another writer got there first (ADR-0001)."""


def _merge(store_dt, landed: Sequence[Landed], classifier, events, now) -> list[dict]:
    changes = [x.change for x in landed]
    stored = store.read(store_dt, {c.key for c in changes})
    planned = plan(changes, stored, classifier.taxonomy_version)
    writes = _classify(planned.writes, classifier, events)
    merged = store.merge(store_dt, writes, now())
    if merged.applied != len(writes):
        parts = sorted({partition(*w.key) for w in writes})
        raise Overtaken(
            f"MERGE applied {merged.applied} of {len(writes)} rows (partitions {parts})"
        )
    return [
        {"type": o} | ({"store_version": merged.version} if o in _WRITES else {})
        for o in planned.outcomes
    ]


def _classify(writes: Sequence[Write], classifier, events: EventLog) -> list[Write]:
    """Classify the Writes that need it in one call; on any failure they become Uncategorized."""
    todo = [w for w in writes if w.needs_classify]
    if not todo:
        return list(writes)
    version = classifier.taxonomy_version
    try:
        results = classifier.classify([w.listing for w in todo])
        found = [_answer(a, version) for _, a in zip(todo, results, strict=True)]
    except Exception as e:  # an outage, a timeout or a bad answer: never stall the partition
        failed = {
            "type": "classify_failed",
            "listings": len(todo),
            "partitions": sorted({partition(*w.key) for w in todo}),
            "error": repr(e)[:500],  # a provider error may echo listing text
        }
        events.emit([failed])
        # Keep an answer whose inputs haven't changed, else Uncategorized; either way flagged so
        # the Backfill reclassifies it (design doc, lifecycle step 5).
        found = [
            replace(w.fallback, needs_reclassify=True)
            if w.fallback
            else Classification(UNCATEGORIZED, 0.0, version, needs_reclassify=True)
            for w in todo
        ]
    answers = iter(found)
    return [
        replace(w, classification=next(answers), needs_classify=False) if w.needs_classify else w
        for w in writes
    ]


def _answer(answer, version: str) -> Classification:
    category, confidence = answer
    confidence = float(confidence)
    if not (isinstance(category, str) and category and 0.0 <= confidence <= 1.0):  # NaN fails too
        raise ValueError(f"bad classifier answer: {answer!r}")
    return Classification(category, confidence, version)


if __name__ == "__main__":
    try:
        main()
        code = 0
    except SystemExit as e:  # argparse: --help or a bad flag
        code = e.code or 0
    except BaseException:  # gave up after ATTEMPTS failed ticks: exit 1 for the supervisor
        traceback.print_exc()
        code = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)  # not sys.exit: Arrow can hang at process exit (spikes/NOTES.md)
