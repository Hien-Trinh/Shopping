"""Ingestion worker: batches from the Landing log to the Listing Store (design doc, lifecycle).

A batch: read -> plan -> classify -> one conditional MERGE -> events -> offsets. Offsets are saved
last, so a crash anywhere earlier replays the batch, which plan's rules make safe (at least once).
`run` loops batches over the partitions one worker owns; `python -m catalog.worker` starts it.
"""

import argparse
import contextlib
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

from catalog import entry, landing, state, store
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
    outcomes, notes = _merge(store_dt, good, classifier, now)
    merged = iter(outcomes)
    results = [{"type": FAILED, "error": error} if error else next(merged) for error in errors]
    # Emitted only once the MERGE committed, so a retried batch doesn't repeat them.
    events.emit(
        [
            *notes,
            *(
                {
                    "submission_id": landed.submission_id,
                    "change_index": landed.change_index,
                    "merchant_id": landed.change.merchant_id,
                    "merchant_product_id": landed.change.merchant_product_id,
                    "partition": landed.partition,
                }
                | result
                for landed, result in zip(batch.changes, results, strict=True)
            ),
        ]
    )
    moved = {p: pos for p, pos in batch.offsets.items() if pos != offsets[p]}
    state.save_offsets(state_dir, moved, landing.table_id(landing_dt))
    return batch


POLL = 0.2  # seconds between reads when caught up: the design doc's "1,000 changes or 200 ms"
ATTEMPTS = 5  # failed ticks in a row before crashing; backoff 1+2+4+8 s stays under 3d's 60 s
COMPACT_EVERY = 100  # batches with changes between compactions (ponytail: untuned until Phase 7)
# Exit codes: 0 stopped, 1 gave up on failed ticks (a restart may fix it), over 1 fatal, so the
# supervisor stops instead: 2 is a bad flag, and these no restart fixes.
FATAL = {state.PartitionTaken: 3, state.OffsetsMismatch: 4, state.CorruptState: 5}
DATA, STATE = Path("data"), Path("state")  # the default directories, shared with the supervisor
SUPERVISOR = "CATALOG_SUPERVISOR"  # set to the supervisor's pid in its children's environment


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
    clock: Callable[[], float] = state.CLOCK,  # the supervisor compares beats with the same one
) -> None:
    """Process the partitions worker `index` of `workers` owns until `stop` is set.

    Any error is retried with backoff and never advances an offset (lifecycle step 8); after
    ATTEMPTS failed ticks in a row it propagates, so the worker crashes and 3d restarts it.
    """
    name, mine = f"{state.WORKER}{index}", owned(index, workers)
    with state.claim(state_dir, mine):  # first: a duplicate worker dies before writing anything
        landing_dt = landing.ensure(str(data / "landing_log"))
        store_dt = store.ensure(str(data / "listing_store"))
        events = EventLog(data / "events", name)
        offsets = state.load_offsets(state_dir, mine, landing.table_id(landing_dt))
        state.beat(state_dir, name, clock())  # before the first batch
        events.emit(
            [{"type": "worker_start", "worker": name, "workers": workers, "pid": os.getpid()}]
        )
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
                if moved:  # for lag and utilization; a tick that moves no offset logs nothing
                    tick = {
                        "type": "batch",
                        "worker": name,
                        "changes": len(batch.changes),
                        "ms": round((clock() - started) * 1000),
                        "head": landing_dt.version(),  # lag of p = head + 1 - next[p][0]
                        "next": moved,
                    }
                    events.emit([tick])
                # Only this thread beats, after the batch: a hung MERGE stops the beats (B1).
                # Before compacting, so a long compaction gets the watchdog's full budget.
                state.beat(state_dir, name, clock())
                if busy >= compact_every:  # owner-only (ADR-0001); a failure retries next tick
                    store.compact(store_dt, mine)
                    busy = 0
            except Exception as e:
                failures += 1
                error = repr(e)[:500]
                logged = [
                    {"type": "tick_failed", "worker": name, "attempt": failures, "error": error}
                ]
                if gave_up := failures >= ATTEMPTS:
                    logged.append({"type": "worker_stop", "worker": name, "error": error})
                with contextlib.suppress(OSError):  # best effort: the error itself matters more
                    events.emit(logged)
                if gave_up:
                    raise
                print(f"{name}: tick failed ({failures}/{ATTEMPTS}):", file=sys.stderr)
                traceback.print_exc()
                stop.wait(2 ** (failures - 1))
                continue
            failures = 0
            if min(v for v, _ in offsets.values()) > landing_dt.version():  # caught up
                stop.wait(POLL)
        events.emit([{"type": "worker_stop", "worker": name}])


def watch(
    alive: Callable[[], bool],
    stop: threading.Event,
    deadline: float = 30.0,
    *,
    sleep: Callable[[float], None] = time.sleep,
    exit: Callable[[int], None] = os._exit,
) -> None:
    """Check `alive()` every second; once it's False, set `stop`, so the tick in progress finishes
    and the claims are released, and exit 1 if the process still runs `deadline` seconds later.

    Run it in a daemon thread: a worker that stops in time has exited by then. One that hasn't is
    stuck in a native call, and would otherwise hold its partition locks forever.
    """
    while alive():
        sleep(1)
    stop.set()
    sleep(deadline)
    exit(1)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)  # a killed supervisor exists until reaped, which a shell does at once
    except ProcessLookupError:
        return False
    return True


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.worker")
    args.add_argument("--index", type=int, required=True)
    args.add_argument("--workers", type=int, required=True)
    args.add_argument("--data", type=Path, default=DATA)
    args.add_argument("--state", type=Path, default=STATE)
    a = args.parse_args(argv)
    try:
        owned(a.index, a.workers)
    except ValueError as e:
        args.error(str(e))  # exit 2: fatal, so the supervisor doesn't restart a bad flag forever
    stop, stopping = threading.Event(), []

    def on_signal(*_):  # Event.set takes a lock the interrupted thread may hold: set it elsewhere
        if not stopping:  # once: a second signal mid-Thread.start would re-enter its lock
            stopping.append(True)
            threading.Thread(target=stop.set).start()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    if pid := os.environ.get(SUPERVISOR):  # so a kill -9ed supervisor leaves no worker behind
        supervisor = int(pid)
        threading.Thread(target=watch, args=(lambda: _alive(supervisor), stop), daemon=True).start()
    # ponytail: FakeClassifier until the real one (step 6b) is chosen by a flag.
    run(a.data, a.state, a.index, a.workers, FakeClassifier(), stop=stop)


_WRITES = {Outcome.WRITTEN, Outcome.RECLASSIFIED}


class Overtaken(RuntimeError):
    """The MERGE applied fewer rows than planned: another writer got there first (ADR-0001)."""


def _merge(store_dt, landed: Sequence[Landed], classifier, now) -> tuple[list[dict], list[dict]]:
    """Event fields per Change, and batch-level events (classify_failed) to emit with them."""
    changes = [x.change for x in landed]
    stored = store.read(store_dt, {c.key for c in changes})
    planned = plan(changes, stored, classifier.taxonomy_version)
    writes, notes = _classify(planned.writes, classifier)
    merged = store.merge(store_dt, writes, now())
    if merged.applied != len(writes):
        parts = sorted({partition(*w.key) for w in writes})
        raise Overtaken(
            f"MERGE applied {merged.applied} of {len(writes)} rows (partitions {parts})"
        )
    outcomes = [
        {"type": o} | ({"store_version": merged.version} if o in _WRITES else {})
        for o in planned.outcomes
    ]
    return outcomes, notes


def _classify(writes: Sequence[Write], classifier) -> tuple[list[Write], list[dict]]:
    """Classify the Writes that need it in one call; on any failure they become Uncategorized.

    Returns the Writes and a classify_failed event if the call failed.
    """
    todo = [w for w in writes if w.needs_classify]
    if not todo:
        return list(writes), []
    version = classifier.taxonomy_version
    notes = []
    try:
        results = classifier.classify([w.listing for w in todo])
        found = [_answer(a, version) for _, a in zip(todo, results, strict=True)]
    except Exception as e:  # an outage, a timeout or a bad answer: never stall the partition
        notes = [
            {
                "type": "classify_failed",
                "listings": len(todo),
                "partitions": sorted({partition(*w.key) for w in todo}),
                "error": repr(e)[:500],  # a provider error may echo listing text
            }
        ]
        # Keep an answer whose inputs haven't changed, else Uncategorized; either way flagged so
        # the Backfill reclassifies it (design doc, lifecycle step 5).
        found = [
            replace(w.fallback, needs_reclassify=True)
            if w.fallback
            else Classification(UNCATEGORIZED, 0.0, version, needs_reclassify=True)
            for w in todo
        ]
    answers = iter(found)
    classified = [
        replace(w, classification=next(answers), needs_classify=False) if w.needs_classify else w
        for w in writes
    ]
    return classified, notes


def _answer(answer, version: str) -> Classification:
    category, confidence = answer
    confidence = float(confidence)
    if not (isinstance(category, str) and category and 0.0 <= confidence <= 1.0):  # NaN fails too
        raise ValueError(f"bad classifier answer: {answer!r}")
    return Classification(category, confidence, version)


if __name__ == "__main__":
    entry.exit_with(main, FATAL)  # a fatal startup error's code, or 1 after giving up
