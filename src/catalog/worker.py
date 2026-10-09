"""Ingestion worker: batches from the Landing log to the Listing Store (design doc, lifecycle).

A batch: read -> decide (plan, classify) -> one conditional MERGE -> events -> offsets. Offsets
are saved last, so a crash anywhere earlier replays the batch, which plan's rules make safe (at
least once).
`run` loops batches over the partitions one worker owns, and bootstraps them when cleanup removed
history they still needed (step-5e.md); `python -m catalog.worker` starts it.
"""

import argparse
import contextlib
import os
import sys
import threading
import traceback
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from deltalake import DeltaTable

from catalog import classify, delta, entry, jev, landing, state, store, taxonomy
from catalog.decide import decide
from catalog.events import EventLog
from catalog.keys import owned, partition
from catalog.landing import Batch, Landed, Position


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
    _apply(store_dt, classifier, events, batch.changes, now)
    moved = {p: pos for p, pos in batch.offsets.items() if pos != offsets[p]}
    state.save_offsets(state_dir, moved, landing.table_id(landing_dt))
    return batch


def _apply(store_dt, classifier, events: EventLog, landed: Sequence[Landed], now) -> None:
    """Read, decide, one conditional MERGE, then the batch's events, for Changes in replay order."""
    stored = store.read(store_dt, {x.change.key for x in landed})
    decision = decide(landed, stored, classifier)  # storage errors and bugs propagate from here
    merged = store.merge(store_dt, decision.writes, now())
    if merged.applied != len(decision.writes):
        parts = sorted({partition(*w.key) for w in decision.writes})
        raise Overtaken(
            f"MERGE applied {merged.applied} of {len(decision.writes)} rows (partitions {parts})"
        )
    # Emitted only once the MERGE committed, so a retried batch doesn't repeat them.
    events.emit(decision.events(merged.version))


def bootstrap(
    landing_dt: DeltaTable,
    store_dt: DeltaTable,
    classifier,
    events: EventLog,
    state_dir: Path,
    offsets: dict[int, Position],
    *,
    name: str,
    stop: threading.Event,
    beat: Callable[[], None],
    clock: Callable[[], float],
    limit: int = 1000,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> None:
    """Re-apply what the Landing log kept for the partitions at the lowest offset version, after
    cleanup removed history they still needed (step-5e.md).

    Every retained row is applied again: plan's rules make the ones applied before harmless (A3).
    Each partition's offset is saved, and updated in `offsets`, as it finishes, so a bootstrap
    that fails or is killed resumes at the partition it was on.
    """
    landing_dt.update_incremental()
    pinned, table = landing_dt.version(), landing.table_id(landing_dt)
    low = min(v for v, _ in offsets.values())
    for p in sorted(p for p, (v, _) in offsets.items() if v == low):
        started, rows = clock(), landing.retained(landing_dt, p)  # the handle stays at the pin
        for i in range(0, len(rows), limit):
            if stop.is_set():  # the unfinished partition is redone at the next start
                return
            _apply(store_dt, classifier, events, rows[i : i + limit], now)
            beat()  # a long bootstrap must not look hung (B1)
        state.save_offsets(state_dir, {p: (pinned + 1, 0)}, table)
        was, offsets[p] = offsets[p], (pinned + 1, 0)  # before the event: if it fails, p is done
        events.emit(
            [
                {
                    "type": "bootstrap",
                    "worker": name,
                    "partition": p,
                    "from": was,
                    "pinned": pinned,
                    "changes": len(rows),
                    "ms": round((clock() - started) * 1000),
                }
            ]
        )


POLL = 0.2  # seconds between reads when caught up: the design doc's "1,000 changes or 200 ms"
ATTEMPTS = 5  # failed ticks in a row before crashing; backoff 1+2+4+8 s stays under 3d's 60 s
COMPACT_EVERY = 100  # batches with changes between compactions (ponytail: untuned until Phase 7)
# Exit codes: 0 stopped, 1 gave up on failed ticks (a restart may fix it), over 1 fatal, so the
# supervisor stops instead: 2 is a bad flag, and these no restart fixes.
FATAL = {
    state.PartitionTaken: 3,
    state.OffsetsMismatch: 4,
    state.CorruptState: 5,
    classify.ModelMissing: 6,
    jev.KeyMissing: 7,
}


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
        reported = getattr(classifier, "usd", None)  # spend already in a batch event
        gap = False  # the last read found history cleanup removed: bootstrap instead of reading
        while not stop.is_set():
            started = clock()
            try:
                if gap:
                    bootstrap(
                        landing_dt, store_dt, classifier, events, state_dir, offsets,
                        name=name, stop=stop, clock=clock, limit=limit,
                        beat=lambda: state.beat(state_dir, name, clock()),
                    )  # fmt: skip
                    gap = False
                    continue  # never resets `failures`: only a good read does, so a gap that
                    # a bootstrap can't cure still ends the worker after ATTEMPTS
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
                    spent = getattr(classifier, "usd", None)
                    if spent is not None:  # a paid classifier: what it spent since the last
                        tick["usd"] = round(spent - reported, 6)  # event, failed ticks too
                    events.emit([tick])
                    reported = spent
                # Only this thread beats, after the batch: a hung MERGE stops the beats (B1).
                # Before compacting, so a long compaction gets the watchdog's full budget.
                state.beat(state_dir, name, clock())
                if busy >= compact_every:  # owner-only (ADR-0001); a failure retries next tick
                    store.compact(store_dt, mine)
                    busy = 0
            except Exception as e:
                gap = gap or delta.history_gone(e)  # a failed bootstrap is retried
                failures += 1
                error = repr(e)[:500]
                logged = [
                    {"type": "tick_failed", "worker": name, "attempt": failures, "error": error}
                ]
                if gave_up := failures >= ATTEMPTS or type(e) in FATAL:  # no retry fixes those
                    logged.append({"type": "worker_stop", "worker": name, "error": error})
                with contextlib.suppress(OSError):  # best effort: the error itself matters more
                    events.emit(logged)
                if gave_up:
                    raise
                # OSError only, as in entry.exit_with (see there for why not ValueError).
                with contextlib.suppress(OSError):  # stderr may be a broken pipe: retry anyway
                    print(f"{name}: tick failed ({failures}/{ATTEMPTS}):", file=sys.stderr)
                    traceback.print_exc()
                stop.wait(2 ** (failures - 1))
                continue
            failures = 0
            if min(v for v, _ in offsets.values()) > landing_dt.version():  # caught up
                stop.wait(POLL)
        reason = getattr(stop, "reason", None)  # set by entry's watch
        events.emit(
            [{"type": "worker_stop", "worker": name} | ({"reason": reason} if reason else {})]
        )


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.worker")
    args.add_argument("--index", type=int, required=True)
    args.add_argument("--workers", type=int, required=True)
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--state", type=Path, default=state.STATE)
    args.add_argument("--classifier", choices=classify.KINDS, default="fake")
    args.add_argument("--models", type=Path, default=classify.MODELS)
    a = args.parse_args(argv)
    try:
        owned(a.index, a.workers)
    except ValueError as e:
        args.error(str(e))  # exit 2: fatal, so the supervisor doesn't restart a bad flag forever
    supervisor = entry.supervisor_pid(args)
    stop = entry.watch_supervisor(supervisor, a.data / "events", f"{state.WORKER}{a.index}")
    if a.classifier == "jev":
        call = jev.http(attempts=1)  # first: a missing key stops it before the model loads
        tax = taxonomy.load()
        shortlist = jev.shortlist(tax, classify.fastembed(a.models), a.models / "texts")
        # ponytail: a static share of Jev's limit; an idle worker's share goes unused
        classifier = jev.JevClassifier(tax, shortlist, call, rate=jev.LIMIT / a.workers)
    elif a.classifier == "student":
        from catalog import student  # here: it loads the eval module too

        classifier = student.pipeline(taxonomy.load(), a.models)  # step-6m.md: no key, no Jev
    elif a.classifier == "embedding":
        classifier = classify.EmbeddingClassifier(taxonomy.load(), classify.fastembed(a.models))
    else:
        classifier = classify.FakeClassifier(fail=a.classifier == "down")
    run(a.data, a.state, a.index, a.workers, classifier, stop=stop)


class Overtaken(RuntimeError):
    """The MERGE applied fewer rows than planned: another writer got there first (ADR-0001)."""


if __name__ == "__main__":
    entry.exit_with(main, FATAL)  # a fatal startup error's code, or 1 after giving up
