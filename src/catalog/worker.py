"""Ingestion worker: one batch from the Landing log to the Listing Store (design doc, lifecycle).

read -> plan -> classify -> one conditional MERGE -> events -> offsets. Offsets are saved last,
so a crash anywhere earlier replays the batch, which plan's rules make safe (at least once).
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

from catalog import landing, state, store
from catalog.classify import UNCATEGORIZED
from catalog.events import EventLog
from catalog.keys import partition
from catalog.landing import Landed, Position
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
) -> dict[int, Position]:
    """Process up to `limit` Changes past `offsets`; returns the new offsets.

    The caller must hold `state.claim()` on every partition in `offsets` (ADR-0001): two workers
    on one partition would redo each other's work and could pick the wrong conflict winner.
    """
    batch = landing.read(landing_dt, offsets, limit)
    results = _apply(store_dt, batch.changes, classifier, events, now)
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
    return batch.offsets


_WRITES = {Outcome.WRITTEN, Outcome.RECLASSIFIED}

# Errors converting one Change's data (a value that slipped past validation). Anything else, such
# as OSError, DeltaError or a bug in our code, is not the data's fault: it propagates, the offset
# isn't advanced, and the loop backs off (design doc, lifecycle step 8; plan-v1.md A6).
_DATA_ERRORS = (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, UnicodeError)


class Overtaken(RuntimeError):
    """The MERGE applied fewer rows than planned: another writer got there first (ADR-0001)."""


def _apply(store_dt, landed: Sequence[Landed], classifier, events, now) -> list[dict]:
    """Event fields per Change. A data error is bisected down to the Change that causes it.

    Halves run in landing order, so this equals applying the Changes one at a time (plan.py).
    """
    try:
        return _merge(store_dt, landed, classifier, events, now)
    except _DATA_ERRORS as e:
        if len(landed) == 1:  # tried twice by now: in the bigger batch, then alone
            return [{"type": FAILED, "error": repr(e)[:500]}]
        mid = len(landed) // 2
        return _apply(store_dt, landed[:mid], classifier, events, now) + _apply(
            store_dt, landed[mid:], classifier, events, now
        )


def _merge(store_dt, landed: Sequence[Landed], classifier, events, now) -> list[dict]:
    changes = [x.change for x in landed]
    stored = store.read(store_dt, {c.key for c in changes})
    planned = plan(changes, stored, classifier.taxonomy_version)
    writes = _classify(planned.writes, classifier, events)
    merged = store.merge(store_dt, writes, now())
    if merged.applied != len(writes):
        raise Overtaken(f"MERGE applied {merged.applied} of {len(writes)} rows")
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
