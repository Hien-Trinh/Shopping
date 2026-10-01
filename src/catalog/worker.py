"""Ingestion worker: one batch from the Landing log to the Listing Store (design doc, lifecycle).

read -> plan -> classify -> one conditional MERGE -> events -> offsets. Offsets are saved last,
so a crash anywhere earlier replays the batch, which plan's rules make safe (at least once).
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from deltalake import DeltaTable

from catalog import landing, state, store
from catalog.classify import UNCATEGORIZED
from catalog.events import EventLog
from catalog.landing import Position
from catalog.plan import Classification, Outcome, Write, plan


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
    """Process up to `limit` Changes past `offsets` (the owned partitions); returns new offsets."""
    batch = landing.read(landing_dt, offsets, limit)
    changes = [landed.change for landed in batch.changes]
    stored = store.read(store_dt, {c.key for c in changes})
    planned = plan(changes, stored, classifier.taxonomy_version)
    writes = _classify(planned.writes, classifier, events)
    # ponytail: trusts the single-writer lock; merged.applied < len(writes) would mean a zombie
    # overtook this worker and some `written` outcomes are wrong. Step 3b checks it.
    merged = store.merge(store_dt, writes, now())
    events.emit(
        {
            "type": outcome,
            "submission_id": landed.submission_id,
            "change_index": landed.change_index,
            "merchant_id": landed.change.merchant_id,
            "merchant_product_id": landed.change.merchant_product_id,
            "partition": landed.partition,
        }
        | ({"store_version": merged.version} if outcome in _WRITES else {})
        for landed, outcome in zip(batch.changes, planned.outcomes, strict=True)
    )
    moved = {p: pos for p, pos in batch.offsets.items() if pos != offsets[p]}
    state.save_offsets(state_dir, moved, landing.table_id(landing_dt))
    return batch.offsets


_WRITES = {Outcome.WRITTEN, Outcome.RECLASSIFIED}


def _classify(writes: Sequence[Write], classifier, events: EventLog) -> list[Write]:
    """Classify the Writes that need it in one call; on any failure they become Uncategorized."""
    todo = [w for w in writes if w.needs_classify]
    if not todo:
        return list(writes)
    version = classifier.taxonomy_version
    try:
        results = classifier.classify([w.listing for w in todo])
        found = [
            Classification(category, confidence, version)
            for _, (category, confidence) in zip(todo, results, strict=True)
        ]
    except Exception as e:  # an outage, a timeout or a bad answer: never stall the partition
        events.emit([{"type": "classify_failed", "listings": len(todo), "error": repr(e)}])
        # Provisional, flagged so the Backfill reclassifies it (design doc, lifecycle step 5).
        found = [Classification(UNCATEGORIZED, 0.0, version, needs_reclassify=True)] * len(todo)
    answers = iter(found)
    return [
        replace(w, classification=next(answers), needs_classify=False) if w.needs_classify else w
        for w in writes
    ]
