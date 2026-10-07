"""What a batch of landed Changes does: the Writes to MERGE and the events to emit once it commits.

No I/O but the classifier call. Storability check, plan, classify, then one Outcome event per
landed Change in landing order; the worker reads the store before and merges after.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from time import monotonic

from catalog import jev, store
from catalog.classify import UNCATEGORIZED
from catalog.envelope import Key
from catalog.keys import partition
from catalog.landing import Landed
from catalog.plan import Classification, Outcome, Stored, Write, plan
from catalog.status import FAILED

_WRITES = {Outcome.WRITTEN, Outcome.RECLASSIFIED}


@dataclass(frozen=True)
class Decision:
    writes: tuple[Write, ...]  # classified: ready to MERGE
    notes: tuple[dict, ...]  # batch-level events: classify, classify_failed
    outcomes: tuple[dict, ...]  # one Outcome event per landed Change, without store_version

    def events(self, store_version: int) -> list[dict]:
        """The events to emit once the MERGE committed at `store_version`."""
        return [
            *self.notes,
            *(
                o | {"store_version": store_version} if o["type"] in _WRITES else o
                for o in self.outcomes
            ),
        ]


def decide(
    landed: Sequence[Landed],
    stored: Mapping[Key, Stored],
    classifier,
    clock: Callable[[], float] = monotonic,
) -> Decision:
    # A Change whose data can't be stored fails alone, before planning, so the rest plan without it
    # (design doc, lifecycle step 8). Any other error propagates: a bug, never the data.
    errors = store.unstorable([x.change.listing for x in landed])
    good = [x.change for x, error in zip(landed, errors, strict=True) if error is None]
    planned = plan(good, stored, classifier.taxonomy_version)
    writes, notes = _classify(planned.writes, classifier, clock)
    outcomes = iter(planned.outcomes)
    results = [
        {"type": FAILED, "error": error} if error else {"type": next(outcomes)} for error in errors
    ]
    return Decision(
        tuple(writes),
        tuple(notes),
        tuple(
            {
                "submission_id": x.submission_id,
                "change_index": x.change_index,
                "merchant_id": x.change.merchant_id,
                "merchant_product_id": x.change.merchant_product_id,
                "partition": x.partition,
            }
            | result
            for x, result in zip(landed, results, strict=True)
        ),
    )


def _classify(writes: Sequence[Write], classifier, clock) -> tuple[list[Write], list[dict]]:
    """Classify the Writes that need it in one call; on any failure, or for those the classifier
    didn't reach (answered None), they become Uncategorized.

    Returns the Writes, a classify event (its size and time, for classify latency) and a
    classify_failed one if any went unanswered.
    """
    todo = [w for w in writes if w.needs_classify]
    if not todo:
        return list(writes), []
    version = classifier.taxonomy_version
    started = clock()
    try:
        results = classifier.classify([w.listing for w in todo])
        found = [
            None if a is None else _answer(a, version) for _, a in zip(todo, results, strict=True)
        ]
        # for the Listings it answered None: a classifier may say why (jev.JevClassifier.error)
        error = getattr(classifier, "error", None)
        reason, error = ("error", error) if error else ("budget", "budget spent")
    except jev.KeyMissing:  # a refused key: no batch can succeed, so stop (exit 7)
        raise
    except Exception as e:  # an outage or a bad answer: never stall the partition
        found, reason = [None] * len(todo), "error"
        error = repr(e)[:500]  # a provider error may echo listing text
    ms = round((clock() - started) * 1000)
    notes = [{"type": "classify", "listings": len(todo), "ms": ms}]
    if missed := [w for w, f in zip(todo, found, strict=True) if f is None]:
        notes.append({
            "type": "classify_failed",
            "listings": len(missed),
            "partitions": sorted({partition(*w.key) for w in missed}),
            "reason": reason,  # budget spent or an error: no matching on the error's text
            "batch": len(todo),
            "error": error,
        })  # fmt: skip
    # Keep an answer whose inputs haven't changed, else Uncategorized; either way flagged so
    # the Backfill reclassifies it (design doc, lifecycle step 5).
    found = [
        f
        or (
            replace(w.fallback, needs_reclassify=True)
            if w.fallback
            else Classification(UNCATEGORIZED, 0.0, version, needs_reclassify=True)
        )
        for w, f in zip(todo, found, strict=True)
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
