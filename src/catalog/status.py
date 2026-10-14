"""Submission status: fold a Submission's events into one Outcome per Change. Pure."""

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum


class Outcome(StrEnum):
    WRITTEN = "written"
    ALREADY_APPLIED = "already_applied"  # same source version, same content: a replay or a retry
    STALE = "stale"  # older source version than stored
    CONFLICT = "conflict"  # same source version, different content: the first one wins
    RECLASSIFIED = "reclassified"  # internal op=reclassify on a live Listing
    SKIPPED = "skipped"  # internal op=reclassify on a missing or deleted Listing
    FAILED = "failed"  # worker gave up on the Change after retries
    REJECTED = "rejected"  # the Ingestion API refused the Change


PENDING = "pending"  # accepted, no Outcome yet

# A crash replay re-reports Changes, and for merchant Changes a replay can only look worse (a
# written Change replays as already_applied or stale). So the best Outcome a Change ever got is its
# Outcome. (An internal reclassify can replay as reclassified after skipped; that is harmless.)
RANK = {
    o: r
    for r, o in enumerate(
        [
            Outcome.WRITTEN,
            Outcome.RECLASSIFIED,
            Outcome.ALREADY_APPLIED,
            Outcome.CONFLICT,
            Outcome.STALE,
            Outcome.SKIPPED,
            Outcome.FAILED,
            Outcome.REJECTED,
        ]
    )
}


@dataclass(frozen=True)
class SubmissionStatus:
    submission_id: str
    merchant_id: str
    outcomes: dict[int, str]  # change_index -> Outcome or "pending"

    @property
    def counts(self) -> Counter:
        return Counter(self.outcomes.values())

    @property
    def done(self) -> bool:
        return PENDING not in self.outcomes.values()


def fold(submission_id: str, events: Iterable[Mapping]) -> SubmissionStatus | None:
    """None when no event mentions the Submission. Events of other types are ignored."""
    merchant_id = None
    outcomes: dict[int, str] = {}
    for e in events:
        if e.get("submission_id") != submission_id:
            continue
        merchant_id, i, kind = e["merchant_id"], e["change_index"], e["type"]
        if kind == "accepted":
            outcomes.setdefault(i, PENDING)
        elif kind in RANK and (
            outcomes.get(i, PENDING) == PENDING or RANK[kind] < RANK[outcomes[i]]
        ):
            outcomes[i] = kind
    if merchant_id is None:
        return None
    return SubmissionStatus(submission_id, merchant_id, dict(sorted(outcomes.items())))
