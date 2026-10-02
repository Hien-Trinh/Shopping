"""Submission status: fold a Submission's events into one Outcome per Change. Pure."""

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from catalog.plan import Outcome

PENDING = "pending"
FAILED = "failed"  # worker gave up on the Change after retries
REJECTED = "rejected"  # the Ingestion API refused the Change

# A crash replay re-reports Changes, and for merchant Changes a replay can only look worse (a
# written Change replays as already_applied or stale). So the best Outcome a Change ever got is its
# Outcome. (An internal reclassify can replay as reclassified after skipped, and a failed Change
# re-planned against newer state can replay as stale or conflict, which is true by then; both are
# harmless.)
_RANK = {
    o: r
    for r, o in enumerate(
        [
            Outcome.WRITTEN,
            Outcome.RECLASSIFIED,
            Outcome.ALREADY_APPLIED,
            Outcome.CONFLICT,
            Outcome.STALE,
            Outcome.SKIPPED,
            FAILED,
            REJECTED,
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
        elif kind in _RANK and (
            outcomes.get(i, PENDING) == PENDING or _RANK[kind] < _RANK[outcomes[i]]
        ):
            outcomes[i] = kind
    if merchant_id is None:
        return None
    return SubmissionStatus(submission_id, merchant_id, dict(sorted(outcomes.items())))
