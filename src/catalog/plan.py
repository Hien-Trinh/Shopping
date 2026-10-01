"""Decide what a batch of Landing-log Changes does to the Listing Store. Pure.

A batch behaves exactly like applying its Changes one at a time, in landing order: batching
only decides when writes are flushed, never what they are.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from catalog.envelope import Change, Content, Key, content_hash


class Outcome(StrEnum):
    WRITTEN = "written"
    ALREADY_APPLIED = "already_applied"  # same source version, same content: a replay or a retry
    STALE = "stale"  # older source version than stored
    CONFLICT = "conflict"  # same source version, different content: the first one wins
    RECLASSIFIED = "reclassified"  # internal op=reclassify on a live Listing
    SKIPPED = "skipped"  # internal op=reclassify on a missing or deleted Listing


@dataclass(frozen=True)
class Classification:
    category: str
    confidence: float
    taxonomy_version: str
    # A provisional answer (Uncategorized after a classifier failure or timeout) to redo later.
    needs_reclassify: bool = False


@dataclass(frozen=True)
class Stored:
    """The part of a Listing Store row the plan needs."""

    source_version: int
    listing: Content | None  # None is a Tombstone
    classification: Classification | None = None


@dataclass(frozen=True)
class Write:
    """A Listing's state after the batch: one MERGE row."""

    key: Key
    source_version: int
    listing: Content | None
    classification: Classification | None  # carried over; None when classifying or a Tombstone
    needs_classify: bool  # plan output only: the worker classifies these before merging
    # Plan output only: what to keep if classifying fails. The stored answer when the classified
    # fields are unchanged (a reclassify or a taxonomy bump); None means Uncategorized.
    fallback: Classification | None = None


@dataclass(frozen=True)
class Plan:
    outcomes: tuple[Outcome, ...]  # one per input Change, same order
    writes: tuple[Write, ...]  # one per Listing whose row changes, in first-touch order


def plan(changes: Sequence[Change], stored: Mapping[Key, Stored], taxonomy_version: str) -> Plan:
    current: dict[Key, tuple[int, Content | None]] = {}
    touched: dict[Key, None] = {}  # ordered set: first-touch order
    reclassify: set[Key] = set()
    outcomes = []
    for c in changes:
        k = c.key
        if k not in current and k in stored:
            current[k] = (stored[k].source_version, stored[k].listing)
        cur = current.get(k)
        if c.op == "reclassify":
            if cur is not None and cur[1] is not None:
                reclassify.add(k)
                touched[k] = None
                outcomes.append(Outcome.RECLASSIFIED)
            else:
                outcomes.append(Outcome.SKIPPED)
        elif cur is None or c.source_version > cur[0]:
            current[k] = (c.source_version, c.listing)
            touched[k] = None
            outcomes.append(Outcome.WRITTEN)
        elif c.source_version < cur[0]:
            outcomes.append(Outcome.STALE)
        elif c.content_hash == content_hash(cur[1]):
            outcomes.append(Outcome.ALREADY_APPLIED)
        else:
            outcomes.append(Outcome.CONFLICT)
    writes = tuple(
        _write(k, current[k], stored.get(k), k in reclassify, taxonomy_version) for k in touched
    )
    return Plan(tuple(outcomes), writes)


def _write(
    k: Key,
    final: tuple[int, Content | None],
    base: Stored | None,
    reclassify: bool,
    taxonomy_version: str,
) -> Write:
    source_version, listing = final
    if listing is None:
        return Write(k, source_version, None, None, needs_classify=False)
    needs = (
        reclassify
        or base is None
        or base.listing is None  # re-created after a delete
        or base.classification is None
        or base.classification.needs_reclassify
        or base.classification.taxonomy_version != taxonomy_version
        or _classified_view(base.listing) != _classified_view(listing)
    )
    if not needs:
        return Write(k, source_version, listing, base.classification, False)
    same_content = (
        base is not None
        and base.listing is not None
        and _classified_view(base.listing) == _classified_view(listing)
    )
    fallback = base.classification if same_content else None
    return Write(k, source_version, listing, None, True, fallback)


def _classified_view(listing: Content) -> tuple:
    """The fields the classifier reads; a price or stock change never reclassifies."""
    return (listing.title, listing.description, listing.attributes)
