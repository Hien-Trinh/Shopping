"""Classifiers: Listing content -> (Primary Category, confidence), a whole batch per call.

The contract the worker relies on: a `taxonomy_version` attribute, and `classify(listings)`
returning one (category, confidence) per Listing, in order, with a non-empty category and a
confidence in 0..1. The classifier owns the design's batch timeout (raise on expiry) and its
confidence threshold (answer Uncategorized below it); the real one is step 6b.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from catalog.envelope import Content

UNCATEGORIZED = "Uncategorized"


@dataclass
class FakeClassifier:
    """Deterministic and instant: the category is the title's first character."""

    taxonomy_version: str = "fake-1"
    fail: bool = False  # every call raises, like an outage or a timeout

    def classify(self, listings: Sequence[Content]) -> list[tuple[str, float]]:
        if self.fail:
            raise RuntimeError("classifier unavailable")
        return [(f"Fake > {listing.title[0]}", 0.9) for listing in listings]
