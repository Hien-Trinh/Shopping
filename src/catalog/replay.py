"""Correctness oracles (plan-v1.md A4): what the Listing Store and Change Export must hold. Pure.

Deliberately a different formulation from catalog.plan, so a shared bug can't hide:
per Listing key, the earliest-landed Change with the highest source version wins.
"""

from collections.abc import Container, Iterable, Mapping

from catalog.envelope import Change, Key

Fingerprint = tuple[int, str, bool]  # (source_version, content_hash, is_tombstone)


def expected_store(
    landed: Iterable[Change], failed: Container[int] = frozenset()
) -> dict[Key, Fingerprint]:
    """The Listing Store after applying the Landing log to an empty store.

    `failed` holds landing positions whose Change failed and was skipped.
    """
    best: dict[Key, Change] = {}
    for i, c in enumerate(landed):
        if c.op == "reclassify" or i in failed:
            continue
        if c.key not in best or c.source_version > best[c.key].source_version:
            best[c.key] = c
    return {k: (c.source_version, c.content_hash, c.listing is None) for k, c in best.items()}


def fingerprint(row: Mapping) -> Fingerprint:
    return (row["source_version"], row["content_hash"], bool(row["is_tombstone"]))


def replay_exports(files: Iterable[Iterable[Mapping]]) -> dict[Key, Fingerprint]:
    """Apply Change Export files in order the way a consumer does: upsert by key, delete removes."""
    view: dict[Key, Fingerprint] = {}
    for rows in files:
        for row in rows:
            k = (row["merchant_id"], row["merchant_product_id"])
            if row["op"] == "delete":
                view.pop(k, None)
            else:
                view[k] = fingerprint(row)
    return view


def live(fingerprints: Mapping[Key, Fingerprint]) -> dict[Key, Fingerprint]:
    """Drop Tombstones: what a consumer of Change Export should see."""
    return {k: fp for k, fp in fingerprints.items() if not fp[2]}


def diff(expected: Mapping[Key, Fingerprint], actual: Mapping[Key, Fingerprint]) -> list[str]:
    """One line per Listing key that differs; empty means the oracle holds."""
    return [
        f"{k}: expected {expected.get(k)}, got {actual.get(k)}"
        for k in sorted(expected.keys() | actual.keys())
        if expected.get(k) != actual.get(k)
    ]
