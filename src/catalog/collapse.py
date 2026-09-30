"""Change Export: collapse a Listing Store change feed into one row per changed Listing. Pure."""

from collections.abc import Iterable, Mapping
from typing import Any

from catalog.envelope import Key

_FEED_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")


def collapse(feed: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Latest row per Listing key, sorted by key, with op = "delete" for Tombstones."""
    latest: dict[Key, Mapping[str, Any]] = {}
    for row in feed:
        if row["_change_type"] == "update_preimage":
            continue
        k = (row["merchant_id"], row["merchant_product_id"])
        if k not in latest or row["_commit_version"] > latest[k]["_commit_version"]:
            latest[k] = row
    out = []
    for k in sorted(latest):
        row = latest[k]
        # Tombstones are kept, so a physical delete is unexpected; export it as a delete anyway.
        deleted = row["is_tombstone"] or row["_change_type"] == "delete"
        out.append(
            {c: v for c, v in row.items() if c not in _FEED_COLUMNS}
            | {"op": "delete" if deleted else "upsert"}
        )
    return out
