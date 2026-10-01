"""Listing Store: current state of every Listing, Tombstones included. One writer per partition."""

import json
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from catalog.envelope import Content, Key, content_hash
from catalog.keys import partition
from catalog.plan import Classification, Stored, Write
from catalog.replay import Fingerprint

SCHEMA = pa.schema(
    [
        ("partition", pa.int32()),
        ("merchant_id", pa.string()),
        ("merchant_product_id", pa.string()),
        ("source_version", pa.int64()),
        ("content_hash", pa.string()),
        ("is_tombstone", pa.bool_()),
        ("title", pa.string()),
        ("description", pa.string()),
        ("price_micros", pa.int64()),
        ("currency", pa.string()),
        ("availability", pa.string()),
        ("attributes", pa.string()),  # JSON object
        ("primary_category", pa.string()),
        ("classify_confidence", pa.float64()),
        ("taxonomy_version", pa.string()),
        ("needs_reclassify", pa.bool_()),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)
_COLUMNS = SCHEMA.names
_CONTENT = ("title", "description", "price_micros", "currency", "availability")

# Single writer per partition makes this a safety net against a zombie worker, not the main rule.
_APPLY_IF = (
    "s.source_version > t.source_version"
    " OR (s._reclassify_only AND s.source_version = t.source_version"
    " AND s.content_hash = t.content_hash)"
)


@dataclass(frozen=True)
class Merged:
    applied: int  # rows inserted or updated; fewer than written means something overtook us
    version: int  # the Listing Store version after the MERGE


def ensure(path: str) -> None:
    try:
        DeltaTable(path)
    except TableNotFoundError:
        DeltaTable.create(
            path,
            schema=SCHEMA,
            partition_by=["partition"],
            configuration={"delta.enableChangeDataFeed": "true"},
        )


def read(path: str, keys: Collection[Key]) -> dict[Key, Stored]:
    if not keys:
        return {}
    parts = sorted({partition(*k) for k in keys})
    table = DeltaTable(path).to_pyarrow_dataset().to_table(filter=pc.field("partition").isin(parts))
    wanted = set(keys)
    out = {}
    for r in table.to_pylist():
        k = (r["merchant_id"], r["merchant_product_id"])
        if k in wanted:
            out[k] = _stored(r)
    return out


def merge(path: str, writes: Sequence[Write], updated_at: datetime) -> Merged:
    """Apply classified Writes in one commit.

    A live Write must carry its Classification by now; `needs_classify` still being True means
    the classifier failed, and is stored as needs_reclassify.
    """
    dt = DeltaTable(path)
    if not writes:
        return Merged(0, dt.version())
    rows = [_row(w, updated_at) for w in writes]
    source = pa.Table.from_pylist(
        rows, schema=SCHEMA.append(pa.field("_reclassify_only", pa.bool_()))
    )
    parts = ", ".join(str(p) for p in sorted({r["partition"] for r in rows}))
    metrics = (
        dt.merge(
            source,
            predicate=(
                f"t.partition IN ({parts}) AND t.partition = s.partition"
                " AND t.merchant_id = s.merchant_id"
                " AND t.merchant_product_id = s.merchant_product_id"
            ),
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update(updates={c: f"s.{c}" for c in _COLUMNS}, predicate=_APPLY_IF)
        .when_not_matched_insert(updates={c: f"s.{c}" for c in _COLUMNS})
        .execute()
    )
    applied = metrics["num_target_rows_updated"] + metrics["num_target_rows_inserted"]
    return Merged(applied, dt.version())


def compact(path: str, partitions: Iterable[int]) -> None:
    """Rewrite small files; only the partitions' owner calls this (ADR-0001)."""
    DeltaTable(path).optimize.compact(
        partition_filters=[("partition", "in", [str(p) for p in partitions])]
    )


def fingerprints(path: str, version: int | None = None) -> dict[Key, Fingerprint]:
    """Every Listing as the replay oracles see it, optionally at an older version."""
    dt = DeltaTable(path, version=version)
    cols = ["merchant_id", "merchant_product_id", "source_version", "content_hash", "is_tombstone"]
    return {
        (r["merchant_id"], r["merchant_product_id"]): (
            r["source_version"],
            r["content_hash"],
            r["is_tombstone"],
        )
        for r in dt.to_pyarrow_dataset().to_table(columns=cols).to_pylist()
    }


def _row(w: Write, updated_at: datetime) -> dict:
    k_merchant, k_product = w.key
    row = dict.fromkeys(_COLUMNS) | {
        "partition": partition(k_merchant, k_product),
        "merchant_id": k_merchant,
        "merchant_product_id": k_product,
        "source_version": w.source_version,
        "content_hash": content_hash(w.listing),
        "is_tombstone": w.listing is None,
        "needs_reclassify": False,
        "updated_at": updated_at,
        "_reclassify_only": not w.content_changed,
    }
    if w.listing is None:
        return row
    if w.classification is None:
        raise ValueError(f"{w.key}: classify (or mark Uncategorized) before merging")
    row |= w.listing.model_dump(include=set(_CONTENT))
    row |= {
        "attributes": json.dumps(w.listing.attributes, sort_keys=True, ensure_ascii=False),
        "primary_category": w.classification.category,
        "classify_confidence": w.classification.confidence,
        "taxonomy_version": w.classification.taxonomy_version,
        "needs_reclassify": w.needs_classify,
    }
    return row


def _stored(r: dict) -> Stored:
    if r["is_tombstone"]:
        return Stored(r["source_version"], None)
    listing = Content(**{c: r[c] for c in _CONTENT}, attributes=json.loads(r["attributes"]))
    cls = Classification(r["primary_category"], r["classify_confidence"], r["taxonomy_version"])
    return Stored(r["source_version"], listing, cls, r["needs_reclassify"])
