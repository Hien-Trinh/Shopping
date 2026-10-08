"""Listing Store: current state of every Listing, Tombstones included. One writer per partition."""

import json
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable

from catalog import delta
from catalog.envelope import Content, Key, content_hash
from catalog.keys import partition
from catalog.plan import Classification, Stored, Write
from catalog.replay import Fingerprint, fingerprint

# Copy-on-write MERGE rewrites every file holding a matched row, so files stay small: a 1 MiB
# file is ~1-3k Listings rewritten per change, not a whole partition (measured).
COMPACT_TARGET = 1 << 20


def _required(name: str, kind: pa.DataType) -> pa.Field:
    return pa.field(name, kind, nullable=False)


SCHEMA = pa.schema(
    [
        _required("partition", pa.int32()),
        _required("merchant_id", pa.string()),
        _required("merchant_product_id", pa.string()),
        _required("source_version", pa.int64()),
        _required("content_hash", pa.string()),
        _required("is_tombstone", pa.bool_()),
        ("title", pa.string()),
        ("description", pa.string()),
        ("price_micros", pa.int64()),
        ("currency", pa.string()),
        ("availability", pa.string()),
        ("attributes", pa.string()),  # canonical JSON object (sorted keys)
        ("primary_category", pa.string()),
        ("classify_confidence", pa.float64()),
        ("taxonomy_version", pa.string()),
        _required("needs_reclassify", pa.bool_()),
        _required("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)
_CONTENT = tuple(f for f in Content.model_fields if f != "attributes")
_FINGERPRINT = [
    "merchant_id",
    "merchant_product_id",
    "source_version",
    "content_hash",
    "is_tombstone",
]

# One writer per partition makes this a safety net against a zombie worker, not the main rule.
# An equal version with equal content is the same Listing state (a reclassify keeps both).
_APPLY_IF = (
    "s.source_version > t.source_version"
    " OR (s.source_version = t.source_version AND s.content_hash = t.content_hash)"
)


@dataclass(frozen=True)
class Merged:
    applied: int  # rows inserted or updated; fewer than written means something overtook us
    version: int  # the Listing Store version after the MERGE


def ensure(path: str) -> DeltaTable:
    # A MERGE writes a partition's output in files up to this size, so a later MERGE rewrites
    # the files holding its rows, not the partition (step 7f). An existing store without the
    # property gets it from the runbook's `alter` line.
    return delta.ensure(path, SCHEMA, **{"delta.targetFileSize": str(COMPACT_TARGET)})


def read(dt: DeltaTable, keys: Collection[Key]) -> dict[Key, Stored]:
    if not keys:
        return {}
    dt.update_incremental()
    # Prune by partition when listing files, not after: an unpruned dataset lists every file in
    # the table (about 1 ms each), which dominated a batch once the store had grown.
    parts = ", ".join(str(p) for p in sorted({partition(*k) for k in keys}))
    scanned = delta.plain(
        dt.to_pyarrow_dataset(file_pruning_predicate=f"partition IN ({parts})").to_table()
    )
    # Narrow to the wanted products in Arrow (not in the scan filter: delta-rs reports string_view
    # columns, which Arrow can't compare with file statistics), so only candidates reach Python.
    candidates = scanned.filter(
        pc.is_in(scanned["merchant_product_id"], pa.array({p for _, p in keys}))
    )
    wanted = set(keys)
    return {
        k: _stored(r)
        for r in candidates.to_pylist()
        if (k := (r["merchant_id"], r["merchant_product_id"])) in wanted
    }


def merge(dt: DeltaTable, writes: Sequence[Write], updated_at: datetime) -> Merged:
    """Apply Writes in one commit. A live Write must be classified by now."""
    if updated_at.utcoffset() is None:
        raise ValueError("updated_at must be timezone-aware")
    dt.update_incremental()
    if not writes:
        return Merged(0, dt.version())
    rows = [_row(w, updated_at) for w in writes]
    parts = ", ".join(str(p) for p in sorted({r["partition"] for r in rows}))
    metrics = (
        dt.merge(
            pa.Table.from_pylist(rows, schema=SCHEMA),
            predicate=(
                f"t.partition IN ({parts}) AND t.partition = s.partition"
                " AND t.merchant_id = s.merchant_id"
                " AND t.merchant_product_id = s.merchant_product_id"
            ),
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all(predicate=_APPLY_IF)
        .when_not_matched_insert_all()
        .execute()
    )
    applied = metrics["num_target_rows_updated"] + metrics["num_target_rows_inserted"]
    return Merged(applied, dt.version())


def compact(dt: DeltaTable, partitions: Iterable[int], target_size: int = COMPACT_TARGET) -> None:
    """Merge small files up to `target_size`; only the partitions' owner calls this (ADR-0001)."""
    dt.update_incremental()
    dt.optimize.compact(
        partition_filters=[("partition", "in", [str(p) for p in partitions])],
        target_size=target_size,
    )


def fingerprints(dt: DeltaTable, version: int | None = None) -> dict[Key, Fingerprint]:
    """Every Listing as the replay oracles see it, optionally at an older version.

    An older version is the current rows rewound through the change feed, newest commit first:
    maintenance's vacuum removes an old version's files within minutes (step 7f) but keeps the
    feed's files for over an hour, and the feed carries every inserted and replaced row whole.
    """
    dt.update_incremental()
    head = dt.version()
    rows = dt.to_pyarrow_dataset().to_table(columns=_FINGERPRINT).to_pylist()
    found = {(r["merchant_id"], r["merchant_product_id"]): fingerprint(r) for r in rows}
    if version is None or version >= head:
        return found
    feed = delta.plain(
        pa.table(dt.load_cdf(starting_version=version + 1, ending_version=head).read_all())
    )
    for r in sorted(feed.to_pylist(), key=lambda r: -r["_commit_version"]):
        key = (r["merchant_id"], r["merchant_product_id"])
        if r["_change_type"] == "insert":  # not there before this commit
            found.pop(key, None)
        elif r["_change_type"] in ("update_preimage", "delete"):  # what it was before
            found[key] = fingerprint(r)
    return found


# What converting a value that slipped past validation raises (plan-v1.md A6): the data's fault.
DATA_ERRORS = (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, UnicodeError)


def unstorable(listings: Sequence[Content | None]) -> list[str | None]:
    """Per listing, why merge() would fail to store it (the error, at most 500 chars), else None.

    The same conversion merge() does, run before any I/O, so a bad value fails only its own Change
    and fails it the same way every time.
    """
    errors: list[str | None] = [None] * len(listings)
    rows: dict[int, dict] = {}
    for i, listing in enumerate(listings):
        try:
            rows[i] = _content(listing)
        except DATA_ERRORS as e:
            errors[i] = repr(e)[:500]
    try:
        pa.Table.from_pylist(list(rows.values()), schema=_CHECKED)
    except DATA_ERRORS:  # rare: find the bad rows one by one
        for i, row in rows.items():
            try:
                pa.Table.from_pylist([row], schema=_CHECKED)
            except DATA_ERRORS as e:
                errors[i] = repr(e)[:500]
    return errors


def _content(listing: Content | None) -> dict:
    """The row columns that come from merchant content; null content columns for a Tombstone."""
    row = {"content_hash": content_hash(listing)}
    if listing is None:
        return row
    attributes = json.dumps(listing.attributes, sort_keys=True, ensure_ascii=False)
    return row | listing.model_dump(include=set(_CONTENT)) | {"attributes": attributes}


_CHECKED = pa.schema([SCHEMA.field(c) for c in ("content_hash", *_CONTENT, "attributes")])


def _row(w: Write, updated_at: datetime) -> dict:
    row = {
        "partition": partition(*w.key),
        "merchant_id": w.key[0],
        "merchant_product_id": w.key[1],
        "source_version": w.source_version,
        "is_tombstone": w.listing is None,
        "needs_reclassify": False,
        "updated_at": updated_at,
    } | _content(w.listing)
    if w.listing is None:
        return row  # content columns are left null
    if w.classification is None:
        raise ValueError(f"{w.key}: classify (or mark Uncategorized) before merging")
    return row | {
        "primary_category": w.classification.category,
        "classify_confidence": w.classification.confidence,
        "taxonomy_version": w.classification.taxonomy_version,
        "needs_reclassify": w.classification.needs_reclassify,
    }


def _stored(r: dict) -> Stored:
    if r["is_tombstone"]:
        return Stored(r["source_version"], None)
    # Trusted data: decode without re-validating, so tightening a limit later can't strand rows.
    listing = Content.model_construct(
        **{c: r[c] for c in _CONTENT}, attributes=json.loads(r["attributes"])
    )
    cls = Classification(
        r["primary_category"],
        r["classify_confidence"],
        r["taxonomy_version"],
        r["needs_reclassify"],
    )
    return Stored(r["source_version"], listing, cls)
