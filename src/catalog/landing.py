"""Landing log: the append-only record of every accepted Change, and the workers' queue (ADR-0002).

Landing order is (commit version, seq): seq is a row's position within its commit.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from catalog.envelope import Change, Content
from catalog.keys import partition

Position = tuple[int, int]  # (commit version, seq)
START: Position = (-1, -1)  # before the first row
MAX_SEQ = 2**31 - 1  # (v, MAX_SEQ): every row of commit v is processed

SCHEMA = pa.schema(
    [
        ("partition", pa.int32()),
        ("seq", pa.int32()),
        ("merchant_id", pa.string()),
        ("merchant_product_id", pa.string()),
        ("source_version", pa.int64()),
        ("op", pa.string()),
        ("listing", pa.string()),  # Content as JSON; null for delete and reclassify
        ("submission_id", pa.string()),
        ("change_index", pa.int32()),
        ("received_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass(frozen=True)
class Landed:
    change: Change
    submission_id: str
    change_index: int
    partition: int
    position: Position


def ensure(path: str) -> None:
    """Create the empty table if it does not exist yet."""
    try:
        DeltaTable(path)
    except TableNotFoundError:
        DeltaTable.create(
            path,
            schema=SCHEMA,
            partition_by=["partition"],
            configuration={"delta.enableChangeDataFeed": "true"},
        )


def append(path: str, entries: Sequence[tuple[str, int, Change]], received_at: datetime) -> int:
    """Append (submission_id, change_index, Change) rows in one commit; returns its version."""
    changes = [c for _, _, c in entries]
    table = pa.table(
        {
            "partition": [partition(c.merchant_id, c.merchant_product_id) for c in changes],
            "seq": list(range(len(changes))),
            "merchant_id": [c.merchant_id for c in changes],
            "merchant_product_id": [c.merchant_product_id for c in changes],
            "source_version": [c.source_version for c in changes],
            "op": [c.op for c in changes],
            "listing": [
                None if c.listing is None else c.listing.model_dump_json() for c in changes
            ],
            "submission_id": [s for s, _, _ in entries],
            "change_index": [i for _, i, _ in entries],
            "received_at": [received_at] * len(changes),
        },
        schema=SCHEMA,
    )
    dt = DeltaTable(path)
    write_deltalake(dt, table, mode="append")
    return dt.version()


@dataclass(frozen=True)
class Batch:
    changes: list[Landed]  # landing order
    offsets: dict[int, Position]  # save these once `changes` are processed


def read(path: str, after: Mapping[int, Position], limit: int, max_versions: int = 100) -> Batch:
    """Up to `limit` Changes past each owned partition's position, in landing order.

    `after` maps each owned partition to its last processed position (START if none). Reads at
    most `max_versions` commits per call so a far-behind worker never loads the whole log, and
    advances past commits holding nothing for these partitions, so an idle stretch can't stall it.
    """
    if not after:
        return Batch([], {})
    dt = DeltaTable(path)
    # Resume inside a commit that was only partly processed (a `limit` cut it), else after it.
    first = max(0, min(v + (seq == MAX_SEQ) for v, seq in after.values()))
    last = min(dt.version(), first + max_versions - 1)
    if first > last:
        return Batch([], dict(after))
    parts = ", ".join(str(p) for p in sorted(after))
    feed = pa.table(
        dt.load_cdf(
            starting_version=first,
            ending_version=last,
            predicate=f"partition IN ({parts})",
        ).read_all()
    ).to_pylist()
    rows = sorted(
        (
            r
            for r in feed
            if r["_change_type"] == "insert"  # retention deletes are not new work
            and (r["_commit_version"], r["seq"]) > after[r["partition"]]
        ),
        key=lambda r: (r["_commit_version"], r["seq"]),
    )
    changes = [_landed(r) for r in rows[:limit]]
    if len(rows) <= limit:  # everything up to `last` was returned
        return Batch(changes, {p: max(pos, (last, MAX_SEQ)) for p, pos in after.items()})
    offsets = dict(after)
    for x in changes:
        offsets[x.partition] = x.position
    return Batch(changes, offsets)


def _landed(r: dict) -> Landed:
    listing = None if r["listing"] is None else Content.model_validate(json.loads(r["listing"]))
    change = Change(
        r["merchant_id"], r["merchant_product_id"], r["source_version"], r["op"], listing
    )
    position = (r["_commit_version"], r["seq"])
    return Landed(change, r["submission_id"], r["change_index"], r["partition"], position)
