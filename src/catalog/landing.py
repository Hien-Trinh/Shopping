"""Landing log: the append-only record of every accepted Change, and the workers' queue (ADR-0002).

Landing order is (commit version, seq): seq is a row's position within its commit. A Position is
the next row to read: everything before it has been processed.
"""

import asyncio
import contextlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError

from catalog import delta
from catalog.envelope import Change, Content
from catalog.events import EventLog
from catalog.keys import PARTITIONS, partition

Position = tuple[int, int]  # (commit version, seq) of the next row to read
START: Position = (0, 0)  # nothing processed; version 0 only creates the table


def _required(name: str, kind: pa.DataType) -> pa.Field:
    return pa.field(name, kind, nullable=False)


SCHEMA = pa.schema(
    [
        _required("partition", pa.int32()),
        _required("seq", pa.int32()),
        _required("merchant_id", pa.string()),
        _required("merchant_product_id", pa.string()),
        _required("source_version", pa.int64()),
        _required("op", pa.string()),
        pa.field("listing", pa.string()),  # Content as JSON; null for delete and reclassify
        _required("submission_id", pa.string()),
        _required("change_index", pa.int32()),
        _required("received_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass(frozen=True)
class Landed:
    change: Change
    submission_id: str
    change_index: int
    partition: int
    position: Position

    # Content holds a dict, so hashing would fail only for upserts; fail for every Landed instead.
    __hash__ = None


@dataclass(frozen=True)
class Batch:
    changes: list[Landed]  # landing order
    offsets: dict[int, Position]  # save these once `changes` are processed


def ensure(path: str) -> DeltaTable:
    return delta.ensure(path, SCHEMA)


def table_id(dt: DeltaTable) -> str:
    """Identifies this Landing log, so offsets saved against another one are refused."""
    return dt.metadata().id


Entry = tuple[str, int, Change, datetime]  # submission_id, change_index, Change, received_at


def append(dt: DeltaTable, entries: Sequence[Entry], events: EventLog | None = None) -> int:
    """Append (submission_id, change_index, Change, received_at) rows in one commit.

    Returns the commit's version. With no entries nothing is committed, and the current version is
    returned. One commit can hold several requests, so each row carries its own received_at. A
    lost commit race, retried once, is logged to `events` as `append_retry`.
    """
    if any(received_at.utcoffset() is None for *_, received_at in entries):
        raise ValueError("received_at must be timezone-aware")
    if not entries:
        return dt.version()
    rows = [
        {
            "partition": partition(*c.key),
            "seq": seq,
            "merchant_id": c.merchant_id,
            "merchant_product_id": c.merchant_product_id,
            "source_version": c.source_version,
            "op": c.op,
            "listing": None if c.listing is None else c.listing.model_dump_json(),
            "submission_id": submission_id,
            "change_index": index,
            "received_at": received_at,
        }
        for seq, (submission_id, index, c, received_at) in enumerate(entries)
    ]
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    # Refreshed first: on a snapshot older than retention's DELETE, every append would fail. A
    # DELETE landing between the refresh and the commit costs one retry.
    dt.update_incremental()
    started = time.monotonic()
    try:
        write_deltalake(dt, table, mode="append")
    except CommitFailedError:
        if events:  # the conflict rate, and the time the lost attempt cost (5d.1 review)
            ms = round((time.monotonic() - started) * 1000)
            with contextlib.suppress(OSError):  # best effort; logged even if the retry fails
                events.emit([{"type": "append_retry", "rows": len(entries), "ms": ms}])
        dt.update_incremental()
        write_deltalake(dt, table, mode="append")
    return dt.version()


WINDOW = 0.1  # seconds a request waits for others to share its commit (A10: at most 10 commits/s)


class Appender:
    """Group commit: one Landing log commit for every request that arrives within the window.

    The window counts from the oldest waiting request, so commits never run back to back. The
    commit runs in a thread, and requests arriving meanwhile queue for the next one.
    """

    def __init__(
        self,
        commit: Callable[[list[Entry]], int],
        window: float = WINDOW,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.commit, self.window, self.clock = commit, window, clock
        # (arrived, entries, the waiting request's future)
        self.queue: asyncio.Queue[tuple[float, Sequence[Entry], asyncio.Future[int]]] = (
            asyncio.Queue()
        )

    async def submit(self, entries: Sequence[Entry]) -> int:
        """Waits for the commit holding `entries`; returns its version or raises its error."""
        done = asyncio.get_running_loop().create_future()
        self.queue.put_nowait((self.clock(), entries, done))
        return await done

    async def run(self) -> None:
        """The appender's loop, one per process; only cancellation stops it."""
        while True:
            group = [await self.queue.get()]
            await asyncio.sleep(group[0][0] + self.window - self.clock())
            while not self.queue.empty():
                group.append(self.queue.get_nowait())
            try:
                version = await asyncio.to_thread(
                    self.commit, [e for _, es, _ in group for e in es]
                )
                error = None
            except Exception as e:  # every request in the group gets it; the loop goes on
                error = e
            for *_, done in group:
                if done.cancelled():  # its client left; its rows landed all the same
                    continue
                if error is None:
                    done.set_result(version)
                else:
                    done.set_exception(error)


def read(
    dt: DeltaTable, after: Mapping[int, Position], limit: int, max_versions: int = 100
) -> Batch:
    """Up to `limit` Changes at or after each owned partition's position, in landing order.

    `after` maps each owned partition to its next position (START if none). Reads at most
    `max_versions` commits per call so a far-behind worker never loads the whole log.
    """
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if not after:
        return Batch([], {})
    dt.update_incremental()
    first = min(v for v, _ in after.values())
    last = min(dt.version(), first + max_versions - 1)
    if first > last:
        return Batch([], dict(after))
    parts = ", ".join(str(p) for p in sorted(after))
    feed = delta.plain(
        pa.table(
            dt.load_cdf(
                starting_version=first,
                ending_version=last,
                predicate=f"partition IN ({parts})",
            ).read_all()
        )
    )
    # Each row's partition position, looked up in Arrow so only the returned rows reach Python.
    next_v = pc.take(
        pa.array([after.get(p, START)[0] for p in range(PARTITIONS)]), feed["partition"]
    )
    next_s = pc.take(
        pa.array([after.get(p, START)[1] for p in range(PARTITIONS)]), feed["partition"]
    )
    version = pc.cast(feed["_commit_version"], pa.int64())
    pending = pc.or_(
        pc.greater(version, next_v),
        pc.and_(pc.equal(version, next_v), pc.greater_equal(feed["seq"], next_s)),
    )
    rows = feed.filter(pc.and_(pc.equal(feed["_change_type"], "insert"), pending)).sort_by(
        [("_commit_version", "ascending"), ("seq", "ascending")]
    )
    changes = [_landed(r) for r in rows.slice(0, limit).to_pylist()]
    # Rows are in global landing order, so every owned row before the cut was returned.
    if rows.num_rows > limit:
        v, seq = changes[-1].position
        done = (v, seq + 1)
    else:
        done = (last + 1, 0)
    return Batch(changes, {p: max(pos, done) for p, pos in after.items()})


REPLAY_ORDER = [
    ("received_at", "ascending"),
    ("submission_id", "ascending"),
    ("change_index", "ascending"),
]


def retained(dt: DeltaTable, p: int) -> list[Landed]:
    """Every Change of partition `p` in the version `dt` has loaded (the pin), in replay order.

    For a worker whose offset is past what cleanup kept (step-5e.md). It never refreshes `dt`, so
    the worker's one handle serves as the pin. Compaction drops the commit a row landed in, so each
    position is `(pin, seq)`, not where the row landed.
    """
    # ponytail: replay order matches landing order unless received_at runs backwards across
    # commits (two requests within a millisecond, or the wall clock stepping back), and it decides
    # only same-version conflicts; a column written at append time would make it exact.
    # ponytail: the partition is read and sorted in memory (about 470k rows at 7 days of 50/s);
    # sort in DuckDB, which spills, if it matters.
    rows = delta.plain(
        dt.to_pyarrow_dataset(file_pruning_predicate=f"partition IN ({p})").to_table()
    ).sort_by(REPLAY_ORDER)
    return [_landed(r | {"_commit_version": dt.version()}) for r in rows.to_pylist()]


def _landed(r: dict) -> Landed:
    # Trusted data: decode without re-validating, so tightening a limit later can't strand old rows.
    listing = None if r["listing"] is None else Content.model_construct(**json.loads(r["listing"]))
    change = Change(
        r["merchant_id"], r["merchant_product_id"], r["source_version"], r["op"], listing
    )
    position = (r["_commit_version"], r["seq"])
    return Landed(change, r["submission_id"], r["change_index"], r["partition"], position)
