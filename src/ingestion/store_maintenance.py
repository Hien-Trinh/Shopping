"""The Listing Store's maintenance, a thread in the streaming job's driver: OPTIMIZE every hour,
a Listing Store Snapshot at the snapshot cadence, VACUUM at the start and then every day
(docs/specs/step-a8.md).

A snapshot is a shallow clone, a table named snapshot_v<version> in one schema, with its log
under <data>/snapshots/. In one tick the order is snapshot, drop, OPTIMIZE, VACUUM: the VACUUM
runs on Delta's default retention, and outside Unity Catalog it breaks a clone that still needs a
file older than that, so no snapshot older than KEEP is left when it runs.
"""

import contextlib
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from pyspark import InheritableThread
from pyspark.errors import PySparkAttributeError
from pyspark.sql import SparkSession

from ingestion import entry

OPTIMIZE_EVERY = timedelta(hours=1)
VACUUM_EVERY = timedelta(hours=24)
KEEP = timedelta(days=7)  # at most Delta's default deletedFileRetentionDuration
POOL = "maintenance"  # fair only where the cluster sets spark.scheduler.mode FAIR (plan-v2, C2)
TICK = 60.0  # seconds between two looks at what is due
_NAME = "snapshot_v"

Action = tuple[str, int]  # (op, version): the snapshot's pin, or the store's head


def due(
    now: datetime,
    last: dict[str, datetime],
    snapshots: list[tuple[int, datetime]],
    head: int,
    *,
    every: timedelta,
) -> list[Action]:
    """What to run now, in order. `last` holds when OPTIMIZE and VACUUM last ran, with no
    "vacuum" before the first one; `snapshots` holds each one's (version, taken).

    One snapshot per version: an idle store gets no second one. The newest is never dropped, so
    the head's snapshot stays and no later clone meets a dropped one's files."""
    versions = [version for version, _ in snapshots]
    snapshot = head not in versions and all(now - taken >= every for _, taken in snapshots)
    newest = head if snapshot else max(versions, default=None)
    actions = [("snapshot", head)] if snapshot else []
    actions += [("drop", v) for v, taken in sorted(snapshots) if now - taken > KEEP and v != newest]
    if now - last["optimize"] >= OPTIMIZE_EVERY:
        actions.append(("optimize", head))
    if "vacuum" not in last or now - last["vacuum"] >= VACUUM_EVERY:
        actions.append(("vacuum", head))
    return actions


def snapshots(spark: SparkSession, schema: str) -> list[tuple[int, datetime]]:
    """Each snapshot's (version, taken), the time of its CLONE commit.

    ponytail: one history read per snapshot, up to 336 at the stress cadence; a name that holds
    the time if a tick ever takes minutes."""
    found = []
    for row in spark.sql(f"SHOW TABLES IN {schema} LIKE '{_NAME}*'").collect():
        history = spark.sql(f"DESCRIBE HISTORY {schema}.{row.tableName}")
        taken = history.orderBy("version").first().timestamp  # naive, in the driver's zone
        found.append((int(row.tableName.removeprefix(_NAME)), taken.astimezone(UTC)))
    return found


def snapshot(spark: SparkSession, data: str, schema: str, version: int) -> None:
    # IF NOT EXISTS: without it a second create of one version fails.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {schema}.{_NAME}{version} "
        f"SHALLOW CLONE delta.`{data}/listing_store` VERSION AS OF {version} "
        f"LOCATION '{data}/snapshots/{_NAME}{version}'"
    )


def drop(spark: SparkSession, schema: str, version: int) -> None:
    """The name only: the clone's log stays for the storage's lifecycle rule (plan-v2, C1)."""
    spark.sql(f"DROP TABLE IF EXISTS {schema}.{_NAME}{version}")


def optimize(spark: SparkSession, data: str) -> None:
    spark.sql(f"OPTIMIZE delta.`{data}/listing_store`").collect()


def vacuum(spark: SparkSession, data: str) -> None:
    spark.sql(f"VACUUM delta.`{data}/listing_store`").collect()


def tick(
    spark: SparkSession,
    data: str,
    schema: str,
    last: dict[str, datetime],
    now: datetime,
    *,
    every: timedelta,
    conflicts: tuple[type[Exception], ...],
) -> None:
    """Run what is due, and print a line for each action. An OPTIMIZE that loses to a MERGE
    (`conflicts`) waits for the next hour; any other error is the caller's."""
    head = DeltaTable.forPath(spark, f"{data}/listing_store").history(1).first().version
    for op, version in due(now, last, snapshots(spark, schema), head, every=every):
        line = {"type": "maintenance", "op": op, "version": version}
        if op == "snapshot":
            snapshot(spark, data, schema, version)
        elif op == "drop":
            drop(spark, schema, version)
        elif op == "optimize":
            last[op] = now
            try:
                optimize(spark, data)
            except conflicts:
                line["ran"] = False  # not an Outcome's word: those are spelled in status.py
        else:
            vacuum(spark, data)
            last[op] = now
        entry.say(line)


def start(
    spark: SparkSession,
    data: str,
    *,
    schema: str,
    every: timedelta,
    conflicts: tuple[type[Exception], ...],
    failed: Callable[[Exception], None],
) -> threading.Event:
    """Tick in a daemon thread until the returned Event is set. An action that raises ends the
    thread, after `failed` got the error."""
    stop = threading.Event()

    def loop() -> None:
        try:
            with contextlib.suppress(PySparkAttributeError):  # serverless: no sparkContext
                spark.sparkContext.setLocalProperty("spark.scheduler.pool", POOL)
            last = {"optimize": datetime.now(UTC)}  # no "vacuum": one runs at the start
            while not stop.is_set():
                tick(spark, data, schema, last, datetime.now(UTC), every=every, conflicts=conflicts)
                stop.wait(TICK)
        except Exception as e:
            failed(e)

    InheritableThread(target=loop, name=POOL, daemon=True).start()
    return stop
