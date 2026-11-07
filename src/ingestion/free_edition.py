"""The checks of the Free Edition run: the ingest query on Databricks' own runtime, on Free
Edition's own storage (docs/specs/step-a3.md). Each check prints one PASS or FAIL line, and never a
time, since the acceptable use policy forbids publishing benchmark results and these logs are
public.

Spark writes the Landing log here, since delta-rs can't commit on a volume. `main` returns
normally rather than through entry.exit_with, since os._exit fails a Databricks run.
"""

import argparse
import os
import shutil
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import pyarrow as pa
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.pandas.types import from_arrow_schema

from ingestion import ingest, landing, replay, tables
from ingestion.envelope import Change, Content
from ingestion.status import Outcome as O

_MERCHANT = "m_1"
_HOUR = "2023-11-14T22"  # the hour of every _submission() below


def _submission(n: int) -> str:
    """A UUIDv7 at 2023-11-14 22:13:20 UTC, as tables.hour reads them."""
    return str(uuid.UUID(f"{1_700_000_000_000:012x}7000{0x8000:04x}{n:012x}"))


def _up(product: str, version: int, title: str = "Red shirt", price: int | str = 10**6) -> Change:
    # model_construct: a bad price has to get past validation, as a poison Change does.
    content = Content.model_construct(
        title=title,
        description="",
        price_micros=price,
        currency="USD",
        availability="in_stock",
        attributes={},
    )
    return Change(_MERCHANT, product, version, "upsert", content)


# Each Change with the Outcome it gets. "tie" holds a higher seq in the earlier commit.
_BACKLOG = [
    [
        (_up("plain", 1), O.WRITTEN),
        (_up("tie", 5, "first"), O.WRITTEN),
        (_up("newer", 3), O.WRITTEN),
        (_up("again", 1), O.WRITTEN),
        (_up("gone", 1), O.WRITTEN),
    ],
    [
        (_up("tie", 5, "second"), O.CONFLICT),
        (_up("newer", 2), O.STALE),
        (_up("again", 1), O.ALREADY_APPLIED),
        (Change(_MERCHANT, "gone", 2, "delete"), O.WRITTEN),
        (_up("spoiled", 1), O.WRITTEN),
        (_up("spoiled", 2, price="free"), O.FAILED),
    ],
]
# In a second batch, so each reads its stored row, a Tombstone for "gone".
_LATER = [
    (_up("plain", 2), O.WRITTEN),
    (_up("gone", 1), O.STALE),
    (_up("newer", 3), O.ALREADY_APPLIED),
]


def _outcome_rows(n: int, changes: list) -> list[tuple]:
    return [(_HOUR, _submission(n), i, o.value) for i, (_, o) in enumerate(changes)]


def _create(
    spark: SparkSession, path: str, schema: pa.Schema, properties: dict, *partition
) -> None:
    builder = DeltaTable.createIfNotExists(spark).location(path)
    builder = builder.addColumns(from_arrow_schema(schema))
    if partition:
        builder = builder.partitionedBy(*partition)
    for name, value in properties.items():
        builder = builder.property(name, value)
    builder.execute()


def _land(spark: SparkSession, data: str, n: int, changes: list) -> None:
    now = datetime.now(UTC)
    entries = [(_submission(n), i, change, now) for i, (change, _) in enumerate(changes)]
    table = spark.createDataFrame(landing.rows(entries))
    table.write.format("delta").mode("append").save(f"{data}/landing_log")


def _run_query(spark: SparkSession, data: str) -> None:
    ingest.start(spark, data, trigger={"availableNow": True}).awaitTermination()


def _outcomes(spark: SparkSession, data: str) -> list[tuple]:  # a list: a duplicate row would show
    return sorted(tuple(r) for r in spark.read.format("delta").load(f"{data}/outcomes").collect())


def _version(spark: SparkSession, data: str, table: str) -> int:
    return DeltaTable.forPath(spark, f"{data}/{table}").history(1).first().version


def _expect(actual, expected) -> None:
    if actual != expected:
        raise ValueError(f"expected {expected}, got {actual}")


def backlog_lands(spark: SparkSession, data: str) -> None:
    _create(spark, f"{data}/landing_log", landing.SCHEMA, landing.PROPERTIES)
    _create(spark, f"{data}/outcomes", tables.OUTCOMES, tables.OUTCOMES_PROPERTIES, "hour")
    for n, changes in enumerate(_BACKLOG):
        _land(spark, data, n, changes)
    _run_query(spark, data)
    landed = [pair for changes in _BACKLOG for pair in changes]
    failed = {i for i, (_, outcome) in enumerate(landed) if outcome is O.FAILED}
    oracle = replay.expected_store([change for change, _ in landed], failed)
    rows = spark.read.format("delta").load(f"{data}/listing_store").collect()
    store = {(r.merchant_id, r.merchant_product_id): replay.fingerprint(r.asDict()) for r in rows}
    _expect(replay.diff(oracle, store), [])


def one_outcome_per_change(spark: SparkSession, data: str) -> None:
    expected = [row for n, changes in enumerate(_BACKLOG) for row in _outcome_rows(n, changes)]
    _expect(_outcomes(spark, data), sorted(expected))


def restart_adds_nothing(spark: SparkSession, data: str) -> None:
    before = _outcomes(spark, data), _version(spark, data, "listing_store")
    _run_query(spark, data)
    _expect((_outcomes(spark, data), _version(spark, data, "listing_store")), before)


def later_commit_reads_its_stored_rows(spark: SparkSession, data: str) -> None:
    before = _outcomes(spark, data)
    _land(spark, data, len(_BACKLOG), _LATER)
    _run_query(spark, data)
    _expect(_outcomes(spark, data), sorted(before + _outcome_rows(len(_BACKLOG), _LATER)))


def store_has_its_layout(spark: SparkSession, data: str) -> None:
    table = DeltaTable.forPath(spark, f"{data}/listing_store")
    detail = table.detail().first()
    _expect(list(detail.clusteringColumns), ["merchant_id", "merchant_product_id"])
    for name in ("delta.enableDeletionVectors", "delta.enableChangeDataFeed"):
        _expect(detail.properties.get(name), "true")
    merges = table.history().filter("operation = 'MERGE'").orderBy(F.desc("version"))
    metrics = merges.first().operationMetrics  # the later commit's, which updates one stored row
    _expect(metrics.get("numTargetDeletionVectorsAdded"), "1")


CHECKS = {
    "the backlog lands as the store oracle says": backlog_lands,
    "one Outcome per Change": one_outcome_per_change,
    "a restart adds nothing": restart_adds_nothing,
    "a later commit reads its stored rows": later_commit_reads_its_stored_rows,
    "the store has its layout": store_has_its_layout,
}


def _passes(name: str, check, spark: SparkSession, data: str) -> bool:
    try:
        check(spark, data)
    except Exception as e:  # one failure must not hide the later checks' lines
        # On one line, whole: the workflow's script cuts it after replacing any email.
        print(f"FAIL {name}: {type(e).__name__}: {' '.join(str(e).split())}", flush=True)
        return False
    print(f"PASS {name}", flush=True)
    return True


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="ingestion-smoke")
    args.add_argument("--data", required=True)  # a string, as ingest.start takes it
    data = args.parse_args(argv).data
    if os.path.exists(data):  # a failed task runs twice, on this same path
        shutil.rmtree(data)
    spark = SparkSession.builder.getOrCreate()
    results = [_passes(name, check, spark, data) for name, check in CHECKS.items()]
    if not all(results):
        raise RuntimeError(f"{results.count(False)} of {len(results)} checks failed")
