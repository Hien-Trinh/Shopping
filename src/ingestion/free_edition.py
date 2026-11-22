"""The checks of the Free Edition run: the ingest query on Databricks' own runtime, on Free
Edition's own storage (docs/specs/step-a3.md). Each check prints one PASS or FAIL line, and never a
time, since the acceptable use policy forbids publishing benchmark results and these logs are
public. `main` ends with "k of n checks passed", which the workflow's script requires, since a
failed task runs twice and a first pass that exits partway with no FAIL line (a SystemExit from a
check, say) leaves only PASS lines.

Spark writes the Landing log here, since delta-rs can't commit on a volume. `main` returns
normally rather than through entry.exit_with, since os._exit fails a Databricks run.

These checks run on `fake`. With `--models`, one more runs the student on the executors, over
Listings it knows (docs/specs/step-a4.md, decision 9).
"""

import argparse
import os
import re
import shutil
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.pandas.types import from_arrow_schema

from ingestion import change_export, classify_udf, ingest, landing, replay, tables
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
# The student's answers on the Mac and on Free Edition's executors (step-a4.md, fact 8).
KNOWN = {
    "Men's cotton crew-neck t-shirt": "Apparel & Accessories > Clothing > Clothing Tops",
    "Stainless steel chef's knife, 8 inch": (
        "Home & Garden > Kitchen & Dining > Kitchen Tools & Utensils"
    ),
    "Wireless bluetooth earbuds with charging case": "Electronics > Audio > Audio Components",
    "Organic green tea, 100 bags": "Food, Beverages & Tobacco > Beverages > Tea & Infusions",
    "Kids' waterproof rain boots": "Apparel & Accessories > Shoes > Baby & Children's Shoes",
    "Yoga mat, non-slip, 6 mm": (
        "Sporting Goods > Fitness & General Exercise Equipment > Yoga & Pilates"
    ),
    "Ceramic coffee mug 350 ml": "Home & Garden > Kitchen & Dining > Tableware",
    "Laptop backpack 15.6 inch": "Luggage & Bags > Backpacks > Laptop Backpacks",
}


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


def _tables(spark: SparkSession, data: str) -> None:
    _create(spark, f"{data}/landing_log", landing.SCHEMA, landing.PROPERTIES)
    _create(spark, f"{data}/outcomes", tables.OUTCOMES, tables.OUTCOMES_PROPERTIES, "hour")


def _land(spark: SparkSession, data: str, n: int, changes: list) -> None:
    now = datetime.now(UTC)
    entries = [(_submission(n), i, change, now) for i, (change, _) in enumerate(changes)]
    table = spark.createDataFrame(landing.rows(entries))
    table.write.format("delta").mode("append").save(f"{data}/landing_log")


def _run_query(
    spark: SparkSession, data: str, classifier: classify_udf.Spec = classify_udf.FAKE
) -> None:
    query = ingest.start(spark, data, trigger={"availableNow": True}, classifier=classifier)
    query.awaitTermination()


def _outcomes(spark: SparkSession, data: str) -> list[tuple]:  # a list: a duplicate row would show
    return sorted(tuple(r) for r in spark.read.format("delta").load(f"{data}/outcomes").collect())


def _version(spark: SparkSession, data: str, table: str) -> int:
    return DeltaTable.forPath(spark, f"{data}/{table}").history(1).first().version


def _expect(actual, expected) -> None:
    if actual != expected:
        raise ValueError(f"expected {expected}, got {actual}")


def backlog_lands(spark: SparkSession, data: str) -> None:
    _tables(spark, data)
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


def export_replays_to_the_store(spark: SparkSession, data: str) -> None:
    """Two files: the store's state at the first start, then one more MERGE's feed, with its
    deletion vector. Replayed, they are the live store."""
    for n in (0, 1):
        if n:
            _land(spark, data, len(_BACKLOG) + n, [(_up("plain", 3), O.WRITTEN)])
            _run_query(spark, data)
        change_export.start(spark, data, trigger={"availableNow": True}).awaitTermination()
    files = change_export.files(spark, data)
    _expect(len(files), 2)
    rows = spark.read.format("delta").load(f"{data}/listing_store").collect()
    store = {(r.merchant_id, r.merchant_product_id): replay.fingerprint(r.asDict()) for r in rows}
    _expect(replay.diff(replay.live(store), replay.replay_exports(files)), [])


def student_classifies(spark: SparkSession, data: str, spec: classify_udf.Spec) -> None:
    """The Listings in KNOWN, through the ingest query on `spec`: each stored with its category,
    at the student's version and not flagged."""
    path = f"{data}/student"
    _tables(spark, path)
    titles = list(KNOWN)
    _land(spark, path, 0, [(_up(f"p{i}", 1, title), O.WRITTEN) for i, title in enumerate(titles)])
    _run_query(spark, path, spec)
    rows = spark.read.format("delta").load(f"{path}/listing_store").collect()
    got = {
        r.merchant_product_id: (r.primary_category, r.taxonomy_version, r.needs_reclassify)
        for r in rows
    }
    _expect(got, {f"p{i}": (KNOWN[t], spec.version, False) for i, t in enumerate(titles)})


CHECKS = {
    "the backlog lands as the store oracle says": backlog_lands,
    "one Outcome per Change": one_outcome_per_change,
    "a restart adds nothing": restart_adds_nothing,
    "a later commit reads its stored rows": later_commit_reads_its_stored_rows,
    "the store has its layout": store_has_its_layout,
    "the export replays to the store": export_replays_to_the_store,
}
STUDENT = "the student classifies on the executors"


# --- probe/a8-free-edition: step A8's statements on serverless (docs/specs/step-a8.md, question 3).
# Not for main. Each line says only PASS, or FAIL with the error's class and condition.
_NAMED, _MANAGED = "default.probe_a8_named", "default.probe_a8_managed"


def _store(data: str) -> str:
    return f"delta.`{data}/listing_store`"


def _clone(spark: SparkSession, data: str, target: str, location: str = "") -> None:
    head = _version(spark, data, "listing_store")
    rows = spark.read.format("delta").load(f"{data}/listing_store").collect()
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} SHALLOW CLONE {_store(data)} VERSION AS OF {head}"
        + (f" LOCATION '{location}'" if location else "")
    )
    _expect(len(spark.sql(f"SELECT * FROM {target}").collect()), len(rows))


def probe_named_with_location(spark: SparkSession, data: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {_NAMED}")
    _clone(spark, data, _NAMED, f"{data}/snapshots/named")


def probe_managed(spark: SparkSession, data: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {_MANAGED}")
    _clone(spark, data, _MANAGED)


def probe_path(spark: SparkSession, data: str) -> None:
    _clone(spark, data, f"delta.`{data}/snapshots/path`")


def probe_optimize(spark: SparkSession, data: str) -> None:
    spark.sql(f"OPTIMIZE {_store(data)}").collect()


def probe_vacuum(spark: SparkSession, data: str) -> None:
    spark.sql(f"VACUUM {_store(data)}").collect()


def probe_history(spark: SparkSession, data: str) -> None:
    """The age of a snapshot is its CLONE commit's time (decision 4)."""
    for target in (_NAMED, _MANAGED, f"delta.`{data}/snapshots/path`"):
        try:
            _expect(spark.sql(f"DESCRIBE HISTORY {target}").collect()[-1].operation, "CLONE")
            return
        except Exception as e:
            last = e
    raise last


def probe_drop(spark: SparkSession, data: str) -> None:
    errors = []
    for target in (_NAMED, _MANAGED):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {target}")
        except Exception as e:
            errors.append(e)
    if errors:
        raise errors[0]


CHECKS |= {
    "probe: a clone to a named table with a location": probe_named_with_location,
    "probe: a clone to a managed named table": probe_managed,
    "probe: a clone to a path": probe_path,
    "probe: optimize": probe_optimize,
    "probe: vacuum": probe_vacuum,
    "probe: a clone's history starts with CLONE": probe_history,
    "probe: drop": probe_drop,
}


def _passes(name: str, check, spark: SparkSession, data: str) -> bool:
    try:
        check(spark, data)
    except Exception as e:  # one failure must not hide the later checks' lines
        # On one line, whole: the workflow's script cuts it after replacing any email.
        text = " ".join(str(e).split())
        if name.startswith("probe"):  # the condition only: these logs are public
            found = re.search(r"\[[A-Z_.0-9]+\]|SQLSTATE: \w+", text)
            text = found.group() if found else "no condition"
        print(f"FAIL {name}: {type(e).__name__}: {text}", flush=True)
        return False
    print(f"PASS {name}", flush=True)
    return True


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="ingestion-smoke")
    args.add_argument("--data", required=True)  # a string, as ingest.start takes it
    args.add_argument("--models", type=Path)  # with train/ beside it: adds the student's check
    a = args.parse_args(argv)
    if os.path.exists(a.data):  # a failed task runs twice, on this same path
        shutil.rmtree(a.data)
    spark = SparkSession.builder.getOrCreate()
    checks = dict(CHECKS)
    if a.models:  # its Spec inside the check, so a missing file prints FAIL beside the other lines
        checks[STUDENT] = lambda s, d: student_classifies(s, d, classify_udf.Spec.student(a.models))
    results = [_passes(name, check, spark, a.data) for name, check in checks.items()]
    print(f"{results.count(True)} of {len(results)} checks passed", flush=True)
    if not all(results):
        raise RuntimeError(f"{results.count(False)} of {len(results)} checks failed")
