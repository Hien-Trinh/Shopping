"""Probes for step A7 (Change Export), on local Spark. Run:
Copy into tests/spark/ (its fixtures), then: .venv-spark/bin/pytest tests/spark/test_probe_a7.py -s
"""

import os

import pyarrow.parquet as pq  # noqa: E402
import pytest  # noqa: E402
from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from support import delete, up  # noqa: E402
from test_ingest import commit, land, run  # noqa: E402

KEY = ("merchant_id", "merchant_product_id")


def feed(spark, data, checkpoint, seen, fail_at=None, **options):
    def batch(df, batch_id):
        rows = df.select("merchant_product_id", "_change_type", "_commit_version", "is_tombstone")
        seen.append((batch_id, sorted(tuple(r) for r in rows.collect())))
        if fail_at:
            raise RuntimeError("crash after the read")

    reader = spark.readStream.format("delta").option("readChangeFeed", "true")
    for k, v in options.items():
        reader = reader.option(k, v)
    q = reader.load(f"{data}/listing_store").writeStream.foreachBatch(batch)
    q = q.option("checkpointLocation", str(checkpoint)).trigger(availableNow=True).start()
    q.awaitTermination()
    return q


def test_probe(spark, tmp_path):  # noqa: F811
    data = tmp_path
    store = f"{data}/listing_store"
    land(data, ["init"], commit(0, [up("a", 1), up("b", 1)]))
    run(spark, data)
    land(data, commit(1, [up("a", 2), delete("b", 2)]))
    run(spark, data)
    DeltaTable.forPath(spark, store).optimize().executeCompaction()
    land(data, commit(2, [up("c", 1)]))
    run(spark, data)
    print(
        "\nHISTORY",
        [(h.version, h.operation) for h in DeltaTable.forPath(spark, store).history().collect()][
            ::-1
        ],
    )

    seen = []
    feed(spark, data, tmp_path / "cp_none", seen)
    print("1 NO startingVersion:", seen)

    seen = []
    feed(spark, data, tmp_path / "cp_zero", seen, startingVersion=0)
    print("2 startingVersion=0:", seen)

    seen = []
    feed(spark, data, tmp_path / "cp_one", seen, startingVersion=0, maxFilesPerTrigger=1)
    print("3 startingVersion=0, maxFilesPerTrigger=1:", seen)

    # an OPTIMIZE alone after the checkpoint: is the batch function called?
    DeltaTable.forPath(spark, store).optimize().executeCompaction()
    land(data, commit(3, [up("d", 1)]))
    run(spark, data)
    DeltaTable.forPath(spark, store).optimize().executeCompaction()
    print(
        "HISTORY",
        [(h.version, h.operation) for h in DeltaTable.forPath(spark, store).history().collect()][
            ::-1
        ],
    )
    seen = []
    q = feed(spark, data, tmp_path / "cp_zero", seen)
    print(
        "4 restart after a MERGE and an OPTIMIZE:",
        seen,
        "progress batches",
        [p.batchId for p in q.recentProgress],
    )
    DeltaTable.forPath(spark, store).optimize().executeCompaction()
    seen = []
    q = feed(spark, data, tmp_path / "cp_zero", seen)
    print(
        "5 restart after nothing new:",
        seen,
        [(p.batchId, p.numInputRows) for p in q.recentProgress],
    )

    # a crash in the batch: the replay's id and rows
    land(data, commit(4, [up("a", 3), up("e", 1)]))
    run(spark, data)
    seen = []
    with pytest.raises(Exception):  # noqa: B017
        feed(spark, data, tmp_path / "cp_zero", seen, fail_at=True)
    land(data, commit(5, [up("f", 1)]))
    run(spark, data)
    feed(spark, data, tmp_path / "cp_zero", seen)
    print("6 crash, then more lands, then replay:", seen)

    # one file: what Spark writes, and who reads it
    all_rows = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .load(store)
    )
    latest = (
        all_rows.filter("_change_type != 'update_preimage'")
        .groupBy(*KEY)
        .agg(F.max_by(F.struct("*"), "_commit_version").alias("r"))
        .select("r.*")
        .withColumn(
            "op",
            F.when(F.col("is_tombstone") | (F.col("_change_type") == "delete"), "delete").otherwise(
                "upsert"
            ),
        )
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
    out = f"{data}/export/000000000000-000000000009.parquet"
    latest.coalesce(1).write.parquet(out)
    print("7 files:", sorted(os.listdir(out)))
    print(
        "7 pyarrow reads the directory:",
        pq.read_table(out).select(["merchant_product_id", "op", "source_version"]).to_pylist(),
    )
    try:
        latest.coalesce(1).write.parquet(out)
    except Exception as e:
        print("7 second write:", type(e).__name__, str(e)[:120])
    back = spark.read.parquet(f"{data}/export/*.parquet").select(
        "merchant_product_id", "op", "_metadata.file_name", "_metadata.file_modification_time"
    )
    print("7 spark reads export/*.parquet:", [tuple(r) for r in back.collect()])
    empty = latest.filter("false")
    empty.coalesce(1).write.parquet(f"{data}/export/empty.parquet")
    print("7 an empty frame writes:", sorted(os.listdir(f"{data}/export/empty.parquet")))
    print(
        "8 store schema nullable:",
        [(f.name, f.nullable) for f in spark.read.parquet(out).schema][:4],
    )
