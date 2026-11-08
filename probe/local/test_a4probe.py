# ruff: noqa
"""A4's local Spark probes. Run from the worktree root:
JAVA_HOME=$(/usr/libexec/java_home -v 21) PYTHONPATH=$S:tests/spark:tests .venv-spark/bin/pytest -p conftest -s $S/test_a4probe.py
"""

import functools
import os
from pathlib import Path

import a4probe
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from ingestion import student, taxonomy

RO = Path(os.environ["A4_MODELS_RO"])


def lines(log: Path, name: str) -> list[str]:
    p = log / name
    return p.read_text().splitlines() if p.exists() else []


def clear(log: Path) -> None:
    for name in ("loads.txt", "calls.txt"):
        (log / name).unlink(missing_ok=True)


def texts(spark, n):
    return spark.range(n).select(
        F.col("id"), F.lit("Red shirt").alias("title"), F.lit("").alias("description")
    )


def test_p1_loads_once_per_worker_across_actions(spark, tmp_path):
    df = texts(spark, 2000)
    for by_ref in (False, True):
        clear(tmp_path)
        f = a4probe.frames(str(tmp_path), by_ref=by_ref)
        for _ in range(3):
            df.repartition(4).mapInPandas(f, df.schema).collect()
        loads = lines(tmp_path, "loads.txt")
        print(
            f"\nP1 by_ref={by_ref} loads",
            len(loads),
            "distinct load pids",
            len({x.split(",")[0] for x in loads}),
        )
    loads, calls = lines(tmp_path, "loads.txt"), lines(tmp_path, "calls.txt")
    print("P1 calls", len(calls), "call pids", len({c.split(",")[0] for c in calls}))
    print("P1 defaultParallelism", spark.sparkContext.defaultParallelism)


def test_p2_partitions_reaching_the_function(spark, tmp_path):
    df = spark.range(1000).withColumn("k", F.col("id") % 100)
    planned = df.groupBy("k").applyInPandas(lambda g: g, df.schema)
    f = a4probe.frames(str(tmp_path))
    for label, shuffle in (("conftest 4", "4"), ("default 200", "200")):
        spark.conf.set("spark.sql.shuffle.partitions", shuffle)
        for repart in (None, 2, 8):
            clear(tmp_path)
            src = planned if repart is None else planned.repartition(repart)
            src.mapInPandas(f, df.schema).collect()
            parts = {c.split(",")[1] for c in lines(tmp_path, "calls.txt")}
            rows = [int(c.split(",")[2]) for c in lines(tmp_path, "calls.txt")]
            print(f"\nP2 shuffle {label} repartition {repart}: partitions {len(parts)} rows {rows}")
    spark.conf.set("spark.sql.shuffle.partitions", "4")


def test_p3_merge_source_evaluations(spark, tmp_path):
    log = tmp_path / "log"
    log.mkdir()
    target = str(tmp_path / "t")
    spark.range(100).select(
        "id", F.lit("0").alias("title"), F.lit("").alias("description")
    ).write.format("delta").save(target)
    src = texts(spark, 150).filter("id >= 50").persist()
    src.count()
    f = a4probe.frames(str(log))
    source = src.repartition(2).mapInPandas(f, src.schema)
    table = DeltaTable.forPath(spark, target).alias("t")
    merged = (
        table.merge(source.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
        .first()
    )
    after_merge = sum(int(c.split(",")[2]) for c in lines(log, "calls.txt"))
    print("\nP3 merge affected", merged.num_affected_rows, "rows through the function", after_merge)
    source.count()
    after_count = sum(int(c.split(",")[2]) for c in lines(log, "calls.txt"))
    print("P3 after source.count()", after_count)
    print("P3 materializeSource", spark.conf.get("spark.databricks.delta.merge.materializeSource"))
    src.unpersist()


def test_p4_partial_in_apply_in_pandas(spark):
    df = spark.range(10).withColumn("k", F.col("id") % 3).withColumn("v", F.lit(""))
    out = df.groupBy("k").applyInPandas(functools.partial(a4probe.keyed, "ver-1"), df.schema)
    print("\nP4", sorted({r.v for r in out.collect()}))


def test_p5_real_student_from_read_only_plain_files(spark, tmp_path):
    tax = taxonomy.load()
    version = student.version(tax.version)
    index = student.index_path(RO, student.FT_REVISION)
    print("\nP5 index exists", index.exists(), index.name)
    titles = [
        "Men's cotton crew-neck t-shirt",
        "Stainless steel chef's knife, 8 inch",
        "Wireless bluetooth earbuds with charging case",
        "Organic green tea, 100 bags",
        "Kids' waterproof rain boots",
        "Yoga mat, non-slip, 6 mm",
        "Ceramic coffee mug 350 ml",
        "Laptop backpack 15.6 inch",
    ]
    df = spark.createDataFrame([(t, "") for t in titles], "title string, description string")
    schema = "title string, description string, category string, confidence double"
    f = a4probe.frames(str(tmp_path), "student", str(RO), str(index), version)
    for _ in range(2):
        rows = df.repartition(2).mapInPandas(f, schema).collect()
    for r in rows:
        print(f"P5 {r.title!r} -> {r.category} {r.confidence:.2f}")
    loads, calls = lines(tmp_path, "loads.txt"), lines(tmp_path, "calls.txt")
    print("P5 loads", len(loads), "calls", len(calls))
    print(
        "P5 worker max RSS MB before load", sorted({int(x.split(",")[1]) // 2**20 for x in loads})
    )
    print("P5 worker max RSS MB", sorted({int(c.split(",")[3]) // 2**20 for c in calls}))
