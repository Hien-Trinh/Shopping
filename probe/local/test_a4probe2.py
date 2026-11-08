# ruff: noqa
"""A4 probes, round 2: integer precision through mapInPandas, and the iterator pandas UDF."""

from pathlib import Path

import a4probe
from delta.tables import DeltaTable
from pyspark.sql import functions as F


def lines(log: Path, name: str) -> list[str]:
    p = log / name
    return p.read_text().splitlines() if p.exists() else []


def test_q1_big_int_with_a_null_through_map_in_pandas(spark):
    df = spark.createDataFrame([(1, 2**53 + 1), (2, None)], "k long, price_micros long")
    back = sorted(df.repartition(1).mapInPandas(a4probe.identity_frames, df.schema).collect())
    print(
        "\nQ1 mapInPandas",
        [(r.k, r.price_micros) for r in back],
        "exact:",
        back[0].price_micros == 2**53 + 1,
    )


def test_q2_iterator_udf_struct_cache_and_merge(spark, tmp_path):
    log = tmp_path / "log"
    log.mkdir()
    target = str(tmp_path / "t")
    spark.createDataFrame(
        [(i, "Old", "Prev") for i in range(100)], "id long, title string, primary_category string"
    ).write.format("delta").save(target)
    src = spark.createDataFrame(
        [
            (i, "Red", i % 2 == 0, "Uncategorized", 2**53 + 1 if i == 60 else None)
            for i in range(50, 150)
        ],
        "id long, title string, needs_classify boolean, primary_category string, price_micros long",
    ).persist()
    planned = src.count()
    answer = a4probe.answer_udf(str(log))(
        F.col("needs_classify"), F.col("title"), F.col("primary_category")
    )
    source = (
        src.repartition(2)
        .withColumn("a", answer)
        .select("id", "title", "price_micros", "a.primary_category")
    )
    table = DeltaTable.forPath(spark, target).alias("t")
    m = (
        table.merge(source.alias("s"), "t.id = s.id")
        .whenMatchedUpdate(set={"title": "s.title", "primary_category": "s.primary_category"})
        .whenNotMatchedInsert(
            values={"id": "s.id", "title": "s.title", "primary_category": "s.primary_category"}
        )
        .execute()
        .first()
    )
    through = sum(int(c.split(",")[1]) for c in lines(log, "calls.txt"))
    print("\nQ2 planned", planned, "affected", m.num_affected_rows, "rows through the UDF", through)
    print("Q2 loads", len(lines(log, "loads.txt")), "pids", len(set(lines(log, "loads.txt"))))
    got = {r.id: r.primary_category for r in spark.read.format("delta").load(target).collect()}
    print("Q2 sample", got[50], got[51], got[99], got[149])
    big = source.filter("id = 60").first()
    print("Q2 price through the UDF's select exact:", big.price_micros == 2**53 + 1)
    src.unpersist()
