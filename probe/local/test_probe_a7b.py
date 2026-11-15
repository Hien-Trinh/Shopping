"""Probe for step A7: can Spark alone tell a complete directory write from a partial one?"""

import os

from pyspark.errors import AnalysisException


def test_probe(spark, tmp_path):
    out = f"{tmp_path}/x.parquet"
    spark.range(3).coalesce(1).write.parquet(out)
    print("\n1 read _SUCCESS:", spark.read.text(f"{out}/_SUCCESS").count())
    os.remove(f"{out}/_SUCCESS")
    try:
        spark.read.text(f"{out}/_SUCCESS").count()
    except AnalysisException as e:
        print("2 no _SUCCESS:", e.getCondition())
    spark.range(5).coalesce(1).write.mode("overwrite").parquet(out)
    print(
        "3 overwrite:",
        sorted(f for f in os.listdir(out) if not f.startswith(".")),
        spark.read.parquet(out).count(),
    )
    print(
        "4 committer:",
        spark.conf.get("spark.sql.sources.commitProtocolClass"),
        spark.sparkContext._jsc.hadoopConfiguration().get(
            "mapreduce.fileoutputcommitter.algorithm.version"
        ),
    )
