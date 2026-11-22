# ruff: noqa
"""Probe for step A8: clones, OPTIMIZE, VACUUM and pools on local delta-spark."""

import os
import threading
from pathlib import Path

from delta.tables import DeltaTable
from pyspark import InheritableThread
from support import delete, up
from test_change_export import export, names
from test_ingest import commit, land, run, version


def show(label, value):
    print(f"\nPROBE {label}: {value}")


def tree(path):
    return sorted(
        str(p.relative_to(path))
        for p in Path(path).rglob("*")
        if p.is_file() and not p.name.endswith(".crc")
    )


def test_probe(spark, tmp_path):
    store = str(tmp_path / "listing_store")
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1), up("c", 1)]))
    run(spark, tmp_path)
    land(tmp_path, commit(1, [up("a", 2), delete("b", 2)]))
    run(spark, tmp_path)
    export(spark, tmp_path)
    v = version(spark, tmp_path / "listing_store")
    show("store version", v)
    show(
        "history ops",
        [
            (r.version, r.operation)
            for r in DeltaTable.forPath(spark, store).history().orderBy("version").collect()
        ],
    )

    # a) clone at a pinned version, path-based
    clone = str(tmp_path / "snapshots" / f"snapshot_v{v - 1}")
    spark.sql(f"CREATE TABLE delta.`{clone}` SHALLOW CLONE delta.`{store}` VERSION AS OF {v - 1}")
    show("clone tree", tree(clone))
    a = sorted(
        map(
            tuple,
            spark.read.format("delta")
            .load(clone)
            .select("merchant_product_id", "source_version", "is_tombstone")
            .collect(),
        )
    )
    b = sorted(
        map(
            tuple,
            spark.read.format("delta")
            .option("versionAsOf", v - 1)
            .load(store)
            .select("merchant_product_id", "source_version", "is_tombstone")
            .collect(),
        )
    )
    show("clone == store@v-1", a == b)
    show("clone rows", a)
    h = DeltaTable.forPath(spark, clone).history().collect()
    show("clone history", [(r.version, r.operation, r.operationParameters) for r in h])
    show(
        "clone detail props",
        DeltaTable.forPath(spark, clone).detail().select("properties", "clusteringColumns").first(),
    )
    # j) again: IF NOT EXISTS, and without
    try:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS delta.`{clone}` SHALLOW CLONE delta.`{store}` VERSION AS OF {v - 1}"
        )
        show(
            "IF NOT EXISTS again",
            "ok, versions "
            + str([r.version for r in DeltaTable.forPath(spark, clone).history().collect()]),
        )
    except Exception as e:
        show("IF NOT EXISTS again", type(e).__name__ + str(e)[:200])
    try:
        spark.sql(
            f"CREATE TABLE delta.`{clone}` SHALLOW CLONE delta.`{store}` VERSION AS OF {v - 1}"
        )
        show("plain again", "ok")
    except Exception as e:
        show("plain again", type(e).__name__ + str(e)[:200])
    try:
        spark.sql(
            f"CREATE OR REPLACE TABLE delta.`{clone}` SHALLOW CLONE delta.`{store}` VERSION AS OF {v - 1}"
        )
        show(
            "OR REPLACE again",
            "ok, versions "
            + str([r.version for r in DeltaTable.forPath(spark, clone).history().collect()]),
        )
    except Exception as e:
        show("OR REPLACE again", type(e).__name__ + str(e)[:200])

    # d) OPTIMIZE: incremental, twice
    before = version(spark, tmp_path / "listing_store")
    m = DeltaTable.forPath(spark, store).optimize().executeCompaction().first().metrics
    show(
        "optimize 1",
        (m.numFilesAdded, m.numFilesRemoved, version(spark, tmp_path / "listing_store") - before),
    )
    spark.sql(f"OPTIMIZE delta.`{store}`")
    show("optimize 2 commits added", version(spark, tmp_path / "listing_store") - before)

    # f) VACUUM default: commits? export file?
    before = version(spark, tmp_path / "listing_store")
    out = spark.sql(f"VACUUM delta.`{store}`").collect()
    show("vacuum default out", out)
    show(
        "vacuum commits",
        [
            (r.version, r.operation)
            for r in DeltaTable.forPath(spark, store).history().orderBy("version").collect()
            if r.version > before
        ],
    )
    n = names(tmp_path)
    export(spark, tmp_path)
    show("export files before/after vacuum+optimize", (n, names(tmp_path)))
    show("clone still reads", spark.read.format("delta").load(clone).count())

    # b) VACUUM RETAIN 0 on source breaks the clone?
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        spark.sql(f"VACUUM delta.`{store}` RETAIN 0 HOURS")
        try:
            show("clone after vacuum 0", spark.read.format("delta").load(clone).count())
        except Exception as e:
            show("clone after vacuum 0", type(e).__name__ + " " + str(e)[:300])
        try:
            spark.sql(
                f"CREATE TABLE delta.`{clone}x` SHALLOW CLONE delta.`{store}` VERSION AS OF {v - 1}"
            )
            show(
                "clone of a vacuumed version", spark.read.format("delta").load(clone + "x").count()
            )
        except Exception as e:
            show("clone of a vacuumed version", type(e).__name__ + " " + str(e)[:300])
    finally:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

    # h) DROP path-based
    try:
        spark.sql(f"DROP TABLE delta.`{clone}`")
        show("drop path table", ("ok", os.path.exists(clone)))
    except Exception as e:
        show("drop path table", type(e).__name__ + " " + str(e)[:200])

    # c) pools
    show("scheduler.mode", spark.sparkContext.getConf().get("spark.scheduler.mode", "unset"))
    seen = {}

    def work():
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "maintenance")
        seen["thread"] = spark.sparkContext.getLocalProperty("spark.scheduler.pool")

    t = InheritableThread(target=work)
    t.start()
    t.join()
    seen["main"] = spark.sparkContext.getLocalProperty("spark.scheduler.pool")
    t = threading.Thread(target=work)
    t.start()
    t.join()
    seen["main after plain thread"] = spark.sparkContext.getLocalProperty("spark.scheduler.pool")
    show("pools", seen)
