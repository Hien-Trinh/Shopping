# ruff: noqa
from pathlib import Path

from delta.tables import DeltaTable
from support import delete, up
from test_ingest import commit, land, run


def show(label, value):
    print(f"\nPROBE {label}: {value}")


def rows(spark, path):
    spark.catalog.clearCache()
    try:
        return sorted(
            r.merchant_product_id for r in spark.read.format("delta").load(path).collect()
        )
    except Exception as e:
        return type(e).__name__ + " " + str(e)[:160].replace("\n", " ")


def parquet(path):
    return sorted(p.name[:14] for p in Path(path).glob("*.parquet"))


def test_probe(spark, tmp_path):
    store = str(tmp_path / "listing_store")
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1), up("c", 1)]))
    run(spark, tmp_path)
    land(tmp_path, commit(1, [up("a", 2), delete("b", 2)]))
    run(spark, tmp_path)
    clone = str(tmp_path / "snapshots" / "snapshot_v1")
    spark.sql(f"CREATE TABLE delta.`{clone}` SHALLOW CLONE delta.`{store}` VERSION AS OF 1")
    show("files before optimize", parquet(store))
    DeltaTable.forPath(spark, store).optimize().executeCompaction()
    show("files after optimize", parquet(store))
    spark.sql(f"VACUUM delta.`{store}`")
    show("files after default vacuum", parquet(store))
    show("clone after default vacuum", rows(spark, clone))
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        spark.sql(f"VACUUM delta.`{store}` RETAIN 0 HOURS")
        show("files after vacuum 0", parquet(store))
        show("clone after vacuum 0", rows(spark, clone))
        spark.sql(f"CREATE TABLE delta.`{clone}x` SHALLOW CLONE delta.`{store}` VERSION AS OF 1")
        show("new clone of vacuumed v1", rows(spark, clone + "x"))
        show("store head", rows(spark, store))
        # the clone's own VACUUM / the clone dir removal leave the store alone
        import shutil

        shutil.rmtree(clone)
        show("store after rmtree(clone)", rows(spark, store))
    finally:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    try:
        spark.sql(f"CREATE OR REPLACE TABLE delta.`{clone}y` SHALLOW CLONE delta.`{store}`")
        spark.sql(f"CREATE OR REPLACE TABLE delta.`{clone}y` SHALLOW CLONE delta.`{store}`")
        show(
            "OR REPLACE twice",
            [r.version for r in DeltaTable.forPath(spark, clone + "y").history().collect()],
        )
    except Exception as e:
        show("OR REPLACE twice", str(e)[:400].replace("\n", " "))
