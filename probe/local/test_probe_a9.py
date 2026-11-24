# ruff: noqa
"""Probe for step A9: what the check job reads on local delta-spark."""
import shutil, time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from support import delete, up
from test_change_export import export, names
from test_ingest import commit, land, run, version

from ingestion import store_maintenance as sm


def show(label, value):
    print(f"\nPROBE {label}: {value}")


def attempt(label, f):
    try:
        show(label, f())
    except Exception as e:
        show(label, f"RAISED {type(e).__name__}: {' '.join(str(e).split())[:300]}")


def test_probe(spark, tmp_path, schema):
    data = str(tmp_path)
    # no export directory at all, then an empty one
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1), up("c", 1)]))
    attempt("read export, no directory", lambda: spark.read.parquet(f"{data}/export/*.parquet").count())
    (tmp_path / "export").mkdir()
    attempt("read export, empty directory", lambda: spark.read.parquet(f"{data}/export/*.parquet").count())
    attempt("read export dir itself, empty", lambda: spark.read.parquet(f"{data}/export").count())
    t0 = datetime.now(UTC)
    run(spark, tmp_path)
    export(spark, tmp_path)
    now = datetime.now(UTC)
    sm.tick(spark, data, schema, {"optimize": now, "vacuum": now}, now, every=timedelta(hours=6), conflicts=())
    land(tmp_path, commit(1, [up("a", 2), delete("b", 2)]))
    run(spark, tmp_path)
    land(tmp_path, commit(2, [up("d", 1)]))
    run(spark, tmp_path)
    export(spark, tmp_path)  # one file over two MERGEs?
    show("export names", names(tmp_path))
    # an export directory emptied by a lifecycle rule
    (tmp_path / "export" / "000000000099-000000000099.parquet").mkdir()
    attempt("read export with one empty file-directory", lambda: spark.read.parquet(f"{data}/export/*.parquet").count())
    df = spark.read.parquet(f"{data}/export/*.parquet")
    meta = df.select("_metadata.file_path", "_metadata.file_modification_time", "_metadata.file_name").distinct().collect()
    show("metadata", [(r.file_path.rsplit("/", 2)[-2], r.file_modification_time, type(r.file_modification_time).__name__) for r in meta])
    show("metadata schema", df.select("_metadata.file_modification_time").schema.simpleString())
    show("t0 utc / now utc", (t0, datetime.now(UTC)))
    show("mtime as epoch vs now", df.select(F.unix_timestamp("_metadata.file_modification_time").alias("m")).agg(F.min("m"), F.max("m")).first()[:] + (time.time(),))
    # the Landing log, whole, with its commit versions
    log = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0).load(f"{data}/landing_log")
    show("landing feed columns", log.columns)
    show("landing feed rows", sorted((r._commit_version, r.seq, r.merchant_product_id, r.source_version, r.op, r._change_type) for r in log.collect()))
    plain = spark.read.format("delta").load(f"{data}/landing_log")
    show("landing plain columns", plain.columns)
    attempt("landing plain with row commit version", lambda: plain.select("_metadata.row_commit_version").count())
    # a clone by path, its pin
    snaps = sorted(p.name for p in (tmp_path / "snapshots").iterdir())
    show("snapshot dirs", snaps)
    clone = f"{data}/snapshots/{snaps[0]}"
    show("clone rows by path", spark.read.format("delta").load(clone).count())
    h = DeltaTable.forPath(spark, clone).history().orderBy("version").first()
    show("clone history", (h.operation, h.operationParameters, h.timestamp))
    # store history: which commits are MERGEs
    hist = DeltaTable.forPath(spark, f"{data}/listing_store").history().orderBy("version").collect()
    show("store history", [(r.version, r.operation) for r in hist])
    # percentiles
    show("percentile", spark.range(1, 101).agg(F.expr("percentile(id, 0.99)"), F.expr("percentile(id, 0.5)")).first()[:])
    # store time travel after a default vacuum
    spark.sql(f"VACUUM delta.`{data}/listing_store`").collect()
    attempt("store versionAsOf 1 after VACUUM", lambda: spark.read.format("delta").option("versionAsOf", 1).load(f"{data}/listing_store").count())
    # dropped clone still readable by path
    spark.sql(f"DROP TABLE {schema}.{snaps[0]}")
    attempt("dropped clone by path", lambda: len(spark.read.format("delta").load(clone).collect()))
