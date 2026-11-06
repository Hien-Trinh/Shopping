# ruff: noqa
"""What the ingest query calls, on Free Edition's serverless. One PASS or FAIL line per check, each
independent of the last, and never a time (the acceptable use policy; the logs are public)."""

import importlib.metadata
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path

ROOT = sys.argv[1] if len(sys.argv) > 1 else ""  # a Python worker imports this module too
RESULTS = f"{ROOT}/results.txt"
HOUR = "2023-11-14T22"
lines: list[str] = []


def say(line: str) -> None:
    print(line, flush=True)
    lines.append(line)
    with open(RESULTS, "w") as f:  # whole file each time: volumes have no append
        f.write("\n".join(lines) + "\n")


def check(name: str):
    def run(fn):
        try:
            detail = fn()
            say(f"PASS {name}" + (f": {detail}" if detail else ""))
        except BaseException as e:
            text = " ".join(str(e).split())[:700]
            say(f"FAIL {name}: {type(e).__module__}.{type(e).__name__}: {text}")

    return run


def session():
    from pyspark.sql import SparkSession

    builder = SparkSession.builder
    if os.environ.get("PROBE_LOCAL"):
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(
            builder.master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.ui.enabled", False)
        )
    return builder.getOrCreate()


def submission(n: int) -> str:
    return str(uuid.UUID(f"{1_700_000_000_000:012x}7000{0x8000:04x}{n:012x}"))


def main() -> int:
    @check("python")
    def _():
        import platform

        return f"{sys.version.split()[0]} {platform.machine()}"

    @check("versions")
    def _():
        out = []
        names = (
            "pyspark", "databricks-connect", "delta-spark", "pandas", "pyarrow", "deltalake",
            "pydantic", "numpy", "fastapi", "fastembed", "ingestion",
        )  # fmt: skip
        for name in names:
            try:
                out.append(f"{name}={importlib.metadata.version(name)}")
            except importlib.metadata.PackageNotFoundError:
                out.append(f"{name}=none")
        return " ".join(out)

    @check("wheel has no Requires-Dist")
    def _():
        assert not importlib.metadata.requires("ingestion"), importlib.metadata.requires(
            "ingestion"
        )

    @check("pypi by url")
    def _():
        with urllib.request.urlopen("https://pypi.org/simple/fastembed/", timeout=20) as r:
            return r.status

    @check("pypi by pip")
    def _():
        with tempfile.TemporaryDirectory() as d:
            args = [sys.executable, "-m", "pip", "download", "-q", "--no-deps", "-d", d]
            done = subprocess.run(
                [*args, "fastembed==0.9.0"], capture_output=True, text=True, timeout=120
            )
            assert done.returncode == 0, done.stderr[-300:]

    @check("python file io on the volume")
    def _():
        d = Path(ROOT) / "io"
        d.mkdir(parents=True, exist_ok=True)
        (d / "a").write_text("x")
        (d / "a").rename(d / "b")
        assert (d / "b").read_text() == "x"
        shutil.rmtree(d)

    for module in ("delta.tables", "delta.exceptions", "deltalake", "ingestion.ingest"):

        @check(f"import {module}")
        def _(module=module):
            __import__(module)

    spark = session()

    @check("session")
    def _():
        return f"spark {spark.version}, connect {spark.conf.get('spark.databricks.clusterUsageTags.clusterName', 'n/a') != 'n/a'}"

    from delta.tables import DeltaTable
    from pyspark.errors import AnalysisException
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    t1 = f"{ROOT}/t1"

    @check("delta save and load by path")
    def _():
        spark.range(3).write.format("delta").save(t1)
        assert spark.read.format("delta").load(t1).count() == 3

    @check("forPath, history, isDeltaTable, toDF")
    def _():
        table = DeltaTable.forPath(spark, t1)
        assert table.history(1).first().version == 0
        assert table.history().agg(F.min("version")).first()[0] == 0
        assert DeltaTable.isDeltaTable(spark, t1)
        assert not DeltaTable.isDeltaTable(spark, f"{ROOT}/absent")
        assert not table.toDF().isEmpty()

    @check("versionAsOf by path")
    def _():
        spark.range(3, 5).write.format("delta").mode("append").save(t1)
        assert spark.read.format("delta").option("versionAsOf", 0).load(t1).count() == 3

    @check("forPath of a missing table")
    def _():
        try:
            DeltaTable.forPath(spark, f"{ROOT}/absent")
        except Exception as e:
            return f"{type(e).__name__}: {' '.join(str(e).split())[:200]}"
        raise AssertionError("no error")

    @check("read of a missing path")
    def _():
        try:
            spark.read.text(f"{ROOT}/absent").isEmpty()
        except AnalysisException as e:
            return f"condition {e.getCondition()}"
        raise AssertionError("no error")

    store = f"{ROOT}/t_store"

    @check("createIfNotExists location clusterBy properties")
    def _():
        builder = DeltaTable.createIfNotExists(spark).location(store)
        columns = StructType([StructField("k", StringType(), False), StructField("v", LongType())])
        builder = builder.addColumns(columns).clusterBy("k")
        builder = builder.property("delta.enableDeletionVectors", "true")
        builder = builder.property("delta.autoOptimize.optimizeWrite", "true")
        builder.property("delta.enableChangeDataFeed", "true").execute()
        detail = DeltaTable.forPath(spark, store).detail().first()
        wanted = (
            "delta.enableDeletionVectors",
            "delta.autoOptimize.optimizeWrite",
            "delta.enableChangeDataFeed",
        )
        assert all(detail.properties.get(p) == "true" for p in wanted), detail.properties
        assert list(detail.clusteringColumns) == ["k"], detail.clusteringColumns
        return f"reader {detail.minReaderVersion} writer {detail.minWriterVersion}"

    @check("merge by path, conditional, execute's metrics")
    def _():
        def merge(rows):
            src = spark.createDataFrame(rows, "k string, v bigint").alias("s")
            m = DeltaTable.forPath(spark, store).alias("t").merge(src, "t.k = s.k")
            return m.whenMatchedUpdateAll("s.v > t.v").whenNotMatchedInsertAll().execute()

        merge([("a", 1), ("b", 1)])
        out = merge([("a", 2), ("b", 0)])
        return f"{type(out).__name__} {out.columns if out is not None else None} {out.first() if out is not None else None}"

    @check("deletion vectors written by the merge")
    def _():
        row = DeltaTable.forPath(spark, store).history(1).first()
        metrics = {k: v for k, v in row.operationMetrics.items() if "eletionVector" in k}
        return f"{row.operation} {metrics}"

    @check("append with txnAppId and txnVersion skips a replay")
    def _():
        path = f"{ROOT}/t_txn"
        for _attempt in range(2):
            w = spark.createDataFrame([(1,)], "a int").write.format("delta").mode("append")
            w.option("txnAppId", "app").option("txnVersion", 7).save(path)
        assert spark.read.format("delta").load(path).count() == 1

    @check("left_semi with broadcast, on a time-travel read")
    def _():
        left = spark.read.format("delta").option("versionAsOf", 0).load(t1)
        keys = F.broadcast(spark.createDataFrame([(1,)], "id long"))
        assert left.join(keys, ["id"], "left_semi").count() == 1

    @check("stream: change feed by path, foreachBatch, applyInPandas with our package, persist")
    def _():
        source, out, ckpt = f"{ROOT}/t_src", f"{ROOT}/t_out", f"{ROOT}/ckpt"
        row = spark.createDataFrame([(1, "a")], "id long, v string")
        row.write.format("delta").option("delta.enableChangeDataFeed", "true").save(source)
        row.write.format("delta").mode("append").save(source)

        def batch_fn(df, batch_id):
            import ingestion.landing  # noqa: F401, the package in foreachBatch's own process

            def group(g):
                import ingestion.keys  # noqa: F401, and in a Python worker

                return g[["id"]]

            s = df.sparkSession
            plan = df.groupBy("id").applyInPandas(group, "id long").persist()
            try:
                rows = [(batch_id, plan.count())]
                s.createDataFrame(rows, "b long, n long").write.format("delta").mode("append").save(
                    out
                )
            finally:
                plan.unpersist()

        r = spark.readStream.format("delta").option("readChangeFeed", "true")
        r = r.option("startingVersion", 0).option("maxBytesPerTrigger", 1).load(source)
        w = r.writeStream.foreachBatch(batch_fn).option("checkpointLocation", ckpt)
        query = w.trigger(availableNow=True).start()
        query.awaitTermination()
        written = sorted(tuple(x) for x in spark.read.format("delta").load(out).collect())
        assert spark.read.schema("id string").json(f"{ckpt}/metadata").first().id == query.id
        assert not spark.read.text(f"{ckpt}/offsets").isEmpty()
        assert not spark.read.text(f"{ckpt}/commits").isEmpty()
        return f"batches {written}"

    import pyarrow as pa
    from pyspark.sql.pandas.types import from_arrow_schema
    from ingestion.probe_support import delete, listing, poison, up

    from ingestion import ingest, landing, replay, tables
    from ingestion.keys import partition
    from ingestion.status import Outcome as O

    backlog = [
        [(up("plain", 1), O.WRITTEN), (up("tie", 5, listing(title="first")), O.WRITTEN),
         (up("newer", 3), O.WRITTEN), (up("again", 1), O.WRITTEN), (up("gone", 1), O.WRITTEN)],
        [(up("tie", 5, listing(title="second")), O.CONFLICT), (up("newer", 2), O.STALE),
         (up("again", 1), O.ALREADY_APPLIED), (delete("gone", 2), O.WRITTEN),
         (up("spoiled", 1), O.WRITTEN), (poison("spoiled", 2), O.FAILED), (up("spoiled", 3), O.WRITTEN)],
        [(up("gone", 1), O.STALE), (up("gone", 3), O.WRITTEN)],
    ]  # fmt: skip
    expected = sorted(
        (HOUR, submission(n), i, o.value)
        for n, changes in enumerate(backlog)
        for i, (_c, o) in enumerate(changes)
    )
    landed = [pair for changes in backlog for pair in changes]
    failed = {i for i, (_c, o) in enumerate(landed) if o is O.FAILED}
    oracle = replay.expected_store([c for c, _o in landed], failed)

    def spark_init(data):
        kept = "interval 7 days"
        logs = {"delta.logRetentionDuration": kept, "delta.deletedFileRetentionDuration": kept}
        log = DeltaTable.createIfNotExists(spark).location(f"{data}/landing_log")
        log = log.addColumns(from_arrow_schema(landing.SCHEMA))
        for k, v in {**logs, "delta.enableChangeDataFeed": "true"}.items():
            log = log.property(k, v)
        log.execute()
        out = DeltaTable.createIfNotExists(spark).location(f"{data}/outcomes")
        out = out.addColumns(from_arrow_schema(tables.OUTCOMES)).partitionedBy("hour")
        for k, v in tables.OUTCOMES_PROPERTIES.items():
            out = out.property(k, v)
        out.execute()

    def spark_commit(data, n, changes):
        now = datetime.now(UTC)
        rows = [
            {"partition": partition(*c.key), "seq": seq, "merchant_id": c.merchant_id,
             "merchant_product_id": c.merchant_product_id, "source_version": c.source_version,
             "op": c.op, "listing": None if c.listing is None else c.listing.model_dump_json(),
             "submission_id": submission(n), "change_index": seq, "received_at": now}
            for seq, c in enumerate(changes)
        ]  # fmt: skip
        table = pa.Table.from_pylist(rows, schema=landing.SCHEMA)
        spark.createDataFrame(table).write.format("delta").mode("append").save(
            f"{data}/landing_log"
        )

    def outcomes(data):
        rows = spark.read.format("delta").load(f"{data}/outcomes").collect()
        return sorted(tuple(r) for r in rows)

    def fingerprints(data):
        rows = spark.read.format("delta").load(f"{data}/listing_store").collect()
        return {
            (r.merchant_id, r.merchant_product_id): replay.fingerprint(r.asDict()) for r in rows
        }

    def run(data):
        ingest.start(spark, data, trigger={"availableNow": True}).awaitTermination()

    def versions(data):
        tables_ = ("outcomes", "listing_store")
        return [
            DeltaTable.forPath(spark, f"{data}/{t}").history(1).first().version for t in tables_
        ]

    def full_run(data):
        ingest.MAX_BYTES = 1  # a batch per commit, so the last commit reads a stored Tombstone
        run(data)
        assert replay.diff(oracle, fingerprints(data)) == []
        assert outcomes(data) == expected, outcomes(data)
        before = versions(data)
        run(data)  # the restart
        assert versions(data) == before
        assert outcomes(data) == expected

    data_spark = f"{ROOT}/data_spark"

    @check("Spark-written Landing log")
    def _():
        spark_init(data_spark)
        for n, changes in enumerate(backlog):
            spark_commit(data_spark, n, [c for c, _o in changes])
        d = DeltaTable.forPath(spark, f"{data_spark}/landing_log").detail().first()
        return f"reader {d.minReaderVersion} writer {d.minWriterVersion} features {sorted(d.tableFeatures or [])}"

    @check("the ingest query on a Spark-written log, and its restart")
    def _():
        full_run(data_spark)

    @check("the query's layout")
    def _():
        table = DeltaTable.forPath(spark, f"{data_spark}/listing_store")
        d = table.detail().first()
        assert list(d.clusteringColumns) == ["merchant_id", "merchant_product_id"], (
            d.clusteringColumns
        )
        dvs = {
            k: v
            for r in table.history().collect()
            if r.operation == "MERGE"
            for k, v in r.operationMetrics.items()
            if "eletionVector" in k and v != "0"
        }
        return f"files {d.numFiles}, deletion vector metrics {dvs}"

    @check("a new commit after the restart")
    def _():
        spark_commit(data_spark, 3, [up("plain", 2)])
        run(data_spark)
        assert (HOUR, submission(3), 0, "written") in outcomes(data_spark)

    data_rs = Path(f"{ROOT}/data_rs")

    @check("delta-rs on the volume: tables.init and landing.append")
    def _():
        import deltalake

        tables.init(data_rs)
        log = deltalake.DeltaTable(str(data_rs / "landing_log"))
        now = datetime.now(UTC)
        for n, changes in enumerate(backlog):
            entries = [(submission(n), i, c, now) for i, (c, _o) in enumerate(changes)]
            landing.append(log, entries)
        return f"version {log.version()}"

    @check("the ingest query on a delta-rs log")
    def _():
        full_run(str(data_rs))

    say("DONE")
    return 0


def run() -> None:
    Path(ROOT).mkdir(parents=True, exist_ok=True)
    from ingestion import entry

    entry.exit_with(main)
