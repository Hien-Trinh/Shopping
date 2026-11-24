"""One local Spark session with Delta for every Spark test (docs/specs/step-g5.md): each JVM
holds about 850 MB, so there is one, and no xdist."""

import os
import re
import sys
import threading
import time
import uuid
from importlib.metadata import distributions
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryManager

from ingestion import store_maintenance

RUNTIME = Path(__file__).parents[2] / "requirements" / "runtime-18.txt"

# A zone that is not UTC, with a half hour in its offset, for Python and for the JVM this starts:
# CI's runner is in UTC, where code that reads a local time as UTC gives the right answer (#207).
os.environ["TZ"] = "Asia/Kolkata"
time.tzset()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    # Every library the env shares with Runtime 18 is at the runtime's version.
    lines = RUNTIME.read_text().splitlines()
    pins = dict(line.split("==") for line in lines if not line.startswith("#"))
    installed = {re.sub(r"[-_.]+", "-", d.name).lower(): d.version for d in distributions()}
    drift = {
        name: (pin, installed[name])
        for name, pin in pins.items()
        if installed.get(name, pin) != pin
    }
    assert not drift, f"not Runtime 18's versions (pin, installed): {drift}: run make spark"
    builder = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.snapshotPartitions", 1)
        .config("spark.sql.shuffle.partitions", 4)  # 1 would hide what each partition writes
        .config("spark.ui.enabled", False)
        # Not the working directory: a table with no LOCATION would land in the checkout (#207).
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        # A VACUUM lists the table's files in this many tasks: the default, 10000, takes minutes.
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", 4)
    )
    os.environ["PYSPARK_PYTHON"] = sys.executable  # workers: not PATH's python3 (a 3.15 on the Mac)
    session = configure_spark_with_delta_pip(builder).getOrCreate()  # adds the matching Delta jar
    java = session.sparkContext._jvm.java.lang.System.getProperty("java.specification.version")
    assert java == "21", f"Runtime 18 runs JDK 21, and this JVM is {java}: set JAVA_HOME"
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def listeners(spark, monkeypatch):
    """`ingest.main` adds a listener to the one shared session: take each off after its test, or
    every later query's lines print once per listener left behind."""
    added, add = [], StreamingQueryManager.addListener
    monkeypatch.setattr(
        StreamingQueryManager, "addListener", lambda self, it: (added.append(it), add(self, it))[1]
    )
    yield
    for listener in added:
        spark.streams.removeListener(listener)


@pytest.fixture(autouse=True)
def no_query_left(spark):
    """`main` returns when one query ends, and the process then exits. Here the session lives on,
    so the other query is stopped: a second `main` can't start a name that is still active."""
    yield
    for query in spark.streams.active:
        query.stop()
    # `main` then returns and stops its maintenance thread, which ends after the action it is in.
    # Wait for it, and drop the snapshots it left in the session's one catalog, or a later `main`
    # would read a snapshot of another test's store.
    for thread in threading.enumerate():
        if thread.name == store_maintenance.POOL:
            thread.join(120)
            assert not thread.is_alive(), "the maintenance thread did not stop"
    for row in spark.sql("SHOW TABLES IN default LIKE 'snapshot_v*'").collect():
        spark.sql(f"DROP TABLE default.{row.tableName}")


@pytest.fixture
def schema(spark, tmp_path):
    """A schema of its own for a test's snapshots (`--snapshots`)."""
    name = f"s_{uuid.uuid4().hex[:12]}"
    spark.sql(f"CREATE SCHEMA {name} LOCATION '{tmp_path}/schema'")
    yield name
    spark.sql(f"DROP SCHEMA {name} CASCADE")
