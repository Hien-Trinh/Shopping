"""One local Spark session with Delta for every Spark test (docs/specs/step-g5.md): each JVM
holds about 850 MB, so there is one, and no xdist."""

import os
import re
import sys
from importlib.metadata import distributions
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

RUNTIME = Path(__file__).parents[2] / "requirements" / "runtime-18.txt"


@pytest.fixture(scope="session")
def spark():
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
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.ui.enabled", False)
    )
    os.environ["PYSPARK_PYTHON"] = sys.executable  # workers: not PATH's python3 (a 3.15 on the Mac)
    session = configure_spark_with_delta_pip(builder).getOrCreate()  # adds the matching Delta jar
    java = session.sparkContext._jvm.java.lang.System.getProperty("java.specification.version")
    assert java == "21", f"Runtime 18 runs JDK 21, and this JVM is {java}: set JAVA_HOME"
    yield session
    session.stop()
