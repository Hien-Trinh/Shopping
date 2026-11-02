"""One local Spark session with Delta for every Spark test (docs/specs/step-g5.md): each JVM
holds about 850 MB, so there is one, and no xdist."""

import os
import sys

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
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
