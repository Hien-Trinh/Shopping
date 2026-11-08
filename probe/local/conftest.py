# ruff: noqa
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "spark_conftest",
    str(__import__("pathlib").Path(__file__).parents[2] / "tests/spark/conftest.py"),
)
_m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)
spark = _m.spark


import pytest


@pytest.fixture(autouse=True)
def _ship_probe(spark):
    import a4probe

    spark.sparkContext.addPyFile(a4probe.__file__)
