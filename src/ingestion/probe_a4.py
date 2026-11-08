# ruff: noqa
"""A throwaway probe for step A4 on Free Edition (never merged): the student on serverless
executors, its files read from a volume. Prints PASS, FAIL and INFO lines, never a time."""

import argparse
import os
import platform
import resource
from importlib.metadata import version as installed
from pathlib import Path

_loaded: dict = {}
_loads = [0]

EXPECTED = {
    "Men's cotton crew-neck t-shirt": "Apparel & Accessories > Clothing > Clothing Tops",
    "Stainless steel chef's knife, 8 inch": "Home & Garden > Kitchen & Dining > Kitchen Tools & Utensils",
    "Wireless bluetooth earbuds with charging case": "Electronics > Audio > Audio Components",
    "Organic green tea, 100 bags": "Food, Beverages & Tobacco > Beverages > Tea & Infusions",
    "Kids' waterproof rain boots": "Apparel & Accessories > Shoes > Baby & Children's Shoes",
    "Yoga mat, non-slip, 6 mm": "Sporting Goods > Fitness & General Exercise Equipment > Yoga & Pilates",
    "Ceramic coffee mug 350 ml": "Home & Garden > Kitchen & Dining > Tableware",
    "Laptop backpack 15.6 inch": "Luggage & Bags > Backpacks > Laptop Backpacks",
}


def say(line: str) -> None:
    print(" ".join(line.split())[:1400], flush=True)


def cached(models: str, index: str, version: str):
    """Module-level, so the closure ships it by reference and the worker keeps its classifier."""
    key = (models, index, version)
    if key not in _loaded:
        from ingestion import student, taxonomy

        _loaded.clear()
        embed, _ = student.embedder(Path(models), threads=1)
        x, y = student.load_index(Path(index), taxonomy.load(), width=384)
        _loaded[key] = student.StudentClassifier(embed, student.Knn(x, y, student.K), version)
        _loads[0] += 1
    return _loaded[key]


def load_count() -> int:
    return _loads[0]


def frames(models: str, index: str, version: str):
    def run(batches):
        from pyspark import TaskContext

        from ingestion.envelope import Content

        classifier = cached(models, index, version)
        for f in batches:
            listings = [
                Content.model_construct(title=t, description=d)
                for t, d in zip(f["title"], f["description"])
            ]
            answers = classifier.classify(listings) if listings else []
            yield f.assign(
                category=[a[0] for a in answers],
                confidence=[float(a[1]) for a in answers],
                pid=os.getpid(),
                loads=load_count(),
                part=TaskContext.get().partitionId(),
                rss_kb=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                // (1024 if platform.system() == "Darwin" else 1),  # bytes on macOS, KB on Linux
                arch=platform.machine(),
            )

    return run


def main(argv=None) -> None:
    args = argparse.ArgumentParser(prog="probe-a4")
    args.add_argument("--models", required=True)
    args.add_argument("--index", required=True)
    args.add_argument("--version", required=True)
    a = args.parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    say(f"INFO driver arch {platform.machine()}")
    for p in ("fastembed", "onnxruntime", "numpy", "tokenizers", "huggingface_hub", "pandas"):
        try:
            say(f"INFO version {p} {installed(p)}")
        except Exception as e:
            say(f"INFO version {p} unknown: {type(e).__name__}")
    try:
        slots = spark.sparkContext.defaultParallelism
        say(f"INFO sparkContext.defaultParallelism works: {slots}")
    except Exception as e:
        say(f"INFO sparkContext raises {type(e).__name__}: {str(e)[:300]}")
    for conf in ("spark.default.parallelism", "spark.sql.shuffle.partitions"):
        try:
            say(f"INFO conf {conf} = {spark.conf.get(conf)}")
        except Exception as e:
            say(f"INFO conf {conf} raises {type(e).__name__}")
    say(f"INFO the driver sees the index on the volume: {os.path.exists(a.index)}")
    df = spark.createDataFrame([(t, "") for t in EXPECTED], "title string, description string")
    schema = (
        "title string, description string, category string, confidence double, pid long,"
        " loads long, part int, rss_kb long, arch string"
    )
    rows = []
    for round_ in (1, 2):
        try:
            got = df.repartition(2).mapInPandas(frames(a.models, a.index, a.version), schema)
            rows += got.collect()
        except Exception as e:
            say(f"FAIL round {round_} raised {type(e).__name__}: {str(e)[:1200]}")
            return
    wrong = {r.title: r.category for r in rows if r.category != EXPECTED[r.title]}
    say(("PASS" if not wrong else f"FAIL {wrong}") + " the student's categories on executors")
    say(
        f"INFO executor arch {sorted({r.arch for r in rows})}, partitions {sorted({r.part for r in rows})}"
    )
    loads: dict = {}
    for r in rows:
        loads[r.pid] = max(loads.get(r.pid, 0), r.loads)
    one = all(v == 1 for v in loads.values())
    say(
        ("PASS" if one else "FAIL")
        + f" one load per Python worker over two rounds (workers {len(loads)},"
        f" loads per worker {sorted(loads.values())})"
    )
    peak = max(r.rss_kb for r in rows)
    say(("PASS" if peak < 2**20 else "FAIL") + " each worker's peak memory under 1 GB")
    say(("PASS" if peak < 3 * 2**18 else "FAIL") + " each worker's peak memory under 0.75 GB")
