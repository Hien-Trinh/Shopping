# ruff: noqa
"""Importable by Spark's Python workers (on PYTHONPATH): a module-level cache, as A4's UDF module
would hold, plus logs of loads and calls."""

import os
import resource
from pathlib import Path

_loaded: dict = {}


def _load(kind, models, index, version):
    if kind == "student":
        from ingestion import student, taxonomy

        tax = taxonomy.load()
        embed, _ = student.embedder(Path(models), threads=1)
        x, y = student.load_index(Path(index), tax, width=384)
        return student.StudentClassifier(embed, student.Knn(x, y, student.K), version)
    return None


def frames(log, kind="count", models="", index="", version="", by_ref=True):
    def _inline(key):
        if key not in _loaded:
            _loaded.clear()
            _loaded[key] = _load(*key[1:])
            with open(f"{key[0]}/loads.txt", "a") as f:
                f.write(f"{os.getpid()},0\n")
        return _loaded[key]

    def run(batches):
        from pyspark import TaskContext

        from ingestion.envelope import Content

        c = (
            cached((log, kind, models, index, version))
            if by_ref
            else _inline((log, kind, models, index, version))
        )
        for frame in batches:
            part = TaskContext.get().partitionId()
            if c is not None and len(frame):
                listings = [
                    Content.model_construct(title=t, description=d)
                    for t, d in zip(frame["title"], frame["description"], strict=True)
                ]
                answers = c.classify(listings)
                frame = frame.assign(
                    category=[a[0] for a in answers], confidence=[a[1] for a in answers]
                )
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            with open(f"{log}/calls.txt", "a") as f:
                f.write(f"{os.getpid()},{part},{len(frame)},{rss}\n")
            yield frame

    return run


def cached(key):
    """Module-level, so cloudpickle ships it by reference and the worker's module keeps _loaded."""
    if key not in _loaded:
        _loaded.clear()
        before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        _loaded[key] = _load(*key[1:])
        with open(f"{key[0]}/loads.txt", "a") as f:
            f.write(f"{os.getpid()},{before}\n")
    return _loaded[key]


def keyed(version, group):
    return group.assign(v=version)


def identity_frames(batches):
    for frame in batches:
        yield frame


_udf_loaded: dict = {}
_udf_loads = [0]


def udf_cached(key):
    if key not in _udf_loaded:
        _udf_loaded.clear()
        _udf_loaded[key] = object()
        _udf_loads[0] += 1
        with open(f"{key}/loads.txt", "a") as f:
            f.write(f"{os.getpid()}\n")
    return _udf_loaded[key]


def answer_udf(log):
    from collections.abc import Iterator

    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("primary_category string, classify_confidence double")
    def run(batches: Iterator[tuple[pd.Series, pd.Series, pd.Series]]) -> Iterator[pd.DataFrame]:
        udf_cached(log)
        for marked, titles, provisional in batches:
            with open(f"{log}/calls.txt", "a") as f:
                f.write(f"{os.getpid()},{len(titles)}\n")
            yield pd.DataFrame(
                {
                    "primary_category": [
                        f"Fake > {t[0]}" if m else p
                        for m, t, p in zip(marked, titles, provisional, strict=True)
                    ],
                    "classify_confidence": [0.9 if m else 0.0 for m in marked],
                }
            )

    return run
