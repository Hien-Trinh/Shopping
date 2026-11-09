"""The classifier on the executors: the ingest query's iterator pandas UDF (docs/specs/step-a4.md).

The driver names the classifier (`Spec`), and the UDF's closure carries it to the executors,
since a broadcast variable needs the `sparkContext` that serverless lacks. Each Python worker
builds the classifier once (`loaded`) and classifies only the rows the plan marked; a Listing it
can't classify keeps the plan's provisional answer, flagged for the Backfill.
"""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

from ingestion import classify, decide, entry, student, taxonomy
from ingestion.envelope import Content
from ingestion.plan import Classification

ANSWER = ["primary_category", "classify_confidence", "taxonomy_version", "needs_reclassify"]
_SCHEMA = "primary_category string, classify_confidence double, taxonomy_version string," + (
    " needs_reclassify boolean"
)


@dataclass(frozen=True)
class Spec:
    """What an executor needs to build the classifier: its kind (`student`, `fake` or `down`),
    the version it stamps and, for the student, its models folder and index."""

    kind: str
    version: str
    models: str = ""
    index: str = ""

    @classmethod
    def student(cls, models: Path) -> "Spec":
        """The student's, worked out on the driver from the `train/` beside `models`, as a
        checkout has it and the volume holds it (step-a4.md, question 1)."""
        train = [models.parent / p for p in student.TRAIN]
        version = student.version(taxonomy.load().version, train=train)
        index = student.index_path(models, student.FT_REVISION, train)
        return cls("student", version, str(models), str(index))


FAKE = Spec("fake", classify.FakeClassifier.taxonomy_version)
DOWN = Spec("down", classify.FakeClassifier.taxonomy_version)  # fake in an outage: every call fails

_loaded: dict[Spec, classify.Classifier] = {}


def loaded(spec: Spec) -> classify.Classifier:
    """This Python worker's classifier. Module-level, so the UDF's closure ships it by reference
    and the worker keeps one across tasks: a dict the UDF named itself would ship as a copy and
    load on every task (step-a4.md, fact 1). A missing model raises ModelMissing, which fails the
    batch (question 3)."""
    if spec not in _loaded:
        _loaded.clear()  # a new spec replaces the old one
        if spec.kind == "student":
            tax, models, index = taxonomy.load(), Path(spec.models), Path(spec.index)
            _loaded[spec] = student.serve(tax, models, index, spec.version, threads=1)
        else:
            _loaded[spec] = classify.FakeClassifier(fail=spec.kind == "down")
    return _loaded[spec]


def classified(writes: DataFrame, spec: Spec, slots: int | None) -> DataFrame:
    """The MERGE's source: `writes` with the classifier's answers in the rows marked
    `needs_classify`, which it drops. Repartitioned to `slots` first, when Spark gives one, so a
    small batch doesn't run as one task (fact 2). The UDF reads only the classifier's columns, so
    a big price never passes through pandas (fact 3)."""
    if slots:
        writes = writes.repartition(slots)

    @pandas_udf(_SCHEMA)
    def answer(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        classifier = loaded(spec)
        for rows in batches:
            yield _answered(classifier, spec.version, rows)

    kept = [c for c in writes.columns if c not in (*ANSWER, "needs_classify")]
    columns = F.struct("needs_classify", "title", "description", *ANSWER)
    return writes.withColumn("_answer", answer(columns)).select(*kept, "_answer.*")


def _answered(classifier, version: str, rows: pd.DataFrame) -> pd.DataFrame:
    """One Arrow batch's answers: the provisional ones, replaced where the classifier answered."""
    out = rows[ANSWER].copy()
    marked = rows[rows["needs_classify"].astype(bool)]
    listings = [
        Content.model_construct(title=t, description=d)
        for t, d in zip(marked["title"], marked["description"], strict=True)
    ]
    for i, found in zip(marked.index, _answers(classifier, version, listings), strict=True):
        if found:
            out.loc[i, ANSWER] = [found.category, found.confidence, found.taxonomy_version, False]
    return out


def _answers(classifier, version: str, listings: Sequence[Content]) -> list[Classification | None]:
    """One answer per Listing, None where the classifier gave none (keep the provisional one).
    One call for the batch; if it fails (raises, or answers the wrong count or out of contract),
    one call per Listing, so only the Listings it can't classify fall back. v1 failed the whole
    call (step-a4.md, decision 4)."""
    if not listings:
        return []
    try:
        found = classifier.classify(listings)
        if len(found) != len(listings):
            raise ValueError(f"{len(found)} answers for {len(listings)} Listings")
        return [None if a is None else decide.validated(a, version) for a in found]
    except entry.Fatal:  # ModelMissing, say: no batch can succeed, so this one fails (question 3)
        raise
    except Exception:  # an outage or a bad answer: never fail the batch for it
        if len(listings) == 1:
            return [None]
        return [a for x in listings for a in _answers(classifier, version, [x])]
