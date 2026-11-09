"""The classifier on executors, through the real ingest query (docs/specs/step-a4.md)."""

import os
import re
import shutil
from pathlib import Path

import pandas as pd
import pytest
from pyspark.errors import StreamingQueryException
from support import listing, up
from test_ingest import ROOT, commit, land, outcomes, run

from ingestion import classify_udf, ingest, student, taxonomy
from ingestion.classify import ModelMissing
from ingestion.envelope import Change, Content
from ingestion.taxonomy import UNCATEGORIZED

# The student's answers on the Mac and on Free Edition's executors (step-a4.md, fact 8).
KNOWN = {
    "Men's cotton crew-neck t-shirt": "Apparel & Accessories > Clothing > Clothing Tops",
    "Stainless steel chef's knife, 8 inch": (
        "Home & Garden > Kitchen & Dining > Kitchen Tools & Utensils"
    ),
    "Wireless bluetooth earbuds with charging case": "Electronics > Audio > Audio Components",
    "Organic green tea, 100 bags": "Food, Beverages & Tobacco > Beverages > Tea & Infusions",
    "Kids' waterproof rain boots": "Apparel & Accessories > Shoes > Baby & Children's Shoes",
    "Yoga mat, non-slip, 6 mm": (
        "Sporting Goods > Fitness & General Exercise Equipment > Yoga & Pilates"
    ),
    "Ceramic coffee mug 350 ml": "Home & Garden > Kitchen & Dining > Tableware",
    "Laptop backpack 15.6 inch": "Luggage & Bags > Backpacks > Laptop Backpacks",
}


def answers(spark, data: Path) -> dict[str, tuple]:
    rows = spark.read.format("delta").load(f"{data}/listing_store").collect()
    return {
        r.merchant_product_id: (
            r.primary_category,
            r.classify_confidence,
            r.taxonomy_version,
            r.needs_reclassify,
        )
        for r in rows
    }


def untitled(mpid: str) -> Change:
    """Past validation, yet storable: fake raises on its empty title (IndexError)."""
    content = Content.model_construct(**dict(listing()) | {"title": ""})
    return Change("m_1", mpid, 1, "upsert", content)


def test_only_marked_rows_reach_the_classifier_and_an_outage_leaves_them_flagged(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("price", 1), up("title", 1)]))
    run(spark, tmp_path)
    changed = [
        up("price", 2, listing(price=2_000_000)),  # a price change isn't marked
        up("title", 2, listing(title="Blue hat")),
        up("new", 1),
    ]
    land(tmp_path, commit(1, changed))
    run(spark, tmp_path, classify_udf.DOWN)  # every call raises, at fake's version
    assert answers(spark, tmp_path) == {
        "price": ("Fake > R", 0.9, "fake-1", False),
        "title": (UNCATEGORIZED, 0.0, "fake-1", True),
        "new": (UNCATEGORIZED, 0.0, "fake-1", True),
    }
    assert {o[-1] for o in outcomes(spark, tmp_path)} == {"written"}


def test_a_listing_the_classifier_raises_on_falls_back_alone(spark, tmp_path):
    # Five rows over local[2]'s two slots: each Arrow batch holds two or more, so a whole-call
    # fallback would take a good Listing down with the bad one.
    names = ["Apple", "Bag", "Cap", "Dish"]
    land(
        tmp_path, ["init"], commit(0, [*(up(n, 1, listing(title=n)) for n in names), untitled("x")])
    )
    run(spark, tmp_path)
    assert answers(spark, tmp_path) == {
        n: (f"Fake > {n[0]}", 0.9, "fake-1", False) for n in names
    } | {"x": (UNCATEGORIZED, 0.0, "fake-1", True)}


def test_a_student_that_cant_load_fails_the_batch_and_commits_nothing(spark, tmp_path):
    (tmp_path / "models").mkdir()
    (tmp_path / "train").symlink_to(ROOT / "train")  # the version reads it; the files are missing
    data = tmp_path / "data"
    land(data, ["init"], commit(0, [up("a", 1)]))
    with pytest.raises(StreamingQueryException, match="ModelMissing"):
        run(spark, data, classify_udf.Spec.student(tmp_path / "models"))
    assert outcomes(spark, data) == []
    assert spark.read.format("delta").load(f"{data}/listing_store").isEmpty()


def test_each_python_worker_keeps_one_classifier_over_batches(spark, tmp_path):
    def held() -> set[tuple[int, tuple[int, ...]]]:
        """Each Python worker's pid and the ids of the classifiers it holds."""

        def ids(batches):
            for _ in batches:
                kept = [id(c) for c in classify_udf._loaded.values()]
                yield pd.DataFrame({"pid": [os.getpid()], "ids": [kept]})

        rows = spark.range(8).repartition(2).mapInPandas(ids, "pid long, ids array<long>")
        return {(r.pid, tuple(r.ids)) for r in rows.collect()}

    land(tmp_path, ["init"], commit(0, [up(f"p{n}", 1) for n in range(8)]))
    run(spark, tmp_path)
    first = held()
    land(tmp_path, commit(1, [up(f"p{n}", 2, listing(title="Blue hat")) for n in range(8)]))
    run(spark, tmp_path)
    assert held() == first
    assert first and all(len(ids) == 1 for _, ids in first)


def test_a_new_spec_replaces_the_workers_classifier(monkeypatch):  # two students won't fit 1 GB
    monkeypatch.setattr(classify_udf, "_loaded", {})
    classify_udf.loaded(classify_udf.FAKE)
    down = classify_udf.loaded(classify_udf.DOWN)
    assert classify_udf._loaded == {classify_udf.DOWN: down}


def test_main_runs_the_student_by_default_with_train_beside_the_models_folder(spark, tmp_path):
    (tmp_path / "models").mkdir()
    with pytest.raises(ModelMissing, match=re.escape(str(tmp_path / student.TRAIN[0]))):
        ingest.main(["--data", str(tmp_path / "data"), "--models", str(tmp_path / "models")])


def test_classified_repartitions_to_the_slot_count_when_spark_gives_one(spark):
    rows = spark.createDataFrame(
        [(False, "Red shirt", "", "Fake > R", 0.9, "fake-1", False)] * 3,
        "needs_classify boolean, title string, description string, primary_category string,"
        " classify_confidence double, taxonomy_version string, needs_reclassify boolean",
    ).coalesce(1)
    assert classify_udf.classified(rows, classify_udf.FAKE, 2).rdd.getNumPartitions() == 2
    assert classify_udf.classified(rows, classify_udf.FAKE, None).rdd.getNumPartitions() == 1


def test_spec_student_names_the_train_files_beside_the_models_folder(tmp_path):
    for path in student.TRAIN:
        (tmp_path / path).parent.mkdir(exist_ok=True)
        shutil.copy(ROOT / path, tmp_path / path)
    spec = classify_udf.Spec.student(tmp_path / "models")
    assert spec.version == student.version(taxonomy.load().version)
    assert spec.index == str(student.index_path(tmp_path / "models", student.FT_REVISION))
    with (tmp_path / student.TRAIN[1]).open("a") as f:
        f.write("\n")
    assert classify_udf.Spec.student(tmp_path / "models").version != spec.version


@pytest.mark.model
def test_the_student_classifies_known_listings_on_the_executors(spark, tmp_path):
    spec = classify_udf.Spec.student(Path("models"))
    titles = list(KNOWN)
    land(
        tmp_path,
        ["init"],
        commit(0, [up(f"p{i}", 1, listing(title=t)) for i, t in enumerate(titles)]),
    )
    run(spark, tmp_path, spec)
    got = {
        k: (category, version, flag)
        for k, (category, _, version, flag) in answers(spark, tmp_path).items()
    }
    assert got == {f"p{i}": (KNOWN[t], spec.version, False) for i, t in enumerate(titles)}
