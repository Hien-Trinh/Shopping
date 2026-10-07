"""The student prototype (docs/specs/step-6l.md, test points 1-5)."""

from pathlib import Path

import numpy as np
import pytest
from support import listing

from catalog import evaluate, student
from catalog.taxonomy import Taxonomy

TAX = Taxonomy("shopify-2026-08", ("Apparel", "Apparel > Shirts", "Toys"))
CLUSTERS = {"Apparel": [1.0, 0.0], "Apparel > Shirts": [0.0, 1.0], "Toys": [-1.0, -1.0]}


def toy(n=30, seed=0):
    """`n` points per class around three separable centres."""
    rng = np.random.default_rng(seed)
    x, y = [], []
    for label, centre in CLUSTERS.items():
        x.append(np.array(centre) + 0.1 * rng.standard_normal((n, 2)))
        y += [label] * n
    return student.unit(np.vstack(x)), y


# 1. Softmax head


def test_softmax_learns_a_separable_set():
    x, y = toy()
    head = student.Softmax.fit(x, y)
    assert [c for c, _ in head.predict(x)] == y


def test_softmax_confidence_is_a_probability():
    x, y = toy()
    confidences = [p for _, p in student.Softmax.fit(x, y).predict(x)]
    assert all(0 <= p <= 1 for p in confidences)


def test_softmax_is_deterministic():
    x, y = toy()
    a, b = student.Softmax.fit(x, y), student.Softmax.fit(x, y)
    assert np.array_equal(a.w, b.w) and np.array_equal(a.b, b.b)


# 2. kNN


def test_knn_the_nearest_class_wins():
    x, y = toy()
    knn = student.Knn(x, y, k=5)
    assert [c for c, _ in knn.predict(student.unit(np.array([[1.0, 0.05]])))] == ["Apparel"]


def test_knn_confidence_is_the_winners_vote_share():
    x = student.unit(np.array([[1.0, 0.0], [1.0, 0.1], [0.0, 1.0]]))
    knn = student.Knn(x, ["A", "A", "B"], k=3)
    ((label, share),) = knn.predict(student.unit(np.array([[1.0, 0.05]])))
    sims = x @ student.unit(np.array([[1.0, 0.05]]))[0]
    assert label == "A"
    assert share == pytest.approx((sims[0] + sims[1]) / sims.sum())


@pytest.mark.parametrize("order", [("A", "A", "B"), ("B", "A", "A")])
def test_knn_a_tied_vote_goes_to_the_higher_similarity(order):
    rows = {"A": [1.0, 0.0], "B": [2.0, 0.0]}  # raw dot products: A 1 + 1, B 2: an exact tie
    knn = student.Knn(np.array([rows[c] for c in order]), list(order), k=3)
    ((label, share),) = knn.predict(np.array([[1.0, 0.0]]))
    assert label == "B" and share == 0.5


def test_knn_scores_in_chunks_with_the_same_result():
    x, y = toy()
    q = x[::7]
    assert student.Knn(x, y, k=5, chunk=2).predict(q) == student.Knn(x, y, k=5).predict(q)


# 3. The cascade


def test_cascade_at_zero_keeps_every_student_answer():
    answers, kept = student.cascade([("A", 0.3), ("B", 0.9)], ["J1", "J2"], tau=0.0)
    assert answers == ["A", "B"] and kept == 2


def test_cascade_above_every_confidence_sends_all_to_the_fallback():
    answers, kept = student.cascade([("A", 0.3), ("B", 0.9)], ["J1", None], tau=1.01)
    assert answers == ["J1", None] and kept == 0


def test_cascade_splits_at_tau():
    answers, kept = student.cascade([("A", 0.3), ("B", 0.9)], ["J1", "J2"], tau=0.5)
    assert answers == ["J1", "B"] and kept == 1


def test_pick_tau_is_the_lowest_threshold_whose_kept_answers_reach_the_target():
    scored = [("A", 0.9, True), ("B", 0.8, True), ("C", 0.4, False), ("D", 0.2, False)]
    assert student.pick_tau(scored, target=0.75, grid=[0.0, 0.3, 0.5, 0.85]) == 0.5


# 4. The vector cache


def test_vectors_are_cached_by_text(tmp_path):
    calls = []

    def embed(texts):
        calls.append(list(texts))
        return np.array([[float(len(t)), 1.0] for t in texts])

    a = student.vectors(["ab", "c"], embed, tmp_path)
    b = student.vectors(["ab", "c"], embed, tmp_path)
    assert np.array_equal(a, b) and len(calls) == 1
    student.vectors(["ab", "cd"], embed, tmp_path)
    assert len(calls) == 2
    assert np.allclose(np.linalg.norm(a, axis=1), 1.0)


# 5. The classifier as an eval candidate


def fake_embed(texts):
    return np.array([[1.0, 0.0] if "shirt" in t.lower() else [0.0, 1.0] for t in texts])


@pytest.mark.parametrize("kind", ["softmax", "knn"])
def test_classifier_answers_a_taxonomy_path_for_every_listing(kind, tmp_path):
    rows = [{"title": "Blue shirt", "description": "", "category": "Apparel > Shirts"}] * 3 + [
        {"title": "Toy train", "description": "", "category": "Toys"}
    ] * 3
    c = student.StudentClassifier.train(TAX, fake_embed, rows, kind, cache=tmp_path, k=3)
    got = c.classify([listing(title="Red shirt"), listing(title="Wooden train")])
    assert [a[0] for a in got] == ["Apparel > Shirts", "Toys"]
    assert all(0 <= a[1] <= 1 for a in got)
    assert c.taxonomy_version.endswith(f"+student-{kind}")


def test_training_rows_outside_the_taxonomy_are_refused(tmp_path):
    rows = [{"title": "x", "description": "", "category": "Kites"}]
    with pytest.raises(ValueError, match="Kites"):
        student.StudentClassifier.train(TAX, fake_embed, rows, "knn", cache=tmp_path)


def test_evaluate_knows_the_student_candidates():
    assert {"student-softmax", "student-knn"} <= set(evaluate.CANDIDATES)


def test_cascade_keeps_a_confidence_equal_to_tau():
    answers, kept = student.cascade([("A", 0.5)], ["J"], tau=0.5)
    assert answers == ["A"] and kept == 1


def test_pick_tau_accepts_a_kept_rate_equal_to_the_target():
    scored = [("A", 0.9, True), ("B", 0.4, False)]
    assert student.pick_tau(scored, target=0.5, grid=[0.0, 0.5]) == 0.0


def test_pick_tau_is_none_when_no_threshold_reaches_the_target():
    assert student.pick_tau([("A", 0.9, False)], target=0.5, grid=[0.0, 0.5]) is None
    assert student.pick_tau([], target=0.5, grid=[0.0]) is None


def test_a_truncated_cache_file_is_recomputed(tmp_path):
    def embed(texts):
        return np.array([[1.0, 2.0] for _ in texts])

    student.vectors(["a"], embed, tmp_path)
    (path,) = tmp_path.glob("student-*.npy")
    path.write_bytes(path.read_bytes()[:20])
    assert np.allclose(
        student.vectors(["a"], embed, tmp_path), student.unit(np.array([[1.0, 2.0]]))
    )
    assert not list(tmp_path.glob("*.tmp"))


def test_classify_answers_nothing_for_no_listings(tmp_path):
    rows = [{"title": "Blue shirt", "description": "", "category": "Apparel > Shirts"}]
    c = student.StudentClassifier.train(TAX, fake_embed, rows, "knn", cache=tmp_path)
    assert c.classify([]) == []


def test_held_out_takes_ten_percent_of_each_source():
    a = [{"title": f"a{i}"} for i in range(100)]
    b = [{"title": f"b{i}"} for i in range(20)]
    fit, held = student.held_out([a, b], seed=0)
    assert sum(r["title"][0] == "a" for r in held) == 10
    assert sum(r["title"][0] == "b" for r in held) == 2
    assert len(fit) == 108


def test_training_reads_the_second_amazon_file():
    # docs/specs/step-6l.2.md, test point 1
    path = Path("train/amazon-opus-2.jsonl")
    assert path in student.TRAIN
    rows = student.load_training([path])
    assert len(rows) > 8000 and all("amazon_category" in r for r in rows)


def test_tau_is_picked_on_the_amazon_rows_only():
    # docs/specs/step-6l.2.md, test point 2: Shopify rows right where Amazon rows are wrong
    held = [{"source": "shopify-train"}] * 4 + [{"amazon_category": "Toys"}] * 2
    scored = [("A", 0.3, True)] * 4 + [("A", 0.3, False), ("A", 0.6, True)]
    grid = [0.0, 0.3, 0.6]
    assert student.pick_tau(scored, target=0.6, grid=grid) == 0.0
    assert student.pick_tau(student.amazon(scored, held), target=0.6, grid=grid) == 0.6


def test_rows_with_an_eval_title_are_dropped():
    rows = [{"title": "Blue Shirt"}, {"title": "Toy train"}]
    assert student.without_titles(rows, ["blue shirt"]) == [{"title": "Toy train"}]


def write_eval(tmp_path):
    import json

    labels = tmp_path / "labels.jsonl"
    items = [("a", "Blue shirt", "Apparel > Shirts"), ("b", "Toy train", "Toys")]
    labels.write_text(
        "".join(
            json.dumps({"id": i, "title": t, "description": "", "category": c}) + "\n"
            for i, t, c in items
        )
    )
    jev = tmp_path / "jev.json"
    jev.write_text(
        json.dumps(
            {"answers": [{"id": "a", "category": "Apparel"}, {"id": "b", "category": "Toys"}]}
        )
    )
    opus = tmp_path / "opus.jsonl"
    opus.write_text("".join(json.dumps({"id": i, "category": c}) + "\n" for i, _, c in items))
    return {"1,020": ([labels], [jev])}, opus


def ticking():
    t = [0.0]

    def clock():
        t[0] += 0.001
        return t[0]

    return clock


SOURCES = [  # titles unlike the eval's, which the report drops from training
    [{"title": "Red shirt", "description": "", "category": "Apparel > Shirts"}] * 10
    + [{"title": "Wooden train", "description": "", "category": "Toys"}] * 10,
    [
        {
            "title": "Green shirt",
            "description": "",
            "category": "Apparel > Shirts",
            "amazon_category": "x",
        }
    ]
    * 10,
]


def test_report_scores_each_student_alone_and_in_the_cascade(tmp_path):
    evals, opus = write_eval(tmp_path)
    body = student.report(
        TAX, fake_embed, SOURCES, tmp_path / "cache", evals=evals, opus=opus, clock=ticking()
    )
    assert "## student-softmax" in body and "## student-knn (k=5)" in body
    assert "Student alone: exact 100.0%, top level 100.0%, two levels 100.0%" in body
    assert "p50 1.0 ms, p99 1.0 ms per Listing" in body
    assert "0 of 2 eval labels never occur in training" in body
    assert "Jev alone: exact 50.0%" in body and "Opus alone: exact 100.0%" in body
    assert "| 0.00 ← τ | 100.0% | 100.0% | 100.0% |" in body
    assert "Bar (beats Jev alone with 70% or more kept local): met at τ = 0.00" in body


def test_report_counts_eval_labels_unseen_in_training(tmp_path):
    evals, opus = write_eval(tmp_path)
    shirts_only = [[r for r in SOURCES[0] if r["category"] != "Toys"], SOURCES[1]]
    body = student.report(
        TAX, fake_embed, shirts_only, tmp_path / "cache", evals=evals, opus=opus, clock=ticking()
    )
    assert "1 of 2 eval labels never occur in training" in body


def test_report_refuses_missing_fallback_answers_before_training(tmp_path):
    evals, opus = write_eval(tmp_path)
    evals["1,020"][1][0].unlink()

    def embed(texts):
        raise AssertionError("embedded before checking the files")

    with pytest.raises(FileNotFoundError, match="jev.json"):
        student.report(TAX, embed, SOURCES, tmp_path / "cache", evals=evals, opus=opus)


def test_main_writes_the_report(tmp_path, monkeypatch):
    monkeypatch.setattr(student.classify, "fastembed", lambda models: None)
    monkeypatch.setattr(student, "load_training", lambda paths: [])
    monkeypatch.setattr(student, "report", lambda *_: "# report\n")
    out = tmp_path / "r" / "student.md"
    student.main(["--models", str(tmp_path), "--out", str(out)])
    assert out.read_text() == "# report\n"
