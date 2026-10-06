"""The student prototype (docs/specs/step-6l.md, test points 1-5)."""

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


def test_knn_a_tied_vote_goes_to_the_higher_similarity():
    x = student.unit(np.array([[1.0, 0.0], [0.0, 1.0]]))
    knn = student.Knn(x, ["A", "B"], k=2)
    query = student.unit(np.array([[1.0, 1.0001]]))  # all but equidistant, a hair nearer B
    ((label, _),) = knn.predict(query)
    assert label == "B"


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


def test_report_scores_each_student_alone_and_in_the_cascade(tmp_path, monkeypatch):
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
    monkeypatch.setattr(student, "EVALS", {"1,020": ([labels], [jev])})
    monkeypatch.setattr(student, "OPUS", opus)
    rows = [{"title": "Blue shirt", "description": "", "category": "Apparel > Shirts"}] * 10 + [
        {"title": "Toy train", "description": "", "category": "Toys"}
    ] * 10
    body = student.report(TAX, fake_embed, rows, tmp_path / "cache")
    assert "## student-softmax" in body and "## student-knn (k=5)" in body
    assert "Student alone: exact 100.0%" in body
    assert "Jev alone: exact 50.0%" in body and "Opus alone: exact 100.0%" in body
    assert "| 1.00 | 0.0% | 50.0% | 100.0% |" in body  # all to the fallback above every confidence
