"""The student prototype (docs/specs/step-6l.md, test points 1-5)."""

from pathlib import Path

import numpy as np
import pytest
from support import listing

from catalog import classify, evaluate, student
from catalog.taxonomy import Taxonomy

TAX = Taxonomy("shopify-2026-08", ("Apparel", "Apparel > Shirts", "Toys"))


@pytest.fixture(autouse=True)
def tiny_support(monkeypatch):
    """The report's τ needs MIN_KEPT held-out rows behind it (step-6m.md); toy sets have 3."""
    monkeypatch.setattr(student, "MIN_KEPT", 1)


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
    assert student.pick_tau(scored, target=0.75, grid=[0.0, 0.3, 0.5, 0.85], min_kept=1) == 0.5


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
    assert student.pick_tau(scored, target=0.5, grid=[0.0, 0.5], min_kept=1) == 0.0


def test_pick_tau_is_none_when_no_threshold_reaches_the_target():
    assert student.pick_tau([("A", 0.9, False)], target=0.5, grid=[0.0, 0.5], min_kept=1) is None
    assert student.pick_tau([], target=0.5, grid=[0.0], min_kept=1) is None


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


def test_report_picks_tau_on_the_amazon_rows_and_marks_both(tmp_path, monkeypatch):
    # step-6l.2.md, 6l.2b point 2 (review round 1): τ from the Amazon rows; the table shows both
    evals, opus = write_eval(tmp_path)
    calls = []

    def pick(scored, *, target):
        calls.append(len(scored))
        return 0.05 if len(calls) % 2 else 0.5

    monkeypatch.setattr(student, "pick_tau", pick)
    body = student.report(
        TAX, fake_embed, SOURCES, tmp_path / "cache", evals=evals, opus=opus, clock=ticking()
    )
    assert calls[:2] == [1, 3]  # 1 of the 3 held-out rows is an Amazon row
    assert "τ = 0.05 on the Amazon held-out rows (τ = 0.50 on all of them)" in body
    assert "| 0.05 ← τ |" in body and "| 0.50 ← τ (all rows) |" in body


def test_report_refuses_a_held_out_set_without_amazon_rows(tmp_path):
    evals, opus = write_eval(tmp_path)
    shopify_only = [
        [{k: v for k, v in r.items() if k != "amazon_category"} for r in s] for s in SOURCES
    ]
    with pytest.raises(ValueError, match="no Amazon rows held out"):
        student.report(TAX, fake_embed, shopify_only, tmp_path / "cache", evals=evals, opus=opus)


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
    monkeypatch.setattr(student.classify, "fastembed", lambda models, model=None, **_: None)
    monkeypatch.setattr(student, "load_training", lambda paths: [])
    seen = {}
    monkeypatch.setattr(student, "report", lambda *_, **kw: seen.update(kw) or "# report\n")
    out = tmp_path / "r" / "student.md"
    student.main(["--models", str(tmp_path), "--out", str(out)])
    assert out.read_text() == "# report\n"
    assert seen["finetuned"] is None  # nothing fine-tuned in tmp_path: the frozen students only


def test_main_download_fetches_the_finetuned_student_and_stops(tmp_path, monkeypatch):
    # 6l.3: `--download` is the student's own, the Hub repo may not exist when classify downloads
    calls = []

    def embedder(models, download):
        calls.append(download)
        return None, "d" * 40

    def load_head(models, revision, download):
        calls.append((revision, download))

    monkeypatch.setattr(student, "embedder", embedder)
    monkeypatch.setattr(student, "load_head", load_head)
    monkeypatch.setattr(student, "build_index", lambda tax, embed, rev, models: calls.append(rev))
    monkeypatch.setattr(student, "report", lambda *_, **__: pytest.fail("no report"))
    student.main(["--models", str(tmp_path), "--download"])
    assert calls == [
        True,
        ("d" * 40, True),
        "d" * 40,
    ]  # the head and the index at the encoder's revision


# docs/specs/step-6l.3.md: the fine-tuned student


def test_softmax_round_trips_through_a_head_file(tmp_path):
    # 6l.3 test point 1
    x, y = toy()
    head = student.Softmax.fit(x, y)
    head.save(tmp_path / "head.npz")
    back = student.Softmax.load(tmp_path / "head.npz")
    assert back.classes == head.classes
    assert back.predict(x) == head.predict(x)


def test_vectors_are_cached_per_model(tmp_path):
    # 6l.3 test point 2: the fine-tuned and frozen vectors of one text never share a file
    calls = []

    def embed(texts):
        calls.append(list(texts))
        return np.array([[1.0, 0.0] for _ in texts])

    student.vectors(["ab"], embed, tmp_path, model="bge-small")
    student.vectors(["ab"], embed, tmp_path, model="student-ft")
    assert len(calls) == 2
    student.vectors(["ab"], embed, tmp_path, model="student-ft")
    assert len(calls) == 2


FT_ROWS = [{"title": "Blue shirt", "description": "", "category": "Apparel > Shirts"}] * 3 + [
    {"title": "Toy train", "description": "", "category": "Toys"}
] * 3


def fake_head():
    """A head fitted on `fake_embed`'s vectors, standing in for the fine-tuning script's."""
    x = student.unit(fake_embed([r["title"] for r in FT_ROWS]))
    return student.Softmax.fit(x, [r["category"] for r in FT_ROWS])


def test_the_finetuned_student_answers_a_taxonomy_path_for_every_listing(tmp_path):
    # 6l.3 test point 3: the head comes from a file, the kNN from the fine-tuned vectors
    c = student.StudentClassifier.train(
        TAX, fake_embed, FT_ROWS, "ft", cache=tmp_path, model="student-ft", head=fake_head()
    )
    got = c.classify([listing(title="Red shirt"), listing(title="Wooden train")])
    assert [a[0] for a in got] == ["Apparel > Shirts", "Toys"]
    assert all(0 <= a[1] <= 1 for a in got)
    assert c.taxonomy_version.endswith("+student-ft+student-ft")
    knn = student.StudentClassifier.train(
        TAX, fake_embed, FT_ROWS, "ft-knn", cache=tmp_path, model="student-ft", k=3
    )
    assert [a[0] for a in knn.classify([listing(title="Red shirt")])] == ["Apparel > Shirts"]
    assert knn.taxonomy_version.endswith("+student-ft+student-ft-knn")


def test_a_missing_finetuned_head_names_the_download(tmp_path):
    with pytest.raises(classify.ModelMissing, match="catalog.student --download"):
        student.load_head(tmp_path)


def test_evaluate_knows_the_finetuned_candidates():
    assert {"student-ft", "student-ft-knn"} <= set(evaluate.CANDIDATES)


def swapped_head():
    """A head that calls shirts Toys and trains Shirts: only a report that uses the supplied head
    (not one it refits) scores 0% with it."""
    x = student.unit(fake_embed([r["title"] for r in FT_ROWS]))
    swap = {"Apparel > Shirts": "Toys", "Toys": "Apparel > Shirts"}
    return student.Softmax.fit(x, [swap[r["category"]] for r in FT_ROWS])


def test_report_lists_the_finetuned_students_when_given(tmp_path):
    # 6l.3 test point 4: the same sections as the frozen students, scored with the given head,
    # plus 6l.3's bar against kNN; a note when there is none
    evals, opus = write_eval(tmp_path)
    kw = dict(evals=evals, opus=opus, clock=ticking())
    body = student.report(
        TAX,
        fake_embed,
        SOURCES,
        tmp_path / "cache",
        finetuned=(fake_embed, swapped_head(), "student-ft@abc123def456"),
        **kw,
    )
    ft = body[body.index("## student-ft\n") : body.index("## student-ft-knn")]
    assert "Student alone: exact 0.0%" in ft and "so this is optimistic" in ft
    assert "Bar 6l.3 (alone 3% over kNN's 100.0%, and a cascade point no worse than kNN's" in ft
    assert ft.count("not met") == 2  # 6l.2's bar and 6l.3's
    knn_ft = body[body.index("## student-ft-knn (k=5)") :]
    assert "Student alone: exact 100.0%" in knn_ft and "Bar 6l.3" in knn_ft
    assert body.count("Bar (beats Jev alone with 70% or more kept local)") == 4
    without = student.report(TAX, fake_embed, SOURCES, tmp_path / "cache", **kw)
    assert "## student-ft" not in without and "catalog.student --download" in without


def test_beats_knn_needs_the_margin_and_a_no_worse_cascade_point():
    # step-6l.3.md, the bar: +3 points alone, and a point at least as accurate at a higher kept
    # rate or more accurate at the same kept rate
    knn_point = (0.837, 0.700)
    better = [(0.90, 0.700), (0.70, 0.750)]
    assert student.beats_knn(0.687, better, 0.657, knn_point)
    assert not student.beats_knn(0.686, better, 0.657, knn_point)  # 2.9 points is not 3
    assert not student.beats_knn(0.70, [(0.837, 0.700)], 0.657, knn_point)  # the same point
    assert not student.beats_knn(0.70, [(0.90, 0.699), (0.80, 0.75)], 0.657, knn_point)
    assert student.beats_knn(0.70, [(0.837, 0.701)], 0.657, knn_point)


def hub_cache(models: Path, rev: str, head: student.Softmax | None = None) -> Path:
    """What `--download` leaves in `models`: the Hub cache layout at one revision."""
    repo = models / f"models--{student.FT_REPO.replace('/', '--')}"
    snapshot = repo / "snapshots" / rev
    (snapshot / "onnx").mkdir(parents=True)
    (snapshot / "onnx" / "model_quantized.onnx").write_bytes(b"onnx")
    for name in student.FILES:
        (snapshot / name).write_text("{}")
    if head:
        head.save(snapshot / student.HEAD)
    (repo / "refs").mkdir()
    (repo / "refs" / "main").write_text(rev)
    return snapshot


def test_the_head_is_loaded_from_the_encoders_revision(tmp_path):
    # review round 1: encoder and head from the same Hub commit, and the revision names the cache
    old, new = "a" * 40, "b" * 40
    hub_cache(tmp_path, new, fake_head())
    assert student.load_head(tmp_path, revision=new).classes == fake_head().classes
    with pytest.raises(classify.ModelMissing, match="catalog.student --download") as e:
        student.load_head(tmp_path, revision=old)
    assert e.value.__cause__ is not None and "(" in str(e.value)  # the cause is in the message
    assert student._name(new) == "student-ft@bbbbbbbbbbbb-int8"


def test_a_missing_finetuned_encoder_names_the_students_download(tmp_path, monkeypatch):
    # review round 1: classify's hint would refetch bge-small, not the fine-tuned encoder
    def missing(models, model, **kwargs):
        raise classify.ModelMissing(f"{model} won't load: {classify.DOWNLOAD}") from ValueError("x")

    monkeypatch.setattr(classify, "fastembed", missing)
    hub_cache(tmp_path, student.FT_REVISION)  # the files are there; the encoder won't load
    with pytest.raises(classify.ModelMissing, match="catalog.student --download") as e:
        student.embedder(tmp_path)
    assert "catalog.classify --download" not in str(e.value) and "ValueError" in str(e.value)


def test_evaluate_dispatches_the_finetuned_candidates(tmp_path, monkeypatch):
    # review round 1: the branch in evaluate.candidate, not only the names
    seen = []
    monkeypatch.setattr(student, "finetuned", lambda tax, models, kind: seen.append(kind))
    evaluate.candidate("student-ft", tmp_path)
    evaluate.candidate("student-ft-knn", tmp_path)
    assert seen == ["ft", "ft-knn"]


def test_main_says_why_the_finetuned_student_is_skipped(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(student.classify, "fastembed", lambda models, model=None, **_: None)
    monkeypatch.setattr(student, "load_training", lambda paths: [])
    monkeypatch.setattr(student, "report", lambda *_, **__: "# report\n")
    student.main(["--models", str(tmp_path), "--out", str(tmp_path / "r.md")])
    assert "no fine-tuned student" in capsys.readouterr().err


def test_finetuned_wires_the_head_into_the_ft_candidate_only(tmp_path, monkeypatch):
    head = fake_head()
    monkeypatch.setattr(student, "embedder", lambda models: (fake_embed, "c" * 40))
    monkeypatch.setattr(student, "load_head", lambda models, revision: (revision, head)[1])
    monkeypatch.setattr(student, "load_training", lambda: FT_ROWS)
    ft = student.finetuned(TAX, tmp_path, "ft")
    assert ft.model is head
    assert ft.taxonomy_version.endswith("+student-ft@cccccccccccc-int8+student-ft")
    knn = student.finetuned(TAX, tmp_path, "ft-knn")
    assert isinstance(knn.model, student.Knn)


# docs/specs/step-6m.md: the student in the pipeline

REV = student.FT_REVISION


def test_the_index_round_trips_and_is_unit_vectors(tmp_path, monkeypatch):
    # 6m test point 1
    monkeypatch.setattr(student, "load_training", lambda: FT_ROWS)
    path = student.build_index(TAX, fake_embed, REV, tmp_path)
    assert path == tmp_path / "student" / f"index-{REV[:12]}-int8.npz"
    x, y = student.load_index(path, TAX, width=2)
    assert x.shape == (6, 2) and x.dtype == np.float32 and np.allclose(np.linalg.norm(x, axis=1), 1)
    assert y == [r["category"] for r in FT_ROWS]


def test_a_bad_index_names_the_download(tmp_path):
    # 6m test point 1: missing, truncated, the wrong shape, not finite
    path = tmp_path / "index.npz"
    with pytest.raises(classify.ModelMissing, match="catalog.student --download"):
        student.load_index(path, TAX, width=2)
    path.write_bytes(b"PK\x03\x04")
    with pytest.raises(classify.ModelMissing, match="catalog.student --download"):
        student.load_index(path, TAX, width=2)
    np.savez(path, x=np.ones((2, 3), np.float32), y=np.array(["Toys", "Toys"]))
    with pytest.raises(classify.ModelMissing, match="width 2"):
        student.load_index(path, TAX, width=2)
    np.savez(path, x=np.array([[np.nan, 1.0]], np.float32), y=np.array(["Toys"]))
    with pytest.raises(classify.ModelMissing, match="finite"):
        student.load_index(path, TAX, width=2)
    np.savez(path, x=np.ones((2, 2), np.float32), y=np.array(["Toys"]))  # x and y disagree
    with pytest.raises(classify.ModelMissing):
        student.load_index(path, TAX, width=2)


def test_an_index_label_outside_the_taxonomy_is_refused_by_name(tmp_path):
    path = tmp_path / "index.npz"
    np.savez(path, x=np.ones((1, 2), np.float32), y=np.array(["Kites"]))
    with pytest.raises(ValueError, match="Kites"):
        student.load_index(path, TAX, width=2)


def test_the_pipeline_classifier_answers_every_listing_from_the_index(tmp_path, monkeypatch):
    # 6m test point 2: a path and a confidence for every Listing, in order, in chunks
    calls = []

    def embed(texts):
        calls.append(len(texts))
        return fake_embed(texts)

    monkeypatch.setattr(student, "load_training", lambda: FT_ROWS)
    monkeypatch.setattr(student, "embedder", lambda models: (embed, REV))
    student.build_index(TAX, fake_embed, REV, tmp_path)
    c = student.pipeline(TAX, tmp_path)
    assert c.classify([]) == []
    got = c.classify([listing(title="Red shirt"), listing(title="Wooden train")])
    assert [a[0] for a in got] == ["Apparel > Shirts", "Toys"]
    assert all(0 <= a[1] <= 1 for a in got)
    calls.clear()
    batch = [listing(title="shirt" if i % 2 else "train") for i in range(600)]
    got = c.classify(batch)
    assert len(got) == 600 and max(calls) <= student.CHUNK and sum(calls) == 600
    assert [a[0] for a in got[:2]] == ["Toys", "Apparel > Shirts"]
    assert all(a is not None and a[0] != classify.UNCATEGORIZED for a in got)
    assert c.taxonomy_version == student.version(TAX.version, REV)
    assert isinstance(c.model, student.Knn) and c.model.k == student.K


def test_the_pipeline_needs_the_index(tmp_path, monkeypatch):
    monkeypatch.setattr(student, "embedder", lambda models: (fake_embed, REV))
    with pytest.raises(classify.ModelMissing, match="catalog.student --download"):
        student.pipeline(TAX, tmp_path)


def test_the_version_names_every_setting(tmp_path, monkeypatch):
    # 6m test point 3
    v = student.version("shopify-2026-08", "a" * 40)
    assert v.startswith("shopify-2026-08+student-ft-knn@aaaaaaaaaaaa+int8+k5+train-")
    assert len(v.rpartition("-")[2]) == 8
    assert student.version("shopify-2026-08", "b" * 40) != v
    monkeypatch.setattr(student, "ONNX", "onnx/model.onnx")
    assert "+fp32+" in student.version("shopify-2026-08", "a" * 40)
    monkeypatch.setattr(student, "ONNX", "onnx/model_quantized.onnx")
    monkeypatch.setattr(student, "K", 7)
    assert "+k7+" in student.version("shopify-2026-08", "a" * 40)
    monkeypatch.setattr(student, "K", 5)
    other = tmp_path / "rows.jsonl"
    other.write_text("{}\n")
    monkeypatch.setattr(student, "TRAIN", (other,))
    assert student.version("shopify-2026-08", "a" * 40) != v  # the training data is in it


def test_pick_tau_needs_min_kept_rows_behind_it():
    # 6m test point 5: the 6l.2b review's deferral
    scored = [("A", 0.9, True), ("B", 0.8, True), ("C", 0.4, False), ("D", 0.2, False)]
    grid = [0.0, 0.3, 0.5, 0.85]
    assert student.pick_tau(scored, target=0.6, grid=grid, min_kept=1) == 0.3
    assert student.pick_tau(scored, target=0.6, grid=grid, min_kept=3) == 0.3
    assert student.pick_tau(scored, target=0.6, grid=grid, min_kept=4) is None


def test_report_prints_the_held_out_bands(tmp_path):
    # 6m test point 5: the accuracy of the band just above each τ, so the knee is visible
    evals, opus = write_eval(tmp_path)
    body = student.report(
        TAX, fake_embed, SOURCES, tmp_path / "cache", evals=evals, opus=opus, clock=ticking()
    )
    assert "Bands on the Amazon held-out rows (τ: right/rows): 1.00: 1/1" in body


def test_bands_count_each_row_once():
    scored = [("A", 0.9, True), ("B", 0.55, False), ("C", 0.5, True), ("D", 0.0, False)]
    assert student.bands(scored, grid=[0.0, 0.5, 1.0]) == [(0.0, 1, 0), (0.5, 3, 2), (1.0, 0, 0)]
