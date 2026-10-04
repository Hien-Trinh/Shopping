import hashlib
import json
import math

import pytest
from support import listing

from catalog import evaluate, taxonomy
from catalog.classify import UNCATEGORIZED, FakeClassifier

SHIRT = "Apparel & Accessories > Clothing > Activewear"
KIDS = "Apparel & Accessories > Clothing > Baby & Children's Clothing"
TOYS = "Toys & Games"


def row(id="a", title="Running shirt", category=SHIRT, **extra):
    return {"id": id, "title": title, "description": "", "category": category} | extra


def write(tmp_path, *rows, raw=None):
    path = tmp_path / "labels.jsonl"
    path.write_text(raw if raw is not None else "".join(json.dumps(r) + "\n" for r in rows))
    return path


def labels(tmp_path, *rows):
    return evaluate.load_labels(write(tmp_path, *rows), taxonomy.load())[0]


# --- load_labels ---------------------------------------------------------------------------


def test_a_good_file_loads_in_order_ignoring_extra_fields(tmp_path):
    got = labels(tmp_path, row("b", "Ball", TOYS, asin="B0"), row("a", "Tee", SHIRT))
    assert [(x.id, x.listing.title, x.category) for x in got] == [
        ("b", "Ball", TOYS),
        ("a", "Tee", SHIRT),
    ]


@pytest.mark.parametrize(
    "bad, message",
    [
        (row("b", category="Apparel > Shirt"), "not in the taxonomy"),
        (row("b", category=UNCATEGORIZED), "not in the taxonomy"),
        (row("a"), "duplicate id"),
        ({"id": "b", "title": "x", "description": ""}, "category"),
        (row("b", title="x" * 151), "title"),
        (row("b", title=""), "title"),
    ],
)
def test_a_bad_line_is_refused_naming_it(tmp_path, bad, message):
    with pytest.raises(ValueError, match=f"line 2.*{message}"):
        labels(tmp_path, row("a"), bad)


def test_bad_json_and_an_empty_file_are_refused(tmp_path):
    with pytest.raises(ValueError, match="line 1"):
        evaluate.load_labels(write(tmp_path, raw="{nope\n"), taxonomy.load())
    with pytest.raises(ValueError, match="no labels"):
        evaluate.load_labels(write(tmp_path, raw=""), taxonomy.load())


# --- run -----------------------------------------------------------------------------------


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class Recording:
    """Answers `answers` in order; each call takes `len(listings)` seconds on `clock`."""

    taxonomy_version = "rec-1"

    def __init__(self, answers, clock, usd=None):
        self.answers, self.clock, self.calls = list(answers), clock, []
        if usd is not None:
            self.usd = usd

    def classify(self, listings):
        self.calls.append([x.title for x in listings])
        self.clock.t += len(listings)
        if not self.calls[1:]:  # the warm-up call
            return [(SHIRT, 1.0)] * len(listings)
        return [self.answers.pop(0) for _ in listings]


def labeled(*categories):
    return [evaluate.Labeled(str(i), listing(f"t{i}"), c) for i, c in enumerate(categories)]


def test_run_warms_up_untimed_then_times_each_batch(tmp_path):
    clock = Clock()
    rec = Recording([(SHIRT, 0.9), (TOYS, 0.4), None], clock)
    result = evaluate.run(rec, labeled(SHIRT, SHIRT, TOYS), taxonomy.load(), batch=2, clock=clock)
    assert rec.calls == [["t0"], ["t0", "t1"], ["t2"]]
    assert result["seconds"] == [2.0, 1.0]
    assert result["batch"] == 2 and result["taxonomy_version"] == "rec-1"
    assert result["answers"] == [
        {"id": "0", "label": SHIRT, "category": SHIRT, "confidence": 0.9, "valid": True},
        {"id": "1", "label": SHIRT, "category": TOYS, "confidence": 0.4, "valid": True},
        {"id": "2", "label": TOYS, "category": None, "confidence": None, "valid": True},
    ]
    assert result["usd"] == 0.0 and 1 < result["rss_mb"] < 100_000  # MB, not bytes or KiB


def test_run_marks_paths_outside_the_taxonomy_invalid():
    result = evaluate.run(FakeClassifier(), labeled(TOYS), taxonomy.load(), batch=1)
    assert result["answers"][0]["valid"] is False


def test_run_fails_on_a_wrong_answer_count_or_an_error():
    class Short(FakeClassifier):
        def classify(self, listings):
            return super().classify(listings)[1:]

    with pytest.raises(ValueError, match="0 answers for 1 Listings from id '0'"):
        evaluate.run(Short(), labeled(TOYS), taxonomy.load(), batch=1)
    with pytest.raises(RuntimeError):
        evaluate.run(FakeClassifier(fail=True), labeled(TOYS), taxonomy.load(), batch=1)


# --- score ---------------------------------------------------------------------------------


def answer(label, category, confidence, valid=True):
    return {
        "id": "x",
        "label": label,
        "category": category,
        "confidence": confidence,
        "valid": valid,
    }


def result(*answers, seconds=(0.001,), usd=0.0):
    return {
        "name": "c",
        "taxonomy_version": "v",
        "batch": 1,
        "seconds": list(seconds),
        "answers": list(answers),
        "usd": usd,
        "rss_mb": 1.0,
    }


def test_score_counts_at_and_above_the_threshold():
    scored = evaluate.score(
        result(
            answer(SHIRT, SHIRT, 0.5),  # exactly at t: counts, correct
            answer(SHIRT, KIDS, 0.6),  # wrong leaf, right levels 1 and 2
            answer(TOYS, TOYS, 0.49),  # below t: Uncategorized
            answer(TOYS, None, None),  # unanswered
        ),
        [0.5],
    )[0.5]
    assert scored == {
        "accuracy": 0.25,
        "precision": 0.5,
        "uncategorized": 0.5,
        "level1": 0.5,
        "level2": 0.5,
    }


def test_score_treats_uncategorized_as_unanswered_and_invalid_as_wrong():
    scored = evaluate.score(
        result(answer(TOYS, UNCATEGORIZED, 0.9), answer(TOYS, "Fake > T", 0.9, valid=False)),
        [0.0],
    )[0.0]
    assert scored["accuracy"] == 0.0 and scored["precision"] == 0.0
    assert scored["uncategorized"] == 0.5


def test_nothing_answered_has_no_precision():
    assert evaluate.score(result(answer(TOYS, TOYS, 0.1)), [0.5])[0.5]["precision"] is None


def test_summary_latency_counts_and_cost():
    seconds = [i / 1000 for i in range(1, 101)]  # 1..100 ms
    got = evaluate.summary(
        result(answer(TOYS, None, None), answer(TOYS, "X", 0.9, False), seconds=seconds, usd=0.5)
    )
    assert got["p50_ms"] == 50.0 and got["p99_ms"] == 99.0
    assert got["per_s"] == pytest.approx(2 / sum(seconds))
    assert got["none"] == 1 and got["invalid"] == 1
    assert got["usd_per_m"] == 250_000.0


# --- main ----------------------------------------------------------------------------------


def test_run_then_report(tmp_path):
    path = write(tmp_path, row("a"), row("b", "Ball", TOYS))
    results, out = tmp_path / "results", tmp_path / "report.md"
    evaluate.main(["run", "--classifier", "fake", "--labels", str(path), "--results", str(results)])
    saved = json.loads((results / "fake.json").read_text())
    assert saved["name"] == "fake" and len(saved["labels_sha256"]) == 64
    assert len(saved["answers"]) == 2

    evaluate.main(["report", "--results", str(results), "--out", str(out)])
    report = out.read_text()
    assert "| fake | fake-1 |" in report and "different label sets" not in report

    (tmp_path / "x").mkdir()
    other = write(tmp_path / "x", row("c"))
    evaluate.main(
        [
            "run",
            "--classifier",
            "fake",
            "--labels",
            str(other),
            "--results",
            str(results),
            "--name",
            "other",
        ]
    )
    evaluate.main(["report", "--results", str(results), "--out", str(out)])
    assert "different label sets" in out.read_text()


def test_a_zero_batch_or_no_results_is_a_usage_error(tmp_path):
    with pytest.raises(SystemExit) as e:
        evaluate.main(["run", "--classifier", "fake", "--batch", "0"])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        evaluate.main(["report", "--results", str(tmp_path)])
    assert e.value.code == 2


# --- review fixes (PR #53) -----------------------------------------------------------------


def test_the_hash_is_of_the_bytes_parsed(tmp_path):
    path = write(tmp_path, row("a"))
    assert (
        evaluate.load_labels(path, taxonomy.load())[1]
        == hashlib.sha256(path.read_bytes()).hexdigest()
    )


def test_blank_lines_a_bom_and_a_raw_line_separator_load(tmp_path):
    path = tmp_path / "labels.jsonl"
    text = json.dumps(row("a", "Tee\u2028shirt"), ensure_ascii=False)
    path.write_bytes(("\ufeff" + text + "\n\n" + json.dumps(row("b")) + "\n\n").encode())
    got, _ = evaluate.load_labels(path, taxonomy.load())
    assert [x.listing.title for x in got] == ["Tee\u2028shirt", "Running shirt"]


def test_a_category_that_is_not_a_string_is_refused_naming_its_line(tmp_path):
    with pytest.raises(ValueError, match="line 1.*not in the taxonomy"):
        labels(tmp_path, row("a", category=["x"]))


def test_a_classifier_error_names_the_batch():
    class Late(FakeClassifier):  # fails after the warm-up, on the first timed batch
        def classify(self, listings):
            self.calls = getattr(self, "calls", 0) + 1
            return super().classify(listings) if self.calls == 1 else 1 / 0

    with pytest.raises(ZeroDivisionError) as e:
        evaluate.run(Late(), labeled(TOYS), taxonomy.load(), batch=1)
    assert any("id '0'" in note for note in e.value.__notes__)


def test_a_paid_candidate_reports_its_spend():
    clock = Clock()
    rec = Recording([(TOYS, 0.9)], clock, usd=0.25)
    assert evaluate.run(rec, labeled(TOYS), taxonomy.load(), batch=1, clock=clock)["usd"] == 0.25


def test_the_embedding_candidate_runs_at_threshold_0_without_a_budget(monkeypatch):
    monkeypatch.setattr(
        evaluate.classify, "fastembed", lambda models: lambda texts: [[1.0, 0.0]] * len(texts)
    )
    got = evaluate.candidate("embedding", None)
    assert got.threshold == 0.0 and got.budget == math.inf


def test_zero_total_time_has_no_rate():
    assert evaluate.summary(result(answer(TOYS, TOYS, 0.9), seconds=[0.0]))["per_s"] is None


def full(name, sha, *answers):
    return result(*answers) | {"name": name, "labels_sha256": sha, "date": "d", "machine": "m"}


def test_render_rows_and_sweep():
    report = evaluate.render(
        [full("c", "a" * 64, answer(TOYS, TOYS, 0.5), answer(TOYS, SHIRT, 0.9))], 0.5
    )
    assert "## Summary at threshold 0.5" in report
    assert "| c | v | `aaaaaaaaaaaa` | 50.0% | 50.0% | 0.0% | 50.0% | 50.0% | 1.0 | 1.0 |" in report
    assert "| 0.50 | 50.0% | 50.0% | 0.0% | 50.0% | 50.0% |" in report
    assert "| 0.55 | 0.0% | 0.0% | 50.0% | 0.0% | 0.0% |" in report


def test_render_groups_rows_by_label_set():
    report = evaluate.render(
        [
            full("x", "b" * 64, answer(TOYS, TOYS, 1)),
            full("y", "a" * 64, answer(TOYS, TOYS, 1)),
            full("z", "b" * 64, answer(TOYS, TOYS, 1)),
        ],
        0.5,
    )
    assert report.index("| y |") < report.index("| x |") < report.index("| z |")


def test_a_bad_result_file_is_named(tmp_path):
    (tmp_path / "old.json").write_text('{"name": "old"}')
    with pytest.raises(ValueError, match="old.json"):
        evaluate.main(["report", "--results", str(tmp_path), "--out", str(tmp_path / "r.md")])
    (tmp_path / "old.json").write_text('{"name": "ol')  # torn
    with pytest.raises(ValueError, match="old.json"):
        evaluate.main(["report", "--results", str(tmp_path), "--out", str(tmp_path / "r.md")])


# --- sample (step-6e.md, 6e.1) -------------------------------------------------------------


def amazon(asin, title="Ball", description=("Round.",), features=("Red",), **extra):
    item = {"parent_asin": asin, "title": title, "description": description, "features": features}
    return item | extra


def source(tmp_path, **categories):
    for name, items in categories.items():
        lines = (json.dumps(x) if isinstance(x, dict) else x for x in items)
        (tmp_path / f"meta_{name}.jsonl").write_text("".join(f"{x}\n" for x in lines))
    return tmp_path.as_uri()


def test_sample_draws_the_same_items_for_the_same_seed(tmp_path):
    url = source(tmp_path, Toys=[amazon(f"t{i}") for i in range(20)])
    draw = [x["id"] for x in evaluate.sample(url, ["Toys"], per_category=6, seed=0)]
    assert len(set(draw)) == 6
    assert draw == [x["id"] for x in evaluate.sample(url, ["Toys"], per_category=6, seed=0)]
    assert draw != [x["id"] for x in evaluate.sample(url, ["Toys"], per_category=6, seed=1)]


def test_sample_reads_only_the_first_lines(tmp_path):
    url = source(tmp_path, Toys=[amazon("a"), amazon("b"), "{torn"])
    got = evaluate.sample(url, ["Toys"], per_category=6, seed=0, lines=2)
    assert sorted(x["id"] for x in got) == ["a", "b"]


def test_sample_shapes_a_listing(tmp_path):
    long = "word " * 40  # 200 characters
    url = source(
        tmp_path,
        Toys=[amazon("a", title=long, description=["One.", "Two."], features=["Red", "Big"])],
    )
    [got] = evaluate.sample(url, ["Toys"], per_category=6, seed=0)
    assert got == {
        "id": "a",
        "title": ("word " * 30).strip(),  # cut at the last space within 150
        "description": "One.\nTwo.\nRed\nBig",
        "amazon_category": "Toys",
    }


def test_sample_hard_cuts_a_title_without_spaces_and_a_long_description(tmp_path):
    url = source(tmp_path, Toys=[amazon("a", title="x" * 200, description=["d" * 6000])])
    [got] = evaluate.sample(url, ["Toys"], per_category=6, seed=0)
    assert got["title"] == "x" * 150 and len(got["description"]) == 5000


def test_sample_treats_a_missing_or_odd_description_as_empty(tmp_path):
    url = source(
        tmp_path,
        Toys=[
            amazon("a", description=None, features="not a list"),
            {"parent_asin": "b", "title": "T"},
        ],
    )
    got = evaluate.sample(url, ["Toys"], per_category=6, seed=0)
    assert sorted((x["id"], x["description"]) for x in got) == [("a", ""), ("b", "")]


def test_sample_skips_empty_titles_and_repeated_ids_across_categories(tmp_path):
    url = source(
        tmp_path,
        Toys=[amazon("a"), amazon("b", title="  "), amazon("c", title=None)],
        Games=[amazon("a"), amazon("d")],
    )
    got = evaluate.sample(url, ["Toys", "Games"], per_category=6, seed=0)
    assert [(x["id"], x["amazon_category"]) for x in got] == [("a", "Toys"), ("d", "Games")]


def test_sample_items_load_once_labeled(tmp_path):
    url = source(tmp_path, Toys=[amazon("a", title="y " * 100)])
    items = evaluate.sample(url, ["Toys"], per_category=6, seed=0)
    assert labels(tmp_path, *[x | {"category": TOYS} for x in items])[0].id == "a"


def test_sample_cli_prints_jsonl(tmp_path, capsys):
    url = source(tmp_path, Toys=[amazon("a")], Games=[amazon("b")])
    evaluate.main(["sample", "--source", url, "--categories", "Toys", "Games"])
    out = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [x["id"] for x in out] == ["a", "b"]


def test_the_embedding_candidate_takes_a_description_length(monkeypatch):
    monkeypatch.setattr(
        evaluate.classify, "fastembed", lambda models: lambda texts: [[1.0, 0.0]] * len(texts)
    )
    assert evaluate.candidate("embedding", None, description=0).description == 0
    assert evaluate.candidate("embedding", None).description == evaluate.classify.DESCRIPTION


# --- review fixes (PR #56) -----------------------------------------------------------------


def test_sample_keeps_the_last_word_when_a_space_falls_right_at_the_cut(tmp_path):
    title = "x" * 145 + " word" + " tail"  # the space at index 150 ends "word"
    url = source(tmp_path, Toys=[amazon("a", title=title)])
    assert evaluate.sample(url, ["Toys"], per_category=6, seed=0)[0]["title"] == title[:150]


def test_sample_names_the_file_and_line_of_a_bad_row(tmp_path):
    url = source(tmp_path, Toys=[amazon("a"), "{torn"], Games=[amazon("b"), "[1, 2]"])
    with pytest.raises(ValueError, match=r"meta_Toys\.jsonl: line 2"):
        evaluate.sample(url, ["Toys"], per_category=6, seed=0)
    with pytest.raises(ValueError, match=r"meta_Games\.jsonl: line 2: not an object"):
        evaluate.sample(url, ["Games"], per_category=6, seed=0)


def test_sample_skips_an_id_that_is_not_text(tmp_path):
    url = source(tmp_path, Toys=[amazon(5), amazon(" "), amazon("a")])
    assert [x["id"] for x in evaluate.sample(url, ["Toys"], per_category=6, seed=0)] == ["a"]


def test_a_failed_category_prints_nothing(tmp_path, capsys):
    url = source(tmp_path, Toys=[amazon("a")])
    with pytest.raises(OSError):
        evaluate.main(["sample", "--source", url, "--categories", "Toys", "Missing"])
    assert capsys.readouterr().out == ""


def test_sample_reports_each_categorys_yield_on_stderr(tmp_path, capsys):
    url = source(tmp_path, Toys=[amazon("a"), amazon("b")], Games=[amazon("c")])
    evaluate.main(
        ["sample", "--source", url, "--categories", "Toys", "Games", "--per-category", "2"]
    )
    assert "Toys 2, Games 1" in capsys.readouterr().err


def test_run_passes_the_description_length_and_records_it(tmp_path, monkeypatch):
    seen = {}

    def candidate(kind, models, *, description):
        seen["description"] = description
        return FakeClassifier()

    monkeypatch.setattr(evaluate, "candidate", candidate)
    path, results = write(tmp_path, row("a")), tmp_path / "results"
    evaluate.main(
        ["run", "--classifier", "fake", "--labels", str(path), "--results", str(results)]
        + ["--description", "0"]
    )
    assert seen == {"description": 0}
    assert json.loads((results / "fake.json").read_text())["description"] == 0


@pytest.mark.parametrize(
    "argv",
    [
        ["run", "--classifier", "fake", "--description", "-1"],
        ["sample", "--per-category", "0"],
    ],
)
def test_a_negative_description_or_no_items_per_category_is_a_usage_error(argv):
    with pytest.raises(SystemExit) as e:
        evaluate.main(argv)
    assert e.value.code == 2


def test_the_laya_candidates_run_without_a_threshold(monkeypatch):
    from catalog import laya

    monkeypatch.setattr(
        evaluate.classify, "fastembed", lambda models: lambda texts: [[1.0, 0.0]] * len(texts)
    )
    monkeypatch.setattr(laya, "mlx", lambda models: lambda text, options: [1.0] * len(options))
    hierarchical = evaluate.candidate("laya-hierarchical", None, description=200)
    assert hierarchical.mode == "hierarchical" and hierarchical.description == 200
    shortlist = evaluate.candidate("laya-shortlist", None, description=200)
    assert shortlist.mode == "shortlist" and shortlist.description == 200
    assert shortlist.shortlist.threshold == 0.0 and shortlist.shortlist.budget == math.inf
    assert shortlist.shortlist.description == 200


def test_the_jev_candidate_shortlists_with_the_embedding(monkeypatch):
    from catalog import jev

    monkeypatch.setattr(
        evaluate.classify, "fastembed", lambda models: lambda texts: [[1.0, 0.0]] * len(texts)
    )
    monkeypatch.setattr(jev, "http", lambda: lambda body: {})
    got = evaluate.candidate("jev-shortlist", None, description=200)
    assert got.description == 200 and got.shortlist.description == 200
    assert got.shortlist.threshold == 0.0 and got.shortlist.budget == math.inf
