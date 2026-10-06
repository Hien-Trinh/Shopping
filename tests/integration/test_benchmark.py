"""Shopify's benchmark as eval and training data (docs/specs/step-6k.md, test points 1-5)."""

import gzip
import hashlib
import http.client
import json
import os
from urllib.error import HTTPError

import pytest

from catalog import benchmark, evaluate, taxonomy

FULL = {
    "Apparel & Accessories",
    "Apparel & Accessories > Clothing",
    "Apparel & Accessories > Clothing > Baby & Children's Clothing",
    "Apparel & Accessories > Clothing > Baby & Children's Clothing > Baby & Children's Swimwear",
    "Toys & Games",
    "Toys & Games > Toys",
    "Toys & Games > Toys > Play Vehicles",
    "Toys & Games > Toys > Play Vehicles > Toy Trains",
}
TAX = {p for p in FULL if p.count(taxonomy.SEP) < taxonomy.DEPTH}
RENAMES = {
    "Apparel & Accessories > Clothing > Baby & Toddler Clothing": (
        "Apparel & Accessories > Clothing > Baby & Children's Clothing"
    )
}


def row(title="Wooden toy train set", description="A train for kids", category=None):
    return {
        "title": title,
        "description": description,
        "category": category or "Toys & Games > Toys > Play Vehicles > Toy Trains",
    }


# 1. English filter


@pytest.mark.parametrize(
    "text",
    [
        "Wooden toy train set with three cars and a track for the kids",
        "Dell 02R713 LTO-3 Tape Drive",  # part numbers, no stopwords either way
    ],
)
def test_english_keeps_english_and_bare_part_numbers(text):
    assert benchmark.english(text)


@pytest.mark.parametrize(
    "text",
    [
        "Holzeisenbahn für Kinder mit drei Wagen und einer Schiene",
        "Tren de madera para los niños con tres vagones y una vía",
        "木製の電車のおもちゃセット",
    ],
)
def test_english_drops_other_languages(text):
    assert not benchmark.english(text)


# 2. Mapping


def test_level3_cuts_a_path_in_the_release_to_its_level3_ancestor():
    path = "Toys & Games > Toys > Play Vehicles > Toy Trains"
    assert benchmark.level3(path, TAX, RENAMES) == "Toys & Games > Toys > Play Vehicles"


def test_level3_keeps_a_level2_path():
    assert benchmark.level3("Toys & Games > Toys", TAX, RENAMES) == "Toys & Games > Toys"


def test_level3_maps_a_renamed_path_through_the_table():
    old = "Apparel & Accessories > Clothing > Baby & Toddler Clothing > Baby & Children's Swimwear"
    assert benchmark.level3(old, TAX, RENAMES) == (
        "Apparel & Accessories > Clothing > Baby & Children's Clothing"
    )


def test_level3_maps_a_dropped_deep_node_to_its_level3_ancestor():
    path = "Toys & Games > Toys > Play Vehicles > Toy Zeppelins"
    assert benchmark.level3(path, TAX, RENAMES) == "Toys & Games > Toys > Play Vehicles"


def test_level3_answers_none_for_an_unknown_path():
    assert benchmark.level3("Toys & Games > Kites", TAX, RENAMES) is None


# 3. Fetch


def page(n, total, start=0):
    rows = [
        {
            "row_idx": start + i,
            "row": {
                "product_title": f"t{start + i}",
                "product_description": "d",
                "product_image": {"src": "https://example.com/x.jpg"},
                "potential_product_categories": ["Toys & Games"],
                "ground_truth_category": "Toys & Games",
            },
            "truncated_cells": [],
        }
        for i in range(n)
    ]
    return {"rows": rows, "num_rows_total": total}


def test_fetch_pages_through_offsets_until_the_total():
    urls = []

    def get(url):
        urls.append(url)
        offset = int(url.split("offset=")[1].split("&")[0])
        return page(min(100, 250 - offset), 250, offset)

    rows = list(benchmark.fetch("test", get=get, sleep=lambda s: None))
    assert [r["title"] for r in rows] == [f"t{i}" for i in range(250)]
    assert [u[u.index("offset=") : u.index("&", u.index("offset=")) + 1] for u in urls] == [
        "offset=0&",
        "offset=100&",
        "offset=200&",
    ]


def test_fetch_keeps_only_title_description_and_category():
    (r,) = benchmark.fetch("test", get=lambda url: page(1, 1), sleep=lambda s: None)
    assert r == {"title": "t0", "description": "d", "category": "Toys & Games"}


def http_error(code):
    return HTTPError("https://example.com", code, "no", {}, None)


def test_fetch_retries_a_429_then_succeeds():
    answers = [http_error(429), page(1, 1)]

    def get(url):
        a = answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    slept = []
    assert len(list(benchmark.fetch("test", get=get, sleep=slept.append))) == 1
    assert slept == [benchmark.BACKOFF]


def test_fetch_pauses_between_pages_but_not_after_the_last():
    slept = []
    rows = benchmark.fetch(
        "test",
        get=lambda url: page(100, 200, int(url.split("offset=")[1].split("&")[0])),
        sleep=slept.append,
    )
    assert len(list(rows)) == 200
    assert slept == [benchmark.PAUSE]


@pytest.mark.parametrize("error", [TimeoutError("read timed out"), ConnectionResetError()])
def test_fetch_retries_a_timed_out_or_reset_read(error):
    answers = [error, page(1, 1)]

    def get(url):
        a = answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    assert len(list(benchmark.fetch("test", get=get, sleep=lambda s: None))) == 1


def test_fetch_gives_up_after_tries_503s_naming_split_and_offset():
    calls = []

    def get(url):
        calls.append(url)
        raise http_error(503)

    with pytest.raises(RuntimeError, match=r"test.*offset 0"):
        list(benchmark.fetch("test", get=get, sleep=lambda s: None))
    assert len(calls) == benchmark.TRIES


def test_fetch_does_not_retry_a_404():
    calls = []

    def get(url):
        calls.append(url)
        raise http_error(404)

    with pytest.raises(HTTPError):
        list(benchmark.fetch("test", get=get, sleep=lambda s: None))
    assert len(calls) == 1


# 5. Shape


def test_shape_cuts_a_long_title_at_a_word_boundary():
    item = benchmark.shape(row(title="word " * 40), "shopify-test")
    assert len(item["title"]) <= 150
    assert item["title"].endswith("word")


def test_shape_drops_an_empty_title():
    assert benchmark.shape(row(title="  "), "shopify-test") is None


@pytest.mark.parametrize("bad", [{"title": None}, {"description": 3}])
def test_shape_drops_a_row_with_a_non_string_field(bad):
    assert benchmark.shape(row() | bad, "shopify-test") is None


def test_shape_id_is_stable_for_the_same_text_and_differs_otherwise():
    a = benchmark.shape(row(), "shopify-test")
    assert a["id"] == benchmark.shape(row(), "shopify-train")["id"]
    assert a["id"] != benchmark.shape(row(description="other"), "shopify-test")["id"]
    assert a["source"] == "shopify-test"


# 4. Build


def build(test, train, n=2):
    return benchmark.build(test, train, TAX, RENAMES, n=n, seed=0)


def test_build_collapses_duplicates_to_one_id():
    ev, tr, counts = build([row(), row()], [row(title="Toy truck"), row(title="Toy truck")])
    assert len(ev) == 1 and len(tr) == 1
    assert counts["duplicates"] == 2


def test_build_never_puts_a_test_product_in_train():
    ev, tr, counts = build([row()], [row(), row(title="Toy truck")], n=1)
    assert {x["id"] for x in ev}.isdisjoint(x["id"] for x in tr)
    assert counts["leaks"] == 1


def test_build_draws_the_same_eval_for_the_same_seed_and_caps_it_at_n():
    test = [row(title=f"Toy train number {i}") for i in range(10)]
    a, _, _ = build(test, [], n=4)
    b, _, _ = build(test, [], n=4)
    assert a == b and len(a) == 4
    everything, _, _ = build(test, [], n=50)
    assert len(everything) == 10


def test_build_drops_and_counts_non_english_and_unmapped_rows():
    test = [
        row(),
        row(title="Holzeisenbahn für die Kinder mit der Schiene"),
        row(title="Toy kite", category="Toys & Games > Kites"),
    ]
    ev, _, counts = build(test, [], n=10)
    assert len(ev) == 1
    assert counts["non_english"] == 1
    assert counts["unmapped"] == {"Toys & Games > Kites": 1}


def test_build_eval_loads_with_load_labels(tmp_path):
    ev, tr, _ = build([row()], [row(title="Toy truck")])
    path = tmp_path / "labels.jsonl"
    path.write_text("".join(json.dumps(x) + "\n" for x in ev))
    tax = taxonomy.Taxonomy("shopify-2026-08", tuple(sorted(TAX)))
    (labeled,), _ = evaluate.load_labels(path, tax)
    assert labeled.category == "Toys & Games > Toys > Play Vehicles"


def test_gz_is_deterministic():
    items = [{"id": "a", "title": "t"}]
    assert benchmark.gz(items) == benchmark.gz(items)
    assert json.loads(gzip.decompress(benchmark.gz(items))) == items[0]


def test_fetch_fails_on_an_empty_page_before_the_total():
    with pytest.raises(RuntimeError, match=r"offset 0: no rows"):
        list(benchmark.fetch("test", get=lambda url: page(0, 5), sleep=lambda s: None))


def test_build_counts_a_row_without_a_category_as_malformed():
    ev, _, counts = build([row() | {"category": None}, row(title="Toy truck")], [], n=10)
    assert len(ev) == 1 and counts["malformed"] == 1


# Review round 1 (PR #90)


def test_english_drops_a_nonzero_tie():
    assert not benchmark.english("the Holzeisenbahn und")


def test_level3_prefers_the_rename_when_the_old_level3_node_still_exists():
    tax = TAX | {"Furniture > Storage > Closets", "Furniture > Storage > Closet Parts"}
    old = "Furniture > Storage > Closets > Closet Rods"
    renames = {old: "Furniture > Storage > Closet Parts > Closet Rods"}
    assert benchmark.level3(old, tax, renames) == "Furniture > Storage > Closet Parts"


def test_level3_takes_the_longest_matching_prefix():
    renames = {
        "Toys & Games > Old": "Toys & Games > Kites",
        "Toys & Games > Old > Trains": "Toys & Games > Toys > Play Vehicles",
    }
    assert benchmark.level3("Toys & Games > Old > Trains > Big", TAX, renames) == (
        "Toys & Games > Toys > Play Vehicles"
    )


def test_fetch_backoff_doubles():
    answers = [http_error(503), http_error(503), http_error(503), page(1, 1)]

    def get(url):
        a = answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    slept = []
    list(benchmark.fetch("test", get=get, sleep=slept.append))
    assert slept == [benchmark.BACKOFF, 2 * benchmark.BACKOFF, 4 * benchmark.BACKOFF]


@pytest.mark.parametrize(
    "error", [http.client.IncompleteRead(b""), json.JSONDecodeError("bad", "<html>", 0)]
)
def test_fetch_retries_a_truncated_or_non_json_body(error):
    answers = [error, page(1, 1)]

    def get(url):
        a = answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a

    assert len(list(benchmark.fetch("test", get=get, sleep=lambda s: None))) == 1


def test_fetch_names_the_last_error_when_it_gives_up():
    def get(url):
        raise http_error(503)

    with pytest.raises(RuntimeError, match="503"):
        list(benchmark.fetch("test", get=get, sleep=lambda s: None))


def test_fetch_yields_nothing_for_an_empty_split():
    assert list(benchmark.fetch("test", get=lambda url: page(0, 0), sleep=lambda s: None)) == []


def test_shape_drops_a_lone_surrogate():
    assert benchmark.shape(row(title="Toy \ud800 train"), "shopify-test") is None


def test_shape_id_is_the_hash_of_the_stored_text():
    item = benchmark.shape(row(title="word " * 40, description="d" * 6000), "shopify-test")
    text = f"{item['title']}\n{item['description']}".encode()
    assert item["id"] == hashlib.sha256(text).hexdigest()[:16]


def test_build_drops_a_train_row_with_a_test_title():
    ev, tr, counts = build([row()], [row(title="WOODEN TOY TRAIN SET", description="x")], n=1)
    assert tr == [] and counts["leaks"] == 1


def test_main_writes_both_files_and_checks_the_revision(tmp_path, monkeypatch, capsys):
    shas = iter(["abc", "abc"])
    monkeypatch.setattr(benchmark, "_get", lambda url: {"sha": next(shas)})
    rows = {"test": [row()], "train": [row(title="Toy truck")]}
    monkeypatch.setattr(benchmark, "fetch", lambda split: iter(rows[split]))
    monkeypatch.setattr(benchmark.taxonomy, "load", lambda: taxonomy.Taxonomy("t", tuple(TAX)))
    ev, tr = tmp_path / "e.jsonl", tmp_path / "t.jsonl.gz"
    benchmark.main(["--eval", str(ev), "--train", str(tr)])
    assert json.loads(ev.read_text())["title"] == "Wooden toy train set"
    assert json.loads(gzip.decompress(tr.read_bytes()))["title"] == "Toy truck"
    assert '"revision": "abc"' in capsys.readouterr().out


def main_with_old_pair(tmp_path, monkeypatch):
    """Runs benchmark.main over an existing pair; returns (eval, train) paths."""
    monkeypatch.setattr(benchmark, "_get", lambda url: {"sha": "abc"})
    rows = {"test": [row()], "train": [row(title="Toy truck")]}
    monkeypatch.setattr(benchmark, "fetch", lambda split: iter(rows[split]))
    monkeypatch.setattr(benchmark.taxonomy, "load", lambda: taxonomy.Taxonomy("t", tuple(TAX)))
    ev, tr = tmp_path / "e.jsonl", tmp_path / "t.jsonl.gz"
    ev.write_text("old eval"), tr.write_bytes(b"old train")
    with pytest.raises(OSError):
        benchmark.main(["--eval", str(ev), "--train", str(tr)])
    assert ev.read_text() == "old eval" and tr.read_bytes() == b"old train"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["e.jsonl", "t.jsonl.gz"]


def test_main_keeps_the_old_pair_when_the_train_rename_fails(tmp_path, monkeypatch):
    replace = os.replace

    def full_disk(src, dst):
        if dst.name == "t.jsonl.gz":
            raise OSError("disk full")
        replace(src, dst)

    monkeypatch.setattr(os, "replace", full_disk)
    main_with_old_pair(tmp_path, monkeypatch)


def test_main_keeps_the_old_pair_when_the_eval_fsync_fails(tmp_path, monkeypatch):
    fsync = os.fsync

    def failing(fd):
        if any(os.fstat(fd).st_ino == t.stat().st_ino for t in tmp_path.glob(".e.jsonl.*.tmp")):
            raise OSError("I/O error")
        fsync(fd)

    monkeypatch.setattr(os, "fsync", failing)
    main_with_old_pair(tmp_path, monkeypatch)


def test_main_refuses_when_the_dataset_changes_during_the_fetch(tmp_path, monkeypatch):
    shas = iter(["abc", "def"])
    monkeypatch.setattr(benchmark, "_get", lambda url: {"sha": next(shas)})
    monkeypatch.setattr(benchmark, "fetch", lambda split: iter([row()]))
    ev = tmp_path / "e.jsonl"
    with pytest.raises(RuntimeError, match="abc.*def"):
        benchmark.main(["--eval", str(ev), "--train", str(tmp_path / "t.gz")])
    assert not ev.exists()
