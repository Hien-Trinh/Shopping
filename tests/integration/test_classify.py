import numpy as np
import pytest
from support import listing

from catalog.classify import (
    UNCATEGORIZED,
    EmbeddingClassifier,
    ModelMissing,
    cache_key,
    fastembed,
    texts,
)
from catalog.taxonomy import Taxonomy

TAXONOMY = Taxonomy("shopify-2026-08", ("Apparel", "Apparel > Shirts", "Toys"))
VECTORS = {  # unit length is the classifier's job, so some of these aren't
    "Apparel": [1, 0, 0],
    "Apparel > Shirts": [0, 2, 0],
    "Toys": [0, 0, 1],
    "Shirt": [0.1, 0.9, 0],
    "Ball": [0, 0.6, 0.8],
    "Jacket": [0.5, 0.5, 0],
    "Anti": [-1, -1, -1],
}


def embed(texts):
    return np.array([VECTORS.get(t, [1, 1, 1]) for t in texts], dtype=float)


class Clock:
    """Advances `step` seconds every time it's read."""

    def __init__(self, step):
        self.t, self.step = 0.0, step

    def __call__(self):
        self.t += self.step
        return self.t


def classifier(**kwargs):
    return EmbeddingClassifier(TAXONOMY, embed, **{"threshold": 0.5} | kwargs)


def test_the_most_similar_path_wins_with_its_cosine_as_confidence():
    (category, confidence), (other, _) = classifier().classify([listing("Shirt"), listing("Ball")])
    assert category == "Apparel > Shirts"
    assert confidence == pytest.approx(0.9 / np.hypot(0.1, 0.9))
    assert other == "Toys"


def test_the_threshold_is_inclusive():
    cos = 0.5 / np.hypot(0.5, 0.5)  # Jacket against Apparel and against Apparel > Shirts
    assert classifier(threshold=cos).classify([listing("Jacket")])[0][0] != UNCATEGORIZED
    assert classifier(threshold=cos + 1e-9).classify([listing("Jacket")]) == [
        (UNCATEGORIZED, pytest.approx(cos))
    ]


def test_a_negative_similarity_is_clamped_to_zero():
    assert classifier(threshold=0.0).classify([listing("Anti")])[0][1] == 0.0


def test_the_text_is_the_title_then_the_description_cut_to_500_characters():
    seen = []
    c = EmbeddingClassifier(TAXONOMY, lambda texts: seen.extend(texts) or embed(texts))
    seen.clear()  # the taxonomy's paths
    c.classify([listing("Shirt"), listing("Hat", description="d" * 600)])
    assert seen == ["Shirt", "Hat " + "d" * 500]


def test_the_version_names_the_taxonomy_and_the_model():
    assert classifier().taxonomy_version == "shopify-2026-08+bge-small-en-v1.5"


def test_chunks_past_the_budget_answer_none_in_order():
    shirts = [listing("Shirt")] * 5
    # Deadline 0.1 + 0.15; the clock reads 0.2, then 0.3 before the 2nd and 3rd chunks.
    answers = classifier(chunk=2, budget=0.15, clock=Clock(0.1)).classify(shirts)
    assert [a and a[0] for a in answers] == ["Apparel > Shirts"] * 4 + [None]


def test_the_first_chunk_is_answered_even_with_no_budget_left():
    answers = classifier(chunk=2, budget=0.0, clock=Clock(1.0)).classify([listing("Shirt")] * 3)
    assert [a and a[0] for a in answers] == ["Apparel > Shirts"] * 2 + [None]


def test_no_listings_need_no_embedding():
    calls = []
    c = EmbeddingClassifier(TAXONOMY, lambda texts: calls.append(texts) or embed(texts))
    assert c.classify([]) == [] and len(calls) == 1  # the taxonomy only


def test_the_taxonomy_is_embedded_in_chunks_too():
    sizes = []
    EmbeddingClassifier(TAXONOMY, lambda texts: sizes.append(len(texts)) or embed(texts), chunk=2)
    assert sizes == [2, 1]  # one run over 1,862 paths peaks at 1.2 GB per worker


def test_a_missing_model_raises_model_missing_without_downloading(tmp_path):
    with pytest.raises(ModelMissing, match="--download"):
        fastembed(tmp_path)
    assert not any(tmp_path.rglob("*.onnx"))


def test_a_model_that_loads_but_wont_embed_is_missing(tmp_path, monkeypatch):
    import fastembed as package

    class Broken:
        def __init__(self, *args, **kwargs):
            pass

        def embed(self, texts, batch_size):
            raise RuntimeError("ONNX runtime error")

    monkeypatch.setattr(package, "TextEmbedding", Broken)
    with pytest.raises(ModelMissing):
        fastembed(tmp_path)


def test_the_version_of_a_classifier_kind_needs_no_model():  # step-6c.md: Backfill's target
    from catalog import classify, taxonomy

    assert classify.taxonomy_version("fake") == classify.FakeClassifier().taxonomy_version
    # down is fake in an outage: the same version, so the Backfill reclassifies only flagged rows
    assert classify.taxonomy_version("down") == classify.FakeClassifier().taxonomy_version
    loaded = EmbeddingClassifier(taxonomy.load(), lambda texts: np.ones((len(texts), 3)))
    assert classify.taxonomy_version("embedding") == loaded.taxonomy_version


def test_the_jev_kind_version_needs_no_model_or_key(monkeypatch):  # step-6f.md, test point 3
    from catalog import classify, jev, taxonomy

    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    tax = taxonomy.load()
    shortlist = EmbeddingClassifier(tax, lambda texts: np.ones((len(texts), 3)))
    loaded = jev.JevClassifier(tax, shortlist, lambda body: {})
    assert "jev" in classify.KINDS
    assert classify.taxonomy_version("jev") == loaded.taxonomy_version
    assert "+bge-small-en-v1.5+deeper+jev-" in loaded.taxonomy_version  # step-6f.md, 6f.3


# --- step-6e.md, 6e.1 ----------------------------------------------------------------------


def test_description_0_embeds_the_title_alone():
    seen = []
    c = EmbeddingClassifier(
        TAXONOMY, lambda texts: seen.extend(texts) or embed(texts), description=0
    )
    seen.clear()
    c.classify([listing("Hat", description="d" * 600)])
    assert seen == ["Hat"]


# --- step-6e.md, 6e.2 ----------------------------------------------------------------------


def test_top_lists_the_k_most_similar_paths_in_order():
    c = classifier()
    assert c.top([listing("Shirt"), listing("Ball")], 2) == [
        ["Apparel > Shirts", "Apparel"],
        ["Toys", "Apparel > Shirts"],
    ]
    assert c.top([], 2) == []


def test_classify_answers_tops_first_path():
    c, items = classifier(threshold=0.0), [listing(t) for t in ("Shirt", "Ball", "Jacket")]
    assert [category for category, _ in c.classify(items)] == [p[0] for p in c.top(items, 1)]


# --- step-6h.md ----------------------------------------------------------------------------


def test_a_category_scores_its_best_text():
    # "Toys" also has the text "Shirt", so a Shirt Listing scores 1.0 for Toys
    c = classifier(
        texts=[
            ("Apparel", "Apparel"),
            ("Apparel > Shirts", "Apparel > Shirts"),
            ("Shirt", "Toys"),  # the best text first, so a last-write-wins bug shows
            ("Toys", "Toys"),
        ],
        threshold=0.0,
    )
    ((category, confidence),) = c.classify([listing("Shirt")])
    assert category == "Toys" and confidence == pytest.approx(1.0)


def test_top_lists_each_category_once_by_its_best_score():
    c = classifier(
        texts=[
            ("Toys", "Toys"),
            ("Shirt", "Toys"),
            ("Ball", "Toys"),
            ("Apparel", "Apparel"),
            ("Apparel > Shirts", "Apparel > Shirts"),
        ]
    )
    assert c.top([listing("Shirt")], 3) == [["Toys", "Apparel > Shirts", "Apparel"]]


def test_no_texts_answers_exactly_as_the_paths_do():
    items = [listing(t) for t in ("Shirt", "Ball", "Jacket", "Anti")]
    pairs = [(p, p) for p in TAXONOMY.paths]
    assert classifier(texts=pairs).classify(items) == classifier().classify(items)
    assert classifier(texts=pairs).top(items, 3) == classifier().top(items, 3)


DEEPER = {
    "Apparel": [],
    "Apparel > Shirts": ["Apparel > Shirts > Tees", "Apparel > Shirts > Polos"],
    "Toys": [],
}


def test_the_path_recipe_is_one_text_per_category():
    assert texts(TAXONOMY, "path", DEEPER) == [(p, p) for p in TAXONOMY.paths]


def test_the_joined_recipe_is_the_path_then_its_descendants_last_names():
    assert texts(TAXONOMY, "joined", DEEPER) == [
        ("Apparel", "Apparel"),
        ("Apparel > Shirts: Tees, Polos", "Apparel > Shirts"),
        ("Toys", "Toys"),
    ]


def test_the_deeper_recipe_adds_each_descendant_for_its_ancestor():
    assert texts(TAXONOMY, "deeper", DEEPER) == [
        ("Apparel", "Apparel"),
        ("Apparel > Shirts", "Apparel > Shirts"),
        ("Toys", "Toys"),
        ("Apparel > Shirts > Tees", "Apparel > Shirts"),
        ("Apparel > Shirts > Polos", "Apparel > Shirts"),
    ]


# --- review fixes (PR #69) -----------------------------------------------------------------


def test_several_texts_per_category_answer_as_one_text_does():
    items = [listing(t) for t in ("Shirt", "Ball", "Jacket", "Anti")]
    doubled = [(p, p) for p in TAXONOMY.paths] * 2  # the max path, same answers
    assert classifier(texts=doubled).classify(items) == classifier().classify(items)
    assert classifier(texts=doubled).top(items, 3) == classifier().top(items, 3)


def test_negative_best_scores_still_rank():
    # Anti is negative to every text; Apparel's only text, Ball, is the most negative
    pairs = [("Ball", "Apparel"), ("Ball", "Apparel"), ("Apparel > Shirts", "Apparel > Shirts")]
    c = classifier(texts=pairs + [("Toys", "Toys")])
    assert c.top([listing("Anti")], 3) == [["Apparel > Shirts", "Toys", "Apparel"]]


@pytest.mark.parametrize(
    "pairs, message",
    [
        ([("a", "Apparel"), ("b", "Apparel > Shirts"), ("c", "Toys"), ("d", "Nope")], "'Nope'"),
        ([("a", "Apparel"), ("b", "Apparel > Shirts")], "no text for 'Toys'"),
    ],
)
def test_texts_must_name_taxonomy_paths_and_cover_them_all(pairs, message):
    with pytest.raises(ValueError, match=message):
        classifier(texts=pairs)


# --- step-6f.md, 6f.3: the text-vector cache ------------------------------------------------


class Recording:
    """`embed` that records every call's texts."""

    def __init__(self):
        self.calls = []

    def __call__(self, texts):
        self.calls.append(list(texts))
        return embed(texts)


def test_a_cached_classifier_answers_alike_without_embedding_the_texts(tmp_path):
    items = [listing(t) for t in ("Shirt", "Ball", "Jacket")]
    classifier().save(tmp_path)
    rec = Recording()
    cached = EmbeddingClassifier(TAXONOMY, rec, threshold=0.5, cache=tmp_path)
    assert rec.calls == [["probe"]]  # only the width check
    assert cached.classify(items) == classifier().classify(items)


def test_a_missing_corrupt_or_misshapen_cache_is_model_missing(tmp_path):
    with pytest.raises(ModelMissing, match="--download"):
        classifier(cache=tmp_path)
    path = tmp_path / f"{cache_key(classifier().model, [(p, p) for p in TAXONOMY.paths])}.npy"
    path.write_bytes(b"not numpy")
    with pytest.raises(ModelMissing):
        classifier(cache=tmp_path)
    for shape in [(2, 3), (3, 4)]:  # a row per text, the model's width
        np.save(path, np.ones(shape))
        with pytest.raises(ModelMissing):
            classifier(cache=tmp_path)


def test_any_text_change_changes_the_key():
    pairs = [(p, p) for p in TAXONOMY.paths]
    assert cache_key("m", pairs) != cache_key("m", [("Toy", "Toys"), *pairs[1:]])
    assert cache_key("m", pairs) != cache_key("n", pairs)


def test_a_failed_save_leaves_no_file(tmp_path, monkeypatch):
    def fail(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr("os.replace", fail)
    with pytest.raises(OSError):
        classifier().save(tmp_path)
    assert list(tmp_path.iterdir()) == []


# --- review fixes (PR #71) -----------------------------------------------------------------


@pytest.mark.parametrize(
    "content",
    [b"", np.full((3, 3), np.nan), np.ones((3, 3), dtype=np.int8)],
    ids=["empty", "nan", "integers"],
)
def test_an_empty_or_unusable_cache_is_model_missing_naming_the_cause(tmp_path, content):
    path = tmp_path / f"{cache_key(classifier().model, [(p, p) for p in TAXONOMY.paths])}.npy"
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        np.save(path, content)
    with pytest.raises(ModelMissing, match="--download") as e:
        classifier(cache=tmp_path)
    assert str(path) in str(e.value)


def test_download_builds_the_cache_the_worker_loads(tmp_path, monkeypatch):
    from catalog import classify, jev, taxonomy

    fake = lambda texts: np.ones((len(texts), 3))  # noqa: E731
    monkeypatch.setattr(classify, "fastembed", lambda models, download=False: fake)
    classify.main(["--download", "--models", str(tmp_path)])
    loaded = jev.shortlist(taxonomy.load(), fake, tmp_path / "texts")  # what the worker does
    assert loaded._paths.shape == (len(loaded.texts), 3)
