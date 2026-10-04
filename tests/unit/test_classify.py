import numpy as np
import pytest
from support import listing

from catalog.classify import UNCATEGORIZED, EmbeddingClassifier, ModelMissing, fastembed
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


def test_a_missing_model_raises_model_missing_without_downloading(tmp_path):
    with pytest.raises(ModelMissing, match="--download"):
        fastembed(tmp_path)
    assert not any(tmp_path.rglob("*.onnx"))
