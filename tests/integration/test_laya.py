import numpy as np
import pytest
from support import listing

from catalog.classify import EmbeddingClassifier
from catalog.laya import LayaClassifier
from catalog.taxonomy import Taxonomy

TAXONOMY = Taxonomy(
    "shopify-2026-08",
    (
        "Apparel",
        "Apparel > Shirts",
        "Apparel > Shirts > Tees",
        "Apparel > Shirts > Polos",
        "Apparel > Hats",
        "Toys",
    ),
)


class Chooser:
    """Answers `probabilities[i]` on the i-th call (or `default`) and records each question."""

    def __init__(self, *probabilities, default=None):
        self.probabilities, self.default, self.asked = list(probabilities), default, []

    def __call__(self, text, options):
        self.asked.append((text, list(options)))
        if self.probabilities:
            return self.probabilities.pop(0)
        return self.default(options)


def first(options):
    return [1.0] + [0.0] * (len(options) - 1)


def test_hierarchical_walks_down_to_a_category_without_children():
    choose = Chooser([0.8, 0.2], [0.4, 0.5], default=first)
    [(path, confidence)] = LayaClassifier(TAXONOMY, choose).classify([listing("Hat")])
    assert [options for _, options in choose.asked] == [["Apparel", "Toys"], ["Shirts", "Hats"]]
    assert path == "Apparel > Hats" and confidence == pytest.approx(0.8 * 0.5)


def test_hierarchical_stops_at_three_levels_multiplying_the_probabilities():
    choose = Chooser([0.9, 0.1], [0.6, 0.4], [0.3, 0.7])
    [(path, confidence)] = LayaClassifier(TAXONOMY, choose).classify([listing("Polo")])
    assert path == "Apparel > Shirts > Polos"
    assert confidence == pytest.approx(0.9 * 0.6 * 0.7)
    assert choose.asked[2][1] == ["Tees", "Polos"]


def test_hierarchical_stops_at_a_childless_first_level_category():
    choose = Chooser([0.3, 0.7])
    assert LayaClassifier(TAXONOMY, choose).classify([listing("Ball")]) == [("Toys", 0.7)]
    assert len(choose.asked) == 1


def test_the_text_is_the_title_then_the_description_cut():
    choose = Chooser(default=first)
    c = LayaClassifier(TAXONOMY, choose, description=3)
    c.classify([listing("Hat", description="abcdef"), listing("Cap")])
    assert [text for text, _ in choose.asked][::3] == ["Hat abc", "Cap"]


def test_shortlist_offers_the_embeddings_k_nearest_paths():
    vectors = {"Apparel > Shirts > Polos": [0, 1], "Apparel > Shirts": [0.6, 0.8], "Polo": [0, 1]}
    embedding = EmbeddingClassifier(
        TAXONOMY,
        lambda texts: np.array([vectors.get(t, [1, 0]) for t in texts], dtype=float),
        threshold=0.0,
    )
    choose = Chooser([0.25, 0.75])
    c = LayaClassifier(TAXONOMY, choose, mode="shortlist", shortlist=embedding, k=2)
    assert c.classify([listing("Polo")]) == [("Apparel > Shirts", 0.75)]
    assert choose.asked == [("Polo", ["Apparel > Shirts > Polos", "Apparel > Shirts"])]


def test_a_probability_list_of_the_wrong_length_fails_the_call():
    with pytest.raises(ValueError, match="3 probabilities for 2 options"):
        LayaClassifier(TAXONOMY, Chooser([0.2, 0.3, 0.5])).classify([listing("Hat")])


def test_the_version_names_the_taxonomy_and_the_mode():
    assert (
        LayaClassifier(TAXONOMY, first).taxonomy_version == "shopify-2026-08+laya-mlx-hierarchical"
    )


def test_shortlist_mode_needs_an_embedding():
    with pytest.raises(ValueError, match="shortlist"):
        LayaClassifier(TAXONOMY, first, mode="shortlist")
