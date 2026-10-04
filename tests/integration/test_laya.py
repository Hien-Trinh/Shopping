import sys

import numpy as np
import pytest
from support import listing

from catalog import laya
from catalog.classify import EmbeddingClassifier, ModelMissing
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


# --- review fixes (PR #57) -----------------------------------------------------------------


def test_every_question_gets_the_cut_text_in_both_modes():
    choose = Chooser(default=first)
    LayaClassifier(TAXONOMY, choose, description=3).classify([listing("Tee", description="abcdef")])
    assert {text for text, _ in choose.asked} == {"Tee abc"} and len(choose.asked) == 3
    embedding = EmbeddingClassifier(TAXONOMY, lambda t: np.ones((len(t), 2)), threshold=0.0)
    choose = Chooser(default=first)
    c = LayaClassifier(TAXONOMY, choose, mode="shortlist", shortlist=embedding, description=3)
    c.classify([listing("Tee", description="abcdef")])
    assert [text for text, _ in choose.asked] == ["Tee abc"]


@pytest.mark.parametrize("p", [[float("nan"), 0.5], [float("inf"), 0.5], [-0.1, 0.5]])
def test_a_probability_that_is_not_finite_or_is_negative_fails_the_call(p):
    with pytest.raises(ValueError, match="not probabilities"):
        LayaClassifier(TAXONOMY, Chooser(p)).classify([listing("Hat")])


def test_a_tie_goes_to_the_first_option():
    assert LayaClassifier(TAXONOMY, Chooser([0.5, 0.5], [0.5, 0.5], [0.5, 0.5])).classify(
        [listing("Tee")]
    ) == [("Apparel > Shirts > Tees", 0.125)]


def test_an_unknown_mode_is_refused():
    with pytest.raises(ValueError, match="mode"):
        LayaClassifier(TAXONOMY, first, mode="shortlst")


class Agent:
    def __init__(self, answers):
        self.answers, self.questions = answers, []

    def predict(self, state, questions):
        self.questions.append((state, questions))
        return {"answers": {"c": {"probabilities": self.answers}}}


def test_the_mlx_chooser_asks_one_choice_and_orders_the_probabilities(monkeypatch, tmp_path):
    agent, loaded = Agent({"Toys": 0.25, "Apparel": 0.75}), []
    fake = type("laya_mlx", (), {"load": staticmethod(lambda path: loaded.append(path) or agent)})
    monkeypatch.setitem(sys.modules, "laya_mlx", fake)
    choose = laya.mlx(tmp_path)
    assert choose("Tee", ["Apparel", "Toys"]) == [0.75, 0.25]
    assert loaded == [str(tmp_path / "laya-mlx")]
    [(state, questions)] = agent.questions
    assert state == "Tee" and questions == {
        "c": {"type": "choice", "instructions": laya.QUESTION, "criteria": ["Apparel", "Toys"]}
    }


def test_the_mlx_chooser_names_an_option_laya_did_not_answer(monkeypatch, tmp_path):
    fake = type("laya_mlx", (), {"load": staticmethod(lambda path: Agent({"Apparel": 1.0}))})
    monkeypatch.setitem(sys.modules, "laya_mlx", fake)
    with pytest.raises(ValueError, match="no probability for 'Toys'"):
        laya.mlx(tmp_path)("Tee", ["Apparel", "Toys"])


def test_a_missing_or_broken_laya_is_model_missing(monkeypatch, tmp_path):
    monkeypatch.setitem(sys.modules, "laya_mlx", None)  # import fails
    with pytest.raises(ModelMissing, match="uv sync --group laya"):
        laya.mlx(tmp_path)

    def broken(path):
        raise RuntimeError("bad safetensors header")

    monkeypatch.setitem(
        sys.modules, "laya_mlx", type("laya_mlx", (), {"load": staticmethod(broken)})
    )
    with pytest.raises(ModelMissing, match="hf download"):
        laya.mlx(tmp_path)
