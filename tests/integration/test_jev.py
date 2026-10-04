import io
import json
from urllib.error import HTTPError

import numpy as np
import pytest
from support import listing

from catalog import jev
from catalog.classify import EmbeddingClassifier
from catalog.jev import JevClassifier
from catalog.taxonomy import Taxonomy

TAXONOMY = Taxonomy("shopify-2026-08", ("Apparel", "Apparel > Shirts", "Toys"))
VECTORS = {"Apparel": [1, 0], "Apparel > Shirts": [0.8, 0.6], "Toys": [0, 1], "Tee": [0.6, 0.8]}


def embedding():
    return EmbeddingClassifier(
        TAXONOMY,
        lambda texts: np.array([VECTORS.get(t, [1, 1]) for t in texts], dtype=float),
        threshold=0.0,
    )


class Api:
    """Answers `choices` in order ({option key: probability}) and records each request body."""

    def __init__(self, *choices, tokens=1000):
        self.choices, self.tokens, self.bodies = list(choices), tokens, []

    def __call__(self, body):
        self.bodies.append(body)
        p = self.choices.pop(0)
        best = max(p, key=p.get)
        return {
            "model": "jev-1.13.0",
            "answers": {"c": {"type": "choice", "choice": best, "probabilities": p}},
            "usage": {"input_tokens": self.tokens, "output_tokens": 30},
        }


def test_one_choice_among_the_shortlist_keyed_by_number():
    api = Api({"1": 0.2, "2": 0.7, "3": 0.1})
    c = JevClassifier(TAXONOMY, embedding(), api, k=3, description=3)
    assert c.classify([listing("Tee", description="abcdef")]) == [("Apparel", 0.7)]
    [body] = api.bodies
    assert body["model"] == jev.MODEL and body["state"] == "Tee abc"
    assert body["questions"]["c"] == {
        "type": "choice",
        "instructions": jev.QUESTION,
        "criteria": {"1": "Apparel > Shirts", "2": "Apparel", "3": "Toys"},  # by similarity
    }


def test_an_answer_outside_the_options_comes_back_as_itself():
    api = Api({"1": 0.4, "2": 0.6})
    api.choices[0]["11"] = 0.9
    assert JevClassifier(TAXONOMY, embedding(), api, k=2).classify([listing("Tee")]) == [
        ("11", 0.9)
    ]


def test_spend_adds_up_from_input_tokens():
    c = JevClassifier(TAXONOMY, embedding(), Api({"1": 1.0}, {"1": 1.0}, tokens=500_000), k=1)
    c.classify([listing("Tee"), listing("Ball")])
    assert c.usd == pytest.approx(2 * 500_000 * jev.USD_PER_TOKEN)
    assert round(jev.USD_PER_TOKEN * 1_000_000, 9) == 0.042


def test_the_version_names_the_taxonomy_and_the_model():
    c = JevClassifier(TAXONOMY, embedding(), Api())
    assert c.taxonomy_version == "shopify-2026-08+jev-1.13.0-shortlist"


# --- the HTTP call -------------------------------------------------------------------------


class Urlopen:
    """Fails with each status in `fails` first, then answers `answer`; records each request."""

    def __init__(self, answer, *fails):
        self.answer, self.fails, self.requests = answer, list(fails), []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        if self.fails:
            code = self.fails.pop(0)
            raise HTTPError(request.full_url, code, "no", {}, io.BytesIO(b'{"detail": "x"}'))
        return io.BytesIO(json.dumps(self.answer).encode())


def test_a_missing_key_refuses_before_any_call(monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="TYPESAFE_API_KEY"):
        jev.http()


def test_the_call_posts_the_body_with_the_key(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "k-test")
    urlopen = Urlopen({"ok": 1})
    assert jev.http(urlopen=urlopen)({"state": "x"}) == {"ok": 1}
    [(request, timeout)] = urlopen.requests
    assert request.full_url == jev.URL and request.get_method() == "POST"
    assert request.get_header("Authorization") == "Bearer k-test"
    assert json.loads(request.data) == {"state": "x"} and timeout > 0


def test_rate_limits_and_overload_retry_with_backoff(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "k-test")
    slept, urlopen = [], Urlopen({"ok": 1}, 429, 529)
    assert jev.http(urlopen=urlopen, sleep=slept.append)({}) == {"ok": 1}
    assert slept == [1, 2] and len(urlopen.requests) == 3


def test_other_errors_and_too_many_retries_fail_without_the_key(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "k-secret")
    with pytest.raises(RuntimeError, match="422") as e:
        jev.http(urlopen=Urlopen({}, 422), sleep=lambda s: None)({})
    assert "k-secret" not in str(e.value)
    with pytest.raises(RuntimeError, match="429"):
        jev.http(urlopen=Urlopen({}, *[429] * 5), sleep=lambda s: None, attempts=5)({})
