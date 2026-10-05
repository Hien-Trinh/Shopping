import io
import json
from urllib.error import HTTPError

import numpy as np
import pytest
from support import listing

from catalog import evaluate, jev, taxonomy
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


# --- review fixes (PR #59) -----------------------------------------------------------------


def test_the_default_shortlist_is_ten_paths():
    tax = Taxonomy("t", tuple(f"P{i}" for i in range(12)))
    flat = EmbeddingClassifier(tax, lambda texts: np.ones((len(texts), 2)), threshold=0.0)
    api = Api({str(i): 0.1 for i in range(1, 11)})
    JevClassifier(tax, flat, api).classify([listing("Tee")])
    assert list(api.bodies[0]["questions"]["c"]["criteria"]) == [str(i) for i in range(1, 11)]


def test_a_failed_call_names_the_listing_and_the_spend_so_far():
    api = Api({"1": 1.0}, tokens=1_000_000)

    def call(body):
        if api.bodies:
            raise RuntimeError("Jev answered 500")
        return api(body)

    c = JevClassifier(TAXONOMY, embedding(), call, k=1)
    with pytest.raises(RuntimeError) as e:
        c.classify([listing("Tee"), listing("Ball")])
    assert any("'Ball'" in n and "$0.0420" in n for n in e.value.__notes__)


@pytest.mark.parametrize(
    "response",
    [{"answers": {"c": {"choice": "1", "probabilities": {}}}}, {"usage": {"input_tokens": 1}}],
)
def test_a_response_of_another_shape_is_named(response):
    c = JevClassifier(TAXONOMY, embedding(), lambda body: response, k=1)
    with pytest.raises(ValueError, match="unexpected Jev response"):
        c.classify([listing("Tee")])


class Flaky(Urlopen):
    def __init__(self, answer, *errors):
        super().__init__(answer)
        self.errors = list(errors)

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        if self.errors:
            raise self.errors.pop(0)
        return io.BytesIO(json.dumps(self.answer).encode())


def test_network_errors_retry_too(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "k-test")
    from urllib.error import URLError

    slept = []
    urlopen = Flaky({"ok": 1}, URLError("reset"), TimeoutError(), ConnectionResetError())
    assert jev.http(urlopen=urlopen, sleep=slept.append)({}) == {"ok": 1}
    assert slept == [1, 2, 4]


def test_retries_run_out_with_the_count_and_no_chained_error(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "k-secret")
    slept, urlopen = [], Urlopen({}, *[429] * 3)
    with pytest.raises(RuntimeError, match="429 after 3 attempts") as e:
        jev.http(urlopen=urlopen, sleep=slept.append, attempts=3)({})
    assert len(urlopen.requests) == 3 and slept == [1, 2]
    assert e.value.__cause__ is None and e.value.__suppress_context__
    with pytest.raises(RuntimeError, match="after 2 attempts") as e:
        jev.http(urlopen=Flaky({}, TimeoutError(), TimeoutError()), sleep=slept.append, attempts=2)(
            {}
        )
    assert e.value.__suppress_context__


def test_a_body_that_is_not_json_is_named(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "k-test")

    def urlopen(request, timeout):
        return io.BytesIO(b"<html>bad gateway</html>")

    with pytest.raises(RuntimeError, match="not JSON"):
        jev.http(urlopen=urlopen)({})


def test_a_redirect_never_carries_the_key(monkeypatch):
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # urllib follows a 302 after a POST with a GET
            seen.append(self.path)
            seen.append(self.headers.get("Authorization"))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")

        def do_POST(self):
            seen.append(self.path)
            if self.path == "/v1/systemone":
                self.send_response(302)
                self.send_header("Location", "/elsewhere")
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                seen.append(self.headers.get("Authorization"))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"{}")

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("TYPESAFE_API_KEY", "k-secret")
    monkeypatch.setattr(jev, "URL", f"http://127.0.0.1:{server.server_port}/v1/systemone")
    try:
        with pytest.raises(RuntimeError, match="302"):
            jev.http(sleep=lambda s: None)({})
    finally:
        server.shutdown()
    assert seen == ["/v1/systemone"]


def test_the_warm_up_call_is_not_charged_to_the_run(tmp_path):
    class Paid:
        taxonomy_version, usd = "paid-1", 0.0

        def classify(self, listings):
            self.usd += len(listings)
            return [("Toys", 1.0)] * len(listings)

    result = evaluate.run(Paid(), labeled_two(), taxonomy.load(), batch=1)
    assert result["usd"] == 2.0


def labeled_two():
    return [evaluate.Labeled(str(i), listing(f"t{i}"), "Toys") for i in range(2)]


def test_an_error_body_that_wont_read_still_gives_the_status(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "k-test")

    class Reset(io.BytesIO):
        def read(self, *args):
            raise ConnectionResetError("reset by peer")

    def urlopen(request, timeout):
        raise HTTPError(request.full_url, 302, "Found", {}, Reset())

    with pytest.raises(RuntimeError, match="Jev answered 302"):
        jev.http(urlopen=urlopen)({})
