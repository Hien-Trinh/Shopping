"""The Jev candidate for the classifier eval (docs/specs/step-6e.md, 6e.3).

Option (d): the embedding's `k` nearest paths, then one Jev `Choice` among them, keyed "1" to "k"
with the paths as their descriptions. Jev is TypeSafe's paid API: the key comes from
TYPESAFE_API_KEY, and `usd` adds up each call's input tokens at the model's price.
"""

import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from catalog import classify, taxonomy

URL = "https://api.typesafe.ai/v1/systemone"
MODEL = "jev-1.13.0"  # pinned, not jev-latest: an alias would change the answers under a result
USD_PER_TOKEN = 0.042 / 1_000_000  # jev-1.13.0's input price; output tokens are free
QUESTION = "Which product category fits this listing?"
RETRY = (429, 529)  # rate limited, overloaded


@dataclass
class JevClassifier:
    taxonomy: taxonomy.Taxonomy
    shortlist: classify.EmbeddingClassifier
    call: Callable[[dict], dict]  # request body -> response body
    k: int = 10
    description: int = classify.DESCRIPTION
    usd: float = 0.0
    taxonomy_version: str = field(init=False)

    def __post_init__(self):
        self.taxonomy_version = f"{self.taxonomy.version}+{MODEL}-shortlist"

    def classify(self, listings) -> list[tuple[str, float]]:
        out = []
        for x, paths in zip(listings, self.shortlist.top(listings, self.k), strict=True):
            options = {str(i): path for i, path in enumerate(paths, 1)}
            question = {"type": "choice", "instructions": QUESTION, "criteria": options}
            body = {"model": MODEL, "state": classify.text(x, self.description)}
            got = self.call(body | {"questions": {"c": question}})
            self.usd += got["usage"]["input_tokens"] * USD_PER_TOKEN
            answer = got["answers"]["c"]
            key = answer["choice"]  # a key outside the options stays as is: evaluate counts it
            out.append((options.get(key, key), float(answer["probabilities"].get(key, 0.0))))
        return out


def http(*, urlopen=urlopen, sleep=time.sleep, attempts: int = 5) -> Callable[[dict], dict]:
    """POST to Jev with the key from TYPESAFE_API_KEY, retrying 429 and 529 with backoff."""
    key = os.environ.get("TYPESAFE_API_KEY")
    if not key:
        raise RuntimeError("TYPESAFE_API_KEY is not set: export your TypeSafe key first")

    def call(body: dict) -> dict:
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        attempt = 0
        while True:
            request = Request(URL, json.dumps(body).encode(), headers, method="POST")
            try:
                with urlopen(request, timeout=30) as response:
                    return json.load(response)
            except HTTPError as e:
                attempt += 1
                if e.code not in RETRY or attempt == attempts:
                    detail = e.read()[:300].decode(errors="replace")  # never the headers
                    raise RuntimeError(f"Jev answered {e.code}: {detail}") from None
                sleep(2 ** (attempt - 1))

    return call
