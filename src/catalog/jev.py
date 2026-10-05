"""The classifier the eval chose (docs/specs/step-6e.md), in the pipeline since step-6f.md.

The embedding's `k` nearest paths, then one Jev `Choice` among them, keyed "1" to "k" with the
paths as their descriptions. Jev is TypeSafe's paid API: the key comes from TYPESAFE_API_KEY, and
`usd` adds up each call's input tokens at the model's price. Calls run in parallel, started no
faster than `rate` per second, until `budget` seconds after `classify` began.
"""

import json
import math
import os
import threading
import time
from collections.abc import Callable
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass, field
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from catalog import classify, taxonomy

URL = "https://api.typesafe.ai/v1/systemone"
MODEL = "jev-1.13.0"  # pinned, not jev-latest: an alias would change the answers under a result
USD_PER_TOKEN = 0.042 / 1_000_000  # jev-1.13.0's input price; output tokens are free
QUESTION = "Which product category fits this listing?"
RETRY = (429, 529)  # rate limited, overloaded
TIMEOUT = 10  # seconds per HTTP call: a batch ends at most this long after its budget
LIMIT = 80  # Jev's requests per second for this key
# The eval's winners (step-6e.md, Outcome): every one of them is in taxonomy_version.
SHORTLIST = 50
DESCRIPTION = 200
THRESHOLD = 0.40
TEXTS = "deeper"  # the shortlist's Category text (step-6h.md; step-6f.md, 6f.3)
BUDGET = 10.0  # seconds per batch: one call takes about 250 ms (step-6f.md, decision 1)


class KeyMissing(RuntimeError):
    """TYPESAFE_API_KEY is unset or empty, or Jev refused it."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):  # urllib would re-send the key to wherever it points
        return None


_urlopen = build_opener(_NoRedirect).open


@dataclass
class JevClassifier:
    taxonomy: taxonomy.Taxonomy
    shortlist: classify.EmbeddingClassifier
    call: Callable[[dict], dict]  # request body -> response body
    k: int = SHORTLIST
    description: int = DESCRIPTION
    threshold: float = THRESHOLD  # 0 keeps every answer raw, an unknown key too (the eval)
    texts: str = TEXTS  # the recipe `shortlist` was built with: in the version
    budget: float = BUDGET
    rate: float = LIMIT  # calls started per second; workers split LIMIT between them
    threads: int = 0  # 0: enough for `rate` calls of up to 500 ms each
    pool: Executor | None = None
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    usd: float = 0.0
    error: str | None = None  # the last classify's failure, when it still answered some
    taxonomy_version: str = field(init=False)

    def __post_init__(self):
        self.taxonomy_version = version(
            self.taxonomy.version,
            self.shortlist.model,
            self.k,
            self.description,
            self.threshold,
            self.texts,
        )
        self.pool = self.pool or ThreadPoolExecutor(self.threads or math.ceil(self.rate / 2))
        self._next = -math.inf  # when the next call may start
        self._lock = threading.Lock()  # for usd: calls add to it from the pool's threads

    def classify(self, listings) -> list[tuple[str, float] | None]:
        """One answer per Listing; None for those never started, because the budget ran out or a
        call failed first. If no call answered and one failed, its error is raised instead."""
        deadline, failed = self.clock() + self.budget, threading.Event()
        self.error = None
        calls = []  # (listing, future)
        for x, paths in self._shortlists(listings):
            if (wait := self._next - self.clock()) > 0:
                self.sleep(wait)
            if failed.is_set() or self.clock() >= deadline:
                break
            self._next = max(self._next, self.clock()) + 1 / self.rate
            calls.append((x, self.pool.submit(self._ask, x, paths, failed, deadline)))
        out, errors = [], []
        for x, future in calls:
            try:
                out.append(future.result())
            except Exception as e:
                errors.append((x, e))
                out.append(None)
        if errors:
            x, e = errors[0]
            if not any(out):
                e.add_note(f"Jev failed on {x.title!r}, ${self.usd:.4f} spent so far")
                raise e
            self.error = repr(e)[:500]  # for classify_failed: not "budget spent"
        return out + [None] * (len(listings) - len(out))

    def _shortlists(self, listings):
        """(Listing, its k paths), a chunk embedded at a time: none past the deadline."""
        for i in range(0, len(listings), n := self.shortlist.chunk):
            part = listings[i : i + n]
            yield from zip(part, self.shortlist.top(part, self.k), strict=True)

    def _ask(self, x, paths, failed: threading.Event, deadline: float) -> tuple[str, float] | None:
        if failed.is_set() or self.clock() >= deadline:  # queued behind slow calls: not started
            return None
        try:
            options = {str(i): path for i, path in enumerate(paths, 1)}
            question = {"type": "choice", "instructions": QUESTION, "criteria": options}
            body = {"model": MODEL, "state": classify.text(x, self.description)}
            got = self.call(body | {"questions": {"c": question}})
            try:
                usd = got["usage"]["input_tokens"] * USD_PER_TOKEN
                with self._lock:  # billed, whether or not the answer parses
                    self.usd += usd
                answer = got["answers"]["c"]
                key = answer["choice"]
                confidence = float(answer["probabilities"].get(key, 0.0))
                if not 0.0 <= confidence <= 1.0:  # NaN too
                    raise ValueError
            except KeyError, TypeError, AttributeError, ValueError:
                raise ValueError(f"unexpected Jev response: {str(got)[:300]}") from None
        except Exception:
            failed.set()  # no new calls: a 429 means slow down, an outage fails them all
            raise
        if key not in options:
            return (key, confidence) if not self.threshold else (classify.UNCATEGORIZED, 0.0)
        if confidence < self.threshold:
            return classify.UNCATEGORIZED, confidence
        return options[key], confidence


def version(
    taxonomy_version: str,
    model: str = classify.MODEL,
    k: int = SHORTLIST,
    description: int = DESCRIPTION,
    threshold: float = THRESHOLD,
    texts: str = TEXTS,
) -> str:
    """Every setting that changes an answer, so changing one reclassifies (step-6e.md, 6)."""
    embedding = classify._version(taxonomy_version, model)
    return f"{embedding}+{texts}+{MODEL}+k{k}+d{description}+t{threshold:.2f}"


def shortlist(tax: taxonomy.Taxonomy, embed, cache=None) -> classify.EmbeddingClassifier:
    """The pipeline's shortlist: TEXTS over `tax`, vectors from `cache` (the worker) or embedded
    now (`--download`, which then saves them). JevClassifier owns the budget."""
    pairs = classify.texts(tax, TEXTS, taxonomy.load_deeper(tax))
    return classify.EmbeddingClassifier(
        tax, embed, description=DESCRIPTION, budget=math.inf, texts=pairs, cache=cache
    )


def http(*, urlopen=_urlopen, sleep=time.sleep, attempts: int = 5) -> Callable[[dict], dict]:
    """POST to Jev with the key from TYPESAFE_API_KEY, retrying 429, 529 and network errors
    with backoff; a refused key (401, 403) raises KeyMissing at once. Redirects are refused, so
    the key only ever goes to URL. The pipeline passes attempts=1: the Backfill is its retry
    (step-6f.md, decision 3)."""
    key = os.environ.get("TYPESAFE_API_KEY")
    if not key:
        raise KeyMissing("TYPESAFE_API_KEY is not set: export your TypeSafe key first")

    def call(body: dict) -> dict:
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        attempt = 0
        while True:
            request = Request(URL, json.dumps(body).encode(), headers, method="POST")
            attempt += 1
            try:
                with urlopen(request, timeout=TIMEOUT) as response:
                    raw = response.read()
            except HTTPError as e:
                if e.code in (401, 403):  # a wrong or revoked key: fatal, as a missing one
                    raise KeyMissing(f"TYPESAFE_API_KEY was refused ({e.code})") from None
                if e.code not in RETRY or attempt == attempts:
                    try:
                        detail = e.read()[:300].decode(errors="replace")  # never the headers
                    except OSError:  # the server may close before the body
                        detail = ""
                    raise RuntimeError(
                        f"Jev answered {e.code} after {attempt} attempts: {detail}"
                    ) from None
            except (URLError, OSError) as e:  # reset, refused, timed out
                if attempt == attempts:
                    raise RuntimeError(f"Jev unreachable after {attempt} attempts: {e}") from None
            else:
                try:
                    return json.loads(raw)
                except ValueError:
                    raise RuntimeError(f"Jev's answer is not JSON: {raw[:300]!r}") from None
            sleep(2 ** (attempt - 1))

    return call
