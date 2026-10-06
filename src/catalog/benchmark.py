"""Shopify's product benchmark as eval and training data (docs/specs/step-6k.md).

`python -m catalog.benchmark` fetches the text of Shopify/product-catalogue (Apache-2.0) through
Hugging Face's datasets-server rows API, keeps English rows, maps each label to our taxonomy at
level 3, and writes `eval/labels-shopify.jsonl` (a draw from the test split) and
`train/shopify.jsonl.gz` (the train split, less any product also in test).
"""

import argparse
import gzip
import hashlib
import http.client
import json
import random
import re
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from importlib.resources import files
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

from catalog import entry, evaluate, taxonomy

DATASET = "Shopify/product-catalogue"
ROWS = "https://datasets-server.huggingface.co/rows?dataset={}&config=default&split={}&offset={}&length={}"
REVISION = f"https://huggingface.co/api/datasets/{DATASET}"
PAGE = 100  # the rows API's maximum
TRIES = 6
BACKOFF = 15  # seconds, doubling: the rows API's 429 carries no Retry-After
PAUSE = 1.0  # seconds between pages, to stay under the rows API's rate limit
RENAMES = files("catalog") / "data" / "shopify-renames.json"  # old prefix -> new prefix
EVAL = Path("eval/labels-shopify.jsonl")
TRAIN = Path("train/shopify.jsonl.gz")
_WORD = re.compile(r"[a-zà-ÿ]+")
_STOPWORDS = {
    "en": "the and for with of to in is on a an your this that it are be you from by or",
    "de": "der die das und für mit ein eine nicht ist von zu den dem auf",
    "fr": "le la les et pour avec une des du est dans sur",
    "es": "el los las y para con una del es en por",
    "it": "il gli e per con una della di che",
    "pt": "o os e para com uma do da em",
    "nl": "de het een en voor met van is op",
    "sv": "och för med en ett av är på",
}
_EN = set(_STOPWORDS["en"].split())
_OTHER = {w for lang, words in _STOPWORDS.items() if lang != "en" for w in words.split()} - _EN


def _get(url: str) -> dict:
    with urlopen(url, timeout=60) as f:  # a stalled read fails instead of hanging
        return json.load(f)


def fetch(
    split: str, *, get: Callable[[str], dict] = _get, sleep: Callable[[float], None] = time.sleep
) -> Iterator[dict]:
    """Every row of `split` as {title, description, category}; nothing else is kept.
    A 429, a 5xx, a timed-out or truncated read, a dropped connection or a body that isn't
    JSON is retried with backoff, `TRIES` tries in all, each retry noted on stderr."""
    offset, total = 0, None
    while total is None or offset < total:
        url = ROWS.format(DATASET, split, offset, PAGE)
        for attempt in range(TRIES):
            try:
                data = get(url)
                break
            except HTTPError as e:
                if e.code != 429 and e.code < 500:
                    raise
                error = e
            # URLError, a timed-out or reset read; a truncated body; a page that isn't JSON
            except (OSError, http.client.HTTPException, ValueError) as e:
                error = e
            if attempt + 1 < TRIES:
                print(f"{split}: offset {offset}: {error!r}, retrying", file=sys.stderr)
                sleep(BACKOFF * 2**attempt)
        else:
            raise RuntimeError(
                f"{split}: offset {offset}: {TRIES} tries, last {error!r}"
            ) from error
        total, rows = data["num_rows_total"], data["rows"]
        if not rows:
            if offset < total:
                raise RuntimeError(f"{split}: offset {offset}: no rows before {total}")
            break
        for r in rows:
            x = r["row"]
            yield {
                "title": x.get("product_title"),
                "description": x.get("product_description"),
                "category": x.get("ground_truth_category"),
            }
        offset += len(rows)
        if offset // PAGE % 50 == 0:
            print(f"{split}: {offset} of {total} rows", file=sys.stderr)
        if offset < total:
            sleep(PAUSE)


def english(text: str) -> bool:
    """Under 3% non-ASCII letters and more English stopwords than other languages'. Text with
    no stopwords either way (part numbers, short titles) counts as English."""
    letters = [c for c in text if c.isalpha()]
    if letters and sum(not c.isascii() for c in letters) / len(letters) >= 0.03:
        return False
    words = _WORD.findall(text.lower())
    en, other = sum(w in _EN for w in words), sum(w in _OTHER for w in words)
    return en > other or en == other == 0


def level3(path: str, tax: set[str], renames: dict[str, str]) -> str | None:
    """`path` cut to level 3, through the rename table first if an entry matches. A deeper
    node the release has since dropped still maps to its level-3 ancestor."""
    candidates = [path]
    for old in sorted(renames, key=len, reverse=True):  # longest prefix first
        if path == old or path.startswith(old + taxonomy.SEP):
            candidates.insert(0, renames[old] + path[len(old) :])
            break
    for p in candidates:
        if (cut := taxonomy.ancestor(p)) in tax:
            return cut
    return None


def shape(r: dict, source: str) -> dict | None:
    """A row as a label-file line with its raw category, or None if it can't be a Listing."""
    title, description = r.get("title"), r.get("description")
    if not isinstance(title, str) or not isinstance(description, str) or not title.strip():
        return None
    title, description = evaluate._cut(title.strip(), 150), description.strip()[:5000]
    try:  # the id hashes the stored text, so it can be recomputed from the files
        digest = hashlib.sha256(f"{title}\n{description}".encode()).hexdigest()[:16]
    except UnicodeEncodeError:  # a lone surrogate, which JSON allows and UTF-8 can't encode
        return None
    return {
        "id": digest,
        "title": title,
        "description": description,
        "category": r.get("category"),
        "source": source,
    }


def _usable(rows, source, tax, renames, seen, counts) -> list[dict]:
    out = []
    for r in rows:
        counts["read"] += 1
        item = shape(r, source)
        if item is None or not isinstance(item["category"], str):
            counts["malformed"] += 1
        elif not english(f"{item['title']} {item['description']}"):
            counts["non_english"] += 1
        elif (category := level3(item["category"], tax, renames)) is None:
            counts["unmapped"][item["category"]] += 1
        elif item["id"] in seen:
            counts["duplicates"] += 1
        else:
            seen.add(item["id"])
            out.append(item | {"category": category})
    return out


def build(
    test: Sequence[dict],
    train: Sequence[dict],
    tax: set[str],
    renames: dict[str, str],
    *,
    n: int,
    seed: int,
) -> tuple[list[dict], list[dict], dict]:
    """The eval draw (`n` usable test rows), every usable train row whose id and title are both
    absent from test, and the counts of what was dropped and why."""
    counts = {"read": 0, "malformed": 0, "non_english": 0, "duplicates": 0, "leaks": 0}
    counts["unmapped"] = Counter()
    test_seen, train_seen = set(), set()
    usable_test = _usable(test, "shopify-test", tax, renames, test_seen, counts)
    usable_train = _usable(train, "shopify-train", tax, renames, train_seen, counts)
    titles = {x["title"].casefold() for x in usable_test}
    kept_train = [
        x for x in usable_train if x["id"] not in test_seen and x["title"].casefold() not in titles
    ]
    counts["leaks"] = len(usable_train) - len(kept_train)
    drawn = random.Random(seed).sample(usable_test, min(n, len(usable_test)))
    return drawn, kept_train, counts


def gz(items: Sequence[dict]) -> bytes:
    """JSON lines, gzipped with no timestamp, so the same rows give the same bytes."""
    text = "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in items)
    return gzip.compress(text.encode(), mtime=0)


def main(argv: Sequence[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m catalog.benchmark")
    p.add_argument("--n", type=int, default=2000)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--eval", type=Path, default=EVAL)
    p.add_argument("--train", type=Path, default=TRAIN)
    a = p.parse_args(argv)
    revision = _get(REVISION)["sha"]
    tax = set(taxonomy.load().paths)
    renames = json.loads(RENAMES.read_text(encoding="utf-8"))
    test, train = list(fetch("test")), list(fetch("train"))
    if (after := _get(REVISION)["sha"]) != revision:  # the rows API serves only the latest
        raise RuntimeError(f"{DATASET} changed during the fetch: {revision} -> {after}")
    ev, tr, counts = build(test, train, tax, renames, n=a.n, seed=a.seed)
    evaluate._write(a.eval, "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in ev))
    evaluate._write(a.train, gz(tr))
    unmapped = counts.pop("unmapped")
    print(json.dumps({"revision": revision, "eval": len(ev), "train": len(tr)} | counts))
    print(f"unmapped: {sum(unmapped.values())} rows, {len(unmapped)} categories")
    for category, k in unmapped.most_common(20):
        print(f"  {k:5}  {category}")


if __name__ == "__main__":
    entry.exit_with(main)
