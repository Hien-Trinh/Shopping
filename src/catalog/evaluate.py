"""The classifier eval: score a candidate on a hand-checked labeled set (docs/specs/step-6d.md).

`python -m catalog.evaluate run --classifier embedding` writes one candidate's answers and timings
to `eval/results/<name>.json`, one candidate per process so peak RSS is its own;
`python -m catalog.evaluate report` turns every result there into `eval/report.md`.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import random
import resource
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlopen

from pydantic import ValidationError

from catalog import classify, entry, state, taxonomy
from catalog.envelope import Content

LABELS = Path("eval/labels.jsonl")
RESULTS = Path("eval/results")
REPORT = Path("eval/report.md")
AMAZON = (
    "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main"
    "/raw/meta_categories"
)
CATEGORIES = [  # Amazon Reviews '23's category files, less Unknown
    "All_Beauty",
    "Amazon_Fashion",
    "Appliances",
    "Arts_Crafts_and_Sewing",
    "Automotive",
    "Baby_Products",
    "Beauty_and_Personal_Care",
    "Books",
    "CDs_and_Vinyl",
    "Cell_Phones_and_Accessories",
    "Clothing_Shoes_and_Jewelry",
    "Digital_Music",
    "Electronics",
    "Gift_Cards",
    "Grocery_and_Gourmet_Food",
    "Handmade_Products",
    "Health_and_Household",
    "Health_and_Personal_Care",
    "Home_and_Kitchen",
    "Industrial_and_Scientific",
    "Kindle_Store",
    "Magazine_Subscriptions",
    "Movies_and_TV",
    "Musical_Instruments",
    "Office_Products",
    "Patio_Lawn_and_Garden",
    "Pet_Supplies",
    "Software",
    "Sports_and_Outdoors",
    "Subscription_Boxes",
    "Tools_and_Home_Improvement",
    "Toys_and_Games",
    "Video_Games",
]
CANDIDATES = (*classify.KINDS, "laya-hierarchical", "laya-shortlist")
THRESHOLDS = [round(0.30 + 0.05 * i, 2) for i in range(13)]  # 0.30 .. 0.90


@dataclass(frozen=True)
class Labeled:
    id: str
    listing: Content
    category: str


def load_labels(path: Path, tax: taxonomy.Taxonomy) -> tuple[list[Labeled], str]:
    """Every label and the SHA-256 of the bytes parsed, or ValueError naming the first bad line:
    a bad label must not score as a miss. Blank lines and a leading BOM are skipped."""
    data = path.read_bytes()
    paths, seen, out = set(tax.paths), set(), []
    for n, line in enumerate(data.decode("utf-8-sig").split("\n"), 1):  # not splitlines: U+2028
        if not line.strip():
            continue
        try:
            x = json.loads(line)
            id, category = str(x["id"]), x["category"]
            content = Content(
                title=x["title"],
                description=x["description"],
                price_micros=1,  # the API requires them; no classifier reads them
                currency="USD",
                availability="in_stock",
            )
        except (ValueError, KeyError, TypeError) as e:  # ValidationError is a ValueError
            reason = _short(e) if isinstance(e, ValidationError) else repr(e)
            raise ValueError(f"{path}: line {n}: {reason}") from None
        if id in seen:
            raise ValueError(f"{path}: line {n}: duplicate id {id!r}")
        if not isinstance(category, str) or category not in paths:
            raise ValueError(f"{path}: line {n}: {category!r} is not in the taxonomy")
        seen.add(id)
        out.append(Labeled(id, content, category))
    if not out:
        raise ValueError(f"{path}: no labels")
    return out, hashlib.sha256(data).hexdigest()


def _short(e: ValidationError) -> str:
    return "; ".join(f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors())


def sample(
    source: str, categories: Sequence[str], *, per_category: int, seed: int, lines: int = 1000
) -> list[dict]:
    """`per_category` items drawn from the first `lines` of each category file under `source`,
    shaped as Listings the API accepts (step-6e.md, 6e.1). A repeated id is kept once."""
    rng, seen, out = random.Random(seed), set(), []
    for category in categories:
        # ponytail: a file's first lines may not be a random slice of it; stream more if it shows
        url = f"{source}/meta_{category}.jsonl"
        with urlopen(url, timeout=60) as f:  # a stalled read fails instead of hanging
            raw = [line for _, line in zip(range(lines), f, strict=False)]
        usable = {}
        for n, line in enumerate(raw, 1):
            try:
                x = json.loads(line)
            except ValueError as e:
                raise ValueError(f"{url}: line {n}: {e}") from None
            if not isinstance(x, dict):
                raise ValueError(f"{url}: line {n}: not an object")
            id, title = x.get("parent_asin"), x.get("title")
            if not isinstance(id, str) or not id.strip() or id in seen:
                continue
            if not isinstance(title, str) or not title.strip():
                continue
            parts = [
                p
                for key in ("description", "features")
                if isinstance(x.get(key), list)
                for p in x[key]
                if isinstance(p, str)
            ]
            usable.setdefault(
                id,
                {
                    "id": id,
                    "title": _cut(title.strip(), 150),
                    "description": "\n".join(parts)[:5000],
                    "amazon_category": category,
                },
            )
        picked = rng.sample(list(usable.values()), min(per_category, len(usable)))
        seen |= {x["id"] for x in picked}
        out += picked
    return out


def _cut(text: str, n: int) -> str:
    """`text` cut to `n` characters at the last space within them, else hard."""
    if len(text) <= n:
        return text
    head = text[: n + 1].rpartition(" ")[0].rstrip()
    return head or text[:n]


def run(
    classifier,
    labeled: Sequence[Labeled],
    tax: taxonomy.Taxonomy,
    *,
    batch: int,
    clock=time.perf_counter,
) -> dict:
    """Classify every Listing `batch` at a time after one untimed warm-up call (lazy loads)."""
    paths = set(tax.paths) | {classify.UNCATEGORIZED}
    classifier.classify([labeled[0].listing])
    answers, seconds = [], []
    for i in range(0, len(labeled), batch):
        chunk = labeled[i : i + batch]
        where = f"{len(chunk)} Listings from id {chunk[0].id!r}"
        start = clock()
        try:
            got = classifier.classify([x.listing for x in chunk])
        except Exception as e:
            e.add_note(f"classifying {where}")
            raise
        seconds.append(clock() - start)
        if len(got) != len(chunk):
            raise ValueError(f"{len(got)} answers for {where}")
        for x, a in zip(chunk, got, strict=True):
            category, confidence = a if a is not None else (None, None)
            answers.append(
                {
                    "id": x.id,
                    "label": x.category,
                    "category": category,
                    "confidence": confidence,
                    "valid": category is None or category in paths,
                }
            )
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # bytes on macOS, KiB on Linux
    return {
        "taxonomy_version": classifier.taxonomy_version,
        "batch": batch,
        "answers": answers,
        "seconds": seconds,
        "usd": float(getattr(classifier, "usd", 0.0)),
        "rss_mb": rss / (2**20 if sys.platform == "darwin" else 2**10),
        "date": datetime.now(UTC).date().isoformat(),
        "machine": f"{platform.platform()}, {os.cpu_count()} CPUs",
    }


def _levels(path: str, n: int) -> list[str]:
    return path.split(taxonomy.SEP)[:n]


def score(result: dict, thresholds: Sequence[float]) -> dict[float, dict]:
    """Per threshold: an answer counts only at or above it; the rest are Uncategorized."""
    answers, out = result["answers"], {}
    n = len(answers)
    for t in thresholds:
        kept = [
            a
            for a in answers
            if a["category"] not in (None, classify.UNCATEGORIZED) and a["confidence"] >= t
        ]
        correct = sum(a["category"] == a["label"] for a in kept)
        out[t] = {
            "accuracy": correct / n,
            "precision": correct / len(kept) if kept else None,
            "uncategorized": 1 - len(kept) / n,
            "level1": sum(_levels(a["category"], 1) == _levels(a["label"], 1) for a in kept) / n,
            "level2": sum(_levels(a["category"], 2) == _levels(a["label"], 2) for a in kept) / n,
        }
    return out


def summary(result: dict) -> dict:
    ms, n = sorted(s * 1000 for s in result["seconds"]), len(result["answers"])

    def rank(p):  # nearest rank
        return ms[math.ceil(p * len(ms)) - 1]

    return {
        "p50_ms": rank(0.5),
        "p99_ms": rank(0.99),
        "per_s": n / total if (total := sum(result["seconds"])) else None,
        "none": sum(a["category"] is None for a in result["answers"]),
        "invalid": sum(not a["valid"] for a in result["answers"]),
        "usd_per_m": result["usd"] / n * 1_000_000,
    }


def _pct(x: float | None) -> str:
    return "–" if x is None else f"{x:.1%}"


def _num(x: float | None) -> str:
    return "–" if x is None else f"{x:.1f}"


def render(results: Sequence[dict], threshold: float) -> str:
    """eval/report.md: one summary row per candidate at `threshold`, then each one's sweep."""
    results = sorted(results, key=lambda r: r["labels_sha256"])  # stable: grouped by label set
    hashes = sorted({r["labels_sha256"] for r in results})
    lines = [
        "# Classifier eval",
        "",
        "Generated by `python -m catalog.evaluate report`. Label set(s): "
        + ", ".join(f"`{h[:12]}`" for h in hashes)
        + ".",
    ]
    if len(hashes) > 1:
        lines.append("")
        lines.append("**Warning: these results come from different label sets.**")
    lines += [
        "",
        f"## Summary at threshold {threshold}",
        "",
        "| Candidate | Version | Labels | Accuracy | Precision | Uncategorized | L1 | L2 "
        "| p50 ms | p99 ms | Listings/s | $/1M | Peak RSS MB | None | Invalid | Batch | Date "
        "| Machine |",
        "|" + "---|" * 18,
    ]
    for r in results:
        s, m = score(r, [threshold])[threshold], summary(r)
        lines.append(
            f"| {r['name']} | {r['taxonomy_version']} | `{r['labels_sha256'][:12]}` "
            f"| {_pct(s['accuracy'])} | {_pct(s['precision'])} | {_pct(s['uncategorized'])} "
            f"| {_pct(s['level1'])} | {_pct(s['level2'])} | {m['p50_ms']:.1f} "
            f"| {m['p99_ms']:.1f} | {_num(m['per_s'])} | {m['usd_per_m']:.2f} "
            f"| {r['rss_mb']:.0f} | {m['none']} | {m['invalid']} | {r['batch']} | {r['date']} "
            f"| {r['machine']} |"
        )
    for r in results:
        lines += [
            "",
            f"## {r['name']}: threshold sweep",
            "",
            "| Threshold | Accuracy | Precision | Uncategorized | L1 | L2 |",
            "|---|---|---|---|---|---|",
        ]
        for t, s in score(r, THRESHOLDS).items():
            lines.append(
                f"| {t:.2f} | {_pct(s['accuracy'])} | {_pct(s['precision'])} "
                f"| {_pct(s['uncategorized'])} | {_pct(s['level1'])} | {_pct(s['level2'])} |"
            )
    return "\n".join(lines) + "\n"


_KEYS = {"name", "labels_sha256", "taxonomy_version", "batch", "answers", "seconds", "usd"}
_KEYS |= {"rss_mb", "date", "machine"}


def _result(path: Path) -> dict:
    """A results file `run` wrote, or ValueError naming it (torn, hand-edited or older)."""
    try:
        r = json.loads(path.read_text())
        missing = _KEYS - r.keys()
    except (ValueError, AttributeError) as e:
        raise ValueError(f"{path}: not a result ({e})") from None
    if missing:
        raise ValueError(f"{path}: not a result, missing {sorted(missing)}")
    return r


def candidate(kind: str, models: Path, *, description: int = classify.DESCRIPTION):
    """The classifier at threshold 0 with no budget: every Listing gets its best path and raw
    confidence, and `score` applies thresholds afterwards (decision 2)."""
    if kind == "fake":
        return classify.FakeClassifier()
    tax = taxonomy.load()
    embedding = None
    if kind in ("embedding", "laya-shortlist"):
        embedding = classify.EmbeddingClassifier(
            tax, classify.fastembed(models), threshold=0.0, budget=math.inf, description=description
        )
    if kind == "embedding":
        return embedding
    from catalog import laya  # here: it loads laya_mlx, an optional Apple Silicon only group

    mode = kind.removeprefix("laya-")
    return laya.LayaClassifier(tax, laya.mlx(models), mode, embedding, description=description)


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.evaluate")
    sub = args.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run")
    r.add_argument("--classifier", choices=CANDIDATES, required=True)
    r.add_argument("--labels", type=Path, default=LABELS)
    r.add_argument("--batch", type=int, default=1)
    r.add_argument("--name")
    r.add_argument("--results", type=Path, default=RESULTS)
    r.add_argument("--models", type=Path, default=classify.MODELS)
    r.add_argument("--description", type=int, default=classify.DESCRIPTION)
    s = sub.add_parser("sample")
    s.add_argument("--source", default=AMAZON)
    s.add_argument("--categories", nargs="+", default=CATEGORIES)
    s.add_argument("--per-category", type=int, default=6)
    s.add_argument("--seed", type=int, default=0)
    p = sub.add_parser("report")
    p.add_argument("--results", type=Path, default=RESULTS)
    p.add_argument("--out", type=Path, default=REPORT)
    p.add_argument("--threshold", type=float, default=classify.THRESHOLD)
    a = args.parse_args(argv)
    if a.command == "run":
        if a.batch < 1:
            args.error("--batch must be at least 1")
        if a.description < 0:
            args.error("--description must be at least 0")
        tax = taxonomy.load()
        labeled, sha = load_labels(a.labels, tax)
        classifier = candidate(a.classifier, a.models, description=a.description)
        result = run(classifier, labeled, tax, batch=a.batch)
        name = a.name or a.classifier
        result |= {"name": name, "labels_sha256": sha, "description": a.description}
        state.save(a.results / f"{name}.json", result)  # whole or not at all
    elif a.command == "sample":  # printed only once every category is read: no half sample
        if a.per_category < 1:
            args.error("--per-category must be at least 1")
        items = sample(a.source, a.categories, per_category=a.per_category, seed=a.seed)
        sys.stdout.write("".join(json.dumps(x) + "\n" for x in items))
        drawn = [x["amazon_category"] for x in items]  # a short category shows here
        print(", ".join(f"{c} {drawn.count(c)}" for c in a.categories), file=sys.stderr)
    else:
        results = [_result(f) for f in sorted(a.results.glob("*.json"))]
        if not results:
            args.error(f"no results in {a.results}")
        a.out.parent.mkdir(parents=True, exist_ok=True)
        tmp = a.out.with_name(f".{a.out.name}.tmp")
        tmp.write_text(render(results, a.threshold))
        os.replace(tmp, a.out)  # a killed report leaves the old one


if __name__ == "__main__":
    entry.exit_with(main)
