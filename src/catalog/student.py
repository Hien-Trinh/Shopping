"""The student prototype: a softmax head or kNN over bge-small vectors (docs/specs/step-6l.md).

Eval only. `evaluate run --classifier student-softmax` (or `student-knn`) scores a student
trained on `train/`; `python -m catalog.student` writes `eval/student.md`: each student alone,
then the cascade (the student where it is sure, Jev or Opus below a threshold) on the evals.
"""

import argparse
import gzip
import hashlib
import json
import random
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from catalog import classify, entry, evaluate, taxonomy
from catalog.envelope import Content

TRAIN = (Path("train/shopify.jsonl.gz"), Path("train/amazon-opus.jsonl"))
DESCRIPTION = 200  # description characters after the title, as Jev reads them (step-6f.md)
CHUNK = 256  # rows per embed call and per kNN similarity block
KS = (5, 10, 20, 50)
TAUS = [round(0.05 * i, 2) for i in range(21)]  # 0.00 .. 1.00
OUT = Path("eval/student.md")
EVALS = {  # label file, Jev's stored answers on it (step-6h.md / 6j), Opus's answers if any
    "1,020": (
        [evaluate.LABELS, Path("eval/labels-sonnet.jsonl")],
        [
            evaluate.RESULTS / "jev-shortlist50-d200-deeper.json",
            evaluate.RESULTS / "jev-shortlist50-d200-deeper-sonnet.json",
        ],
    ),
    "Shopify 2,000": (
        [Path("eval/labels-shopify.jsonl")],
        [evaluate.RESULTS / "jev-shortlist50-d200-deeper-shopify.json"],
    ),
}
OPUS = evaluate.RESULTS / "opus-agents.jsonl"


def unit(x: np.ndarray) -> np.ndarray:
    return x / np.maximum(np.linalg.norm(x, axis=1, keepdims=True), 1e-12)


@dataclass
class Softmax:
    """One linear layer and a softmax over the Categories seen in training."""

    classes: list[str]
    w: np.ndarray
    b: np.ndarray

    @classmethod
    def fit(cls, x: np.ndarray, y: Sequence[str], *, l2=1e-5, steps=300, lr=0.5) -> Softmax:
        """Full-batch Adam from zero weights, so the same data gives the same weights. lr and l2
        were tuned on the held-out rows: 45% held-out exact, against 25% at lr 0.05."""
        classes = sorted(set(y))
        index = {c: i for i, c in enumerate(classes)}
        target = np.zeros((len(y), len(classes)))
        target[np.arange(len(y)), [index[c] for c in y]] = 1
        w, b = np.zeros((x.shape[1], len(classes))), np.zeros(len(classes))
        m = [np.zeros_like(w), np.zeros_like(b)]
        v = [np.zeros_like(w), np.zeros_like(b)]
        for t in range(1, steps + 1):
            p = _softmax(x @ w + b)
            if not np.isfinite(p).all():
                raise ValueError(f"softmax training diverged at step {t}")
            g = (p - target) / len(y)
            grads = [x.T @ g + l2 * w, g.sum(axis=0)]
            for i, (param, grad) in enumerate(zip((w, b), grads, strict=True)):
                m[i] = 0.9 * m[i] + 0.1 * grad
                v[i] = 0.999 * v[i] + 0.001 * grad**2
                param -= lr * (m[i] / (1 - 0.9**t)) / (np.sqrt(v[i] / (1 - 0.999**t)) + 1e-8)
        return cls(classes, w, b)

    def predict(self, x: np.ndarray) -> list[tuple[str, float]]:
        p = _softmax(x @ self.w + self.b)
        best = p.argmax(axis=1)
        return [(self.classes[i], float(p[n, i])) for n, i in enumerate(best)]


def _softmax(z: np.ndarray) -> np.ndarray:
    e = np.exp(z - z.max(axis=1, keepdims=True))
    return e / e.sum(axis=1, keepdims=True)


@dataclass
class Knn:
    """The `k` most similar training vectors vote, weighted by similarity."""

    x: np.ndarray
    y: Sequence[str]
    k: int = 10
    chunk: int = CHUNK

    def predict(self, q: np.ndarray) -> list[tuple[str, float]]:
        out = []
        k = min(self.k, len(self.y))
        for start in range(0, len(q), self.chunk):
            sims = q[start : start + self.chunk] @ self.x.T
            top = np.argpartition(-sims, k - 1, axis=1)[:, :k]
            for row, idx in zip(sims, top, strict=True):
                votes, nearest = {}, {}
                for i in idx:
                    s = max(float(row[i]), 0.0)
                    votes[self.y[i]] = votes.get(self.y[i], 0.0) + s
                    nearest[self.y[i]] = max(nearest.get(self.y[i], 0.0), float(row[i]))
                label = max(votes, key=lambda c: (votes[c], nearest[c]))
                total = sum(votes.values())
                out.append((label, votes[label] / total if total else 0.0))
        return out


def cascade(
    answers: Sequence[tuple[str, float]], fallback: Sequence[str | None], *, tau: float
) -> tuple[list[str | None], int]:
    """The student's answer where its confidence is at least `tau`, else the fallback's; and
    how many stayed with the student."""
    out, kept = [], 0
    for (label, confidence), other in zip(answers, fallback, strict=True):
        if confidence >= tau:
            out.append(label)
            kept += 1
        else:
            out.append(other)
    return out, kept


def pick_tau(scored: Sequence[tuple[str, float, bool]], *, target: float, grid=TAUS) -> float:
    """The lowest threshold whose kept answers (label, confidence, right) are right at least
    `target` of the time: below it, the student would do worse than the fallback's average."""
    for tau in grid:
        kept = [right for _, c, right in scored if c >= tau]
        if kept and sum(kept) / len(kept) >= target:
            return tau
    return grid[-1]


def vectors(texts: Sequence[str], embed: Callable, cache: Path) -> np.ndarray:
    """Unit vectors of `texts`, from `cache` when the same texts were embedded before."""
    key = hashlib.sha256(json.dumps([classify.MODEL, list(texts)]).encode()).hexdigest()
    path = cache / f"student-{key}.npy"
    if path.exists():
        return np.load(path)
    x = unit(np.vstack([embed(texts[i : i + CHUNK]) for i in range(0, len(texts), CHUNK)]))
    cache.mkdir(parents=True, exist_ok=True)
    np.save(path, x)
    return x


def _text(title: str, description: str) -> str:
    return f"{title} {description[:DESCRIPTION]}".strip()


@dataclass
class StudentClassifier:
    """A trained student under the worker's `classify` contract, for the eval."""

    embed: Callable
    model: Softmax | Knn
    taxonomy_version: str

    @classmethod
    def train(cls, tax, embed, rows, kind: str, *, cache: Path, k: int = 10):
        paths = set(tax.paths)
        if bad := sorted({r["category"] for r in rows} - paths):
            raise ValueError(f"training labels not in the taxonomy: {bad[:5]}")
        x = vectors([_text(r["title"], r["description"]) for r in rows], embed, cache)
        y = [r["category"] for r in rows]
        model = Softmax.fit(x, y) if kind == "softmax" else Knn(x, y, k)
        version = f"{tax.version}+{classify.MODEL.rpartition('/')[2]}+student-{kind}"
        return cls(embed, model, version)

    def classify(self, listings: Sequence[Content]) -> list[tuple[str, float]]:
        x = unit(np.asarray(self.embed([_text(x.title, x.description) for x in listings])))
        return self.model.predict(x)


def load_training(paths: Sequence[Path] = TRAIN) -> list[dict]:
    rows = []
    for path in paths:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as f:
            rows += [json.loads(line) for line in f if line.strip()]
    return rows


def _labels(paths) -> list[dict]:
    return [json.loads(line) for p in paths for line in p.read_text().splitlines() if line]


def _answers(paths) -> dict[str, str | None]:
    out = {}
    for p in paths:
        if p.exists():
            out |= {a["id"]: a["category"] for a in json.loads(p.read_text())["answers"]}
    return out


def _exact(answers, labels) -> float:
    return sum(a == x["category"] for a, x in zip(answers, labels, strict=True)) / len(labels)


def report(tax, embed, rows, cache: Path, *, seed=0) -> str:
    """Train on 90% of `rows`, pick k and the threshold on the other 10%, then score each
    student alone and in the cascade on every eval: the body of `eval/student.md`."""
    rows = list(rows)
    random.Random(seed).shuffle(rows)
    cut = len(rows) // 10
    held, fit = rows[:cut], rows[cut:]
    x_held = vectors([_text(r["title"], r["description"]) for r in held], embed, cache)
    jev_1020 = _answers(EVALS["1,020"][1])
    labels_1020 = _labels(EVALS["1,020"][0])
    target = _exact([jev_1020[x["id"]] for x in labels_1020], labels_1020)
    opus = {
        json.loads(line)["id"]: json.loads(line)["category"]
        for line in OPUS.read_text().splitlines()
        if line
    }
    lines = [
        "# The student (step 6l)",
        "",
        f"Generated by `python -m catalog.student`. Trained on {len(fit):,} rows of `train/`, "
        f"{len(held):,} held out to pick k and the threshold τ: the lowest τ whose kept "
        f"held-out answers are right at least as often as Jev on the 1,020 ({target:.1%}).",
        "",
    ]
    for kind in ("softmax", "knn"):
        best = None
        for k in KS if kind == "knn" else (None,):
            c = StudentClassifier.train(tax, embed, fit, kind, cache=cache, k=k or 10)
            got = c.model.predict(x_held)
            acc = sum(g[0] == r["category"] for g, r in zip(got, held, strict=True)) / len(held)
            if best is None or acc > best[0]:
                best = (acc, k, c, got)
        acc, k, c, got = best
        scored = [(g[0], g[1], g[0] == r["category"]) for g, r in zip(got, held, strict=True)]
        tau = pick_tau(scored, target=target)
        name = f"student-{kind}" + (f" (k={k})" if k else "")
        lines += [f"## {name}", "", f"Held-out exact {acc:.1%}; τ = {tau:.2f}.", ""]
        for eval_name, (label_paths, jev_paths) in EVALS.items():
            labels = _labels([p for p in label_paths if p.exists()])
            if not labels:
                continue
            listings = [
                Content(
                    title=x["title"],
                    description=x["description"],
                    price_micros=1,
                    currency="USD",
                    availability="in_stock",
                )
                for x in labels
            ]
            start = time.perf_counter()
            answers = []
            for i in range(0, len(listings), CHUNK):
                answers += c.classify(listings[i : i + CHUNK])
            ms = (time.perf_counter() - start) * 1000 / len(listings)
            jev = _answers(jev_paths)
            lines += [
                f"### {eval_name}",
                "",
                f"Student alone: exact {_exact([a[0] for a in answers], labels):.1%}, "
                f"{ms:.1f} ms per Listing in chunks of {CHUNK}.",
                "",
            ]
            fallbacks = []
            if all(x["id"] in jev for x in labels):
                fallbacks.append(("Jev", [jev[x["id"]] for x in labels]))
            if all(x["id"] in opus for x in labels):
                fallbacks.append(("Opus", [opus[x["id"]] for x in labels]))
            if not fallbacks:
                lines += ["No fallback answers stored for this set: no cascade.", ""]
                continue
            header = " | ".join(f"Exact, {n} below τ" for n, _ in fallbacks)
            lines += [
                f"| τ | Kept local | {header} |",
                "|---|---|" + "---|" * len(fallbacks),
            ]
            for t in TAUS:
                cells = []
                for _, fb in fallbacks:
                    out, kept = cascade(answers, fb, tau=t)
                    cells.append(f"{_exact(out, labels):.1%}")
                mark = " ← τ" if t == tau else ""
                lines.append(f"| {t:.2f}{mark} | {kept / len(labels):.1%} | {' | '.join(cells)} |")
            for n, fb in fallbacks:
                lines.append(f"\n{n} alone: exact {_exact(fb, labels):.1%}.")
            lines.append("")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m catalog.student")
    p.add_argument("--models", type=Path, default=classify.MODELS)
    p.add_argument("--out", type=Path, default=OUT)
    a = p.parse_args(argv)
    embed = classify.fastembed(a.models)
    body = report(taxonomy.load(), embed, load_training(), a.models / "student")
    evaluate._write(a.out, body)


if __name__ == "__main__":
    entry.exit_with(main)
