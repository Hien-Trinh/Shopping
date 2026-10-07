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

from catalog import classify, entry, evaluate, state, taxonomy
from catalog.envelope import Content

TRAIN = (
    Path("train/shopify.jsonl.gz"),
    Path("train/amazon-opus.jsonl"),
    Path("train/amazon-opus-2.jsonl"),
)
DESCRIPTION = 200  # description characters after the title, as Jev reads them (step-6f.md)
CHUNK = 256  # rows per embed call and per kNN similarity block
KS = (5, 10, 20, 50)
TAUS = [round(0.05 * i, 2) for i in range(21)]  # 0.00 .. 1.00
OUT = Path("eval/student.md")
FT_MODEL = "student-ft"  # bge-small fine-tuned end-to-end by train/finetune.py (step-6l.3.md)
FT_REPO = "hientrinh/listing-student-ft"  # the public Hub repo the script pushes to
HEAD = "head.npz"  # the linear head beside the ONNX encoder in that repo
DOWNLOAD = "run python -m catalog.student --download"
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

    def save(self, path: Path) -> None:
        """`head.npz`: what the fine-tuning script writes and the eval loads (step-6l.3.md)."""
        with state.atomic(path) as f:
            np.savez(f, classes=np.array(self.classes), w=self.w, b=self.b)

    @classmethod
    def load(cls, path: Path) -> Softmax:
        with np.load(path) as f:
            return cls([str(c) for c in f["classes"]], f["w"], f["b"])


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


def pick_tau(
    scored: Sequence[tuple[str, float, bool]], *, target: float, grid=TAUS
) -> float | None:
    """The lowest threshold whose kept answers (label, confidence, right) are right at least
    `target` of the time (below it, the student would do worse than the fallback's average);
    None if no threshold gets there."""
    for tau in grid:
        kept = [right for _, c, right in scored if c >= tau]
        if kept and sum(kept) / len(kept) >= target:
            return tau
    return None


def amazon(scored: Sequence, held: Sequence[dict]) -> list:
    """The scored held-out answers of the Amazon rows only (they carry `amazon_category`): τ is
    picked on Listings like ours, not on the Shopify rows that dominate `train/` (step-6l.2.md)."""
    return [s for s, r in zip(scored, held, strict=True) if _is_amazon(r)]


def _is_amazon(row: dict) -> bool:
    return "amazon_category" in row


def _tau(t: float | None) -> str:
    return f"τ = {t:.2f}" if t is not None else "no τ reaches the target"


def vectors(
    texts: Sequence[str], embed: Callable, cache: Path, *, model: str = classify.MODEL
) -> np.ndarray:
    """Unit vectors of `texts`, from `cache` when `model` embedded the same texts before."""
    key = hashlib.sha256(json.dumps([model, list(texts)]).encode()).hexdigest()
    path = cache / f"student-{key}.npy"
    try:
        return np.load(path)
    except OSError, ValueError, EOFError:  # missing, or cut short by a killed run
        pass
    x = unit(np.vstack([embed(texts[i : i + CHUNK]) for i in range(0, len(texts), CHUNK)]))
    with state.atomic(path) as f:
        np.save(f, x)
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
    def train(
        cls,
        tax,
        embed,
        rows,
        kind: str,
        *,
        cache: Path,
        k: int = 10,
        model: str = classify.MODEL,
        head: Softmax | None = None,
    ):
        """`kind` is softmax, knn, ft or ft-knn; `model` names `embed` for the vector cache and
        the version; `head` is the fine-tuning script's, used in place of fitting one."""
        paths = set(tax.paths)
        if bad := sorted({r["category"] for r in rows} - paths):
            raise ValueError(f"training labels not in the taxonomy: {bad[:5]}")
        x = vectors([_text(r["title"], r["description"]) for r in rows], embed, cache, model=model)
        y = [r["category"] for r in rows]
        fitted = Knn(x, y, k) if kind.endswith("knn") else head or Softmax.fit(x, y)
        version = f"{tax.version}+{model.rpartition('/')[2]}+student-{kind}"
        return cls(embed, fitted, version)

    def classify(self, listings: Sequence[Content]) -> list[tuple[str, float]]:
        if not listings:
            return []
        x = unit(np.asarray(self.embed([_text(x.title, x.description) for x in listings])))
        return self.model.predict(x)


def embedder(models: Path, *, download: bool = False) -> Callable:
    """An `embed` over the fine-tuned encoder, fetched from the Hub like bge-small."""
    from fastembed import TextEmbedding
    from fastembed.common.model_description import ModelSource, PoolingType

    if FT_MODEL not in {m["model"] for m in TextEmbedding.list_supported_models()}:
        TextEmbedding.add_custom_model(
            FT_MODEL,
            PoolingType.CLS,
            normalization=True,
            sources=ModelSource(hf=FT_REPO),
            dim=384,
            model_file="onnx/model_quantized.onnx",
        )
    return classify.fastembed(models, FT_MODEL, download=download)


def load_head(models: Path, *, download: bool = False) -> Softmax:
    """The fine-tuned head from the same Hub snapshot as the encoder."""
    from huggingface_hub import hf_hub_download

    try:
        path = hf_hub_download(FT_REPO, HEAD, cache_dir=str(models), local_files_only=not download)
    except Exception as e:  # not downloaded yet (hub raises its own LocalEntryNotFoundError)
        raise classify.ModelMissing(f"{FT_REPO}/{HEAD} isn't in {models}: {DOWNLOAD}") from e
    return Softmax.load(Path(path))


def finetuned(tax, models: Path, kind: str) -> StudentClassifier:
    """The `student-ft` and `student-ft-knn` eval candidates, trained on all of `train/`."""
    head = load_head(models) if kind == "ft" else None
    return StudentClassifier.train(
        tax,
        embedder(models),
        load_training(),
        kind,
        cache=models / "student",
        model=FT_MODEL,
        head=head,
    )


def load_training(paths: Sequence[Path] = TRAIN) -> list[dict]:
    rows = []
    for path in paths:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as f:
            rows += [json.loads(line) for line in f if line.strip()]
    return rows


def held_out(sources: Sequence[Sequence[dict]], *, seed: int) -> tuple[list[dict], list[dict]]:
    """A random 10% of each source held out (the rest to fit), so every source is represented
    in the threshold choice in proportion."""
    fit, held = [], []
    for rows in sources:
        rows = list(rows)
        random.Random(seed).shuffle(rows)
        cut = len(rows) // 10
        held += rows[:cut]
        fit += rows[cut:]
    return fit, held


def without_titles(rows: Sequence[dict], titles) -> list[dict]:
    """`rows` less any whose title (case-folded) is an eval title, so no eval Listing trains."""
    banned = {t.casefold() for t in titles}
    return [r for r in rows if r["title"].casefold() not in banned]


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


def _scores(answers, labels) -> tuple[float, float, float]:
    """Exact, top-level and two-level agreement of `answers` with the labels."""
    out = []
    for depth in (None, 1, 2):
        cut = (lambda p: p) if depth is None else (lambda p, d=depth: p and p.split(" > ")[:d])
        hits = sum(cut(a) == cut(x["category"]) for a, x in zip(answers, labels, strict=True))
        out.append(hits / len(labels))
    return tuple(out)


def _ms(classifier, listings, clock, n=100) -> tuple[float, float]:
    """p50 and p99 of one-Listing classify calls over the first `n` Listings."""
    times = []
    for x in listings[:n]:
        start = clock()
        classifier.classify([x])
        times.append((clock() - start) * 1000)
    times.sort()
    return times[len(times) // 2], times[min(len(times) - 1, int(0.99 * len(times)))]


def _listing(x: dict) -> Content:
    return Content(
        title=x["title"],
        description=x["description"],
        price_micros=1,  # the envelope requires them; no classifier reads them
        currency="USD",
        availability="in_stock",
    )


def report(
    tax,
    embed,
    sources,
    cache: Path,
    *,
    evals=EVALS,
    opus=OPUS,
    clock=time.perf_counter,
    seed=0,
    finetuned: tuple[Callable, Softmax] | None = None,
) -> str:
    """Fit on 90% of each training source (less any eval title), pick k and the threshold on
    the other 10%, then score each student alone and in the cascade on every eval: the body of
    `eval/student.md`. `finetuned` is the fine-tuned encoder's `embed` and its head (6l.3),
    scored the same way. Missing fallback answers for the first eval fail before any work."""
    first_labels, first_fallback = next(iter(evals.values()))
    for p in [*first_labels, *first_fallback, opus]:
        if not p.exists():
            raise FileNotFoundError(f"{p}: the report needs it (step-6l.md)")
    eval_labels = {name: _labels([p for p in lp if p.exists()]) for name, (lp, _) in evals.items()}
    titles = [x["title"] for labels in eval_labels.values() for x in labels]
    fit, held = held_out([without_titles(rows, titles) for rows in sources], seed=seed)
    n_amazon = sum(map(_is_amazon, held))
    if not n_amazon:  # τ is picked on these: none means an Amazon file is missing from TRAIN
        raise ValueError("no Amazon rows held out: τ is picked on them (step-6l.2.md)")
    held_texts = [_text(r["title"], r["description"]) for r in held]
    first_name = next(iter(evals))
    jev_first = _answers(first_fallback)
    target = _exact(
        [jev_first.get(x["id"]) for x in eval_labels[first_name]], eval_labels[first_name]
    )
    opus_answers = {
        json.loads(line)["id"]: json.loads(line)["category"]
        for line in opus.read_text().splitlines()
        if line
    }
    seen = {r["category"] for r in fit}
    lines = [
        "# The student (steps 6l, 6l.2 and 6l.3)",
        "",
        f"Generated by `python -m catalog.student`. Fit on {len(fit):,} rows of `train/` (any eval "
        f"title dropped), {len(held):,} held out (10% of each source) to pick k and the "
        f"threshold τ: the lowest τ whose kept answers on the {n_amazon:,} Amazon held-out rows "
        f"are right at least as often as Jev on the {first_name} ({target:.1%}).",
        "",
    ]
    students = [("softmax", embed, classify.MODEL, None), ("knn", embed, classify.MODEL, None)]
    if finetuned:
        ft_embed, head = finetuned
        students += [("ft", ft_embed, FT_MODEL, head), ("ft-knn", ft_embed, FT_MODEL, None)]
    else:
        lines += [f"No fine-tuned student in the models directory ({DOWNLOAD}, step-6l.3.md).", ""]
    for kind, embed, model, head in students:
        x_held = vectors(held_texts, embed, cache, model=model)
        best = None
        for k in KS if kind.endswith("knn") else (None,):
            c = StudentClassifier.train(
                tax, embed, fit, kind, cache=cache, k=k or 10, model=model, head=head
            )
            got = c.model.predict(x_held)
            acc = sum(g[0] == r["category"] for g, r in zip(got, held, strict=True)) / len(held)
            if best is None or acc > best[0]:
                best = (acc, k, c, got)
        acc, k, c, got = best
        scored = [(g[0], g[1], g[0] == r["category"]) for g, r in zip(got, held, strict=True)]
        tau, tau_all = (
            pick_tau(amazon(scored, held), target=target),
            pick_tau(scored, target=target),
        )
        name = f"student-{kind}" + (f" (k={k})" if k else "")
        lines += [
            f"## {name}",
            "",
            f"Held-out exact {acc:.1%}; {_tau(tau)} on the Amazon held-out rows "
            f"({_tau(tau_all)} on all of them).",
            "",
        ]
        for eval_name, (_, fallback_paths) in evals.items():
            labels = eval_labels[eval_name]
            if not labels:
                continue
            listings = [_listing(x) for x in labels]
            x_eval = vectors(
                [_text(x["title"], x["description"]) for x in labels], embed, cache, model=model
            )
            answers = c.model.predict(x_eval)
            exact, top, two = _scores([a[0] for a in answers], labels)
            p50, p99 = _ms(c, listings, clock)
            unseen = sum(x["category"] not in seen for x in labels)
            lines += [
                f"### {eval_name}",
                "",
                f"Student alone: exact {exact:.1%}, top level {top:.1%}, two levels {two:.1%}; "
                f"p50 {p50:.1f} ms, p99 {p99:.1f} ms per Listing, one at a time. "
                f"{unseen} of {len(labels)} eval labels never occur in training.",
                "",
            ]
            jev = _answers(fallback_paths)
            fallbacks = []
            if all(x["id"] in jev for x in labels):
                fallbacks.append(("Jev", [jev[x["id"]] for x in labels]))
            if all(x["id"] in opus_answers for x in labels):
                fallbacks.append(("Opus", [opus_answers[x["id"]] for x in labels]))
            if not fallbacks:
                lines += ["No fallback answers stored for this set: no cascade.", ""]
                continue
            header = " | ".join(f"Exact, {n} below τ" for n, _ in fallbacks)
            lines += [f"| τ | Kept local | {header} |", "|---|---|" + "---|" * len(fallbacks)]
            met = None
            for t in TAUS:
                cells = []
                for n, fb in fallbacks:
                    out, kept = cascade(answers, fb, tau=t)
                    cells.append(f"{_exact(out, labels):.1%}")
                    beats = _exact(out, labels) > _exact(fb, labels)
                    if n == "Jev" and kept / len(labels) >= 0.7 and met is None and beats:
                        met = t
                mark = " ← τ" if t == tau else " ← τ (all rows)" if t == tau_all else ""
                lines.append(f"| {t:.2f}{mark} | {kept / len(labels):.1%} | {' | '.join(cells)} |")
            lines.append("")
            for n, fb in fallbacks:
                lines.append(f"{n} alone: exact {_exact(fb, labels):.1%}.")
            if fallbacks[0][0] == "Jev":
                verdict = f"met at τ = {met:.2f}" if met is not None else "not met"
                lines.append(f"\nBar (beats Jev alone with 70% or more kept local): {verdict}.")
            lines.append("")
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m catalog.student")
    p.add_argument("--models", type=Path, default=classify.MODELS)
    p.add_argument("--out", type=Path, default=OUT)
    p.add_argument(
        "--download", action="store_true", help="fetch the fine-tuned student from the Hub and stop"
    )
    a = p.parse_args(argv)
    if a.download:
        embedder(a.models, download=True)
        load_head(a.models, download=True)
        return
    embed = classify.fastembed(a.models)
    try:
        finetuned = (embedder(a.models), load_head(a.models))
    except classify.ModelMissing:
        finetuned = None  # the report says so; the frozen students are still scored
    body = report(
        taxonomy.load(),
        embed,
        [load_training([p]) for p in TRAIN],
        a.models / "student",
        finetuned=finetuned,
    )
    with state.atomic(a.out) as f:
        f.write(body.encode())


if __name__ == "__main__":
    entry.exit_with(main)
