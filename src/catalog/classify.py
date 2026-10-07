"""Classifiers: Listing content -> (Primary Category, confidence), a whole batch per call.

The contract the worker relies on: a `taxonomy_version` attribute, and `classify(listings)`
returning one answer per Listing, in order: (category, confidence) with a non-empty category and a
confidence in 0..1, or None for a Listing the classifier didn't reach within its budget. The
classifier owns the design's batch budget and its confidence threshold (answer Uncategorized
below it). `python -m catalog.classify --download` fetches the embedding model (step-6b.md).
"""

import argparse
import hashlib
import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from catalog import entry, state
from catalog.envelope import Content

UNCATEGORIZED = "Uncategorized"
MODEL = "BAAI/bge-small-en-v1.5"
MODELS = Path("models")  # where --download puts it and workers load it from (gitignored)
THRESHOLD = 0.5  # ponytail: provisional; 6e picks the threshold from the eval
DESCRIPTION = 500  # characters of the description embedded after the title


class ModelMissing(RuntimeError):
    """The embedding model isn't in the models directory, or won't load."""


@dataclass
class FakeClassifier:
    """Deterministic and instant: the category is the title's first character."""

    taxonomy_version: str = "fake-1"
    fail: bool = False  # every call raises, like an outage or a timeout

    def classify(self, listings: Sequence[Content]) -> list[tuple[str, float]]:
        if self.fail:
            raise RuntimeError("classifier unavailable")
        return [(f"Fake > {listing.title[0]}", 0.9) for listing in listings]


@dataclass
class EmbeddingClassifier:
    """The Category path most similar to each Listing's text (cosine), embedding in chunks until
    `budget` seconds are spent; Listings past that answer None (step-6b.md, decision 1)."""

    taxonomy: object  # a taxonomy.Taxonomy (that module imports this one): version and paths
    embed: Callable[[Sequence[str]], np.ndarray]  # texts -> one vector per row
    model: str = MODEL
    threshold: float = THRESHOLD
    description: int = DESCRIPTION  # characters of the description after the title
    budget: float = 0.2  # the design's batch timeout
    chunk: int = 16  # about 160 ms with 500-character descriptions, so it overruns `budget` by
    # at most that; the cost per Listing barely depends on it
    clock: Callable[[], float] = time.monotonic
    texts: Sequence[tuple[str, str]] | None = None  # (text, path) pairs; None: each path itself
    cache: Path | None = None  # load the text vectors from here, never embed them (6f.3)
    taxonomy_version: str = field(init=False)

    def __post_init__(self):
        self.taxonomy_version = _version(self.taxonomy.version, self.model)
        paths = self.taxonomy.paths
        if self.texts is None:
            self.texts = [(p, p) for p in paths]
        index = {p: i for i, p in enumerate(paths)}
        for _, p in self.texts:
            if p not in index:
                raise ValueError(f"a text names {p!r}, which isn't in {self.taxonomy.version}")
        self._owner = np.array([index[p] for _, p in self.texts])  # each text's Category
        if missing := set(paths) - {p for _, p in self.texts}:  # it would never be shortlisted
            raise ValueError(f"no text for {min(missing)!r}")
        self._plain = self._owner.tolist() == list(range(len(paths)))  # one text per path
        if self.cache is not None:
            self._paths = self._load(self.cache / f"{cache_key(self.model, self.texts)}.npy")
            return
        words = [t for t, _ in self.texts]  # in chunks: one ONNX run over all peaks at 1.2 GB
        self._paths = _unit(
            np.vstack(
                [self.embed(words[i : i + self.chunk]) for i in range(0, len(words), self.chunk)]
            )
        )

    def _load(self, path: Path) -> np.ndarray:
        """The cached text vectors, or ModelMissing: a worker must never embed them at start,
        or four starting together outlast the 60 s watchdog (step-6f.md, 6f.3)."""
        try:
            vectors = np.load(path, allow_pickle=False)
        except Exception as e:  # missing, empty (EOFError), truncated, unreadable, not .npy
            raise ModelMissing(f"no usable text vectors at {path} ({e!r}): {DOWNLOAD}") from e
        width = np.asarray(self.embed(["probe"])).shape[1]
        if not (
            isinstance(vectors, np.ndarray)
            and np.issubdtype(vectors.dtype, np.floating)
            and np.isfinite(vectors).all()
        ):
            raise ModelMissing(f"{path} doesn't hold finite float vectors: {DOWNLOAD}")
        if vectors.shape != (len(self.texts), width):
            raise ModelMissing(
                f"{path} has shape {vectors.shape}, not ({len(self.texts)}, {width}): {DOWNLOAD}"
            )
        return _unit(vectors)

    def save(self, directory: Path) -> Path:
        """Write the text vectors where `cache=directory` finds them, whole or not at all."""
        path = directory / f"{cache_key(self.model, self.texts)}.npy"
        with state.atomic(path) as f:
            np.save(f, self._paths)
        return path

    def classify(self, listings: Sequence[Content]) -> list[tuple[str, float] | None]:
        deadline = self.clock() + self.budget
        answers: list[tuple[str, float] | None] = []
        for i in range(0, len(listings), self.chunk):
            if i and self.clock() >= deadline:  # the first chunk always runs: progress
                break
            for row in self._similarity(listings[i : i + self.chunk]):
                best = int(row.argmax())
                confidence = min(max(float(row[best]), 0.0), 1.0)
                path = self.taxonomy.paths[best] if confidence >= self.threshold else UNCATEGORIZED
                answers.append((path, confidence))
        return answers + [None] * (len(listings) - len(answers))

    def top(self, listings: Sequence[Content], k: int) -> list[list[str]]:
        """Each Listing's `k` most similar paths, most similar first, with no budget: the
        shortlist a Laya or Jev choice picks from (step-6e.md)."""
        out = []
        for i in range(0, len(listings), self.chunk):
            for row in self._similarity(listings[i : i + self.chunk]):
                out.append([self.taxonomy.paths[j] for j in np.argsort(-row, kind="stable")[:k]])
        return out

    def _similarity(self, listings: Sequence[Content]) -> np.ndarray:
        """Listings x Categories: each Category's best text (step-6h.md)."""
        by_text = _unit(self.embed([text(x, self.description) for x in listings])) @ self._paths.T
        if self._plain:
            return by_text
        best = np.full((len(listings), len(self.taxonomy.paths)), -np.inf)
        # ponytail: 0.40 s per 1,000 Listings for 14,605 texts (step-7a.md), but Jev only shortlists
        # about 200 per batch. Sort the texts by Category and use np.maximum.reduceat (0.02 s) if
        # something ever shortlists whole batches.
        np.maximum.at(best.T, self._owner, by_text.T)
        return best


def text(listing: Content, description: int = DESCRIPTION) -> str:
    """What a classifier reads: the title, then the description's first characters."""
    return f"{listing.title} {listing.description[:description]}".strip()


DOWNLOAD = "run python -m catalog.classify --download"


def cache_key(model: str, texts: Sequence[tuple[str, str]]) -> str:
    """Names the vectors of exactly these texts under this model."""
    return hashlib.sha256(json.dumps([model, list(map(list, texts))]).encode()).hexdigest()


RECIPES = ("path", "joined", "deeper")  # Category text: step-6h.md


def texts(taxonomy, recipe: str, deeper: dict[str, list[str]]) -> list[tuple[str, str]]:
    """(text, path) pairs for `recipe`: each path; each path then its descendants' last names;
    or each path and every descendant's full path, scoring for its ancestor."""
    paths = taxonomy.paths
    if recipe == "path":
        return [(p, p) for p in paths]
    if recipe == "joined":
        return [
            (f"{p}: {', '.join(d.rpartition(' > ')[2] for d in deeper[p])}" if deeper[p] else p, p)
            for p in paths
        ]
    return [(p, p) for p in paths] + [(d, p) for p in paths for d in deeper[p]]


KINDS = (
    "fake",
    "embedding",
    "jev",
    "student",
    "down",
)  # a worker's and the Backfill's --classifier
# down: the fake one in an outage, every call failing (the chaos runner's, step-7b.md)


def taxonomy_version(kind: str) -> str:
    """The version a `--classifier` kind stamps, without loading the model (step-6c.md)."""
    if kind in ("fake", "down"):
        return FakeClassifier.taxonomy_version
    from catalog import jev, taxonomy  # here: both import this module

    if kind == "jev":
        return jev.version(taxonomy.load().version)
    if kind == "student":
        from catalog import student  # here: it imports this module

        return student.version(taxonomy.load().version)
    return _version(taxonomy.load().version, MODEL)


def _version(taxonomy_version: str, model: str) -> str:
    return f"{taxonomy_version}+{model.rpartition('/')[2]}"


def _unit(vectors: np.ndarray) -> np.ndarray:
    return vectors / np.maximum(np.linalg.norm(vectors, axis=1, keepdims=True), 1e-12)


def fastembed(models: Path, model: str = MODEL, *, download: bool = False, **kwargs):
    """An `embed` function over fastembed's `model` in `models`; offline unless `download`.
    `kwargs` go to `TextEmbedding` (the student passes `specific_model_path`, step-6m.md)."""
    from fastembed import TextEmbedding  # here: importing onnxruntime takes a second

    def embed(texts: Sequence[str]) -> np.ndarray:  # one ONNX run: the classifier sizes chunks
        return np.array(list(loaded.embed(list(texts), batch_size=max(len(texts), 1))))

    try:
        loaded = TextEmbedding(
            model, cache_dir=str(models), local_files_only=not download, **kwargs
        )
        embed(["probe"])  # a model that loads but won't run is as good as missing
    except Exception as e:  # missing or corrupt: fastembed raises ValueError, onnxruntime others
        raise ModelMissing(f"{model} won't load from {models}: {DOWNLOAD}") from e
    return embed


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.classify")
    args.add_argument("--download", action="store_true", required=True)
    args.add_argument("--models", type=Path, default=MODELS)
    models = args.parse_args(argv).models
    embed = fastembed(models, download=True)
    from catalog import jev, taxonomy  # here: both import this module

    jev.shortlist(taxonomy.load(), embed).save(models / "texts")  # about 2 minutes (6f.3)


if __name__ == "__main__":
    entry.exit_with(main)
