"""Classifiers: Listing content -> (Primary Category, confidence), a whole batch per call.

The contract the worker relies on: a `taxonomy_version` attribute, and `classify(listings)`
returning one answer per Listing, in order: (category, confidence) with a non-empty category and a
confidence in 0..1, or None for a Listing the classifier didn't reach within its budget. The
classifier owns the design's batch budget and its confidence threshold (answer Uncategorized
below it). `python -m catalog.classify --download` fetches the embedding model (step-6b.md).
"""

import argparse
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from catalog import entry
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
    taxonomy_version: str = field(init=False)

    def __post_init__(self):
        self.taxonomy_version = _version(self.taxonomy.version, self.model)
        paths = self.taxonomy.paths  # in chunks: one ONNX run over all of them peaks at 1.2 GB
        self._paths = _unit(
            np.vstack(
                [self.embed(paths[i : i + self.chunk]) for i in range(0, len(paths), self.chunk)]
            )
        )

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
        return _unit(self.embed([text(x, self.description) for x in listings])) @ self._paths.T


def text(listing: Content, description: int = DESCRIPTION) -> str:
    """What a classifier reads: the title, then the description's first characters."""
    return f"{listing.title} {listing.description[:description]}".strip()


KINDS = ("fake", "embedding")  # a worker's and the Backfill's --classifier


def taxonomy_version(kind: str) -> str:
    """The version a `--classifier` kind stamps, without loading the model (step-6c.md)."""
    if kind == "fake":
        return FakeClassifier.taxonomy_version
    from catalog import taxonomy  # here: taxonomy imports this module

    return _version(taxonomy.load().version, MODEL)


def _version(taxonomy_version: str, model: str) -> str:
    return f"{taxonomy_version}+{model.rpartition('/')[2]}"


def _unit(vectors: np.ndarray) -> np.ndarray:
    return vectors / np.maximum(np.linalg.norm(vectors, axis=1, keepdims=True), 1e-12)


def fastembed(models: Path, model: str = MODEL, *, download: bool = False):
    """An `embed` function over fastembed's `model` in `models`; offline unless `download`."""
    from fastembed import TextEmbedding  # here: importing onnxruntime takes a second

    def embed(texts: Sequence[str]) -> np.ndarray:  # one ONNX run: the classifier sizes chunks
        return np.array(list(loaded.embed(list(texts), batch_size=max(len(texts), 1))))

    try:
        loaded = TextEmbedding(model, cache_dir=str(models), local_files_only=not download)
        embed(["probe"])  # a model that loads but won't run is as good as missing
    except Exception as e:  # missing or corrupt: fastembed raises ValueError, onnxruntime others
        raise ModelMissing(
            f"{model} won't load from {models}: run python -m catalog.classify --download"
        ) from e
    return embed


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.classify")
    args.add_argument("--download", action="store_true", required=True)
    args.add_argument("--models", type=Path, default=MODELS)
    fastembed(args.parse_args(argv).models, download=True)


if __name__ == "__main__":
    entry.exit_with(main)
