"""A worker's classifier by `--classifier` kind, and the version each kind stamps.

`python -m catalog.classifiers --download` fetches the embedding model and builds the Jev
shortlist's vectors (step-6b.md, step-6f.md 6f.3).
"""

import argparse
import os
from collections.abc import Sequence
from pathlib import Path

from catalog import classify, entry, jev, student, taxonomy

KINDS = ("fake", "embedding", "jev", "student", "down")  # a worker's and the Backfill's kinds
# down: the fake one in an outage, every call failing (the chaos runner's, step-7b.md)


def load(kind: str, models: Path, *, workers: int) -> classify.Classifier:
    """The classifier one of `workers` workers runs, loaded from `models`."""
    if kind == "jev":
        call = jev.http(attempts=1)  # first: a missing key stops it before the model loads
        tax = taxonomy.load()
        shortlist = jev.shortlist(tax, classify.fastembed(models), models / "texts")
        # ponytail: a static share of Jev's limit; an idle worker's share goes unused
        return jev.JevClassifier(tax, shortlist, call, rate=jev.LIMIT / workers)
    if kind == "student":
        # step-6m.md: no key, no Jev; the cores split over the workers, which embed at once
        threads = max(1, (os.cpu_count() or 1) // workers)
        return student.pipeline(taxonomy.load(), models, threads=threads)
    if kind == "embedding":
        return classify.EmbeddingClassifier(taxonomy.load(), classify.fastembed(models))
    return classify.FakeClassifier(fail=kind == "down")


def version(kind: str) -> str:
    """The version a kind stamps, without loading the model (step-6c.md)."""
    if kind in ("fake", "down"):
        return classify.FakeClassifier.taxonomy_version
    if kind == "jev":
        return jev.version(taxonomy.load().version)
    if kind == "student":
        return student.version(taxonomy.load().version)
    return classify.version(taxonomy.load().version)


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.classifiers")
    args.add_argument("--download", action="store_true", required=True)
    args.add_argument("--models", type=Path, default=classify.MODELS)
    models = args.parse_args(argv).models
    embed = classify.fastembed(models, download=True)
    jev.shortlist(taxonomy.load(), embed).save(models / "texts")  # about 2 minutes (6f.3)


if __name__ == "__main__":
    entry.exit_with(main)
