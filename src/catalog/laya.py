"""Laya candidates for the classifier eval (docs/specs/step-6e.md, 6e.2).

Laya answers a `choice` question with a probability per option. `hierarchical` asks for the
level-1 Category, then the chosen one's children, until a Category has none; `shortlist` asks once
among the embedding's `k` nearest paths. Run them with
`uv run --group laya python -m catalog.evaluate run --classifier laya-hierarchical`.
"""

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from catalog import classify, taxonomy
from catalog.envelope import Content

QUESTION = "Which product category fits this listing?"
MODES = ("hierarchical", "shortlist")
CHECKPOINT = "aac6fef/laya-mlx"


@dataclass
class LayaClassifier:
    taxonomy: taxonomy.Taxonomy
    choose: Callable[[str, Sequence[str]], Sequence[float]]  # text, options -> probabilities
    mode: str = "hierarchical"
    shortlist: classify.EmbeddingClassifier | None = None  # for mode="shortlist"
    k: int = 10
    description: int = classify.DESCRIPTION
    taxonomy_version: str = field(init=False)

    def __post_init__(self):
        if self.mode not in MODES:
            raise ValueError(f"mode {self.mode!r} is not one of {MODES}")
        if self.mode == "shortlist" and self.shortlist is None:
            raise ValueError("shortlist mode needs an embedding classifier")
        self.taxonomy_version = f"{self.taxonomy.version}+laya-mlx-{self.mode}"
        self._children: dict[str, list[str]] = {}
        for path in self.taxonomy.paths:
            parent, _, name = path.rpartition(taxonomy.SEP)
            self._children.setdefault(parent, []).append(name)

    def classify(self, listings: Sequence[Content]) -> list[tuple[str, float]]:
        if self.mode == "shortlist":
            tops = self.shortlist.top(listings, self.k)
            return [
                self._ask(classify.text(x, self.description), t)
                for x, t in zip(listings, tops, strict=True)
            ]
        return [self._walk(classify.text(x, self.description)) for x in listings]

    def _walk(self, text: str) -> tuple[str, float]:
        path, confidence = "", 1.0
        while names := self._children.get(path):
            name, p = self._ask(text, names)
            path, confidence = f"{path}{taxonomy.SEP}{name}" if path else name, confidence * p
        return path, confidence

    def _ask(self, text: str, options: Sequence[str]) -> tuple[str, float]:
        p = list(self.choose(text, options))
        if len(p) != len(options):
            raise ValueError(f"{len(p)} probabilities for {len(options)} options")
        if not all(math.isfinite(x) and x >= 0 for x in p):  # a NaN would win max() silently
            raise ValueError(f"not probabilities: {p} for {list(options)}")
        best = max(range(len(p)), key=p.__getitem__)  # a tie goes to the first option
        return options[best], float(p[best])


def mlx(models: Path) -> Callable[[str, Sequence[str]], list[float]]:
    """Laya on MLX from `models/laya-mlx` (needs Apple Silicon, so the real one runs by hand)."""
    try:
        import laya_mlx

        agent = laya_mlx.load(str(models / "laya-mlx"))
    except Exception as e:  # not installed, missing or corrupt: laya_mlx raises various types
        raise classify.ModelMissing(
            "Laya won't load: uv sync --group laya, then uv run --group laya hf download "
            f"{CHECKPOINT} --local-dir {models / 'laya-mlx'}"
        ) from e

    def choose(text: str, options: Sequence[str]) -> list[float]:
        question = {"type": "choice", "instructions": QUESTION, "criteria": list(options)}
        got = agent.predict(text, {"c": question})["answers"]["c"]["probabilities"]
        if missing := [o for o in options if o not in got]:
            raise ValueError(f"Laya gave no probability for {missing[0]!r} among {list(options)}")
        return [got[o] for o in options]

    return choose
