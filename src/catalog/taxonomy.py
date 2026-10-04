"""The catalog taxonomy: Shopify's product taxonomy cut to 3 levels (docs/specs/step-6a.md).

The committed file keeps the release's line format, `{GID} : {Ancestor} > ... > {Name}` under a
`#` header ending in the release date. Update it from a new release's `categories.en.txt` with
`python -m catalog.taxonomy categories.en.txt > src/catalog/data/shopify-taxonomy.txt`.
"""

import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

from catalog.classify import UNCATEGORIZED
from catalog.entry import exit_with

DEPTH = 3
SEP = " > "
DEFAULT = files("catalog") / "data" / "shopify-taxonomy.txt"
_VERSION = re.compile(r"# Shopify Product Taxonomy - Categories: (\S+)$")


@dataclass(frozen=True)
class Taxonomy:
    version: str
    paths: tuple[str, ...]


def _path(line: str) -> str:
    return line.partition(" : ")[2].strip()


def trim(text: str, depth: int = DEPTH) -> str:
    """Keep the header and the Categories at depth 1 to `depth`, in order. Shopify's own
    Uncategorized is dropped: it would collide with the fallback (decision 7)."""
    kept = [
        line
        for line in text.splitlines()
        if line.startswith("#")
        or (line.strip() and _path(line) != UNCATEGORIZED and _path(line).count(SEP) < depth)
    ]
    return "".join(f"{line}\n" for line in kept)


def ancestor(path: str, depth: int = DEPTH) -> str:
    """A deeper Category's depth-`depth` ancestor; the path itself if not deeper."""
    return SEP.join(path.split(SEP)[:depth])


def load(path: Path | None = None) -> Taxonomy:
    """Parse and validate a trimmed file; ValueError on anything off."""
    source = path or DEFAULT
    return parse(source.read_text(encoding="utf-8"), source)


def parse(text: str, source) -> Taxonomy:
    """`load` on text already read; `source` names it in errors."""
    lines = text.splitlines()
    match = _VERSION.match(lines[0]) if lines else None
    if not match:
        raise ValueError(f"{source}: no version header")
    paths: dict[str, None] = {}  # ordered, with set lookups
    for n, line in enumerate(lines, 1):
        if line.startswith("#") or not line.strip():
            continue
        p = _path(line)
        names = p.split(SEP)
        if (
            " : " not in line
            or not all(name.strip() and ">" not in name for name in names)
            or len(names) > DEPTH
        ):
            raise ValueError(f"{source}: bad Category on line {n}: {line!r}")
        if p == UNCATEGORIZED:
            raise ValueError(f"{source}: line {n} is {UNCATEGORIZED}, the fallback's name")
        if p in paths:
            raise ValueError(f"{source}: line {n}: {p!r} appears twice")
        if len(names) > 1 and SEP.join(names[:-1]) not in paths:
            raise ValueError(
                f"{source}: line {n}: the parent of {p!r} is missing or comes after it"
            )
        paths[p] = None
    if not paths:
        raise ValueError(f"{source}: no Categories")
    return Taxonomy(f"shopify-{match[1]}", tuple(paths))


def main(argv: Sequence[str] | None = None) -> None:
    """Print the trimmed file; a raw file that trims to an invalid taxonomy exits 1."""
    (raw,) = sys.argv[1:] if argv is None else argv
    trimmed = trim(Path(raw).read_text(encoding="utf-8"))
    parse(trimmed, raw)
    sys.stdout.buffer.write(trimmed.encode())


if __name__ == "__main__":
    exit_with(main)
