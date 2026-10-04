import subprocess
import sys

import pytest

from catalog.taxonomy import Taxonomy, ancestor, load, trim

HEADER = (
    "# Shopify Product Taxonomy - Categories: 2026-08\n"
    "# Format: {GID} : {Ancestor name} > ... > {Category name}\n"
)
G = "gid://shopify/TaxonomyCategory/"
RAW = HEADER + (
    "\n"
    f"{G}ap        : Animals & Pet Supplies\n"
    f"{G}ap-1      : Animals & Pet Supplies > Live Animals\n"
    f"{G}ap-2      : Animals & Pet Supplies > Pet Supplies\n"
    f"{G}ap-2-1    : Animals & Pet Supplies > Pet Supplies > Bird Supplies\n"
    f"{G}ap-2-1-1  : Animals & Pet Supplies > Pet Supplies > Bird Supplies > Bird Cages\n"
    f"{G}na        : Uncategorized\n"
    f"{G}tm        : Time: Clocks\n"
)
TRIMMED = HEADER + (
    f"{G}ap        : Animals & Pet Supplies\n"
    f"{G}ap-1      : Animals & Pet Supplies > Live Animals\n"
    f"{G}ap-2      : Animals & Pet Supplies > Pet Supplies\n"
    f"{G}ap-2-1    : Animals & Pet Supplies > Pet Supplies > Bird Supplies\n"
    f"{G}tm        : Time: Clocks\n"
)


# trim and ancestor


def test_trim_drops_deep_categories_and_shopifys_uncategorized_and_keeps_the_rest_in_order():
    assert trim(RAW) == TRIMMED


def test_trim_of_a_trimmed_file_changes_nothing():
    assert trim(TRIMMED) == TRIMMED


def test_ancestor_cuts_a_path_to_depth_3():
    six = "A > B > C > D > E > F"
    assert ancestor(six) == "A > B > C"
    assert ancestor("A > B > C") == "A > B > C"
    assert ancestor("A") == "A"


# load


def write(tmp_path, text):
    path = tmp_path / "taxonomy.txt"
    path.write_text(text)
    return path


def test_load_parses_version_and_paths_in_file_order(tmp_path):
    assert load(write(tmp_path, TRIMMED)) == Taxonomy(
        "shopify-2026-08",
        (
            "Animals & Pet Supplies",
            "Animals & Pet Supplies > Live Animals",
            "Animals & Pet Supplies > Pet Supplies",
            "Animals & Pet Supplies > Pet Supplies > Bird Supplies",
            "Time: Clocks",
        ),
    )


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (f"{G}a : A\n", "version"),  # no header
        ("# Something else\n" + f"{G}a : A\n", "version"),
        (HEADER, "no Categories"),
        (HEADER + "A > B\n", "line 3"),  # no ' : '
        (HEADER + f"{G}a : \n", "line 3"),  # empty name
        (HEADER + f"{G}a : A\n{G}b : A >  > C\n", "line 4"),  # empty middle name
        (HEADER + f"{G}a : A\n{G}b : A > B\n{G}c : A > B > C\n{G}d : A > B > C > D\n", "line 6"),
        (HEADER + f"{G}a : A\n{G}b : A\n", "twice"),
        (HEADER + f"{G}a : A > B\n", "parent"),
        (HEADER + f"{G}na : Uncategorized\n", "Uncategorized"),
    ],
)
def test_load_rejects_a_bad_file(tmp_path, text, message):
    with pytest.raises(ValueError, match=message):
        load(write(tmp_path, text))


# the committed file


def test_committed_file_is_shopify_2026_08_trimmed_to_depth_3():
    taxonomy = load()
    assert taxonomy.version == "shopify-2026-08"
    assert len(taxonomy.paths) == 1862
    assert max(p.count(" > ") for p in taxonomy.paths) == 2


def test_committed_file_is_already_trimmed():
    from catalog.taxonomy import DEFAULT

    text = DEFAULT.read_text()
    assert trim(text) == text


# main


def test_main_prints_the_trimmed_file(tmp_path):
    raw = write(tmp_path, RAW)
    out = subprocess.run(
        [sys.executable, "-m", "catalog.taxonomy", str(raw)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout == TRIMMED
