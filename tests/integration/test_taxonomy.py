import gzip
import os
import subprocess
import sys

import pytest

from catalog.taxonomy import Taxonomy, ancestor, deeper, load, load_deeper, main, trim

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
        (HEADER + f"{G}a : A\n{G}b : A > > C\n", "line 4"),  # '>' left inside a name
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


def taxonomy_main(raw, **env):
    return subprocess.run(
        [sys.executable, "-X", "utf8=0", "-m", "catalog.taxonomy", str(raw)],
        capture_output=True,
        env={**os.environ, **env},
    )


def test_main_prints_the_trimmed_file(tmp_path, capsysbinary):
    main([str(write(tmp_path, RAW))])
    assert capsysbinary.readouterr().out.decode() == TRIMMED
    out = taxonomy_main(write(tmp_path, RAW))  # the module entry point too
    assert (out.returncode, out.stdout.decode()) == (0, TRIMMED)


def test_main_fails_on_a_file_that_is_not_a_taxonomy(tmp_path, capsysbinary):
    with pytest.raises(ValueError, match="version"):
        main([str(write(tmp_path, "<html>rate limited</html>\n"))])
    assert capsysbinary.readouterr().out == b""
    out = taxonomy_main(write(tmp_path, "<html>rate limited</html>\n"))
    assert out.returncode == 1
    assert out.stdout == b""


def test_utf8_names_survive_a_latin_1_locale(tmp_path):
    text = HEADER + f"{G}a : Café\n"
    path = tmp_path / "taxonomy.txt"
    path.write_bytes(text.encode())
    check = (
        "from pathlib import Path; from catalog.taxonomy import load; "
        f"assert load(Path({str(path)!r})).paths == ('Caf\\u00e9',)"  # ASCII argv, any locale
    )
    latin = {**os.environ, "LC_ALL": "en_US.ISO8859-1"}
    subprocess.run([sys.executable, "-X", "utf8=0", "-c", check], env=latin, check=True)
    out = taxonomy_main(path, LC_ALL="en_US.ISO8859-1", PYTHONIOENCODING="latin-1")
    assert out.stdout.decode() == text


# --- deeper (step-6h.md) -------------------------------------------------------------------

FULL = RAW + (
    f"{G}ap-2-1-2  : Animals & Pet Supplies > Pet Supplies > Bird Supplies > Bird Food\n"
    f"{G}ap-2-1-1-1 : Animals & Pet Supplies > Pet Supplies > Bird Supplies > Bird Cages > Large\n"
)
TAX = Taxonomy(
    "shopify-2026-08",
    (
        "Animals & Pet Supplies",
        "Animals & Pet Supplies > Live Animals",
        "Animals & Pet Supplies > Pet Supplies",
        "Animals & Pet Supplies > Pet Supplies > Bird Supplies",
        "Time: Clocks",
    ),
)
BIRDS = "Animals & Pet Supplies > Pet Supplies > Bird Supplies"


def test_deeper_groups_descendants_under_their_ancestor_in_file_order():
    got = deeper(FULL, TAX)
    assert got[BIRDS] == [
        f"{BIRDS} > Bird Cages",
        f"{BIRDS} > Bird Food",
        f"{BIRDS} > Bird Cages > Large",
    ]
    assert got["Time: Clocks"] == [] and list(got) == list(
        TAX.paths
    )  # every Category, Uncategorized out


def test_deeper_refuses_another_release():
    with pytest.raises(ValueError, match="2026-05.*shopify-2026-08"):
        deeper(FULL.replace("2026-08", "2026-05", 1), TAX)


def test_deeper_refuses_a_node_whose_ancestor_is_not_in_the_taxonomy():
    orphan = FULL + f"{G}zz-1-1-1  : Zoo > Cages > Big > Steel\n"
    with pytest.raises(ValueError, match=r"line \d+.*Zoo > Cages > Big"):
        deeper(orphan, TAX)


def test_the_committed_full_release_matches_the_committed_taxonomy():
    tax = load()
    got = load_deeper(tax)
    assert list(got) == list(tax.paths)
    assert sum(map(len, got.values())) > 12_000
    assert (
        "Apparel & Accessories > Clothing > Activewear > Activewear Pants > Leggings"
        in got["Apparel & Accessories > Clothing > Activewear"]
    )


def test_load_deeper_reads_a_given_release_and_names_a_corrupt_one(tmp_path):
    good, bad = tmp_path / "full.txt.gz", tmp_path / "bad.txt.gz"
    good.write_bytes(gzip.compress(FULL.encode()))
    assert load_deeper(TAX, good)[BIRDS][0] == f"{BIRDS} > Bird Cages"
    bad.write_bytes(gzip.compress(FULL.encode())[:40])
    with pytest.raises(Exception) as e:
        load_deeper(TAX, bad)
    assert any("bad.txt.gz" in n for n in getattr(e.value, "__notes__", []))
