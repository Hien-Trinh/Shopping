"""scripts/red.py on a throwaway file and test."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]
SOURCE = "answer = 42\nnote = 1\n"
TEST = """\
from pathlib import Path


def test_the_answer():
    assert "answer = 42" in Path(__file__).with_name("source.txt").read_text()
"""


def red(tmp_path, old, new):
    source, test = tmp_path / "source.txt", tmp_path / "test_source.py"
    source.write_text(SOURCE)
    test.write_text(TEST)
    args = [sys.executable, "scripts/red.py", str(source), old, new, str(test)]
    return subprocess.run(args, capture_output=True, text=True, cwd=ROOT)


def test_a_mutation_the_test_catches_is_red_shows_where_and_restores_the_file(tmp_path):
    done = red(tmp_path, "42", "43")
    assert done.returncode == 0, done.stdout + done.stderr
    assert "test_source.py:5: in test_the_answer" in done.stdout
    assert (tmp_path / "source.txt").read_text() == SOURCE


def test_a_mutation_the_test_misses_survives(tmp_path):
    done = red(tmp_path, "note = 1", "note = 2")
    assert done.returncode == 1, done.stdout + done.stderr
    assert "survives" in done.stdout
    assert (tmp_path / "source.txt").read_text() == SOURCE


def test_an_old_string_that_is_not_unique_runs_nothing(tmp_path):
    done = red(tmp_path, " = ", " != ")
    assert done.returncode == 2, done.stdout + done.stderr
    assert "appears 2 times" in done.stdout
    assert (tmp_path / "source.txt").read_text() == SOURCE
