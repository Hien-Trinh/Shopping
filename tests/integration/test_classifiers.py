"""The classifier factory (`catalog.classifiers`), the classifier contract and the fatal errors."""

import ast
import importlib
import math
import subprocess
import sys
from importlib.util import find_spec
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from support import listing

from catalog import classifiers, classify, entry, jev, laya, state, student, supervisor, taxonomy

MODULES = ("classify", "taxonomy", "jev", "student", "decide", "metrics", "evaluate", "worker")


def imported(module: str) -> set[str]:
    """The catalog modules `module` imports anywhere in its source, functions included."""
    tree = ast.parse(Path(find_spec(f"catalog.{module}").origin).read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "catalog":
            names |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and (node.module or "").startswith("catalog."):
            names.add(node.module.removeprefix("catalog."))
        elif isinstance(node, ast.Import):
            names |= {a.name.removeprefix("catalog.") for a in node.names}
    return names


@pytest.mark.parametrize("module", [*MODULES, "supervisor", "classifiers"])
def test_each_module_imports_first_in_a_fresh_interpreter(module):  # no import cycle
    done = subprocess.run([sys.executable, "-c", f"import catalog.{module}"], capture_output=True)
    assert done.returncode == 0, done.stderr.decode()


def test_the_seam_imports_no_adapter_and_nothing_imports_back_up():
    assert not imported("classify") & {"jev", "student", "evaluate", "classifiers", "laya"}
    assert "evaluate" not in imported("student")
    assert "worker" not in imported("supervisor")
    assert "jev" not in imported("decide") and "jev" not in imported("worker")
    assert "classify" not in imported("taxonomy")
    users = {m for m in (*MODULES, "supervisor", "backfill") if "classifiers" in imported(m)}
    assert users == {"worker", "backfill"}


# --- the contract ---------------------------------------------------------------------------


def test_every_adapter_is_a_classifier():
    for adapter in (
        classify.FakeClassifier,
        classify.EmbeddingClassifier,
        jev.JevClassifier,
        student.StudentClassifier,
        laya.LayaClassifier,
    ):
        assert issubclass(adapter, classify.Classifier)


def test_a_classifier_is_free_and_silent_unless_it_says_otherwise():
    fake = classify.FakeClassifier()
    assert (fake.usd, fake.error) == (None, None)  # None: free, and "budget spent"
    shortlist = SimpleNamespace(model=classify.MODEL)  # only its name goes in the version
    paid = jev.JevClassifier(taxonomy.load(), shortlist, lambda body: {}, threads=1)
    assert (paid.usd, paid.error) == (0.0, None)


# --- the fatal errors -----------------------------------------------------------------------


def test_each_fatal_error_has_its_own_code_and_stops_the_supervisor():
    for module in ("state", "classify", "jev"):  # every module that defines one
        importlib.import_module(f"catalog.{module}")
    # the catalog's own: a test's refused class can linger until garbage collected
    codes = {e: e.code for e in entry.Fatal.__subclasses__() if e.__module__.startswith("catalog.")}
    assert codes == {
        state.PartitionTaken: 3,
        state.OffsetsMismatch: 4,
        state.CorruptState: 5,
        classify.ModelMissing: 6,
        jev.KeyMissing: 7,
    }
    assert set(codes.values()) <= set(entry.FATAL_CODES)
    assert {2, *entry.FATAL_CODES} == supervisor.FATAL_CODES  # a new code needs no edit there


# --- the loader -----------------------------------------------------------------------------


def ones(texts):
    return np.ones((len(texts), 3))


@pytest.fixture
def http(monkeypatch):
    """The kwargs jev.http is called with; its calls answer nothing."""
    got = {}
    monkeypatch.setattr(jev, "http", lambda **kw: got.update(kw) or (lambda body: {}))
    return got


@pytest.fixture
def models(tmp_path, monkeypatch, http):
    """A models directory holding the jev shortlist's vectors and the student's index, with the
    embedders and Jev's HTTP faked: what `--download` leaves, without a model."""
    monkeypatch.setattr(classify, "fastembed", lambda models: ones)
    tax = taxonomy.load()
    jev.shortlist(tax, ones).save(tmp_path / "texts")  # what classifiers --download does

    def shirt(texts):
        return np.array([[1.0, 0.0] if "shirt" in t.lower() else [0.0, 1.0] for t in texts])

    monkeypatch.setattr(student, "embedder", lambda models, **_: (shirt, student.FT_REVISION))
    path = student.index_path(tmp_path, student.FT_REVISION)
    path.parent.mkdir(parents=True)
    np.savez(path, x=np.eye(2, dtype=np.float32), y=np.array(tax.paths[:2]))
    return tmp_path


def test_the_jev_kind_loads_with_the_pipeline_settings(models, http):  # PR #63
    c = classifiers.load("jev", models, workers=4)
    assert http["attempts"] == 1  # no retries in a batch: the Backfill retries
    assert (c.rate, c.budget, c.threshold, c.k, c.description) == (20, 10, 0.40, 50, 200)
    assert (c.shortlist.description, c.shortlist.budget) == (200, math.inf)
    assert c.texts == "deeper" and c.shortlist.cache == models / "texts"  # step-6f.md, 6f.3
    assert len(c.shortlist.texts) > 14_000
    with pytest.raises(classify.ModelMissing, match="catalog.classifiers --download"):  # exit 6
        classifiers.load("jev", models / "empty", workers=4)


def test_the_jev_kind_without_a_key_stops_before_the_model_loads(monkeypatch, tmp_path):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.setattr(classify, "fastembed", lambda models: pytest.fail("loaded the model"))
    with pytest.raises(jev.KeyMissing):  # exit 7
        classifiers.load("jev", tmp_path, workers=4)


def test_the_student_kind_loads_from_the_index(models, monkeypatch):
    # step-6m.md, test point 4: the index and the pinned encoder, nothing of Jev
    monkeypatch.setattr(jev, "http", lambda **kw: pytest.fail("Jev is not in the loop"))
    c = classifiers.load("student", models, workers=4)
    assert isinstance(c, student.StudentClassifier)
    assert [a[0] for a in c.classify([listing("Blue shirt")])] == [taxonomy.load().paths[0]]
    with pytest.raises(classify.ModelMissing, match="catalog.student --download"):  # exit 6
        classifiers.load("student", models / "empty", workers=4)


def test_the_fake_kinds_need_no_model(tmp_path):
    assert classifiers.load("fake", tmp_path, workers=4) == classify.FakeClassifier()
    with pytest.raises(RuntimeError, match="unavailable"):  # down: an outage (step-7b.md)
        classifiers.load("down", tmp_path, workers=4).classify([listing("Shirt")])
    assert classifiers.KINDS[-1] == "down"


@pytest.mark.parametrize("kind", classifiers.KINDS)
def test_the_version_of_a_kind_is_what_it_loads_and_needs_no_model(models, monkeypatch, kind):
    # step-6c.md: the Backfill's target, with no model, no key and no index
    expected = classifiers.load(kind, models, workers=4).taxonomy_version
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.setattr(classify, "fastembed", lambda models: pytest.fail("loaded the model"))
    monkeypatch.setattr(student, "embedder", lambda models, **_: pytest.fail("loaded the student"))
    assert classifiers.version(kind) == expected


def test_the_versions_name_their_model():
    fake = classify.FakeClassifier().taxonomy_version
    # down is fake in an outage: the same version, so the Backfill reclassifies only flagged rows
    assert classifiers.version("fake") == classifiers.version("down") == fake
    assert "+bge-small-en-v1.5+deeper+jev-" in classifiers.version("jev")  # step-6f.md, 6f.3
    expected = student.version(taxonomy.load().version, student.FT_REVISION)
    assert classifiers.version("student") == expected  # step-6m.md, test point 3
    assert "+student-ft-knn@" in expected and "jev" not in expected


def test_download_builds_the_cache_the_worker_loads(tmp_path, monkeypatch):
    monkeypatch.setattr(classify, "fastembed", lambda models, download=False: ones)
    classifiers.main(["--download", "--models", str(tmp_path)])
    loaded = jev.shortlist(taxonomy.load(), ones, tmp_path / "texts")  # what the worker does
    assert loaded._paths.shape == (len(loaded.texts), 3)
    assert "catalog.classifiers --download" in classify.DOWNLOAD
