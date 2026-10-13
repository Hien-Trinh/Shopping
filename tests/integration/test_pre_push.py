"""The pre-push hook: a push that changes only Markdown skips `make check`; any other runs it,
logging its output to .git/pre-push.log and failing with its status."""

import os
import subprocess
from pathlib import Path

import pytest

HOOK = Path(__file__).parents[2] / ".githooks" / "pre-push"
ZERO = "0" * 40


@pytest.fixture
def repo(tmp_path):
    """A git repo with one commit holding a.py and a.md, and a `make` that only says it ran."""
    bin_ = tmp_path / "bin"
    bin_.mkdir()
    (bin_ / "make").write_text('#!/bin/sh\necho "make $*"\n')
    (bin_ / "make").chmod(0o755)
    work = tmp_path / "repo"
    work.mkdir()
    for args in (["init", "-q", "-b", "main"], ["config", "user.email", "t@t"],
                 ["config", "user.name", "t"]):  # fmt: skip
        git(work, *args)
    (work / "a.py").write_text("x = 1\n")
    (work / "a.md").write_text("# a\n")
    git(work, "add", ".")
    git(work, "commit", "-qm", "base")
    git(work, "update-ref", "refs/remotes/origin/main", "HEAD")
    return work


def git(work: Path, *args: str) -> str:
    # without GIT_*: run from a hook (make check in pre-push), git points them at the outer repo
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return subprocess.run(["git", *args], cwd=work, env=env, capture_output=True, text=True,
                          check=True).stdout.strip()  # fmt: skip


def push(repo, change) -> tuple[str, str]:
    """Commit `change(repo)` and run the hook for a push of it over the base commit."""
    base = git(repo, "rev-parse", "HEAD")
    change(repo)
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "change")
    head = git(repo, "rev-parse", "HEAD")
    return base, head


def hook(repo, stdin: str, status: int = 0) -> str:
    env = {"PATH": f"{repo.parent / 'bin'}:/usr/bin:/bin"}  # the fixture's `make`
    done = subprocess.run(["sh", str(HOOK)], cwd=repo, input=stdin, env=env,
                          capture_output=True, text=True)  # fmt: skip
    assert done.returncode == status, done.stderr
    return done.stdout.strip()


def test_a_markdown_only_push_skips_the_gate(repo):
    base, head = push(repo, lambda r: (r / "a.md").write_text("# b\n"))
    assert hook(repo, f"refs/heads/x {head} refs/heads/x {base}\n") == ""


@pytest.mark.parametrize(
    "change",
    [
        lambda r: (r / "a.py").write_text("x = 2\n"),
        lambda r: git(r, "mv", "a.py", "b.md"),  # a rename lists only its new name by default
    ],
    ids=["code", "code renamed to .md"],
)
def test_a_push_changing_anything_else_runs_the_gate(repo, change):
    base, head = push(repo, change)
    assert hook(repo, f"refs/heads/x {head} refs/heads/x {base}\n") == "make check"


def test_a_new_branch_is_compared_with_main_and_an_unknown_base_runs_the_gate(repo):
    _, head = push(repo, lambda r: (r / "a.py").write_text("x = 2\n"))
    assert hook(repo, f"refs/heads/x {head} refs/heads/x {ZERO}\n") == "make check"
    assert hook(repo, f"refs/heads/x {head} refs/heads/x {'f' * 40}\n") == "make check"


def test_deleting_a_branch_runs_nothing(repo):
    head = git(repo, "rev-parse", "HEAD")
    assert hook(repo, f"(delete) {ZERO} refs/heads/x {head}\n") == ""


@pytest.mark.parametrize("status", [0, 3])
def test_the_gate_output_is_logged_and_its_status_is_the_push_status(repo, status):
    (repo.parent / "bin" / "make").write_text(
        f'#!/bin/sh\necho "make $*"\necho oops >&2\nexit {status}\n'
    )
    base, head = push(repo, lambda r: (r / "a.py").write_text("x = 2\n"))
    out = hook(repo, f"refs/heads/x {head} refs/heads/x {base}\n", status)
    assert out == "make check\noops"
    assert (repo / ".git" / "pre-push.log").read_text() == "make check\noops\n"
    assert not (repo / ".git" / "pre-push.log.status").exists()
