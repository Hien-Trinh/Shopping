"""The pre-push hook runs `make lint` and fails with its status, unless the push only deletes
branches."""

import subprocess
from pathlib import Path

import pytest

HOOK = Path(__file__).parents[2] / ".githooks" / "pre-push"
ZERO = "0" * 40
DELETE = f"(delete) {ZERO} refs/heads/x {'a' * 40}\n"
PUSH = f"refs/heads/y {'b' * 40} refs/heads/y {ZERO}\n"


def hook(tmp_path, stdin: str, make_status: int = 0) -> tuple[int, str]:
    """Run the hook with a `make` on PATH that says it ran and exits with `make_status`."""
    (tmp_path / "make").write_text(f'#!/bin/sh\necho "make $*"\nexit {make_status}\n')
    (tmp_path / "make").chmod(0o755)
    done = subprocess.run(["sh", str(HOOK)], cwd=tmp_path, input=stdin, capture_output=True,
                          text=True, env={"PATH": f"{tmp_path}:/usr/bin:/bin"})  # fmt: skip
    return done.returncode, done.stdout.strip()


@pytest.mark.parametrize("status", [0, 3])
def test_a_push_runs_make_lint_and_fails_with_its_status(tmp_path, status):
    assert hook(tmp_path, PUSH, status) == (status, "make lint")


def test_a_push_that_only_deletes_branches_runs_nothing(tmp_path):
    assert hook(tmp_path, DELETE * 2, 3) == (0, "")
    assert hook(tmp_path, DELETE + PUSH, 3) == (3, "make lint")  # a delete doesn't hide a push
