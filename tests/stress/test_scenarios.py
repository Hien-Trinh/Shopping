"""Every chaos scenario ends with the three oracles holding (plan-v1.md Phase 7, step-7b.md)."""

import json

import pytest

from catalog import chaos

pytestmark = pytest.mark.slow


@pytest.mark.parametrize("scenario", chaos.SCENARIOS)
def test_the_oracles_hold_after(scenario, tmp_path, capsys):
    code = chaos.main([scenario, "--seconds", "15", "--dir", str(tmp_path / "run")])
    summary = json.loads(capsys.readouterr().out)
    assert code == 0, summary
