"""The chaos runner and the three oracles over a whole system (docs/specs/step-7b.md)."""

import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from deltalake import CommitProperties, DeltaTable

from catalog import chaos, export, landing, merchants, snapshots
from catalog.envelope import Change


@pytest.fixture
def system(tmp_path):
    made = chaos.System(tmp_path)
    yield made
    made.close()


def test_each_oracle_catches_its_kind_of_wrong(system):
    system.start()
    # A short load with 10 poison Changes: each has a failed event, so the store oracle skips it.
    got = chaos.run(system, "poison", seconds=3, rate=20, seed=1, settle=60)
    assert got["ok"], got
    assert got["outcomes"]["failed"] == 10
    data, state_dir = system.data, system.root / "state"

    # store: a Change landed after the system stopped, so no worker applied it.
    merchant = merchants.verify(system.root / merchants.DB, system.key).merchant_id
    late = Change(merchant, "p1", 2**50, "delete", None)
    log = landing.ensure(str(data / "landing_log"))
    landing.append(log, [("late", 0, late, datetime.now(UTC))])
    (line,) = chaos.oracles(data, state_dir)["store"]
    assert line.startswith(f"{(merchant, 'p1')}: expected {(2**50, late.content_hash, True)}")

    # export: the last export file lost, as a consumer missing one would be: exactly its keys.
    assert chaos.oracles(data, state_dir)["export"] == []
    last = export.files(data / "export")[-1]
    lost = {(r["merchant_id"], r["merchant_product_id"]) for r in chaos._rows(last)}
    last.unlink()
    named = chaos.oracles(data, state_dir)["export"]
    assert named and {line.split(": ")[0] for line in named} <= {str(k) for k in lost}

    # snapshot: a row gone from the newest snapshot (still pinned to the same version).
    newest = snapshots.existing(data / "snapshots")[-1]
    copy = DeltaTable(str(newest))
    pinned = snapshots.pinned(newest)
    key = copy.to_pyarrow_dataset().to_table(columns=["merchant_product_id"])[0][0].as_py()
    kept = CommitProperties(custom_metadata={"pinned_version": str(pinned)})
    copy.delete(f"merchant_product_id = '{key}'", commit_properties=kept)
    (line,) = chaos.oracles(data, state_dir)["snapshot"]
    assert line.startswith(f"{newest.name}: ") and f"'{key}')" in line


def test_a_scenario_runs_in_process_and_its_oracles_hold(tmp_path, capsys):
    assert chaos.main(["kill-worker", "--seconds", "5", "--dir", str(tmp_path / "run")]) == 0
    summary = json.loads(capsys.readouterr().out)
    # the three killed workers, each restarted; not the nine stopped at the end (PR #79 review)
    assert summary["process_exits"] == 3
    assert summary["oracles"] == {"store": "ok", "export": "ok", "snapshot": "ok"}
    landed = summary["metrics"]["freshness_s"]  # step 7c.2: the run's numbers come with it
    assert landed["changes"] > 0 and 0 <= landed["p50"] <= landed["p99"]


def test_a_used_dir_is_refused(tmp_path, capsys):
    (tmp_path / "left").write_text("")
    with pytest.raises(SystemExit) as stopped:
        chaos.main(["steady", "--dir", str(tmp_path)])
    assert stopped.value.code == 2
    assert "empty" in capsys.readouterr().err


def test_the_procfile_rewrite_rescales_and_sets_the_classifier_and_the_guard():
    text = chaos.procfile(chaos.PROCFILE.read_text(), 9, workers=3, classifier="down",
                          min_free=5)  # fmt: skip
    lines = text.splitlines()
    assert [line.split(":")[0] for line in lines if line.startswith("worker-")] == [
        "worker-0",
        "worker-1",
        "worker-2",
    ]
    assert all("--workers 3 --classifier down" in line for line in lines if "worker-" in line)
    assert "api: python -m catalog.api --port 9 --min-free 5" in lines
    assert "backfill: python -m catalog.backfill --classifier down" in lines


def test_the_shipped_procfile_runs_the_student_and_the_rewrite_never_loads_it():
    shipped = chaos.PROCFILE.read_text()
    lines = [line for line in shipped.splitlines() if line.startswith(("worker-", "backfill:"))]
    assert len(lines) == 5 and all(line.endswith("--classifier student") for line in lines)
    assert "student" not in chaos.procfile(shipped, 9)  # the e2e test needs no model
    with pytest.raises(ValueError, match="matched 0 lines, not 5"):
        chaos.procfile(shipped.replace("--classifier student", "--classifier jev"), 9)
    with pytest.raises(ValueError, match="would load the student"):  # a line the rewrite misses
        chaos.procfile(shipped + "extra: python -m catalog.x --classifier student\n", 9)


def fake_load(stdout):
    def load(self, *flags):
        code = f"import sys; sys.stdout.write({stdout!r})"
        return subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)

    return load


@pytest.mark.parametrize(
    ("stdout", "why"),
    [("", "printed no summary"),
     ('{"accepted": 0, "statuses": {"500": 3}, "errors": 0}', "nothing landed")],
)  # fmt: skip
def test_a_load_that_lands_nothing_fails_the_run(system, monkeypatch, stdout, why):
    system.start()
    monkeypatch.setattr(chaos.System, "load", fake_load(stdout))
    with pytest.raises(chaos.Failed, match=why):  # not ok on oracles that compared nothing
        chaos.run(system, "steady", seconds=1, rate=5, seed=0, settle=30)


def test_a_fault_that_does_nothing_fails_the_run(system, monkeypatch):
    system.start()
    monkeypatch.setitem(chaos.SCENARIOS, "duplicates", chaos.SCENARIOS["steady"])
    got = chaos.run(system, "duplicates", seconds=2, rate=10, seed=0, settle=60)
    assert got["oracles"] == {"store": "ok", "export": "ok", "snapshot": "ok"}
    assert got["problems"] == ["the fault left no already_applied Outcomes"]
    assert not got["ok"]


def test_settling_reports_how_many_changes_are_pending(tmp_path, monkeypatch):
    monkeypatch.setattr(chaos, "pending", lambda data: 3)
    system = SimpleNamespace(check=lambda: None, data=tmp_path, root=tmp_path)
    with pytest.raises(chaos.Failed, match="^3 Changes pending after 1 s$"):
        chaos.settle_down(system, "steady", 1)


def test_a_supervisor_slow_to_stop_is_a_failure_not_a_traceback(tmp_path):
    def slow(timeout):
        raise subprocess.TimeoutExpired("supervisor", timeout)

    system = chaos.System(tmp_path)
    system.supervisor = SimpleNamespace(send_signal=lambda sig: None, wait=slow)
    with pytest.raises(chaos.Failed, match="over 60 s"):
        system.stop()


def test_close_kills_its_loads_and_only_live_catalog_processes(tmp_path, monkeypatch):
    system = chaos.System(tmp_path)
    load = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    system.loads.append(load)
    sleep = "import time; time.sleep(60)"
    ours = subprocess.Popen([sys.executable, "-c", sleep, "catalog.worker"])
    other = subprocess.Popen([sys.executable, "-c", sleep])  # a reused pid: not ours
    monkeypatch.setattr(chaos.System, "pids", lambda self: [ours.pid, other.pid])
    try:
        system.close()
        assert load.poll() is not None
        assert ours.wait(timeout=5) == -9
        assert other.poll() is None
    finally:
        for proc in (ours, other):
            proc.kill()
            proc.wait()


@pytest.mark.parametrize("flag", ["--seconds", "--rate", "--settle"])
@pytest.mark.parametrize("value", ["nan", "inf", "0"])
def test_a_flag_that_is_not_a_finite_positive_number_is_refused(capsys, flag, value):
    with pytest.raises(SystemExit) as stopped:
        chaos.main(["steady", flag, value])
    assert stopped.value.code == 2


def _ci_jobs() -> dict[str, str]:
    """ci.yml's jobs by name, each as its block of text."""
    ci = (Path(__file__).parents[2] / ".github/workflows/ci.yml").read_text()
    parts = re.split(r"^  ([\w-]+):$", ci, flags=re.M)
    return dict(zip(parts[1::2], parts[2::2], strict=True))


def test_ci_runs_every_scenario_and_nothing_else():
    """stress-smoke's matrix (step-7d.md, 7d.1) can't drift from SCENARIOS."""
    leg = _ci_jobs()["stress-smoke-leg"]
    listed = re.search(r"^\s+scenario: \[(.*)\]$", leg, re.M)
    assert listed, "no scenario matrix in stress-smoke-leg"
    assert [s.strip() for s in listed[1].split(",")] == list(chaos.SCENARIOS)
    assert "catalog.chaos ${{ matrix.scenario }} " in leg  # each leg runs its own scenario
    assert "if: failure() || cancelled()" in leg  # a timed-out leg still uploads its events


def test_the_stress_smoke_gate_fails_unless_every_leg_passed():
    """A skipped required check counts as passed, so the gate must run and compare."""
    gate = _ci_jobs()["stress-smoke"]
    assert "needs: stress-smoke-leg" in gate
    assert "if: always()" in gate
    assert 'test "${{ needs.stress-smoke-leg.result }}" = success' in gate
