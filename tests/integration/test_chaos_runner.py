"""The chaos runner and the three oracles over a whole system (docs/specs/step-7b.md)."""

import json
from datetime import UTC, datetime

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

    # export: the last export file lost, as a consumer missing one would be.
    assert chaos.oracles(data, state_dir)["export"] == []
    export.files(data / "export")[-1].unlink()
    assert chaos.oracles(data, state_dir)["export"] != []

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
    assert summary["process_exits"] >= 1  # the killed workers, each restarted
    assert summary["oracles"] == {"store": "ok", "export": "ok", "snapshot": "ok"}


def test_a_used_dir_is_refused(tmp_path, capsys):
    (tmp_path / "left").write_text("")
    with pytest.raises(SystemExit) as stopped:
        chaos.main(["steady", "--dir", str(tmp_path)])
    assert stopped.value.code == 2
    assert "empty" in capsys.readouterr().err
