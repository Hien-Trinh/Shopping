"""The Free Edition run's checks, on local Spark (docs/specs/step-a3.md)."""

import pytest

from ingestion import free_edition, ingest


def checks_passed(capsys, data):
    free_edition.main(["--data", str(data)])
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == len(free_edition.CHECKS)
    assert all(line.startswith("PASS ") for line in lines), lines


def test_every_check_passes_and_a_second_pass_on_the_same_data_starts_clean(
    spark, tmp_path, capsys
):
    checks_passed(capsys, tmp_path)  # a failed task runs twice on one --data
    checks_passed(capsys, tmp_path)


def test_a_failed_check_prints_fail_the_later_checks_still_print_and_main_raises(
    spark, tmp_path, capsys, monkeypatch
):
    monkeypatch.setattr(ingest, "merge", lambda path, writes: None)
    with pytest.raises(RuntimeError, match=r"\d of 5 checks failed"):
        free_edition.main(["--data", str(tmp_path)])
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("FAIL the backlog lands as the store oracle says")
    assert len(lines) == len(free_edition.CHECKS)
