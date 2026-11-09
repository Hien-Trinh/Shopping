"""The Free Edition run's checks, on local Spark (docs/specs/step-a3.md, step-a4.md)."""

import pytest
from test_ingest import ROOT

from ingestion import classify_udf, free_edition, ingest


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
    monkeypatch.setattr(ingest, "merge", lambda path, writes, planned: None)
    with pytest.raises(RuntimeError, match=r"\d of 5 checks failed"):
        free_edition.main(["--data", str(tmp_path)])
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("FAIL the backlog lands as the store oracle says")
    assert len(lines) == len(free_edition.CHECKS)


def test_a_wipe_that_fails_stops_the_run(tmp_path):
    data = tmp_path / "locked" / "data"
    data.mkdir(parents=True)
    data.parent.chmod(0o555)
    try:
        with pytest.raises(PermissionError):
            free_edition.main(["--data", str(data)])
    finally:
        data.parent.chmod(0o755)


def test_each_later_check_fails_on_what_it_checks(spark, tmp_path, monkeypatch):
    data = str(tmp_path)
    free_edition.main(["--data", data])

    def adds_an_outcome(session, path):
        row = "hour string, submission_id string, change_index int, outcome string"
        rows = session.createDataFrame([("h", "s", 0, "written")], row)
        ingest.append_outcomes(f"{path}/outcomes", rows, "a test", 0)

    with pytest.raises(ValueError):  # the later commit's rows are there too
        free_edition.one_outcome_per_change(spark, data)
    monkeypatch.setattr(free_edition, "_run_query", lambda session, path: None)
    with pytest.raises(ValueError):  # landed, but no query reported them
        free_edition.later_commit_reads_its_stored_rows(spark, data)
    monkeypatch.setattr(free_edition, "_run_query", adds_an_outcome)
    with pytest.raises(ValueError):
        free_edition.restart_adds_nothing(spark, data)
    feed_off = "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')"
    spark.sql(f"ALTER TABLE delta.`{data}/listing_store` {feed_off}")
    with pytest.raises(ValueError):
        free_edition.store_has_its_layout(spark, data)


def test_the_student_check_on_fake_prints_fail(spark, tmp_path, capsys):
    def on_fake(session, data):
        free_edition.student_classifies(session, data, classify_udf.FAKE)

    assert not free_edition._passes(free_edition.STUDENT, on_fake, spark, str(tmp_path))
    assert capsys.readouterr().out.startswith(f"FAIL {free_edition.STUDENT}: ValueError: expected")


def test_models_that_cant_load_print_fail_beside_the_other_checks_and_main_raises(
    spark, tmp_path, capsys
):
    models = tmp_path / "models"  # no train/ beside it, so the student's Spec raises
    with pytest.raises(RuntimeError, match="1 of 6 checks failed"):
        free_edition.main(["--data", str(tmp_path / "data"), "--models", str(models)])
    lines = capsys.readouterr().out.splitlines()
    assert lines[:5] == [f"PASS {name}" for name in free_edition.CHECKS]
    assert lines[5].startswith(f"FAIL {free_edition.STUDENT}: ModelMissing")
    assert len(lines) == 6


@pytest.mark.model
def test_main_with_the_real_models_prints_only_pass_lines(spark, tmp_path, capsys):
    free_edition.main(["--data", str(tmp_path), "--models", str(ROOT / "models")])
    lines = capsys.readouterr().out.splitlines()
    assert lines == [f"PASS {name}" for name in [*free_edition.CHECKS, free_edition.STUDENT]]
