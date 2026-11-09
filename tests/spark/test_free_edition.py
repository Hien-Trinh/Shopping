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
