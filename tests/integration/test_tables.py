"""The three cross-engine tables and their init step (docs/specs/step-a1.md)."""

import shutil
import time
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError
from support import up

from ingestion import api, events, landing, tables
from ingestion.events import EventLog

NAMES = ["landing_log", "submission_log", "outcomes"]
NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)


def opened(data):
    return [DeltaTable(str(data / name)) for name in NAMES]


def layout(dt):
    """Partition columns and protocol: what decides which engines can read and write it."""
    p = dt.protocol()
    protocol = (p.min_reader_version, p.min_writer_version, p.reader_features, p.writer_features)
    return dt.metadata().partition_columns, protocol


def test_init_creates_the_three_tables_on_their_layouts(tmp_path):
    tables.init(tmp_path)
    log, submissions, outcomes = opened(tmp_path)
    assert layout(log) == ([], (1, 4, None, None))  # delta-rs appends, Spark streams its feed
    assert layout(submissions) == layout(outcomes) == (["hour"], (1, 2, None, None))
    config = log.metadata().configuration
    assert config["delta.enableChangeDataFeed"] == "true"
    assert config["delta.logRetentionDuration"] == "interval 7 days"
    assert config["delta.deletedFileRetentionDuration"] == "interval 7 days"
    for dt in (submissions, outcomes):  # read as snapshots, never as a feed
        assert "delta.enableChangeDataFeed" not in dt.metadata().configuration
    pinned = outcomes.metadata().configuration  # what delta-rs reads after Spark appends
    assert pinned["delta.appendOnly"] == "true"
    assert pinned["delta.enableDeletionVectors"] == "false"
    assert pinned["delta.checkpointPolicy"] == "classic"


def test_init_creates_only_what_is_missing(tmp_path):
    tables.init(tmp_path)
    landing.append(opened(tmp_path)[0], [("s1", 0, up("a", 1), NOW)])
    kept = [(dt.metadata().id, dt.version()) for dt in opened(tmp_path)]
    shutil.rmtree(tmp_path / "submission_log")  # as if the last init was killed before it
    tables.main(["--data", str(tmp_path)])  # the CLI, as a deploy runs it
    log, submissions, outcomes = opened(tmp_path)
    assert [(dt.metadata().id, dt.version()) for dt in (log, outcomes)] == [kept[0], kept[2]]
    assert submissions.version() == 0 and submissions.metadata().id != kept[1][0]


def test_outcomes_refuse_a_delete(tmp_path):
    tables.init(tmp_path)
    outcomes = opened(tmp_path)[2]
    row = {"hour": "2026-09-30T12", "submission_id": "s1", "change_index": 0, "outcome": "written"}
    write_deltalake(outcomes, pa.Table.from_pylist([row], schema=tables.OUTCOMES), mode="append")
    with pytest.raises(CommitFailedError, match="append-only"):
        outcomes.delete("true")


@pytest.fixture
def away_from_utc(monkeypatch):
    """A local time 7 hours off UTC, so an hour taken in local time shows (CI runs in UTC)."""
    monkeypatch.setenv("TZ", "Asia/Ho_Chi_Minh")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def test_each_record_lands_in_its_submissions_utc_hour(tmp_path, away_from_utc):
    tables.init(tmp_path)
    submissions = opened(tmp_path)[1]
    last, first = datetime(2026, 9, 30, 12, 59, 59, 999000, tzinfo=UTC), NOW.replace(hour=13)
    ids = [str(api.uuid7_at(int(at.timestamp() * 1000))) for at in (last, first)]
    tables.record(submissions, [(ids[0], 0, "m_1", None, last), (ids[1], 0, "m_1", None, first)])
    rows = submissions.to_pyarrow_table(columns=["submission_id", "hour"]).to_pylist()
    assert {r["submission_id"]: r["hour"] for r in rows} == {
        ids[0]: "2026-09-30T12",  # one window, two hours: each Submission's own
        ids[1]: "2026-09-30T13",
    }


def test_a_lost_race_on_the_submission_log_is_retried_and_logged(tmp_path, monkeypatch):
    tables.init(tmp_path)
    submissions, sid = opened(tmp_path)[1], str(api.uuid7_at(int(NOW.timestamp() * 1000)))
    tables.record(submissions, [(sid, 0, "m_1", None, NOW)])  # a row for the DELETE to remove
    refresh, lost = submissions.update_incremental, []

    def refresh_then_lose_the_race():
        refresh()
        if not lost:
            lost.append(DeltaTable(submissions.table_uri).delete("true"))

    monkeypatch.setattr(submissions, "update_incremental", refresh_then_lose_the_race)
    tables.record(submissions, [(sid, 1, "m_1", None, NOW)], EventLog(tmp_path / "events", "api"))
    [retry] = events.read(tmp_path / "events")
    assert (retry["type"], retry["table"], retry["rows"]) == ("append_retry", "submission_log", 1)
    assert submissions.to_pyarrow_table()["change_index"].to_pylist() == [1]
