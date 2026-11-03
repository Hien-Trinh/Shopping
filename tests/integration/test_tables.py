"""The three cross-engine tables and their init step (docs/specs/step-a1.md)."""

from datetime import UTC, datetime

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError
from support import up

from ingestion import landing, tables

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


def test_a_second_init_changes_nothing(tmp_path):
    tables.init(tmp_path)
    landing.append(opened(tmp_path)[0], [("s1", 0, up("a", 1), NOW)])
    before = [(dt.metadata().id, dt.version()) for dt in opened(tmp_path)]
    tables.main(["--data", str(tmp_path)])  # the CLI, as a deploy runs it
    assert [(dt.metadata().id, dt.version()) for dt in opened(tmp_path)] == before


def test_outcomes_refuse_a_delete(tmp_path):
    tables.init(tmp_path)
    outcomes = opened(tmp_path)[2]
    row = {"hour": "2026-09-30T12", "submission_id": "s1", "change_index": 0, "outcome": "written"}
    write_deltalake(outcomes, pa.Table.from_pylist([row], schema=tables.OUTCOMES), mode="append")
    with pytest.raises(CommitFailedError, match="append-only"):
        outcomes.delete("true")
