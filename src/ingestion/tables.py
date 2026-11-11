"""The Submission log (the API's, with delta-rs) and Outcomes (the Ingestion job's, with Spark),
and the init step that creates them and the Landing log (docs/specs/step-a1.md). Both are
partitioned by the hour of the Submission's UUIDv7, so a Submission's rows sit in one partition."""

import argparse
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

from ingestion import delta, entry, landing, state
from ingestion.envelope import Change
from ingestion.events import EventLog


def _schema(*columns: tuple[str, pa.DataType]) -> pa.Schema:
    return pa.schema([pa.field(name, kind, nullable=False) for name, kind in columns])


SUBMISSION_LOG = _schema(
    ("hour", pa.string()),  # a string: Delta keeps a timestamp partition without its time zone
    ("submission_id", pa.string()),
    ("change_index", pa.int32()),
    ("merchant_id", pa.string()),
    ("accepted", pa.bool_()),  # false: rejected
    ("received_at", pa.timestamp("us", tz="UTC")),
)
OUTCOMES = _schema(
    ("hour", pa.string()),
    ("submission_id", pa.string()),
    ("change_index", pa.int32()),
    ("outcome", pa.string()),  # a status.Outcome
)
# Spark writes Outcomes and delta-rs reads it, so nothing delta-rs can't read: no deletion
# vectors, classic checkpoints. Only the Ingestion job writes it, and only appends (rule 1).
# Optimized writes: one file a batch, not one per shuffle partition (174 at Spark's default).
OUTCOMES_PROPERTIES = {
    "delta.appendOnly": "true",
    "delta.enableDeletionVectors": "false",
    "delta.checkpointPolicy": "classic",
    "delta.autoOptimize.optimizeWrite": "true",
}

# submission_id, change_index, merchant_id, the Change if accepted (None if rejected), received_at
Record = tuple[str, int, str, Change | None, datetime]


BACKFILL = "backfill-"  # a Backfill round's submission id: this, then a UUIDv7 (step-a5.md)


def uuid7_at(ms: int) -> uuid.UUID:
    """A UUIDv7 (RFC 9562) whose 48-bit timestamp is `ms`: version 7, variant 10, 74 random bits.
    A stand-in for uuid.uuid7(), which is 3.14+; Databricks runs 3.12."""
    # A uuid4's low 76 bits are already v7's: 12 random, variant 10, 62 random.
    return uuid.UUID(int=(ms << 80) | (7 << 76) | (uuid.uuid4().int & ((1 << 76) - 1)))


def hour(submission_id: str) -> str:
    """The partition of a Submission's rows, or a Backfill round's: its UUIDv7's millisecond
    timestamp, to the hour."""
    ms = uuid.UUID(submission_id.removeprefix(BACKFILL)).int >> 80
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H")


def record(dt: DeltaTable, records: Sequence[Record], events: EventLog | None = None) -> int:
    """Append each record's Submission log row in one commit; returns its version."""
    rows = [
        {"hour": hour(s), "submission_id": s, "change_index": i, "merchant_id": m,
         "accepted": c is not None, "received_at": at}
        for s, i, m, c, at in records
    ]  # fmt: skip
    return landing.commit(dt, pa.Table.from_pylist(rows, schema=SUBMISSION_LOG), events)


def init(data: Path) -> None:
    """Create whichever of the three tables is missing; an existing one is never touched. The
    API only opens its two, since two replicas starting on an empty store would both create them."""
    landing.ensure(str(data / "landing_log"))
    delta.ensure(str(data / "submission_log"), SUBMISSION_LOG, partition_by=["hour"])
    delta.ensure(str(data / "outcomes"), OUTCOMES, partition_by=["hour"], **OUTCOMES_PROPERTIES)


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m ingestion.tables")
    args.add_argument("--data", type=Path, default=state.DATA)
    init(args.parse_args(argv).data)


if __name__ == "__main__":
    entry.exit_with(main)
