"""Writes tables as the API does, with its own delta-rs 1.6.6. The Spark env holds Runtime 18's
1.1.4, so test_ingest runs this in the uv env (docs/specs/step-a2.md, question 1).

Usage: land.py DATA, with a JSON list of steps on stdin; prints one JSON answer per step.
"""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from deltalake import DeltaTable

from ingestion import entry, landing, tables
from ingestion.envelope import Change, Content


def _commit(data: Path, rows: list[list]) -> int:
    now = datetime.now(UTC)
    entries = [
        (submission, i, Change(*key, listing and Content.model_construct(**listing)), now)
        for submission, i, *key, listing in rows
    ]
    return landing.append(DeltaTable(str(data / "landing_log")), entries)


def _delete(data: Path, predicate: str) -> int:
    return DeltaTable(str(data / "landing_log")).delete(predicate)["num_deleted_rows"]


def _outcomes(data: Path) -> list:
    table = DeltaTable(str(data / "outcomes"))
    protocol = table.protocol()
    return [
        table.to_pyarrow_table().num_rows,
        protocol.min_reader_version,
        protocol.min_writer_version,
        protocol.reader_features,
        protocol.writer_features,
    ]


def main() -> None:
    data = Path(sys.argv[1])
    steps = {
        "init": lambda: tables.init(data),
        "commit": lambda rows: _commit(data, rows),
        "delete": lambda predicate: _delete(data, predicate),
        "outcomes": lambda: _outcomes(data),
    }
    print(json.dumps([steps[name](*args) for name, *args in json.load(sys.stdin)]))


if __name__ == "__main__":
    entry.exit_with(main)
