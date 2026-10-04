"""Shared helpers for the local Delta tables (ADR-0002)."""

import fcntl
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

LOG_RETENTION_HOURS = 1  # Delta log files kept; maintenance's guard uses the same hour


def ensure(path: str, schema: pa.Schema) -> DeltaTable:
    """Open the table, creating it first if it does not exist.

    Creation is serialized by a lock file: processes started together would otherwise race to
    create it, and the losers either crash or land a second CREATE with a new table id.
    """
    lock = Path(f"{local(path)}.lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            return DeltaTable(path)
        except TableNotFoundError:
            return DeltaTable.create(
                path,
                schema=schema,
                partition_by=["partition"],
                configuration={
                    "delta.enableChangeDataFeed": "true",
                    # Only maintenance removes log files, never past the slowest reader. Delta's
                    # own cleanup goes by age alone (docs/specs/step-5d.md, decision 4).
                    "delta.enableExpiredLogCleanup": "false",
                    "delta.logRetentionDuration": f"interval {LOG_RETENTION_HOURS} hours",
                },
            )


def local(path: str) -> Path:
    """A filesystem path for a table given as a path or a file:// URI (DeltaTable.table_uri)."""
    parsed = urlparse(path)
    return Path(unquote(parsed.path)) if parsed.scheme == "file" else Path(path)


def plain(table: pa.Table) -> pa.Table:
    """Cast string_view columns, which delta-rs returns but Arrow can't yet compare, to string."""
    fields = [f.with_type(pa.string()) if f.type == pa.string_view() else f for f in table.schema]
    return table.cast(pa.schema(fields))
