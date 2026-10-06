"""Metrics: saved DuckDB SQL over the events and the Listing Store (design doc, Observability).

DuckDB only reads: each run opens a throwaway in-memory connection (plan-v1, DuckDB's role).
Events are loaded from events.read (complete lines only) as (type, ts, e), `e` the raw JSON
line, because events of many shapes would infer a sparse schema. A Change counts once, by
(submission_id, change_index), with its best Outcome as status.fold ranks them (Phase 7), and
falls in the window when its first Outcome does.

    python -m catalog.metrics [--since 10m] [--data DIR]
"""

import argparse
import json
import re
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from catalog import entry, events, state, status
from catalog.classify import UNCATEGORIZED

# Each merchant Change the API accepted within the read range: when, its first Outcome (the
# window and freshness) and its best one (the rates). Only an accepted Change counts, so a
# Backfill reclassify or a rejected Change never does, nor a replay of a Change accepted before
# the read range (its first Outcome wasn't read either).
_CHANGES = """
    WITH a AS (SELECT e->>'submission_id' AS s, (e->>'change_index')::INT AS i,
                      min(ts) AS accepted
               FROM events WHERE type = 'accepted' GROUP BY ALL),
    o AS (SELECT e->>'submission_id' AS s, (e->>'change_index')::INT AS i,
                 min(ts) AS first, arg_min(type, rank) AS best
          FROM events JOIN ranks ON type = kind GROUP BY ALL)
    SELECT * FROM o JOIN a USING (s, i) WHERE first >= $since
"""
QUERIES = {
    "freshness_s": f"""
        SELECT count(*) AS changes, quantile_cont((first - accepted) / 1000, 0.5) AS p50,
               quantile_cont((first - accepted) / 1000, 0.99) AS p99
        FROM ({_CHANGES})
    """,
    "outcomes": f"""
        SELECT count(*) AS changes, avg((best = 'stale')::INT) AS stale,
               avg((best = 'conflict')::INT) AS conflict, avg((best = 'failed')::INT) AS failed
        FROM ({_CHANGES})
    """,
    "classify_ms": """
        SELECT count(*) AS calls, sum((e->>'listings')::INT) AS listings,
               quantile_cont((e->>'ms')::INT, 0.5) AS p50,
               quantile_cont((e->>'ms')::INT, 0.99) AS p99
        FROM events WHERE type = 'classify' AND ts >= $since
    """,
    "append_retries": """
        SELECT count(*) AS count, avg((e->>'ms')::INT) AS mean_ms
        FROM events WHERE type = 'append_retry' AND ts >= $since
    """,
}
# {key: value} metrics: one row per key
GROUPED = {
    # lag = head + 1 - next[p][0] (design doc), with each partition's latest position against
    # the newest head any worker read: a stalled worker logs no batch, so its own last one
    # would hide its lag. Over the whole read range, so a partition stalled all window shows.
    "lag_commits": """
        WITH b AS (SELECT ts, e, unnest(json_keys(e->'next')) AS p
                   FROM events WHERE type = 'batch')
        SELECT p, (SELECT max((e->>'head')::BIGINT) FROM b) + 1
                  - arg_max((e->'next'->p->>0)::BIGINT, ts)
        FROM b GROUP BY p ORDER BY p::INT
    """,
    # the share of the window each worker spent in ticks that moved an offset
    "utilization": """
        SELECT e->>'worker' AS w, round(sum((e->>'ms')::INT) / ($now - $since), 6)
        FROM events WHERE type = 'batch' AND ts >= $since GROUP BY w ORDER BY w
    """,
    "refused": """
        SELECT e->>'reason' AS r, count(*) FROM events
        WHERE type = 'refused' AND ts >= $since GROUP BY r ORDER BY r
    """,
}
_UNCATEGORIZED = """
    SELECT count(*) AS live, avg((primary_category = $u)::INT) AS share
    FROM listings WHERE NOT is_tombstone
"""


def compute(data: Path, since: datetime, now: datetime) -> dict:
    """Every metric over events from `since` to `now` (timezone-aware), JSON-ready."""
    # An hour wider for `accepted`: a Change accepted just before the window still has a freshness.
    found = events.read(data / "events", since - timedelta(hours=1))
    table = pa.table({
        "type": [e["type"] for e in found],
        "ts": pa.array([e["ts"] for e in found], pa.int64()),
        "e": [json.dumps(e) for e in found],
    })  # fmt: skip
    ranks = pa.table({"kind": [str(k) for k in status.RANK], "rank": list(status.RANK.values())})
    params = {"since": int(since.timestamp() * 1000), "now": int(now.timestamp() * 1000)}
    with duckdb.connect() as db:
        db.register("events_raw", table)
        db.execute("CREATE VIEW events AS SELECT type, ts, e::JSON AS e FROM events_raw")
        db.register("ranks", ranks)

        def run(sql: str) -> duckdb.DuckDBPyRelation:
            used = {k: v for k, v in params.items() if f"${k}" in sql}
            return db.execute(sql, used)

        out: dict = {"since": since.isoformat()}
        for name, sql in QUERIES.items():
            cursor = run(sql)
            row = cursor.fetchone()
            out[name] = {c[0]: _plain(v) for c, v in zip(cursor.description, row, strict=True)}
        for name, sql in GROUPED.items():
            out[name] = {k: _plain(v) for k, v in run(sql).fetchall()}
        try:
            listings = DeltaTable(str(data / "listing_store")).to_pyarrow_dataset()
        except TableNotFoundError:  # none yet; a corrupt one raises
            out["uncategorized"] = None
        else:
            db.register("listings", listings)
            live, share = db.execute(_UNCATEGORIZED, {"u": UNCATEGORIZED}).fetchone()
            out["uncategorized"] = {"live": live, "share": share}
    return out


def _plain(v):
    return float(v) if hasattr(v, "is_finite") else v  # DuckDB's DECIMAL comes back a Decimal


def duration(text: str) -> float:
    """Seconds in `90`, `90s`, `10m`, `1.5h` or `2d`; more than 0."""
    if not (m := re.fullmatch(r"(\d+(?:\.\d+)?)([smhd]?)", text)) or not float(m[1]):
        raise argparse.ArgumentTypeError(f"want a duration like 10m or 1h, got {text!r}")
    return float(m[1]) * {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[m[2]]


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.metrics")
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--since", type=duration, default=3600.0, help="how far back, e.g. 10m")
    a = args.parse_args(argv)
    now = datetime.now(UTC)
    print(json.dumps(compute(a.data, now - timedelta(seconds=a.since), now)))


if __name__ == "__main__":
    entry.exit_with(main)
