"""Metrics: saved DuckDB SQL over the events and the Listing Store (design doc, Observability).

DuckDB only reads: each run opens a throwaway in-memory connection (plan-v1, DuckDB's role).
Events are loaded from events.read (complete lines only) as (type, ts, e), `e` the raw JSON
line, because events of many shapes would infer a sparse schema. A Change counts once, by
(submission_id, change_index), with its best Outcome as status.fold ranks them (Phase 7), and
falls in the window when its first Outcome does.

    python -m catalog.metrics [--since 3600] [--data DIR]
"""

import argparse
import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable

from catalog import entry, events, state, status
from catalog.classify import UNCATEGORIZED

# Each Change's first Outcome (the window and freshness) and best one (the rates). Backfill
# reclassifies and rejected Changes never landed as a merchant's: they're left out.
_CHANGES = """
    SELECT e->>'submission_id' AS s, (e->>'change_index')::INT AS i,
           min(ts) AS first, arg_min(type, rank) AS best
    FROM events JOIN ranks ON type = kind
    WHERE type <> 'rejected' AND NOT starts_with(e->>'submission_id', 'backfill-')
    GROUP BY ALL HAVING min(ts) >= $since
"""
QUERIES = {
    "freshness_s": f"""
        WITH c AS ({_CHANGES}),
        a AS (SELECT e->>'submission_id' AS s, (e->>'change_index')::INT AS i, min(ts) AS ts
              FROM events WHERE type = 'accepted' GROUP BY ALL)
        SELECT count(*) AS changes, quantile_cont((first - a.ts) / 1000, 0.5) AS p50,
               quantile_cont((first - a.ts) / 1000, 0.99) AS p99
        FROM c JOIN a USING (s, i)
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
    # each partition's latest batch event: lag = head + 1 - next[p][0] (design doc)
    "lag_commits": """
        WITH b AS (SELECT ts, e, unnest(json_keys(e->'next')) AS p
                   FROM events WHERE type = 'batch' AND ts >= $since)
        SELECT p, arg_max((e->>'head')::BIGINT + 1 - (e->'next'->p->>0)::BIGINT, ts)
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
        except Exception:  # no Listing Store yet (delta-rs raises its own TableNotFoundError)
            out["uncategorized"] = None
        else:
            db.register("listings", listings)
            live, share = db.execute(_UNCATEGORIZED, {"u": UNCATEGORIZED}).fetchone()
            out["uncategorized"] = {"live": live, "share": share}
    return out


def _plain(v):
    return float(v) if hasattr(v, "is_finite") else v  # DuckDB's DECIMAL comes back a Decimal


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.metrics")
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--since", type=float, default=3600.0, help="seconds back from now")
    a = args.parse_args(argv)
    now = datetime.now(UTC)
    print(json.dumps(compute(a.data, now - timedelta(seconds=a.since), now)))


if __name__ == "__main__":
    entry.exit_with(main)
