"""Step 7d.2a: the six deferred costs on the 1M Listing Store (docs/specs/step-7d.md, 7d.2).

With the supervisor stopped, from the worktree holding the 1M store:

    uv run python spikes/stress_costs.py [--data data] [--repeats 3] [--version 50]

Times each step of one worker batch: 1,000 random keys from worker 0's 16 partitions. The MERGE
runs on the real store, once, rewriting the rows with their own content (the replay oracles still
hold). Prints JSON; measures only, ships no fix.
"""

import argparse
import json
import random
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable

from catalog import delta, landing, state, store
from catalog.keys import owned, partition
from catalog.plan import Write

LIMIT, MINE = 1000, owned(0, 4)  # a worker's batch, on the shipped Procfile's 4 workers


def timed(f, repeats):
    """(result, median ms, every run's ms)."""
    runs = []
    for _ in range(repeats):
        t = time.perf_counter()
        r = f()
        runs.append(round((time.perf_counter() - t) * 1000, 1))
    return r, statistics.median(runs), runs


def pruned(dt, parts):
    return dt.to_pyarrow_dataset(file_pruning_predicate=f"partition IN ({parts})")


def sample_keys(dt, seed=7):
    ds = pruned(dt, ", ".join(map(str, MINE)))
    rows = ds.to_table(columns=["merchant_id", "merchant_product_id"]).to_pylist()
    picked = random.Random(seed).sample(rows, LIMIT)
    return {(r["merchant_id"], r["merchant_product_id"]) for r in picked}


def read_keys_first(dt, keys):
    """The planned fix: scan the key columns, then `take` only the matching rows."""
    ds = pruned(dt, ", ".join(str(p) for p in sorted({partition(*k) for k in keys})))
    ids = ds.to_table(columns=["merchant_product_id"])["merchant_product_id"]
    hit = pc.indices_nonzero(pc.is_in(ids, pa.array({p for _, p in keys})))
    return delta.plain(ds.take(hit)).num_rows


def merge_share(dt, stored):
    """One MERGE of the keys' own rows; files removed and added, from the commit's log entry."""
    writes = [
        Write(k, s.source_version, s.listing, s.classification, False) for k, s in stored.items()
    ]

    before = dt.file_uris()
    t = time.perf_counter()
    merged = store.merge(dt, writes, datetime.now(UTC))
    ms = round((time.perf_counter() - t) * 1000)
    log = delta.local(dt.table_uri) / "_delta_log" / f"{merged.version:020}.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    mine = [f for f in before if any(f"partition={p}/" in f for p in MINE)]
    removed = sum("remove" in a for a in actions)
    return {
        "merge_ms": ms,
        "files_before": len(before),
        "files_in_worker_partitions": len(mine),
        "removed": removed,
        "added": sum("add" in a for a in actions),
        "share_of_worker_files": round(removed / len(mine), 3),
        "files_after": len(DeltaTable(dt.table_uri).file_uris()),
    }


def offset_saves(table, repeats):
    """One tick's save: every owned partition moved (the sequential load lands all of them)."""
    with tempfile.TemporaryDirectory() as d:
        offsets = {p: (103, 0) for p in MINE}
        _, ms, runs = timed(lambda: state.save_offsets(Path(d), offsets, table), repeats)
        files = len(list((Path(d) / "offsets").iterdir()))
        return {"files_written": files, "ms": ms, "runs_ms": runs}


def landing_slices(dt, version, max_versions):
    """landing.read at LIMIT slices from commit `version`, as worker 0 would."""
    after, slices, rows = {p: (version, 0) for p in MINE}, [], 0
    while min(v for v, _ in after.values()) <= version:
        t = time.perf_counter()
        batch = landing.read(dt, after, LIMIT, max_versions=max_versions)
        slices.append(round((time.perf_counter() - t) * 1000))
        after, rows = batch.offsets, rows + len(batch.changes)
    return {"rows": rows, "slices": len(slices), "ms_per_slice": slices, "ms": sum(slices)}


def commit_failures(root):
    lines = [json.loads(x) for f in root.glob("*/*.jsonl") for x in f.read_bytes().splitlines()]
    batches = [e for e in lines if e["type"] == "batch"]
    failed = [e for e in lines if e["type"] == "tick_failed"]
    named = [e for e in failed if "CommitFailedError" in e["error"] or "Overtaken" in e["error"]]
    return {
        "events": len(lines),
        "accepted": sum(e["type"] == "accepted" for e in lines),
        "batch": len(batches),
        "tick_failed": len(failed),
        "named": len(named),
        "rate": len(named) / len(batches),
        "batch_ms_mean": round(statistics.mean(e["ms"] for e in batches)),
        "partitions_moved": round(statistics.mean(len(e["next"]) for e in batches), 1),
    }


def metrics_24h(data, events, changes, repeats):
    cmd = [sys.executable, "-m", "catalog.metrics", "--since", "24h", "--data", str(data)]
    _, ms, runs = timed(lambda: subprocess.run(cmd, check=True, capture_output=True), repeats)
    per_change = events / changes
    scaled = ms / 1000 * per_change * 50 * 86400 / events  # linear in events parsed
    return {
        "ms": ms,
        "runs_ms": runs,
        "events": events,
        "events_per_change": round(per_change, 3),
        "scaled_24h_at_50_per_s_s": round(scaled, 1),
    }


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--repeats", type=int, default=3)
    args.add_argument("--version", type=int, default=50, help="the Landing log commit to re-read")
    a = args.parse_args()
    dt = store.ensure(str(a.data / "listing_store"))
    ld = landing.ensure(str(a.data / "landing_log"))
    keys = sample_keys(dt)
    out = {"store_version": dt.version(), "files": len(dt.file_uris()), "keys": len(keys)}
    stored, ms_all, runs_all = timed(lambda: store.read(dt, keys), a.repeats)
    rows, ms_keys, runs_keys = timed(lambda: read_keys_first(dt, keys), a.repeats)
    out["store_read"] = {
        "all_columns_ms": ms_all,
        "runs": runs_all,
        "keys_first_ms": ms_keys,
        "runs_keys_first": runs_keys,
        "rows_taken": rows,
    }
    out["merge"] = merge_share(dt, stored)
    out["offsets"] = offset_saves(landing.table_id(ld), a.repeats)
    out["landing_read"] = {
        "one_commit": landing_slices(ld, a.version, 1),
        "default_max_versions": landing_slices(ld, a.version, 100),
    }
    counts = out["commit_failed"] = commit_failures(a.data / "events")
    out["metrics"] = metrics_24h(a.data, counts["events"], counts["accepted"], a.repeats)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
