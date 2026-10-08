# Runbook

How to run the pipeline on one Mac: start, stop, rescale, recover, reset and read results. Every command runs from the repo root. Terms follow [CONTEXT.md](../CONTEXT.md); the design is in [the design doc](design-commerce-ingestion-pipeline.md).

## What's where

| Path | What | Safe to delete? |
|---|---|---|
| `Procfile` | the processes the supervisor runs: 4 workers, the API, Change Export, maintenance, Catalog Snapshots, the Backfill | no |
| `data/merchants.sqlite` | Merchants, their key hashes, and the `audit` table | **no**: every Merchant would need a new key |
| `data/landing_log/`, `data/listing_store/` | the two Delta tables | only in a reset |
| `data/events/<hour>/` | one JSONL file per process run; kept 3 days | only in a reset |
| `data/export/`, `data/snapshots/` | Change Export files (3 days) and Catalog Snapshots (7 days) | only in a reset |
| `state/` | offsets, the export watermark, heartbeats, and the locks that keep one process per role | only in a reset, and only with `data/`'s tables |
| `models/` | the embedding model, shortlist vectors and the fine-tuned student | yes; `--download` fetches them again |

## First run

```bash
uv sync
uv run python -m catalog.student --download              # once: the workers' classifier (step 6m), the encoder and its index, about 10 min the first time, into models/
uv run python -m catalog.classify --download             # only for `--classifier jev` or `embedding` and the eval: bge-small and the shortlist, about 2 min
uv run python -m catalog.merchants create --currency USD # prints merchant_id and key, once
```

Store the key: it is never shown again, and only its hash is kept. `rotate <merchant_id>` issues a new one (the old one stops at once); `revoke <merchant_id>` stops the Merchant. Each create, rotate and revoke adds a row to the `audit` table:

```bash
sqlite3 data/merchants.sqlite "SELECT * FROM audit ORDER BY rowid DESC LIMIT 10"
```

## Start

```bash
caffeinate -dims uv run python -m catalog.supervisor
```

No key: the workers classify with the student, locally. Only `--classifier jev` in the Procfile needs `TYPESAFE_API_KEY` exported (never logged).

`caffeinate` keeps the Mac awake: sleep pauses every process and skews freshness (plan-v1 B7). The API listens on `127.0.0.1:8000` only. The supervisor restarts any process that exits, and a worker that stops beating for 60 s. A worker's fatal exit (see Exit codes) stops everything instead.

Send a batch (`batch.json` is `{"changes": [...]}`, at most 32 MB):

```bash
curl -s -X POST localhost:8000/listings:batch -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' -d @batch.json
curl -s localhost:8000/submissions/<submission_id> -H "Authorization: Bearer $KEY"
```

Or a generated load (the key goes through the environment, never a flag):

```bash
CATALOG_API_KEY=$KEY uv run python -m catalog.load --rate 50 --changes 3000
```

## Stop

Ctrl-C in the supervisor's terminal, or `kill <supervisor pid>` (SIGTERM). Every process gets one shared 10 s to finish its tick, then anything still running is killed. The supervisor exits 0.

Find the pid with `pgrep -f catalog.supervisor`.

## Rescale

1. Stop.
2. In the `Procfile`, set `--workers N` on every worker line, and keep exactly the lines `worker-0` to `worker-(N-1)`. For example, 4 to 3: delete the `worker-3` line and change `--workers 4` to `--workers 3` on the other three.
3. Start.

Offsets are kept per partition, so partitions move to their new owner with their positions. A worker line left over with `--index` at or above `--workers` exits 2, which stops everything: fix the Procfile.

## Recover

**The supervisor was `kill -9`ed.** Its children notice within a second and stop. A worker stuck in a native call is force-exited 30 s later and leaves a `watch_exit` event. Wait until none is left, then start:

```bash
until ! pgrep -f 'catalog\.(worker|api|export|maintenance|snapshots|backfill)' >/dev/null; do sleep 1; done
caffeinate -dims uv run python -m catalog.supervisor
```

A chaos run's processes match the pattern too, so let any chaos run finish first. Starting sooner is safe but fails: the new workers find their partitions still locked and exit 3, so the new supervisor stops with 3.

**A chaos runner was `kill -9`ed.** Its supervisor runs in its own session and keeps going. Stop it with `pkill -f catalog.supervisor`. Its scratch dir stays in the temp folder.

**Another supervisor is running.** A second one exits 3 at once with "another one holds state/supervisor.lock". Stop the first.

## Reset

Starts the catalog empty and keeps the Merchants. Stop first, then:

```bash
rm -rf data/landing_log data/listing_store data/events data/export data/snapshots state
```

Delete `state/` with the tables, never alone and never without them: offsets saved against one Landing log are refused against another (worker exit 4).

A Listing Store created before step 7f lacks the file-size property, so its MERGEs still rewrite whole partitions. Set it once, with the system stopped; files written before stay large until a MERGE or a compaction touches their partition:

```bash
uv run python -c "from deltalake import DeltaTable; DeltaTable('data/listing_store').alter.set_table_properties({'delta.targetFileSize': str(1 << 20)})"
```

## Read results

```bash
uv run python -m catalog.metrics --since 10m      # also 90s, 1h, 2d
```

One JSON object, over the Changes whose first Outcome falls in the window. Each Change is counted once, with its best Outcome.

| Field | Meaning | Look for |
|---|---|---|
| `freshness_s` | seconds from `accepted` to the first Outcome, p50 and p99 | the SLO: p99 under 300 s at 50/s |
| `lag_commits` | per partition, Landing log commits not yet read | a partition that keeps growing: its worker is stuck |
| `classify_ms` | time per classifier call, p50 and p99 | the student takes a few ms a Listing; Jev was about 250 ms a call |
| `outcomes` | stale, conflict and failed rates | failed above 0: poison data, see `failed` events |
| `uncategorized` | share of live Listings that are Uncategorized | a jump: a classifier outage; the Backfill fixes it |
| `utilization` | per worker, the share of the window spent in ticks | near 1: add workers |
| `append_retries` | lost commit races in the API and the Backfill | rare; each costs `mean_ms` |
| `refused` | refused requests by reason | `low_disk`: under 5 GiB free; `no_key`, `unknown_key`, `revoked`; `too_large` (32 MB), `bad_json`, `bad_batch` |

The events behind them are plain JSONL. For example, the last exits:

```bash
cat data/events/*/supervisor-*.jsonl | grep process_exit | tail
```

| Event | Says |
|---|---|
| `process_exit` | a process ended: `reason` exit, stale, fatal, spawn or shutdown, with its `code` |
| `worker_stop`, `api_stop`, `export_stop`, `snapshots_stop`, `maintenance_stop`, `backfill_stop` | how it stopped: `error` if one ended it, `reason: supervisor_gone` if its supervisor died. |
| `watch_exit` | a process stuck after its supervisor died, force-exited |
| `tick_failed` | a worker's failed tick, retried with backoff (5 in a row and it exits 1) |
| `store_vacuum` | maintenance removed the Listing Store's dead files (step 7f): `removed` and `bytes`; `kept_versions` a reader can still need; `pin_stale` if a Snapshots pin older than an hour was ignored; `commit_failed` if a MERGE landed meanwhile (what it left goes next pass) |
| `classify_failed` | Listings left Uncategorized: `reason` budget or error |
| `refused`, `rejected` | a request or a Change the API turned away |

## Exit codes

| Process | Code | Meaning | Do |
|---|---|---|---|
| supervisor | 0 | stopped by a signal | nothing |
| supervisor | 3 | another supervisor holds `state/supervisor.lock` | stop the other one |
| supervisor | 2–7 | a worker's fatal code, below | as below |
| worker | 1 | gave up after 5 failed ticks | restarted by itself; read its `tick_failed` events |
| worker | 2 | a bad flag (e.g. `--index` outside `--workers`) | fix the Procfile |
| worker | 3 | its partitions are locked by another worker | wait for the old workers (see Recover) |
| worker | 4 | its offsets belong to another Landing log | reset both `state/` and the tables |
| worker | 5 | a state file is corrupt | read the error; restore or reset |
| worker | 6 | a model, its vectors or the student's index is missing or stale | the download the message names: `catalog.student --download` for the student, `catalog.classify --download` for bge-small and the shortlist |
| worker | 7 | `--classifier jev` only: `TYPESAFE_API_KEY` is unset, or Jev refused it (401 or 403) | export a working key |
| others | 1 | any error; restarted by the supervisor | read its `<process>_stop` event |

## Chaos runs

```bash
uv run python -m catalog.chaos kill-worker     # or steady, bulk, out-of-order, duplicates,
                                               # poison, classifier-outage, kill-supervisor,
                                               # rescale, disk-full
```

Each run builds a scratch system in a temp folder, runs 60 s of load at 20/s with its fault in the middle, waits for every Change to settle, checks the three oracles, and prints one JSON line with the result and its `metrics`. Exit 0 means every oracle held. Exit 1 means one failed or the system never settled; the line names the differing keys, and the scratch dir is kept. Exit 2 means a bad flag or a non-empty `--dir`, and 130 means Ctrl-C.
