# Stress report

The full runs of [step 7d.2](specs/step-7d.md) on one Mac. Part 7d.2a (Oct 7): the 1M initial load on the `fake` classifier and the six deferred costs on that store. Part 7d.2b, the SLO run and the 10k bulk on the student, runs after step 6m.2 and decides the Phase 7 exit. Commands follow [the runbook](runbook.md); terms follow [CONTEXT.md](../CONTEXT.md).

## The machine

| | |
|---|---|
| Hardware | Apple M4, 10 cores, 16 GiB RAM |
| OS, Python | macOS 15.7.4, Python 3.14.3 under `uv run` |
| Disk | one 228 GiB volume; 11 GiB free before the load, 7.0 GiB after the supervisor stopped |
| Processes | the shipped `Procfile`, every classifier flag set to `fake`: 4 workers, the API, Change Export, maintenance, Catalog Snapshots, the Backfill |

The API refuses writes under 5 GiB free (`low_disk`), so free space was sampled every 30 s beside `du data/`. It never fell under 6.0 GiB and nothing was refused.

## 7d.2a, point 1: the 1M initial load

`catalog.load --changes 1000000 --keys 1000000 --order sequential --batch 1000 --rate 0`, 16 generator processes, under `/usr/bin/time -p`. Every Change is a new key, so the run measures storage, not the classifier.

| | |
|---|---|
| Load wall time | 24.7 s (user 16.0 s, sys 1.8 s) |
| Sent / accepted / errors | 1,000,000 / 1,000,000 / 0 (1,000 requests, all `202`) |
| Changes/s accepted | 40,775 |
| Request latency, p50 / p99 / max | 335 / 664 / 826 ms |
| Landing log after the load | 102 commits of about 10,000 Changes each (group commit), 379 MB, 6,529 Parquet files |
| Time to the last Outcome | 838 s from the load's start (814 s from its end) |
| Outcomes/s, four workers | 1,193 over the whole run |
| Outcomes | 1,000,000 `written`; 0 `already_applied`, `stale`, `conflict`, `failed`; 0 `tick_failed` |
| Freshness, whole run, p50 / p99 | 368.6 / 802.5 s |
| Classify, 1,002 calls over 1,000,000 Listings, p50 / p99 | 1 / 3 ms |
| Worker utilization, 20 min window | 0.69 each |

Freshness is dominated by the queue: the load lands 1M Changes in 25 s and the workers drain them at about 1,200/s, 24 times the 50/s target, so the p99 of 802 s is the time to drain the queue, not a per-Change cost. The sequential order lands every partition in every commit, so `lag_commits` was the same on all 64 partitions at every sample.

| Sample | Outcomes | Lag (commits, every partition) | Freshness p50 / p99, last 10 min | Utilization | Free | `data/` |
|---|---|---|---|---|---|---|
| 06:35:43Z, start | 0 | 0 | | 0 | 11 GiB | 108 KB |
| 06:40:59Z | 381,000 | 71 | 89.0 / 307 s | 0.53 | 9.0 GiB | 1.0 GB |
| 06:47:13Z | 773,000 | 28 | 361.7 / 665.4 s | 1.00 | 8.4 GiB | 1.6 GB |
| 06:50:15Z, settled | 1,000,000 | 0 | 368.6 / 802.5 s (whole run) | 0.69 (whole run) | 6.0 GiB | 1.9 GB |

The store after the run:

| | Listing Store | Landing log | Events |
|---|---|---|---|
| Size | 1.1 GB | 379 MB | 390 MB (2,002,036 lines) |
| Files | 33,534 (32,522 Parquet, 1,023 log) | 6,633 (6,529 Parquet, 106 log) | 5 JSONL files |
| Delta version | 1,010 | 102 | |
| Rows | 1,000,000 live, 0 tombstones, 0 Uncategorized | | |

Free space fell from 11 to 6.0 GiB while `data/` grew to 1.9 GB. The two big drops (10 to 7.8 GiB, 8.4 to 6.2 GiB) each coincided with the sampler's `catalog.metrics --since 10m`, which loads every event into memory, and free space partly recovered between them; swap stood at 11.8 GB of 13 GB at the end. The likely cause is swap files growing on the volume during metrics runs: a hypothesis, not verified. `catalog.metrics --since 20m` over the whole run's 2M events took 14.0 s.

## 7d.2a, point 2: the six costs

Measured by [`spikes/stress_costs.py`](../spikes/stress_costs.py), run once on the 1M store with the supervisor stopped (76 s wall; 3 repeats, median). The batch is 1,000 random keys (seed 7) from worker 0's 16 partitions, the shape of one worker batch: a batch never spans other workers' partitions, and in exploration 1,000 keys over all 64 partitions made `store.read` scan the whole store (6.7 s), which the system never does. Context from the run's own events: a worker's full 1,000-Change batch took 3,313 ms on average.

| Cost | Number | Verdict |
|---|---|---|
| a. `store.read`, all columns vs key columns first | all columns 1,967 ms per 1,000 keys (1,412 / 1,967 / 2,158); keys first then `take` 1,788 ms (1,784 / 1,788 / 1,928), same rows. The 16 partitions hold 864 of the store's 3,488 live files, about 320 KB each against the 1 MiB compaction target | fine at this size: about 2 s per full batch, and a tick at 50/s carries about 12 Changes. The planned fix buys about 9%, inside the run-to-run spread, because the time is per-file overhead (864 files opened), not column decoding. The lever, if ever needed, is fewer files (compaction cadence) |
| b. share of files one MERGE rewrites | 252 of the worker's 864 files removed (29.2%; 7.2% of all 3,488), 16 added (one per partition), 19.4 MB out / 20.8 MB in, 210,914 rows copied to update 1,000; 2,144 ms (scan 1,873 ms, rewrite 38 ms) | fine at this size: 29% against the plan's 63% guess, 2.1 s per full batch. Each MERGE coalesces what it rewrites into one file per partition, so it compacts as it goes (3,488 to 3,252 files) |
| c. one tick's offset saves | 16 files (one per owned partition, each an fsynced temp file and a rename), 4.3 ms (4.4 / 4.2 / 4.3). The run's ticks moved 16 partitions each on average | fine at this size: 0.1% of a batch; the plan's 2.7 ms was the right order |
| d. `landing.read` over one bulk commit at `limit` slices | commit 50 (13,000 rows; worker 0's share 3,243): 4 slices of 1,000 at 31 / 25 / 24 / 15 ms, 95 ms in all, reading that commit alone. With the worker's default `max_versions=100` the same slices re-read commits 50 to 102 through the change feed: 211 / 139 / 132 / 133 ms, 615 ms | fine at this size: 15 to 31 ms per slice on one commit, 0.13 to 0.21 s per slice 53 commits behind, 4 to 6% of a batch. The planned fix is not needed for the SLO |
| e. `CommitFailedError` between workers | 0 `tick_failed` events of any kind against 1,006 `batch` events (the plan's 3c figure was 1 in 412); 0 `append_retry` events on the API side | fine at this size: four workers committing about 1,200 Outcomes/s into one table for 14 minutes hit no conflict; the loop's retry stays as the safety net |
| f. `catalog.metrics --since 24h` over the run's events | 17.7 s (17.8 / 17.7 / 17.6) over 2,002,056 events, 2.002 events per Change; 24 h at 50/s is 4.32M Changes, 8.65M events, 76 s scaled linearly (60 s from the 14.0 s reading right after the run) | **a new plan row**: over the spec's 60 s line. The fix the spec names, loading the events straight into DuckDB instead of `events.read` and three JSON parses, as its own step. A 24 h window would also hold 8.6M events as Python dicts plus an Arrow copy; this run's 2M took the Mac to 11.8 GB of swap, which the DuckDB load fixes too |

Methods, in brief. (a) `store.read(dt, keys)` as shipped against the same partition-pruned dataset read for `merchant_product_id` only, `is_in`, then `Dataset.take`; equality of the rows checked in exploration. (b) one MERGE on the real store, no copy (disk was tight, and 7d.2b reclassifies the store anyway): each key rewritten with its own stored version, content and classification, so the replay fingerprints are unchanged and only `updated_at` and the file layout moved; files counted from the add and remove actions of log entry 1011. (c) `state.save_offsets()` into a temporary state directory with all 16 partitions moved, the real Landing log's table id. (d) `landing.read` from `(50, 0)` on every owned partition, `limit=1000`, looped until every offset passed commit 50. (e) every JSONL line under the run's events hour, counted by type. (f) `python -m catalog.metrics --since 24h` as a subprocess, wall time.

State after this step: Listing Store at version 1011 (the measurement MERGE: 3,252 live files, 1,000,000 rows, 0 tombstones), Landing log untouched at version 102, `state/` untouched (offsets went to a temp dir), `data/` 2.0 GB, 9.0 GiB free.

## 7d.2b, after 6m.2

The runs on the shipped classifier wait for the student in the pipeline ([step 6m.2](specs/step-6m.md)). They are the amended points 3 and 4 of 7d.2:

- **The SLO run**: the shipped `Procfile` with `--classifier student`, the `backfill` line kept, on top of this 1M store. The Backfill reclassifies the 1M `fake`-versioned Listings onto the student's version, for nothing, and its time to leave no row on the old version is itself a measurement. Then 30 minutes of `catalog.load --rate 50 --keys 1000000` and `catalog.metrics --since 35m`. Pass: p99 freshness under 300 s. Recorded: every `metrics` field, `classify_ms`, the Backfill's time.
- **The 10k bulk**: after a reset, one 10k batch on the same `Procfile`: freshness of its Changes, and a worker's full 1,000-Change batch time (the `batch` and `classify` events) against the 60 s heartbeat.

This worktree's 1M store (its `data/` and `state/`, at version 1011 after the measurement MERGE) is kept for the SLO run. No key is needed for either run.

## The Phase 7 exit

Open. The exit (every scenario ends with the three oracles passing, and p99 freshness under 5 minutes at 50/s) is measured on the student, in 7d.2b. What 7d.2a settles: the 1M load lands and drains cleanly at 24 times the target rate with no stale, conflict or failed Outcomes; five of the six deferred costs are fine at this size and need no fix; one, the 24 h metrics window, gets a plan row (events straight into DuckDB). The chaos scenarios pass at low rate on every PR (`stress-smoke`, 7d.1).
