# Stress report

The full runs of [step 7d.2](specs/step-7d.md) on one Mac. Part 7d.2a (Oct 7): the 1M initial load on the `fake` classifier and the six deferred costs on that store. Part 7d.2b (Oct 7, after step 6m.2): the SLO run and the 10k bulk on the student; the exit stays open, blocked by the disk. Step 7f.2 (Oct 9 and 10): the rerun on the T9 after 7f.1; the exit passes, with the rate this Mac reaches recorded. Commands follow [the runbook](runbook.md); terms follow [GLOSSARY.md](../GLOSSARY.md).

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

## 7d.2b, point 3: the SLO run on the student

The shipped `Procfile` with `--classifier student` (the fp32 encoder and the 41k-row index in `models/`), on top of the 1M store from 7d.2a (version 1011), no key in the environment. Three runs, because the first two filled the disk.

**Run 1, the Backfill kept** (19:47 to 19:55 UTC, 7.2 minutes; the load at 50/s from 19:50). The Backfill appended 1,000 reclassifies a tick; the gaps between ticks were 24 s for the first five, then 50 to 89 s once the workers lagged (it never lets more than 1,000 reclassifies sit ahead of Merchant Changes). 9,036 `reclassified` Outcomes in 6.9 minutes, 1,307 a minute. **The time to leave no row on the old version was not measured**: the run stopped for disk at 1.5% done. By extrapolation, about 12.7 hours at that rate under the 50/s load, or about 6.7 hours at the unloaded cadence (a tick every 24 s, 2,500 a minute); the watcher's own count of live rows still on the old version fell 2,400 a minute, which includes the rows the load's updates moved. Either way the bound is the worker's time over a 1,000-Change batch (16.6 to 39.3 s with the other three workers busy, see point 4) and the Backfill's scan of the store, not the classifier. Meanwhile, over the window's 9,743 Merchant Changes (`metrics --since 15m`): freshness p50 13.8 s, p99 57.8 s; 1 stale, 0 conflict, 0 failed; `classify_ms` p50 1,102 ms, p99 28.9 s over 158 calls of 119 Listings on average; lag up to 54 commits on one worker's partitions while it held a 1,000-row batch; `append_retries` 0; refused 0; utilization 0.33 to 0.35. Stopped at 7 minutes: the store had grown from 1.2 to 3.6 GB on disk and free space had fallen from 18 to 8.7 GiB, towards the API's 5 GiB guard.

| Reclaim, with nothing running | |
|---|---|
| `DeltaTable.vacuum(retention_hours=0, enforce_retention_duration=False)` | 16,858 files removed; the store 3,583 MB to 755 MB on disk; 18 GiB free |
| Live data | 126 MB in 1,265 files (the generator's texts repeat, so Parquet compresses them hard); the rest of the 755 MB is the Delta log and its checkpoints |

**Run 2, the `backfill` line removed** (19:57 to 20:28 UTC, 31 minutes at 50/s, as the Oct 5 design had it). Void as the SLO run: counted from the events, the API accepted 63,693 of the 90,000 Changes and refused 26,307 (29%) for `low_disk`, logging 77,397 `503` refusals with the generator's retries (the generator's own summary, which counts requests, says 46,050 accepted and 42,271 refused). The spec's failure table calls a run that trips the guard void.

| | |
|---|---|
| The store on disk | 755 MB at the start, 10.9 GB after 8 minutes; free space 18.6 GiB to 5.1 GiB at 20:05, when the refusals began |
| MERGE commits in the 31 minutes | 4,775 (155 a minute, p50 9 Changes each): each removed p50 7 files / 10.8 MB (p90 20 MB, max 25 MB) and added the same, copying about 78,000 rows to update 7; 55 GB written and left dead |
| Why they stay | `LOG_RETENTION_HOURS = 1`: maintenance vacuums nothing younger than an hour, and the 7d.2a MERGE measurement (cost b) showed each MERGE coalescing a partition's files into one, so every later MERGE on that partition rewrites all of it |
| Vacuums during the run | maintenance's 4 (`VACUUM END`) and 11 by hand at retention 0 from a watcher under 7 GiB free (each reported a commit conflict with the writers but freed 5 to 10 GB), after which the API accepted again, so the run alternated between accepting and refusing |
| Freshness over the 63,693 Changes run 2 accepted, recounted from the events (`accepted` to the first Outcome) | p50 1.8 s, **p99 50.5 s**, max 60.2 s |
| `metrics --since 35m` (80,099 Changes: its window also spans the end of run 1) | freshness p50 2.8 s, p99 51.7 s; stale 0.6% (the generator's retries after a `503` land behind a newer `source_version`), conflict 0, failed 0; `classify_ms` p50 221 ms over calls of about 14 Listings, p99 4.9 s; utilization 0.69; lag 0 commits on every partition; 2 `tick_failed` (`CommitFailedError`, retried); Uncategorized 0 |
| Request latency at the API | p50 97 ms, p99 1,372 ms, max 7.9 s |

**Verdict: a new plan row (7f).** Over the Changes the API accepted, freshness stayed under the line (p99 50.5 s against 300 s), but a run that refused 29% of its Changes is not the SLO result. At 50/s on 1M rows the MERGE churn under the one-hour vacuum floor needs about 100 GB of headroom an hour, and this Mac's guard trips in 8 minutes: the disk-guard scenario, uninvited. 7d.2a's cost (b) measured one MERGE's time and file share and called it fine; its disk side at 50/s is what breaks. Run 3 is the 10k bulk below, after the reset the spec asks for.

## 7d.2b, point 4: the 10k bulk on the student

After a reset (`rm -rf data/landing_log data/listing_store data/events data/export data/snapshots state`), the shipped `Procfile` (`--classifier student`, the `backfill` line back): one `catalog.load --changes 10000 --batch 10000 --rate 0 --processes 1`.

| | |
|---|---|
| The batch | accepted in one request, 249 ms; 10,000 `202` |
| Settled | 94 s from the request to the 10,000th Outcome; freshness p50 71.2 s, p99 92.6 s; 0 stale, conflict or failed; Uncategorized 0 |
| A worker's full 1,000-Change batch | 34.2 to 37.7 s (`batch`), of which `classify` 34.0 to 37.3 s, with all four workers embedding at once; the half batches at the end 19 s. Against the 60 s heartbeat: under, at 57 to 63% of it |
| One worker alone, measured by hand with nothing else running | 1,000 Listings in 9.0 s (9.0 ms a Listing), 256 in 2.4 s |
| The machine's throughput | the four together classify 4,000 Listings in 35 s: about 110 a second, the same work shared over the 10 cores (each ONNX session uses them all). A 1M bulk is about 2.5 hours of classification (Jev: 3.5 hours at 80 calls/s and about $62); the 50/s SLO load takes about 45% of it |
| Disk | 20 GiB free before, 19 after |

**Verdict: fine at this size.** The batch stays under the heartbeat with four workers. The time is the machine's, not the classifier's: 9 s of embedding stretched to 35 s because four ONNX sessions share the ten cores. `student.py`'s note names the knob if that ever matters, fastembed's `threads` per worker.

## 7f.2: the SLO run after 7f.1, on the T9

Step 7f.1 shipped (1 MiB files; maintenance's minute-level sweep of the store's dead files; the Snapshots pin; `--unique-text`). The amended point 3 was rerun with the tables on the T9 external drive (APFS over USB, 797 GiB free), a fresh 1M load with `--unique-text`, the shipped `Procfile` on the student with the `backfill` line kept, and no vacuum by hand. Three runs on Oct 9 and a fourth on Oct 10, after the drive's USB link was found running at USB 2.0 speed.

**The 1M load** (`fake`, 16 generator processes, unique text): accepted in 84 s (11,900 Changes/s; the T9's per-request latency p50 0.8 s, p99 14 s), drained in 811 s; every Change `written`, 0 stale, conflict or failed; freshness p50 351 s, p99 780 s (the queue); 1,002 classify calls of 1,000 (`fake`: p50 1 ms, p99 3 ms); `append_retries` 0; lag 0 at the end; utilization 0.24 a worker. The store landed at 746 MB in 3,489 files of about 1 MiB, the feed at 493 MB: unique text compresses to about 0.75 KB a row, six times 7d.2b's repeating text. The drain time and the batch times of this whole section are on a USB drive and under the memory pressure described below, and are not comparable with 7d.2a's.

**Run 1, everything on the T9, 16 generator processes** (06:53 to 10:14 UTC). The generator reached 7.5 Changes a second, not 50: its requests took p50 0.96 s, p99 13 s, max 30 s, and it fell 2.8 hours behind its schedule. All 90,000 Changes were accepted (36 generator timeouts, retried), none refused. Over all of them, freshness p50 6.8 s, **p99 62.4 s**, max 112 s; 89,439 `written`, 561 `stale` (retries behind a newer source version), 0 conflict or failed; `append_retries` 0; lag 0; Uncategorized 0; utilization 0.88 to 0.92 a worker (7d.2b's run 2 had 0.69 at 50 a second: the Backfill's 1,000-Change batches and the thrash, see run 3). The `metrics --since 35m` window at the end: 13,293 Changes, p50 5.3 s, p99 48.2 s.

| The churn, held | |
|---|---|
| Sweeps | 120 `store_vacuum` events: 114,422 files, 82.8 GB removed over 3.3 hours (about 420 MB a minute, the whole churn) |
| The store on disk | 2 to 3 GB most of the time (three minutes of churn), 5 to 11 GB when the sweep lagged |
| Free space on the T9 | 795 GiB before, 784 GiB after: the feed grew 493 MB to 1.1 GB, the Landing log 397 to 787 MB, events to 497 MB, and the store's own dead files held up to 11 GB while the sweep lagged; no `low_disk` refusal |
| Bytes written to the store | about 84 GB in 3.3 hours, derived: the 82.8 GB the sweep removed (every file a MERGE rewrote) plus the live 0.75 GB and the feed's 0.6 GB; about 420 MB a minute at 7.5 Changes a second, the known cost |
| A sweep pass | p50 18 s, p90 65 s, max 337 s; gaps between sweeps p50 61 s, p90 178 s, max 956 s: an unlink on the USB drive costs 3 to 35 ms under the write load, and the 10-minute maintenance pass (p50 54 s, max 150 s) shares the thread |
| The hourly delta-rs vacuum | removed 421,727 files the lagging sweep had not: the safety net worked |
| **Maintenance restarts: 165.** | The hourly vacuum's own `VACUUM END` commit conflicts with a MERGE that lands during its deletion (`CommitFailedError: a concurrent transaction deleted data this operation read`), after the files are gone. Every restart began a pass, found an hour of dead files, deleted them and crashed at the commit. **Fixed in this step**: the pass reports `vacuum_commit_failed` and goes on, as the sweep already did |
| Workers | 8,916 batches (44 a minute), p50 5 Changes, ms p50 3.1 s, p99 32.5 s; the 51 full 1,000-Change batches (the Backfill's) p50 29.4 s, max 59.6 s against the 60 s heartbeat; `classify_ms` p50 92 ms, p99 14.9 s. **111 `tick_failed`** with the same commit-conflict message, retried; worker-0 gave up five times (five failed ticks in a row) and was restarted, worker-2 once; no Outcome was lost. 7d.2a saw 0 in 1,006 batches, 7d.2b 2 in 4,848; the log of the window is cleaned, so which concurrent commit each MERGE lost to is not recorded; an open question for the rerun, whose 30-minute log survives |
| The Backfill | 244,000 reclassified in 201 minutes, 1,213 a minute, 679,739 rows still on `fake`'s version at the stop: the 1M would take about 14 hours, as 7d.2b extrapolated |

**Run 2, the Landing log, events and state on the internal SSD, the store on the T9, 16 processes** (10:23 UTC, 12.5 minutes): the generator held 50.0 Changes a second for its first six minutes, then the API refused: swap, which lives on the internal volume, grew from 10 to 13 GB and took the volume from 9.5 GiB free to 3.8, under the 5 GiB guard. 17,904 Changes accepted, 19,614 refusals, stopped. The `metrics --since 35m` window at its stop (22,917 Changes, spanning run 1's tail): freshness p50 131 s, **p99 337 s, over the line**, 0 stale, conflict or failed, utilization 0.60 to 0.63.

**Run 3, everything on the T9, 8 processes** (10:40 UTC, 20 minutes): 4.7 Changes a second accepted, no refusal. The catalog's processes held 2.4 to 3.1 GB resident, swap 12.5 to 12.9 GB of 14 (the desktop and the other sessions hold about 10.6 GB of it with the catalog stopped), and the watcher's own one-minute samples drifted to eight minutes apart: the Mac was thrashing. Its 35-minute window (15,387 Changes, spanning run 2's tail): freshness p50 170 s, p99 339 s, utilization 0.67 to 0.72. Those two p99s are what thrash does to a 50 a second queue, not a measurement of the SLO.

**The drive was the limit (corrected Oct 10).** The idle append above hid it: the T9 had negotiated a USB 2.0 link (480 Mb/s; 34 MB/s sequential writes) and Spotlight was indexing it. A rerun of run 3 on Oct 9 at 22:31 UTC, with swap at 2 to 6 GB, still took 5.4 Changes a second: under the workers' and the sweep's writes an fsync on the T9 took p50 48 ms, max 503 ms, the Landing log's group commits came 0.42 s apart (p50; p90 2 s), and the generator's 8 processes each block on their POST, so the rate was 8 over the request latency. That run was cancelled after 41,911 Changes. On a 10 Gb/s port with Spotlight off the T9 writes 1,000 MB/s and fsyncs in 0.14 ms idle. The memory thrash above was real, but runs 1 and 3 were most likely held to their rate by the link first.

**Run 4, everything on the T9 at 10 Gb/s, 8 processes** (Oct 10, 03:27 to 04:37 UTC, run 3's script unchanged, `main` at #112): all 90,000 Changes accepted (`202`), none refused; the generator reached **21.3 Changes a second**, its requests p50 334 ms, p99 1.1 s, max 10.7 s. Over all 90,000 (`metrics --since 75m`): freshness p50 1.3 s, **p99 24.2 s**; 0 stale, conflict or failed; lag 0 in the run's own window (`metrics --since 35m` before the stop: 38,449 Changes, p99 24.7 s); utilization 0.77 to 0.78 a worker. The CPU set the rate: load average about 17 on 10 cores, each of the four student workers at 60 to 90% of a core beside the Backfill. Swap 2.4 to 6.3 GB.

| Run 4 | |
|---|---|
| The churn | 70 sweeps removed 100,216 files, 87.6 GB in 71 minutes (about 1.2 GB a minute), 0 errors; the store 5.5 to 12.8 GB on disk; the T9 789 to 782 GiB free, the internal volume 15 to 13 GiB |
| The hourly delta-rs vacuum | 8 `vacuum_commit_failed`, reported and carried on (the fix above); while it runs the sweep waits, about 5 minutes once an hour |
| Workers | 13,318 batches, ms p50 641, p99 19.2 s, max 29.1 s (912 Changes) against the 60 s heartbeat; **94 `tick_failed`** (`CommitFailedError`, the same message), six worker restarts, no Outcome lost. The store's log for the run is kept under `.stress/7f2/` for the conflict question |
| The Backfill | 141,000 reclassified in 71 minutes, about 2,000 a minute |

**The workers' conflicts are not the vacuum's doing.** The review of this report raced three MERGE processes against a vacuum at retention 0 for 40 s (3,100 MERGE commits, 0 failures; the vacuum lost 2,501 of 2,600 of its own commits) and against another partition's compaction (3,400 MERGEs, 2,570 compactions, 0 conflicts). What remains untested is a worker against its own compaction on the same partition, and run 4's log, kept under `.stress/7f2/`, holds the answer. Run 1's 165 maintenance restarts were also the case the sweep's `ponytail:` note names: each restart reread the hour of log files the cleanup had left before its first sweep.

**Verdict (Oct 9, before run 4).** The disk churn is solved: the sweep removed 83 GB over three hours at the rate the MERGEs produced it, with free space moving only by the feed, the Landing log and the sweep's lag, no refusal and no error, on a drive 40 times slower per unlink than the internal one. The SLO at 50 Changes a second is still unmeasured, now for memory: 16 GB of RAM with about 11 GB of swap already held by the desktop and the other sessions cannot run four workers with the student, the Backfill, the API and the generator at that rate. Costs recorded: the rewrite churn at about 420 MB a minute (the known cost, decision Oct 8); the feed at about 190 MB an hour at this rate (7 days: about 32 GB); the sweep's pass time on USB. Open: the workers' commit conflicts at 1.2% of ticks.

**Verdict (Oct 10, run 4).** At 21 Changes a second for 70 minutes the system held every line the exit names: no refusal, freshness p99 24 s against 300 s over every accepted Change, no stale, conflict or failed Outcome, lag 0, the churn swept at 1.2 GB a minute with no error. The 50 a second the run asked for is past this Mac's CPU with four fp32 student workers and the Backfill running; that is the machine, not a defect the run found. Open: the workers' commit conflicts (94 in 13,318 batches, 0.7%).

## The Phase 7 exit

**Passed, Oct 10, with the rate recorded.** Run 4 of 7f.2 ran the SLO for 70 minutes on the 1M store and the student: 90,000 Changes accepted and none refused, freshness p99 24.2 s over all of them, 0 stale, conflict or failed, lag 0, the store's churn swept as it came. The generator reached 21.3 Changes a second, not 50: this Mac's 10 cores are the ceiling with four student workers and the Backfill; the decision (Oct 10) is that the rate is the hardware's, and a run at 50 a second belongs on a larger machine when one is the target. Carried forward: the workers' commit conflicts (0.7% of batches, retried, no Outcome lost), with run 4's log kept to name the conflicting commit. The chaos scenarios pass at low rate on every PR (`stress-smoke`, 7d.1). What 7d.2a settled stands.

State after this step: the 1M store (about 250k rows on the student's version, the rest on `fake`'s), its Landing log, events and state are on the T9 under `shopping-stress/`, reached from the stress worktree through `data` and `state` symlinks; `models/` is the main checkout's. Logs and the raw numbers are under `.stress/7f2/`, untracked.
