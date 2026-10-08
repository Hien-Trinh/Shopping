# Step 5a: Change Export (mini PRD)

Status: approved Oct 3. Confirmed: adopting a file on crash recovery (decision 1), no file for an empty range (decision 2), zero-padded names (decision 3), the 1,000-version cap (decision 4), restarts by the supervisor (decision 5), and the test points. Plan row: [plan-v1.md, PR steps, 5a](../plan-v1.md), and Phase 5's tests "the export oracle" and "re-running after a crash between writing the file and the watermark produces identical output". Design: the [Change Export row and lifecycle step 9](../design-commerce-ingestion-pipeline.md), A4, A9 and A13 in [plan-v1.md](../plan-v1.md), and [ADR-0002](../adr/0002-local-first-delta-no-queue.md) ("its watermark is the last exported table version, advanced only after the export file is written"). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

The Listing Store is written by the workers, but nothing sends its changes on toward serving. The design's last step (lifecycle step 9) and its freshness goal (a change exported within 5 minutes at p99) have no code. The pure half already exists: `collapse` turns a change feed into one row per Listing key, and `replay.replay_exports` and `replay.live` are the export oracle's two sides. What's missing is the shell: reading the change feed since a watermark, writing the file, advancing the watermark, and a process the supervisor runs.

## Solution

1. `catalog/export.py`:
   - `tick(store_dt, export_dir, state_dir, events, max_versions)`: reads the Listing Store change feed from the watermark + 1 up to the current version (at most `max_versions` versions), collapses it, writes `data/export/<v1>-<v2>.parquet` atomically, then saves the watermark. Returns whether it is still behind.
   - `files(export_dir)`: the export files in version order, for the oracle and any consumer.
   - `run` and `main`: the loop (a tick every 60 s, or at once while behind), an exclusive lock, signals and the supervisor watch, as the worker does.
2. `state.py`: the watermark file (`state/export_watermark.json`, `{table, version}`) and a lock on `state/locks/export.lock`.
3. `Procfile`: an `export: python -m catalog.export` line, so the supervisor runs and restarts it.

## User stories

1. As a serving consumer, I read the export files in order, upsert each row by Listing key and drop each `op=delete` key, and I end up with exactly the live Listings in the Listing Store at the watermark (the export oracle).
2. As a serving consumer, each file holds at most one row per Listing key, the Listing's latest state in that version range, with every Listing Store column plus `op`.
3. As the operator, a change is exported within about a minute of its MERGE, so the 5-minute p99 freshness goal has room for the Landing log and the workers.
4. As the operator, a crash or `kill -9` at any point neither loses a change nor writes a second, overlapping file for the same versions.
5. As the operator, an idle store, or one that only compacted, writes no files.
6. As the operator, I can see each export in the events (`export`: version range, rows, deletes, ms, head) to measure export lag.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| First run, empty state | Watermark starts at -1; the first file starts at version 0 |
| Merges, a delete, a reclassify, then a tick | One file `<w+1>-<head>`; rows collapsed per key (a reclassify exports as an upsert, a Tombstone as `op=delete`); the export oracle holds at the watermark |
| No new versions | No file, no event, watermark unchanged |
| Only compaction commits in range | No file (they carry no change-feed rows); watermark advances past them |
| More than `max_versions` versions behind | A file per `max_versions` versions; `tick` reports it is behind and `run` ticks again at once |
| Crash after the file is written, before the watermark is saved | Next tick finds the file starting at watermark + 1, adopts its end as the watermark and writes nothing new for those versions: the same files as an uninterrupted run |
| Crash while writing the file | Only a hidden temp file exists; the next tick rewrites the same range. Its temp file is ignored by `files()` |
| Watermark saved against another Listing Store (recreated since) | `OffsetsMismatch` at startup, as for a worker's offsets |
| Watermark file torn or not JSON | `CorruptState`, as for offsets |
| A second exporter on the same state directory | `PartitionTaken`-style refusal from the lock; it exits before reading anything |
| Version ≥ 10 | Files still order by version: names are zero-padded, so a plain sort (or DuckDB's `read_parquet('export/*.parquet')`) is version order |
| Any error reading or writing (disk full, I/O) | The process exits 1 without moving the watermark; the supervisor restarts it |
| SIGTERM, or the supervisor dies (`kill -9`) | Stops after the current tick, as a worker does |
| Watermark older than what VACUUM kept | Out of scope: gap recovery is 5b. Until then the read fails and the exporter crash-loops visibly |

## Implementation decisions

1. **A crash between file and watermark is healed by adopting the file.** Each tick first looks for `<w+1>-*.parquet`; if one exists, its end becomes the watermark. The alternative, re-exporting from the watermark, would write an overlapping file whose end differs (the head moved), and consumers would replay those versions twice. Adopting is safe because a file only appears complete (written to a temp file, fsynced, then `os.replace`d).
2. **No file for an empty range.** An idle store would otherwise write 1,440 empty files a day. The watermark still advances, so compaction commits are passed once.
3. **File names are zero-padded to 12 digits** (`000000000041-000000000057.parquet`), so name order is version order. The design says `<v1>-<v2>.parquet`; this keeps that and only fixes the sort.
4. **At most `max_versions` versions per file (default 1,000).** Steady state at 50 changes/s is up to about 1,200 MERGEs a minute, so one or two files a tick. The cap keeps a far-behind exporter making progress rather than loading the whole history. Ceiling: `collapse` runs in Python, so a 1M initial load (about 1,000 rows per version) still puts about 1M rows in memory per file. `ponytail:` comment; measure in Phase 7 and, if it matters, cap by rows or collapse in Arrow.
5. **Errors exit the process; the supervisor restarts it.** No retry loop of its own: the watermark only moves after a file is written, so a restart is always safe. The supervisor treats only workers' exit codes as fatal and watches only workers' heartbeats, and that stays unchanged: a stuck or crash-looping exporter shows up as export lag, not as a stopped system.
6. **One exporter per state directory**, held by `flock` on `state/locks/export.lock`. Two exporters with different heads would interleave overlapping files out of order and break the oracle. `state.claim`'s lock code is shared rather than copied.
7. **The watermark is the last exported Listing Store version plus the store's table id**, saved with `state.save` like the offsets, and checked against the store the same way (`OffsetsMismatch`).
8. **Every Listing Store column is exported, plus `op`**, which is what `collapse` already returns. Consumers get `updated_at` and the classification fields without a second read.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`export.tick`** (new, red first, `tests/integration/test_export.py`, real Delta in `tmp_path`, Listing Store writes through `store.merge`): the export oracle (`replay.diff(replay.live(store.fingerprints(dt, w)), replay.replay_exports(export.files(dir)))`) after upserts, a delete, a reclassify and a compaction; no file when idle or after compaction only; the `max_versions` split; the crash between file and watermark (restore the old watermark, merge more, tick again: the files match an uninterrupted run's); a leftover temp file ignored; a recreated store refused; ordering past version 9.
  2. **`export.run`** (new, red first): a second exporter on a held lock is refused; `stop` ends the loop.
  3. **The end-to-end test** (changed, `tests/integration/test_e2e.py`): the shipped `Procfile` now starts the exporter too, run with a short interval flag; after the workers finish, it waits for the watermark to reach the Listing Store's version and asserts the export oracle. That covers the `Procfile` line, `main` and the default directories.
- **No sleeps tuned to the machine.** `tick` is driven directly; only the end-to-end test waits, on the watermark, with a timeout.
- **Coverage:** `export.py` and `state.py` stay under the 90% package gate. `collapse` and `replay` keep their 100%.

## Out of scope

- Gap recovery when the watermark is older than what VACUUM kept (5b, A18).
- Deleting export files after 3 days (A13): moved to 5d with the other retention, so one step owns all pruning.
- A heartbeat watchdog for the exporter (B1's hang inside a native read).
- Serving, or any consumer beyond the oracle.
- Measuring freshness and export lag under load (Phase 7 metrics).
- Power loss: as everywhere, process crashes only (ADR-0002).

## Size

About 80 lines of production code (about 65 in `export.py`, 10 in `state.py`, 1 in the `Procfile`) and about 150 of tests, so under 300.
