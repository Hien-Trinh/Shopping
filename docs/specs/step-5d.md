# Step 5d: Retention and maintenance (mini PRD)

Status: approved Oct 3. Confirmed: the history guard (decision 1), refreshing before each append (decision 3), the Listing Store's vacuum (decision 5), the two-PR split (Size), and the test points. Plan row: [plan-v1.md, PR steps, 5d](../plan-v1.md), and Phase 5's tests "Landing log retention and compaction don't break worker reads" and "retention never deletes or vacuums past the slowest partition's offset". Design: the [Landing log and Change Export rows, and the retention line](../design-commerce-ingestion-pipeline.md), A4, A13, A18 and B2 in [plan-v1.md](../plan-v1.md), and [ADR-0002](../adr/0002-local-first-delta-no-queue.md) ("Retention is capped by one laptop disk"). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Nothing is ever deleted. The Landing log, the events and the export files grow without limit, and so does the Delta history of both tables: every replaced data file and every commit's log file stays on disk. The design keeps 7 days of Landing log and 3 days of events and export files, but no code does it, and the API's disk guard (503 below 5 GB) would eventually refuse every write.

Cleaning up has a catch: a reader that is behind still needs the old history. A spike on delta-rs 1.6.6 showed that vacuuming after a compaction makes the change feed unreadable for versions a worker hasn't read yet ("Object at location … not found"). Cleanup must never pass the slowest reader.

## Solution

1. `catalog/maintenance.py`, a new process run by the supervisor:
   - `tick(landing_dt, store_dt, data, state_dir, events, now)`: one cleanup pass over both tables and the two file directories, in an order that frees disk before it writes anything (decision 6). Emits one `maintenance` event.
   - `run` and `main`: a tick every 10 minutes, an exclusive lock, signals and the supervisor watch, as the exporter does.
2. `landing.append` refreshes its table before each commit and retries once on a commit conflict (decision 3).
3. `delta.ensure` creates tables with Delta's own log cleanup off and a 1-hour log retention, so only `maintenance` removes log files, and only behind the guard (decision 4).
4. `events.prune` and `export.prune`, and the API answering 404 before any read for a Submission id dated before the events horizon.
5. `state.py`: a lock on `state/locks/maintenance.lock`. `Procfile`: `maintenance: python -m catalog.maintenance`.

## User stories

1. As the operator, the disk holds about 7 days of Landing log and 3 days of events and export files, without my doing anything.
2. As the operator, a stopped or slow worker or exporter never loses a change to cleanup: whatever it hasn't read yet stays readable until it has.
3. As a Merchant, sending changes while cleanup runs never fails because of it.
4. As the operator, each pass logs what it removed and what it held back, and for which reader, so I can see a stuck reader holding the disk.
5. As the operator, a forged Submission id dated years ago answers 404 at once instead of scanning every event hour.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| All workers caught up, rows older than 7 days | Deleted; worker reads and new appends unaffected |
| One partition's offset older than the cutoff (worker stopped) | No rows deleted, no vacuum past its version, no log cleanup past it; the event names the slowest partition. Once it catches up, the next pass cleans |
| A partition with no offset file yet | Counts as version 0: nothing older than the table's creation is unread, so cleanup proceeds only if the table is younger than the cutoff |
| Exporter behind (watermark old) | The Listing Store's vacuum keeps every file its unread versions need; its change feed still reads |
| No export yet (watermark -1) | Treated as version 0, as above |
| The slowest reader's version has no log left (state reset against old data) | Every guarded step is skipped and the event says so. Recovering that reader is 5e |
| An API append on a handle opened before a DELETE | Succeeds (today it fails every time after: "a concurrent transaction deleted data", seen in the spike) |
| A DELETE commits between the API's refresh and its commit | The append retries once and succeeds |
| `_change_data` files of read versions, older than the retention | Deleted; the change feed from the slowest reader's version still reads |
| `_change_data` files of an unread version | Kept, by the same retention as the vacuum |
| A Catalog Snapshot copy running during a vacuum (5c) | Its pinned version's files are replaced only after it pinned, and the vacuum keeps replaced files at least 1 hour, so the copy reads them |
| Compaction while the API appends | Both succeed (spike B2) |
| A Submission id dated before the events horizon | 404 `not found`, logged as `expired_submission`, before any event file is opened |
| An id dated just inside the horizon | Its hour is still on disk: the hour pruned is only one whose last second is past the horizon |
| Export files older than 3 days | Deleted, but only files that end at or below the watermark. An un-adopted file (crash before the watermark) stays |
| A second `maintenance` on the same state directory | Refused by the lock; exits before touching anything |
| Disk full | File pruning and vacuums run first and only delete; compaction or the DELETE then fails, the process exits 1 and the supervisor restarts it |
| `kill -9` mid-pass | Each Delta step is one atomic commit; an uncommitted file is untracked and a later vacuum removes it |
| Watermark or offset saved against another table | `OffsetsMismatch`, as for the worker and the exporter |
| SIGTERM, or the supervisor dies | Stops after the current tick, as the exporter does |

## Implementation decisions

1. **The history guard.** For each table, the slowest reader's next version `n` is `min(offset version)` over all 64 partitions for the Landing log, and `watermark + 1` for the Listing Store. Cleanup may drop history older than time `t` only if `n` is past the head (nothing unread) or `n`'s commit is at or after `t` (read with `DeltaTable(path, version=n).history(1)`, checked in the spike). Commits are in time order, so everything older than `t` has been read. If `n`'s log is gone, the guard fails closed.
2. **What runs, per table:**
   - **Landing log:** compaction every tick (safe beside appends, spike B2). Rows with `received_at` before `now - 7 days - 1 hour` are deleted if the guard holds at `now - 7 days`. The hour covers the gap between a request's `received_at` and its commit (`ponytail:` comment: a commit stuck longer than an hour could lose rows; B1's hang was 5 minutes). Workers read only `insert` rows, so the DELETE's change-feed rows are ignored (checked in the spike).
   - **Both tables:** vacuum with `retention_hours = max(1, ceil(the slowest reader's lag in hours))`, so it never removes a file an unread version needs and never blocks. 1 hour is the minimum because delta-rs takes whole hours and also removes untracked files older than the retention, which could be a commit still being written.
   - **Both tables:** files under `_change_data/` older than the vacuum's retention plus 1 hour are deleted by `maintenance` itself. delta-rs's vacuum skips paths starting with `_`, so the change-feed files of every MERGE and DELETE would otherwise stay forever (5b's side finding, confirmed: all 4 survived a zero-retention vacuum). A file is written before its commit, so its modification time is at or before that commit's; the extra hour keeps the file of the slowest reader's own version.
   - **Both tables:** `cleanup_metadata()` (log files older than the 1-hour log retention) if the guard holds at `now - 1 hour`.
3. **`landing.append` refreshes before each commit and retries once on a commit conflict.** In the spike, an appender that never refreshed failed 119 of 136 appends after one DELETE, and every later one, because it kept committing on a snapshot older than the DELETE. With a refresh first, 117 appends beside 5 DELETEs had no errors. The retry covers a DELETE landing between the refresh and the commit. Without this, the API would 500 every submission after the first retention pass.
4. **Log retention is set when a table is created:** `delta.enableExpiredLogCleanup = false` and `delta.logRetentionDuration = interval 1 hours`, checked in a spike. Delta's own cleanup is time-based and would pass a slow reader. The 1 hour bounds log files on disk (at 10 commits/s, about 36k files). Tables created before this step keep Delta's defaults: reset `data/` and `state/` (local data only).
5. **The Listing Store gets a vacuum and log cleanup too, guarded by the export watermark.** No step had them, but A18 and 5b assume a vacuum exists, and the store's replaced files are its largest growth (see Risks). The Listing Store keeps tombstones forever (A17), so no rows are deleted.
6. **Order within a tick: delete files, then write.** Export and event pruning, both vacuums and both log cleanups only delete, so they run first and free disk even when it's full. Landing log compaction and the DELETE write new files, so they run last.
7. **Events and export files are pruned by age, with the retention as a shared constant.** `events.RETENTION = 3 days`: an hour directory goes once its last second is older than that, and the API answers 404 for an id older than that, so the two can't disagree. Export files go by modification time once older than 3 days, and only if they end at or below the watermark.
8. **One `maintenance` process, one per state directory**, with errors exiting the process for the supervisor to restart, as the exporter does. Two would race their DELETEs and vacuums. `state`'s lock code is shared.
9. **Retentions are constants, not flags.** The API and `maintenance` must agree on the events horizon, and nothing yet needs to change them. `tick` takes `now`, and the vacuum's minimum hours, for the tests.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`maintenance.tick`** (new, red first, `tests/integration/test_maintenance.py`, real Delta in `tmp_path`, Landing log writes through `landing.append`, offsets through `state.save_offsets`): rows older than the cutoff deleted when every partition is caught up (with `now` 8 days ahead), and kept while one partition is behind; after compaction and a vacuum (minimum 0 hours) with a partition behind, `landing.read` from its offset still returns its rows, and the same test without the guard reproduces the spike's failure; `_change_data` files deleted when caught up and kept for a reader behind, with its change feed still reading; the Listing Store's change feed still reads from the watermark after a vacuum; log cleanup runs when caught up and is skipped when behind (table property lowered in the test); the guard failing closed when the slowest version's log is gone; export files pruned only at or below the watermark; event hours pruned by `now`.
  2. **`landing.append`** (changed, red first, `tests/integration/test_landing.py`): an append on a handle opened before another handle's DELETE succeeds.
  3. **The API lookup** (changed, red first, `tests/integration/test_api.py`): an id dated before `now - events.RETENTION` gets 404 `expired_submission` with no event file opened; one just inside still reads.
  4. **`maintenance.run`** (new, red first): a second process on a held lock is refused; `stop` ends the loop.
  5. **The end-to-end test** (unchanged assertions): the shipped `Procfile` now starts `maintenance` too, so its first pass runs beside the API's appends and the workers, and the store and export oracles must still hold.
- **No sleeps tuned to the machine.** Time comes from `now`; only the real commit timestamps are wall clock.
- **Coverage:** `maintenance.py` under the 90% package gate; the pure modules keep their 100%.

## Risks

- **The Listing Store's replaced files may outgrow the disk at full load.** ADR-0002 accepts that a MERGE rewrites the ~1 MiB file around each changed key. At 50 changes/s that is roughly 50 MiB/s of replaced files, about 180 GB an hour, and the 1-hour minimum vacuum can't keep up with 27 GB free. This is an estimate, not a measurement: Phase 7 measures it. If it holds, the fix is a design change (smaller files, bigger worker batches, or a lower load target), not part of this step.

## Out of scope

- Workers or the exporter below the horizon (state lost or reset): 5e and 5b.
- Pruning Catalog Snapshots: 5c.
- Hidden temp files left by a `kill -9` in `state/` and `data/export/`: small, and no step creates many.
- Changing retentions without a code change (flags or config).
- Upgrading tables created before this step: reset `data/` and `state/`.
- Running the A4 store oracle on a Landing log older than 1 hour of log history: it already runs only from an empty store.
- Power loss: process crashes only (ADR-0002).

## Size

About 135 lines of production code and 210 of tests, over the 300 limit, so two PRs:

1. **5d.1, the Delta tables:** `maintenance.py` with the guard, both tables' cleanup, `run`/`main`, the lock, the `Procfile` line, `landing.append`'s refresh and `delta.ensure`'s properties. About 105 lines plus 160 of tests (test points 1 to 2, 4 to 5, without the file pruning).
2. **5d.2, the files:** `events.prune`, `export.prune` (both called from the tick) and the API's 404 at the horizon. About 30 lines plus 60 of tests (test point 3 and the pruning half of 1).
