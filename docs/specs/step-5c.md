# Step 5c: Catalog Snapshots and pruning (mini PRD)

Status: approved Oct 3. Confirmed: Delta table snapshots (decision 1), the pinned version in the commit metadata (decision 2), hidden temp and delete folders (decision 3), due time and pruning from the newest name, always keeping the newest (decisions 4 and 5), restarts by the supervisor (decision 6), one snapshotter (decision 7), and the test points. Plan row: [plan-v1.md, PR steps, 5c](../plan-v1.md), and Phase 5's tests "snapshots equal the pinned version" and "pruning at 7 days (with an injected clock)". Design: the [Catalog Snapshots row, the state-on-disk layout and oracle 3](../design-commerce-ingestion-pipeline.md), and [ADR-0002](../adr/0002-local-first-delta-no-queue.md) ("Catalog Snapshots are explicit copies, not Delta time travel" and "the Landing log, Listing Store and Catalog Snapshots are Delta tables"). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

The design promises a copy of the Listing Store every 6 hours, kept 7 days, and a snapshot oracle (each snapshot equals the Listing Store at its pinned version). None of it has code: no process takes snapshots, nothing prunes them, and 5e's bootstrap for a worker below the retention horizon has no retained snapshot to start from.

## Solution

1. `catalog/snapshots.py`:
   - `take(dt, snapshot_dir, now)`: pins the Listing Store's current version, streams that version into a new Delta table in a hidden temp folder, then renames it to `data/snapshots/<ts>/`. Returns the folder.
   - `prune(snapshot_dir, now, keep)`: deletes snapshots older than `keep` (7 days), but never the newest one.
   - `existing(snapshot_dir)`: the complete snapshots, oldest first; hidden folders are skipped.
   - `pinned(path)`: the Listing Store version a snapshot copied, for the oracle and 5e.
   - `tick(dt, snapshot_dir, events, now, every, keep)`: takes a snapshot if the newest is at least `every` (6 h) old, prunes, and returns the seconds until the next one is due.
   - `run` and `main`: the loop, an exclusive lock, signals and the supervisor watch, as the exporter does.
2. `state.py`: `claim_snapshots`, a lock on `state/locks/snapshots.lock` through the existing `_hold`.
3. `Procfile`: a `snapshots: python -m catalog.snapshots` line.

## User stories

1. As the operator, I find a snapshot of the Listing Store no more than about 6 hours old in `data/snapshots/`, and DuckDB reads it like the Listing Store (`DeltaTable(path).to_pyarrow_dataset()`).
2. As the operator, each snapshot equals the Listing Store at its pinned version, Tombstones included (the snapshot oracle).
3. As the operator, snapshots older than 7 days are deleted, so they don't fill the disk, but I always keep at least one.
4. As 5e's bootstrap, I can read a snapshot's pinned version and follow the change feed from there.
5. As the operator, a crash or `kill -9` at any point never leaves a half-written or half-deleted snapshot that looks complete.
6. As the operator, I see each snapshot and each prune in the events (`snapshot`: name, version, rows, ms; `snapshot_pruned`: name).

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| First start, no snapshot yet | Takes one at once, even of an empty store |
| Merges, deletes, a reclassify and a compaction, then a snapshot | Snapshot equals the Listing Store at the pinned version, row for row; the store moving on afterwards doesn't change it |
| Workers commit while the copy is running | The copy reads only the pinned version; later commits aren't in it |
| Restart 2 h after the last snapshot | No new snapshot; next one due 4 h later. The due time comes from the newest snapshot's name, not the process start |
| Down for 2 days, then restarted | One snapshot at once (no catch-up burst), then every 6 h |
| `kill -9` during the copy | Only a hidden temp folder is left; the next start deletes it. `existing` never shows it |
| `kill -9` during a prune | The snapshot was renamed to a hidden folder before deleting, so no half-deleted snapshot is visible; the next start finishes deleting it |
| Snapshots 8, 6 and 1 days old | The 8-day one is pruned; the others stay |
| Only snapshots older than 7 days (the snapshotter was down) | The newest is kept until a newer one exists |
| Two snapshots in the same second | Refused: the rename fails because the folder exists; the process exits and the supervisor restarts it. Only reachable by hand, since one snapshotter holds the lock |
| A second snapshotter on the same state directory | `PartitionTaken`-style refusal from the lock before anything is read or deleted |
| Disk full or any I/O error mid-copy | The temp folder is removed, the process exits 1, the supervisor restarts it |
| SIGTERM, or the supervisor dies (`kill -9`) | The 6 h wait ends at once; a copy in progress finishes first |
| Wall clock jumps backwards | Out of scope: due times and ages use the wall clock, since snapshot names must mean dates across restarts |

## Implementation decisions

1. **A snapshot is a Delta table**, as ADR-0002 and "DuckDB's role" already say: `write_deltalake` from the pinned version's Arrow dataset, streamed (no full table in memory), partitioned by `partition` like the Listing Store. The Listing Store's change feed is not enabled on it.
2. **The pinned version and the Listing Store's table id are stored in the snapshot's own commit** (`CommitProperties(custom_metadata=...)`), read back by `pinned`. The folder keeps the design's name, `data/snapshots/<ts>/`, with `<ts>` as UTC `YYYYMMDDTHHMMSSZ` so name order is time order.
3. **Complete or invisible.** Written to `.<ts>.<uuid>.tmp/`, then `os.rename`d into place. Pruning renames to `.<ts>.<uuid>.deleting/` before `rmtree`. Any hidden folder is a leftover and is deleted at startup, which is safe because the lock allows one snapshotter.
4. **Due time from the newest snapshot's name**, compared with an injected `now`: restarts don't add snapshots, and a long outage takes one, not a burst.
5. **Pruning by age from the name, never the newest.** Mtime would be reset by a copy or backup. Keeping the newest means there is always one for 5e, even after a long outage.
6. **Errors exit the process; the supervisor restarts it**, as for Change Export (5a, decision 5). The supervisor's watch stays limited to workers.
7. **One snapshotter per state directory**, via `state._hold`. Two would delete each other's temp folders at startup.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`snapshots.take` and `pinned`** (new, red first, `tests/integration/test_snapshots.py`, real Delta in `tmp_path`, Listing Store writes through `store.merge`): the snapshot oracle (`replay.diff(store.fingerprints(dt, v), store.fingerprints(snapshot))` empty, plus full row equality sorted by key) after upserts, a delete, a reclassify and a compaction; a merge after the snapshot doesn't change it; an empty store; a failure mid-copy (a write that raises) leaves no folder.
  2. **`snapshots.tick` and `prune`** (new, red first, injected `now`): due and not due from the newest name; one snapshot after a long outage; the 7-day prune; the newest kept when all are old; hidden leftovers deleted; `existing` ignores hidden folders.
  3. **`snapshots.run`** (new, red first): a second snapshotter on a held lock is refused; `stop` ends the wait.
  4. **The end-to-end test** (changed, `tests/integration/test_e2e.py`): the shipped `Procfile` now starts the snapshotter too; at the end, at least one snapshot exists and the snapshot oracle holds for each. That covers the `Procfile` line, `main` and the default directories.
- **No sleeps tuned to the machine.** `tick` takes `now`; only the end-to-end test waits, with a timeout.
- **Coverage:** `snapshots.py` and `state.py` stay under the 90% package gate.

## Out of scope

- 5e's worker bootstrap from a snapshot: this step only makes `pinned` available.
- Listing Store VACUUM. Whichever step adds it must keep old versions for longer than a snapshot copy takes, or the copy fails (and is retried).
- Skipping a snapshot when the store hasn't changed: 28 copies over 7 days. At 1M Listings that may be several GB against A13's disk budget; measure in Phase 7.
- Verifying snapshots in production: the oracle runs in tests and Phase 7's chaos runs.
- Power loss: process crashes only (ADR-0002).

## Size

About 75 lines of production code (about 65 in `snapshots.py`, 5 in `state.py`, 1 in the `Procfile`) and about 150 of tests, so under 300.
