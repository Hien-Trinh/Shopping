# Step 7f: the Listing Store's rewrite churn (mini PRD)

Status: draft Oct 7, for your review. Decided in chat Oct 7: small files for the Listing Store and a minute-level vacuum of its dead files (options 2 and 3 of the five weighed). Questions 1 (the `alter` line by hand), 2 (no plan row for the append-only store: a Future-work line in the design doc, and 7f.2's report records the churn as the known cost) and 3 (the load generator's `--unique-text` flag, and 7f.2 runs with its tables on the T9 external drive) answered Oct 8. Plan row: [plan-v1.md, PR steps, 7f](../plan-v1.md). Builds on [step-7d.md](step-7d.md) and [docs/stress.md](../stress.md) (7d.2b: the SLO run cannot complete), [step-5d.md](step-5d.md) (maintenance never vacuums past the slowest reader), [ADR-0001](../adr/0001-partition-by-listing-key.md) (only a partition's owner writes it) and [ADR-0002](../adr/0002-local-first-delta-no-queue.md) (Delta, so the tables move to Databricks as they are). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Delta never changes a file in place: a MERGE rewrites every file that holds a matched row and leaves the old file on disk until a vacuum. The Listing Store is written by one MERGE per Ingestion worker tick, and 7d.2b measured what that costs at 50 Changes a second over 1M Listings ([stress.md](../stress.md), point 3):

| | |
|---|---|
| A tick (p50 9 Changes) rewrites | p50 7 files / 10.8 MB on the stress store, whose rows compress to 126 bytes; 194 to 387 MB on real-sized rows (1.2 KB), measured on a probe: every touched partition, whole, because delta-rs writes a partition's MERGE output as one file up to 100 MB |
| Ticks a minute, four workers | 155 |
| Dead files a minute | 1.7 GB on the stress store, about 30 GB on real rows |
| Kept for | at least an hour: maintenance's floor follows the Landing log's `LOG_RETENTION_HOURS` |
| Result | the API's 5 GiB `low_disk` guard trips in 8 minutes and refuses 29% of the run's Changes; the Phase 7 exit stays open |

Probes on delta-rs 1.6.6 (the latest) fixed the limits: a MERGE honours `delta.targetFileSize`, but the writer makes no file smaller than about 1,000 rows, so one Change costs at least one 1,000-row rewrite (about 1.2 MB on real rows, 3.6 GB a minute at 50/s); deletion vectors, Delta's merge-on-read answer, are read but never written by delta-rs (its issue #4512); vacuum retention is whole hours or zero, lite mode (`full=False`) removes only files the log marks removed and never an in-flight write, and `keep_versions` protects a version's files. Fewer, fuller ticks do not help: with small files each Change rewrites its own file; with big files a full tick rewrites every partition.

## Solution: two PRs

| PR | What | Gate |
|---|---|---|
| **7f.1** | The Listing Store writes 1 MiB files; maintenance vacuums its dead files every minute, keeping what any reader can still need; the load generator's `--unique-text` flag | None |
| **7f.2** | The amended 7d.2 point 3 rerun on a fresh 1M load with `--unique-text`, the tables on the T9 external drive (the `backfill` line kept, no vacuum by hand) and the Phase 7 exit decided; appended to `docs/stress.md`, with the rewrite churn recorded as the known cost of the local engine | A free afternoon of the Mac with the T9 mounted: the load, its drain, 30 min at 50/s, then the Backfill's hours if you want its time to zero |

### 7f.1: small files and a minute-level vacuum

1. **The file size.** `store.ensure` creates the table with `"delta.targetFileSize": str(COMPACT_TARGET)` next to the other properties ([delta.py](../../src/catalog/delta.py)): one constant for what the MERGE writes and what compaction merges up to, so they cannot drift. A store that exists without the property gets it from the runbook's one-line `alter` (Reset section), by hand: the files written before stay large until a MERGE or a compaction touches their partition, one full rewrite each, then stay small. The probe: a 9-Change MERGE rewrites 11 MB instead of 194, and the worker's partition read moves from 67 to 104 ms.
2. **The store vacuum** is a second cadence inside maintenance, `STORE_VACUUM = 60` s between passes against the pass every 10 minutes: a lite vacuum of the Listing Store at retention 0 (`full=False`: files the log marks removed, never an untracked file, so a MERGE writing its output is safe) with `keep_versions` naming every version committed in the last `GRACE = 2` minutes (from the log's history timestamps) and the version a Catalog Snapshots pass has pinned (point 3). The Landing log keeps its hour: a worker far behind reads its change feed. The store's change-data files keep `_clean`'s hourly cutoff: Change Export reads them from the watermark, and they are not Parquet data files. One event per pass, `store_vacuum` with `removed`, `mb`, `kept_versions` and `ms`; a pass that removed nothing emits nothing.
3. **Readers, and why the grace covers them.** Each process that reads the Listing Store loads the current version when it starts a pass. Ingestion workers read only their own partitions, whose files only they replace (ADR-0001), so their snapshot is never stale. The Backfill's `pending` scan and `catalog.metrics` load a version and finish in seconds: inside the grace. Catalog Snapshots streams a pinned version for as long as a 1M-row copy takes, so it writes `state/snapshot_pin.json` (`{"version": v, "started": ts}`) before the copy and removes it after, on failure too; maintenance keeps that version while the file exists, and ignores a pin older than an hour (a crashed pass) with a `pin_stale` field in the event. The chaos runner's oracles read pinned snapshots from their own directories, not the store.
4. **`--unique-text`** on `catalog.load` appends the Listing key to each description, so every row's text is distinct and compresses like a real one: the generator's 1,020 texts repeat about 1,000 times each over 1M rows, Parquet dictionary-encodes them to 126 bytes a row, and 7d.2b's churn came out 10 to 20 times smaller than real rows give. Off by default, so the chaos runner and CI are unchanged; the report states when it was on.
5. **Headroom** becomes churn times about three minutes (the grace plus the cadence): 4 to 11 GB at 50/s on real rows, under the 18 GiB this Mac has free, and the free-space line stays flat instead of falling. The churn itself does not shrink below one 1,000-row file per Change: the disk still takes 1.5 to 3.6 GB a minute of writes at 50/s on real rows. That is the cost 7f.2 records as known. The design doc's Future work gains one line: the append-only store (workers append row versions, readers keep the newest per Listing key, the owner compacts a partition once about 20% of its rows are superseded) or deletion-vector writes once delta-rs ships them, either of which removes the churn; neither is a plan row.

### 7f.2: the rerun

The amended point 3 of [step-7d.md](step-7d.md): the shipped `Procfile` on the student, the `backfill` line kept, a fresh 1M load with `--unique-text` (the stress worktree's store was consumed by 7d.2b's reset), then 30 minutes at 50/s. The tables (`data/` and `state/`) live on the T9 external drive (APFS, 797 GiB free, about 40 times the internal volume's headroom), reached through the `--data` and `--state` flags or a symlink from the stress worktree, so the run has room even before the vacuum is proven; the API's `low_disk` guard then reads the T9's free space. Recorded: every `metrics` field, `classify_ms`, `store_vacuum` events (files and MB a minute), the free-space line, the bytes written, and the Backfill's rate. The report marks that the drain time and `batch` ms are on a USB drive and not comparable with 7d.2a's, and that the rows no longer repeat. The Phase 7 exit is decided on it.

## User stories

1. As you, a stress run at 50/s on 1M Listings runs for as long as the Mac has a few GB free, and the free-space line tells you the churn instead of the guard tripping.
2. As the Ingestion worker, a tick's MERGE rewrites the files that hold its Listings, not its whole partitions, and a read lists a few more files for it.
3. As Catalog Snapshots, a pass finishes on the version it pinned, however many MERGEs land meanwhile.
4. As the Backfill or `catalog.metrics`, a scan that takes seconds never loses a file.

## Failure scenarios

| Scenario | Expected |
|---|---|
| A MERGE is writing its output when the store vacuum runs | The output files are untracked until the commit; lite mode never removes an untracked file (probed) |
| A reader loaded a version in the last 2 minutes and a MERGE replaced one of its files | The version is in `keep_versions`: its files stay |
| A Snapshots pass takes longer than the grace | Its pin keeps its version for the pass; a pin older than an hour is a crashed pass and is ignored, reported in the event |
| The pin file is left by a killed Snapshots process | Ignored after an hour; Snapshots removes it at its next start |
| Maintenance is down | Dead files accumulate as today, an hour at most once it returns; the disk guard still protects the API |
| The vacuum's commit conflicts with a worker's MERGE | As in 7d.2b's hand vacuums: the files are removed, the `VACUUM END` commit is retried next pass; the event says `commit_failed` |
| An existing store without the property | Files stay large and the churn stays as 7d.2b measured; the runbook's `alter` is the fix; `store_vacuum` events show the MB a minute either way |
| A compaction (every 100 batches) | Merges a partition's small files up to the same 1 MiB target, so file counts stop growing; unchanged |
| A reader older than the grace that is not Snapshots | None exists today; a new one must pin or finish inside the grace, and this spec says so in the design doc's maintenance text |
| The churn is still too high for the run | The run records it; question 2's step is the next lever |

## Implementation decisions

1. **Lite vacuum at retention 0 with `keep_versions`, not an hourly retention.** delta-rs takes whole hours; zero plus an explicit list of kept versions is the only way to a minute-level floor with the library's own vacuum, and lite mode is what makes zero safe for in-flight writes. Rejected: a hand-rolled vacuum over the log's `remove` actions (the same logic, ours to maintain) and retention 1 h (the problem).
2. **Maintenance owns it, not the workers.** One process, one cadence, one event; the workers stay writers. Rejected: each worker vacuuming its own partitions after its MERGE (vacuum is table-wide in delta-rs).
3. **1 MiB, the existing compaction target.** The writer's floor is about 1,000 rows, so a smaller target changes nothing; a larger one rewrites more per Change.
4. **A pin file for Snapshots, not a longer grace.** A 1M-row copy can take longer than any grace that keeps the headroom small; the pin is one small file and the only reader that needs it.
5. **The rerun on a fresh 1M load**, not the 10k store 7d.2b left: the exit is about 1M rows.
6. **Not deletion vectors and not the append-only store.** delta-rs cannot write the first; the second is a design change, kept as a Future-work line, not a step (decided Oct 8). This step makes the measurement possible and records the churn as the known cost; it does not make 50/s cheap on one Mac.

## Testing decisions (test points for your OK)

1. **Small files** (new, red first, `tests/integration/test_store.py`): a MERGE of three rows into a partition written as several 1 MiB files removes only the files holding those rows, not the partition; a store `ensure` creates carries the property.
2. **The store vacuum** (new, red first, `tests/integration/test_maintenance.py`, fake clock): removes a file the log marked removed before the grace; keeps the files of a version committed inside the grace; keeps the files of the pinned version while the pin exists and ignores a pin older than an hour; never removes an untracked file in the table directory; emits the event with the counts, nothing when nothing was removed.
3. **The pin** (new, red first, `tests/integration/test_snapshots.py`): written before the copy with the pinned version, removed after it, removed when the copy raises, removed at start if left over.
4. **The cadences** (extend the maintenance loop test): the store vacuum runs every `STORE_VACUUM` seconds, the full pass every `INTERVAL`, each on its own clock.
5. **`--unique-text`** (new, red first, `tests/unit/test_load.py` or where the generator's tests live): with the flag every generated description ends in its Listing key and no two rows share a text; without it the texts are the labeled set's, as today.
6. **e2e** (changed): a `store_vacuum` event appears in a run that MERGEd, and the three oracles still hold.
7. **By hand** (7f.2): the rerun, in the report.

## Questions

1. **Existing stores get the property from the runbook's `alter` line, by hand**, not from `store.ensure` at every process start (several processes setting it at once would conflict). **Agreed (Oct 8).**
2. **The append-only store** (workers append row versions, readers keep the newest per Listing key, the owner compacts a partition once about 20% of its rows are superseded) cuts the churn about a hundred-fold and is the shape Kafka's log compaction and Hudi's merge-on-read take; deletion-vector writes in delta-rs would do the same with no code, when they ship. Add it as plan row 7g, after the exit is decided? **No (Oct 8): a Future-work line in the design doc, and 7f.2's report records the churn as the known cost.**
3. **The load generator's text repeats** (1,020 distinct descriptions over 1M rows, 126 bytes a row after compression), which made 7d.2b's churn 10 to 20 times smaller than real rows would give. Add a `--unique-text` flag that appends the key to each description, and use it for 7f.2? **Yes (Oct 8), and 7f.2 runs with its tables on the T9 external drive.**
4. **`GRACE = 2` minutes and `STORE_VACUUM = 60` s**: headroom of about three minutes of churn. OK?
5. **The test points above.**

## Out of scope

Deletion vectors (not in delta-rs), the append-only store (a Future-work line), finer partitions or a bucket column (the same ceiling as small files), a hot/cold split of the row (helps price and stock Changes only), the vacuum of the Landing log (unchanged, an hour).

## Size

7f.1 about 130 lines of code with tests (the property, the second cadence and its vacuum, the pin, the event, the generator's flag, the runbook and design doc lines); 7f.2 an afternoon of the Mac with the T9 and a report section.
