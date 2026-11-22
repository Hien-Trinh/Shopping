# Step A8: the Listing Store's maintenance (mini PRD)

Status: draft Nov 22, questions 1 to 3 answered (Option A for each), the Free Edition probe is owed. Plan row: [plan-v2.md, PR steps, A8](../plan-v2.md). Decisions from [Change Export and maintenance][t156] (items 2 to 4) and the [Oct 28 review][t166] (items 4 and 10). v1's steps: [step-5c.md](step-5c.md) (snapshots) and v1's `maintenance.py`. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

The streaming job writes the Listing Store with a MERGE per batch, and nothing tidies it: small files pile up, dead files stay, and there are no Listing Store Snapshots. v1's maintenance and snapshots are delta-rs processes under a supervisor, and S2 deletes them. The snapshot oracle (A9) has nothing to read until this step. Ten facts shape it. "Probed" means local Spark on Nov 21 (PySpark 4.1.4, delta-spark 4.4.1, JDK 21, `local[2]`), with the files in `probe/local/` on the unmerged branch [`probe/a8`][probe]. "Docs" means Databricks' pages, fetched Nov 21.

1. **A shallow clone is a log and nothing else.** Probed: `CREATE TABLE … SHALLOW CLONE delta.\`<store>\` VERSION AS OF v LOCATION '<path>'` writes only `_delta_log/` at the location, and its rows equal the store's at `v`. Its history holds one `CLONE` commit with `sourceVersion` and a timestamp. It copies the store's properties and clustering columns.
2. **A clone can have a name on local Spark.** Probed: the target can be `default.snapshot_v1` in the session's catalog, with `LOCATION`. `SHOW TABLES IN default LIKE 'snapshot_v*'` lists it and `spark.table` reads it. The local catalog is in memory, so a second process sees the clone only by its path.
3. **In Unity Catalog the target must be a name.** Docs: "If `table_name` itself is a path instead of a table identifier, the operation will fail" ([CLONE][clone]). `LOCATION` makes an external clone, and a clone of an external table must be external ([clones in Unity Catalog][cloneuc]). Not found in the docs: whether the source can be a path there.
4. **A second create of the same clone is safe only with `IF NOT EXISTS`.** Probed: with it, the second statement does nothing. Without it, or with `OR REPLACE`, it fails (`DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION`, `DELTA_UNSUPPORTED_NON_EMPTY_CLONE`).
5. **`DROP TABLE` leaves the clone's files.** Probed locally, and docs for a Unity Catalog external table ([DROP TABLE][drop]). A new clone at the same location then fails (fact 4's first error).
6. **A vacuum past retention breaks a clone, outside Unity Catalog.** Probed: after an `OPTIMIZE` and `VACUUM … RETAIN 0 HOURS` on the store, reading the clone fails with `FILE_NOT_EXIST`, and so does a new clone of that version. A `VACUUM` at the default leaves a fresh clone readable. Docs say Unity Catalog keeps the files a clone references, for external tables too ([clones in Unity Catalog][cloneuc]), which the [Oct 28 review][t166] did not trust. This step keeps the review's rule: it holds in both places.
7. **`OPTIMIZE` is incremental, and an idle one adds no commit.** Probed on the clustered store: the first `OPTIMIZE` removed 2 files and added 1, in one commit; the second added no commit. Docs: on Databricks an `OPTIMIZE` can't conflict with the MERGE under row-level concurrency ([row-level concurrency][rlc]). Open-source Delta has no such rule, and `ingest.apply_batch` already retries the MERGE on a conflict (A5).
8. **`VACUUM` adds two commits,** `VACUUM START` and `VACUUM END`. Probed, and Databricks logs them by default for external tables ([VACUUM][vacuum]). Neither they nor an `OPTIMIZE` commit reached the export query: no new export file (A7's fact 2, probed again).
9. **A pool is set per thread.** Probed: `sparkContext.setLocalProperty("spark.scheduler.pool", …)` inside a `pyspark.InheritableThread` holds in that thread and not in the main one. A pool only shares the cluster fairly when `spark.scheduler.mode` is `FAIR`; local Spark leaves it unset (FIFO), and no page gave Databricks' default for a job cluster. Serverless has no `sparkContext` ([limits][limits]), as `ingest.start` already handles.
10. **Two Spark test files read stdout** (`grep -l readouterr tests/`): `tests/spark/test_free_edition.py` and `tests/spark/test_liveness.py`. Since A7 the first drops every `{"type": ` line, so a new line type changes neither.

## Solution

One thread in the job's driver, started by `ingest.main` after the two queries. Every minute it asks a pure function what is due, and runs it:

```text
maintenance thread (pool "maintenance")
  at start     VACUUM                       then every 24 h
  every 1 h    OPTIMIZE
  every 6 h    snapshot: CREATE TABLE IF NOT EXISTS <schema>.snapshot_v<head>
                           SHALLOW CLONE delta.`<data>/listing_store` VERSION AS OF <head>
                           LOCATION '<data>/snapshots/snapshot_v<head>'
               then drop each snapshot older than 7 days, but never the newest
```

In one tick the order is: snapshot, drop, `OPTIMIZE`, `VACUUM`. A `VACUUM` then never runs while a snapshot older than the retention still exists. Each action prints one `maintenance` line. An action that fails ends the job (question 2), and the job's retries start it again.

## User stories

1. As the check job (A9), I find at least one Listing Store Snapshot pinned after the first MERGE, by its name and by its path.
2. As an operator, I read in the driver's log when each `OPTIMIZE`, `VACUUM` and snapshot ran, and at which version.
3. As the ingest query, I never wait behind an `OPTIMIZE` for the whole cluster.
4. As a stress run, I get a snapshot every 30 minutes from one argument.

## Failure scenarios

| Failure | What happens |
| --- | --- |
| The job dies during an `OPTIMIZE` or a `VACUUM` | Each is one Delta commit or none. The next start runs `VACUUM` at once and `OPTIMIZE` an hour later |
| The job dies during a clone | The location may hold a partial log and no table. That version gets no snapshot: the next create fails on the non-empty location, which ends the job once, and by the restart the head has moved. If the store is idle the failure repeats: the runbook says to delete that one directory (S3) |
| The head has not moved since the last snapshot | `IF NOT EXISTS` makes the create a no-op (fact 4). No second snapshot of one version |
| A store idle for 7 days | The newest snapshot is never dropped, so the head's snapshot stays and no create meets a dropped clone's files (fact 5) |
| An action raises (a conflict on open-source Delta, storage, a permission) | The thread stops both queries and `main` raises the error (question 2). Exception: an `OPTIMIZE` that loses a conflict is skipped until the next hour, with a line |
| An `OPTIMIZE` commits between the ingest batch's read and its MERGE | Databricks: no conflict (fact 7). Local: the batch's retry (A5) |
| A snapshot older than the retention exists when `VACUUM` runs | Can't happen in one job: the drop runs before the `VACUUM` in each tick, and both use 7 days. A unit test holds the two constants together |
| An action hangs | Nothing notices in this step: the watchdog reads only the two queries. Deferred to P3's smoke check, which times one `OPTIMIZE` |
| Serverless (Free Edition) | No `sparkContext`: the thread runs without a pool (fact 9) |
| The job's restart loses the thread's timers | `VACUUM` runs at each start (the plan's rule). `OPTIMIZE` waits an hour. A snapshot is due when the newest one's `CLONE` commit is older than the cadence, read from the catalog, so a restart loop makes no snapshot per start |

## Implementation decisions

1. **`src/ingestion/store_maintenance.py`** (new; v1's `maintenance.py` and `snapshots.py` stay until S2), with three parts:
   - `due(now, last, snapshots, *, every) -> list[Action]`, pure, no pyspark import: what to run now, from the last run times and each snapshot's `(version, taken)`. It joins `[tool.mutmut]`'s `source_paths` only if it sits in its own module; it stays in this one, tested by plain unit tests in `tests/spark/`, since a second module for 20 lines is not worth a file.
   - `tick(spark, data, schema, last, now, *, every)`: reads the snapshots, calls `due`, runs each action, prints its line.
   - `start(spark, data, *, schema, every, failed)`: an `InheritableThread`, daemon, that sets the pool and calls `tick` every 60 s with `datetime.now(UTC)`.
2. **Snapshots are named tables in one schema, in both places** (question 1): `--snapshots SCHEMA`, default `default`. The source is the store's path, the name is `snapshot_v<version>`, the location is `<data>/snapshots/snapshot_v<version>`.
3. **The pinned version is the head when the tick runs,** from `DeltaTable.history(1)`. A snapshot's age is its `CLONE` commit's timestamp (`DESCRIBE HISTORY`, one query per snapshot).
   `ponytail:` at the stress cadence that is up to 336 history reads per snapshot tick; one `SHOW TABLE EXTENDED` or a name with the time in it if a tick ever takes minutes.
4. **Constants:** `OPTIMIZE_EVERY` 1 h, `VACUUM_EVERY` 24 h, `KEEP` 7 days, and `--snapshot-every SECONDS`, default 21600 (6 h); stress runs pass 1800. `KEEP` must not exceed Delta's default `deletedFileRetentionDuration`, which this step leaves unset on the table.
5. **Plain statements:** `OPTIMIZE delta.\`<store>\``, `VACUUM delta.\`<store>\`` with no `RETAIN`, `DROP TABLE IF EXISTS <schema>.snapshot_v<n>`. No `OPTIMIZE FULL`, no `REORG … PURGE` ([t156][t156], item 2).
6. **The pool** is `maintenance`, set inside the thread, skipped on `PySparkAttributeError`. C2's cluster sets `spark.scheduler.mode FAIR` (fact 9): one clause in the C2 row.
7. **A failed action ends the job** (question 2): the thread records the error and stops both queries; `main` raises it after `liveness.watch` returns. `main` gains about 5 lines.
8. **The `maintenance` line:** `{"type": "maintenance", "op": "optimize" | "vacuum" | "snapshot" | "drop", "version": n}`, through `entry.say`. `version` is the store's head for `optimize` and `vacuum`, and the snapshot's pin for the other two.
9. **The job deletes no clone directory.** A dropped clone leaves a log of a few KB (fact 5). C1's lifecycle rule gains the `snapshots/` prefix at 8 days, one day past `KEEP`: one clause in the C1 row.
10. **Free Edition** (question 3).
11. **plan-v2 changes, in this spec's PR:** the A8 row names this spec and says the newest snapshot is never dropped; the C1 and C2 clauses of decisions 6 and 9.

## Testing decisions (test points for your OK)

Tests that read stdout: `tests/spark/test_liveness.py` gains test 8; `tests/spark/test_free_edition.py` needs no change (fact 10).

- **`tests/spark/test_store_maintenance.py`** (new), in `make spark`, three local runs before the PR:
  1. `due`, with an injected time: `VACUUM` at the start and 24 h later; `OPTIMIZE` after 1 h, not before; a snapshot when none exists and when the newest is older than the cadence; drops for snapshots older than 7 days, never the newest; the order snapshot, drop, optimize, vacuum. Red: no module.
  2. `KEEP` is at most 7 days, Delta's default retention.
  3. A snapshot's rows equal the store's at its pin, after later MERGEs, an `OPTIMIZE` and a default `VACUUM` on the store. Its name is `snapshot_v<pin>` and its location is under `<data>/snapshots/`.
  4. A second tick at the same head adds no snapshot and raises nothing.
  5. With an injected time 8 days on: the old snapshots are dropped from the catalog, the newest stays.
  6. An `OPTIMIZE` tick on a store with small files leaves fewer files and the same rows; the store oracle and the export oracle hold after `tick` ran between batches.
  7. The thread's pool: inside `start`'s thread the property is `maintenance`; in the main thread it is unset.
- **`tests/spark/test_liveness.py`,** through `ingest.main`:
  8. `main` starts the thread, and a `maintenance` line prints. An action that raises fails `main` with that error.
- **Free Edition:** per question 3.

## Questions

1. **How does the job name a snapshot?** Answered Nov 21: Option A.

   A snapshot must be a named table in Unity Catalog (fact 3). On local Spark it can be a named table or a bare path (fact 2).
   - **Option A (recommended): a named table in a schema, in both places.** One statement and one code path: `--snapshots SCHEMA`. Local tests then run the cloud's statement. The local catalog is in memory, so the local check job (A9, another process) reads the clones by path. Risk: the docs don't say that the source can be a path in Unity Catalog; P3's smoke check answers it, and the fallback is one more argument that names the store's table.
   - **Option B: a bare path locally, a named table in the cloud.** Two statements, two ways to list and to drop, and the cloud's way runs first in the paid trial.

2. **What happens when a maintenance action fails?** Answered Nov 21: Option A.

   v1's rule is "any error propagates: the supervisor restarts it".
   - **Option A (recommended): the job fails,** and its 3 retries restart it. One rule, the same as a failed export query (A7). An `OPTIMIZE` that loses a conflict is the one exception: it is skipped until the next hour.
   - **Option B: print a line and try again at the next tick.** The job keeps running, but a `VACUUM` or a snapshot that fails for days is seen only by someone who reads the log.

3. **Does Free Edition check this step?** Answered Nov 22: Option A.

   Free Edition is serverless: no pool, and no page says whether a shallow clone with a `LOCATION`, `OPTIMIZE` or `VACUUM` works there on a table under a volume.
   - **Option A (recommended): one probe run first.** A throwaway branch runs the three statements once (one run of the unpublished quota). The code PR then adds one check, "a snapshot reads as the store at its pin", for what the probe showed to work.
   - **Option B: no Free Edition check for A8.** The first run on Databricks is P3's smoke check.

## Out of scope

- The snapshot oracle and its guard: A9.
- The Landing log's upkeep: A10.
- `spark.scheduler.mode` and the schema's Terraform: C2. The lifecycle rule: C1.
- Timing one `OPTIMIZE` and its effect on a batch; a watchdog for a hung action: P3.
- The runbook's line for a partial clone directory: S3.
- Deleting v1's `maintenance.py` and `snapshots.py`: S2.
- Deleting `probe/a8`: when Phase A's probes end.

## Size

About 110 changed lines, tests left out, so one PR: `store_maintenance.py` about 90, `ingest.py` about 12, plan-v2 about 4, and about 15 in `free_edition.py` if question 3 adds a check. About 200 of tests. A7 came in near its estimate; the cap is 150.

[t156]: https://github.com/Hien-Trinh/Shopping/issues/156
[t166]: https://github.com/Hien-Trinh/Shopping/issues/166
[probe]: https://github.com/Hien-Trinh/Shopping/tree/probe/a8
[clone]: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-clone
[cloneuc]: https://learn.microsoft.com/en-us/azure/databricks/tables/operations/clone-unity-catalog
[drop]: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-drop-table
[rlc]: https://learn.microsoft.com/en-us/azure/databricks/optimizations/isolation/row-level-concurrency
[vacuum]: https://learn.microsoft.com/en-us/azure/databricks/tables/operations/vacuum
[limits]: https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/limitations
