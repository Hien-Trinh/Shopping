# Step A1: the three cross-engine tables and their init step (mini PRD)

Status: approved Nov 2: the decisions, the test points, and question 1 (no Spark test in A1). Plan row: [plan-v2.md, PR steps, A1](../plan-v2.md). Decisions from [Landing log and Outcomes][t136], [API on Container Apps][t161] and the [Oct 28 review][t166]. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

v2 splits the pipeline at the Landing log: the API writes it and the Submission log with delta-rs, and the streaming job reads the Landing log's change feed and writes Outcomes with Spark ([ADR-0003](../adr/0003-azure-databricks-split-at-landing-log.md)). Today:

- The Landing log is v1's: 64 partitions (up to 640 files/s in a bulk load) and one hour of history, so a stream more than an hour behind could find its commits cleaned.
- There is no Submission log: the API records accepted and rejected Changes only as v1's JSONL events. There is no Outcomes table.
- The API creates the Landing log when it's missing. Two replicas starting on an empty store would both try, and the lock file that serializes v1's processes is local to one machine.
- `landing.retained` picks a partition with `file_pruning_predicate`, which prunes files by their min/max statistics, not rows. On an unpartitioned log it would hand a v1 worker other partitions' Changes.

## Solution

One PR:

1. **The Landing log** goes unpartitioned (the `partition` column stays), with `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` at 7 days. Rows go after 7 days through the retention DELETE that v1's maintenance runs today and A10's upkeep runs in v2.
2. **The Submission log:** one row per Change of each Submission, accepted or rejected, partitioned by the hour of the Submission's UUIDv7. The API appends it in the same group-commit window, after the Landing log commit and before the 202.
3. **Outcomes,** created empty: `delta.appendOnly`, `delta.enableDeletionVectors=false`, `delta.checkpointPolicy=classic`, partitioned by the same hour. A2's Ingestion job writes it.
4. **One init step,** `python -m ingestion.tables --data DIR`, creates whichever of the three is missing and leaves existing ones alone. The API only opens its two tables and refuses to start without them. v1's supervisor runs the init before it starts anything.
5. **`landing.retained`** filters rows by partition, as well as pruning files.

## User stories

1. As the operator of two API replicas, I want neither replica to create a table, so that two starting on an empty store can't race to create the Landing log.
2. As the operator, I want one init command that creates the three tables and is safe to re-run, so that a deploy runs it before the API starts and a rerun changes nothing.
3. As the streaming job (A2), I want 7 days of Landing log history and removed files, so that any outage under 7 days resumes from its checkpoint.
4. As GET /submissions/{id} (S1), I want every accepted and rejected Change in the Submission log before the 202, partitioned by its Submission's hour, so that a lookup reads one partition and a 202'd Submission is never a 404.
5. As the API reading the Outcomes Spark writes (S1), I want Outcomes pinned to what delta-rs reads (no deletion vectors, classic checkpoints) and append-only, so that no Databricks default makes it unreadable.
6. As a v1 worker until S2, I want `landing.retained` to return only my partition's rows, so that an unpartitioned log hands me no other worker's Changes.

## Failure scenarios

Seeded from lean-review's finder angles.

| Scenario | Expected |
|---|---|
| Two API replicas start on an empty store | Both refuse to start (exit 2, naming the init command) and create nothing. Container Apps restarts them until the init has run |
| The init runs again, after a restart, or twice at once on one machine | Existing tables are opened, never replaced: same ids and versions. Locally, `delta.ensure`'s lock file serializes creation, as for v1's processes |
| The init is killed after creating one table | The next run creates the rest |
| The Landing log commit fails | No Submission log commit: every request in the window gets a 500, and no Submission log row names a Change that didn't land |
| The Submission log commit fails, or the API is killed between the two commits | The Landing log rows stay, with no Submission log rows. No 202 went out, so the Merchant resends, and the duplicates replay as `already_applied` (rule 3). Freshness counts only Submission log Changes ([plan-v2, C](../plan-v2.md)) |
| The API is killed after both commits, before the 202 | Both logs hold the Submission. The Merchant resends under a new submission id, and the duplicates replay as `already_applied` |
| Two replicas' commits race on either log | delta-rs rebases a blind append onto the newer version. A lost race is retried once, as v1's Landing log append is, and logged as `append_retry` with its table |
| Every Change in a request is rejected | It goes through the window too: Submission log rows, and no Landing log commit |
| A window spans an hour boundary | Each Submission's rows go to its own hour: one commit, two partitions |
| A Landing log row turns 7 days old | v1's maintenance DELETE removes it, as now; A10's upkeep in v2. The 7-day file and history windows keep every version a stream under 7 days behind still reads |
| v1's maintenance vacuums the Landing log | Unchanged: it vacuums by its offset gate with `enforce_retention_duration=False`, past the 7-day file window. v1's readers are gated, and no Spark stream runs beside it. A10's upkeep keeps the window |
| The tables are wiped and re-initialized while an API replica runs | Outside v2's flow: an exit attempt wipes `base` before `run`, and so the API, exists. Probed Nov 2: a stale delta-rs handle fails each append with `Invalid table version` while the new table is behind it, and appends to it with no gap in its log once it's ahead. A restart opens the new table |
| An existing table with another layout (a pre-A1 local `data/landing_log`: 64 partitions, an hour of history) | The init opens it as it is. v2's runs start clean (rule 7); a local run gets v2's layout once `data/` is deleted |
| A DELETE, UPDATE or MERGE on Outcomes | Refused by both engines. Probed Nov 2: delta-rs raises `CommitFailedError`, Spark `DELTA_CANNOT_MODIFY_APPEND_ONLY` |
| Spark appends to Outcomes (A2) | Probed Nov 2: 12 Spark appends with `txnAppId`/`txnVersion` and one replayed batch, which added nothing. The protocol stayed reader 1 / writer 2 with no table features, and delta-rs read Spark's classic checkpoint at version 10 |

## Implementation decisions

1. **`src/ingestion/tables.py`** (new) holds the Submission log and Outcomes (their schemas, `hour` and the Submission log's append) and `init`, with its CLI through `entry.exit_with`. The Landing log stays defined in `landing.py`, and `init` creates it through `landing.ensure`.
2. **`delta.ensure` gains `partition_by`** (default `["partition"]`, the Listing Store's), and the change feed becomes each table's own setting: on for the Landing log and the Listing Store, which streams read; off for the Submission log and Outcomes, whose readers read snapshots. Delta's own log cleanup stays off on every table, and nothing cleans the Submission log or Outcomes: each run starts clean (rule 7), so they hold one run.
3. **The Landing log:** no partition columns; `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` are `interval 7 days`. delta-rs 1.6.6 makes it reader 1 / writer 4 with no table features (probed).
4. **The Submission log** (`data/submission_log`), one row per Change, all columns non-null:

   | Column | Type | Why |
   |---|---|---|
   | `hour` | string, the partition | The Submission's UUIDv7 hour in UTC, `2026-11-02T13`, as the events name their hours |
   | `submission_id` | string | |
   | `change_index` | int32 | |
   | `merchant_id` | string | GET's owner check |
   | `accepted` | bool | false means rejected |
   | `received_at` | timestamp (µs, UTC) | The request's arrival, the value its Landing log rows carry |

   `hour` is a string because Delta stores a timestamp partition value without its time zone. The Listing key and source version stay in the Landing log, which freshness (A9) can join on `(submission_id, change_index)`; a rejected Change may have no valid key. The rejection errors stay in the 202's body.
5. **`tables.hour(submission_id)`:** the UUIDv7's 48-bit millisecond timestamp, cut to the hour. A2 computes Outcomes' `hour` the same way. The Submission's hour, not the Outcome's, keeps all of a Submission's rows in one partition of each table, whenever its Outcomes land.
6. **Outcomes** (`data/outcomes`): `hour`, `submission_id`, `change_index` and `outcome` (a string), all non-null, partitioned by `hour`, with the three properties above. These are the columns GET's best-Outcome rule and A9's `written` filter read. A2 adds any it needs, at no migration cost, since every run starts clean. The init creates it (version 0, no rows); after that only the Ingestion job writes it (rule 1).
7. **The commit:** the API hands the group commit one record per Change, accepted or rejected: `(submission_id, change_index, merchant_id, the Change or None, received_at)`. The commit appends the accepted ones to the Landing log, then all of them to the Submission log, and only then does the 202 go out. A request whose Changes are all rejected now waits for the window too (up to 100 ms, plus the commit), instead of answering at once. `Appender` becomes generic in what it groups.
8. **One retry rule for both logs:** v1's append with one retry moves from `landing.append` to `delta.append`. Its `append_retry` event gains `table` (the table's directory name), since two logs now retry.
9. **The API opens, never creates:** `create_app` opens the Landing log and the Submission log after the registry check, so a missing one fails before uvicorn binds, as a missing registry does. `main` turns `TableNotFoundError` into exit 2 with the init command. v1's other processes keep `landing.ensure` (create if missing, under the lock): they run only locally, after the supervisor's init, until S2.
10. **v1's supervisor** runs `tables.init(state.DATA)` before it starts the Procfile, so local runs, `test_e2e` and v1's `stress-smoke` legs find the tables.
11. **`landing.retained`** keeps `file_pruning_predicate`, which drops a file only when its statistics rule the partition out, and adds the row filter `partition == p` to the scan.
12. **GET is unchanged:** it folds v1's events until S1, so the API writes both the Submission log and v1's `accepted` and `rejected` events until S2.

## Testing decisions (test points for your OK)

All in `make check`, on real local Delta tables:

- **`tests/integration/test_tables.py`** (new):
  1. The init creates the three tables on their layouts: the Landing log unpartitioned with its change feed and both 7-day windows, the other two partitioned by `hour` with no change feed, Outcomes with its three properties, and no table features anywhere (reader 1; writer 4 for the Landing log, 2 for the others). Red: no init.
  2. A second init changes nothing: same ids and versions, rows kept.
  3. Outcomes refuses a DELETE.
- **`tests/integration/test_api.py`:**
  4. A 202 follows both commits: the Submission log holds a row per Change (accepted and rejected, the Submission's hour, its Merchant), and the Landing log holds the accepted ones. Red: no Submission log.
  5. A request whose Changes are all rejected writes Submission log rows and no Landing log commit.
  6. A failed Landing log append is a 500 with no Submission log rows (the existing test gains one assertion).
  7. A failed Submission log append is a 500, with the Landing log rows kept.
  8. The API refuses to start without its tables: `create_app` raises, `main` exits 2 naming the init command, and nothing is created. Red: today it creates the Landing log.

  Each existing test that starts the API runs the init first, one line each.
- **`tests/integration/test_landing.py`:** the existing `retained` test goes red on the unpartitioned layout (its compaction puts partitions 3 and 40 in one file) and green with the row filter, so it is the filter's test. The retry test checks the event's `table`. The rest of the file now runs on the final layout.
- **`tests/integration/test_maintenance.py`:** `test_new_tables_leave_log_cleanup_to_maintenance` checks the Landing log's 7 days and the store's hour.
- **Spark:** none in A1 (question 1). The Nov 2 probes go in the PR's Evidence.

## Questions

1. **Does A1 carry a Spark test of the cross-engine contract?** The Spark env holds Runtime 18's libraries, whose deltalake is 1.1.4, so the API's delta-rs 1.6.6 isn't importable there: the probes ran each engine in its own env.
   - No: A1's tests are delta-rs only, and the probes go in Evidence. A2's ingest tests need delta-rs-written Landing logs inside Spark tests anyway, so A2's spec picks the way: a subprocess into the uv env, or deltalake 1.6.6 in the Spark env outside its drift check.
   - Yes: pick that way now, and A1 grows by about 40 lines.

   I recommend no. The choice is A2's to weigh against its own tests, and the probes already show the contract holds.

   **Answer (Nov 2): no.**

## Out of scope

- The Ingestion job's Outcomes append (`txnAppId`, `txnVersion`): A2.
- GET reading the Submission log and Outcomes: S1. Deleting v1's events: S2.
- The upkeep's 7-day DELETE, `OPTIMIZE` and `VACUUM`: A10. v1's maintenance runs as it is until then.
- Retention for the Submission log and Outcomes: none is needed while every run starts clean.
- The ADLS path and the lock file's cloud form: B1. Where the init runs in the cloud, before the API's revision: C2 and C3.
- Registering the tables in Unity Catalog as external tables: C2.

## Size

About 230 changed lines: `tables.py` about 70; `delta.py`, `landing.py`, `api.py` and `supervisor.py` about 50 between them; about 110 of tests.

[t136]: https://github.com/Hien-Trinh/Shopping/issues/136
[t161]: https://github.com/Hien-Trinh/Shopping/issues/161
[t166]: https://github.com/Hien-Trinh/Shopping/issues/166
