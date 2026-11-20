# Step A7: Change Export (mini PRD)

Status: approved Nov 20: the decisions, the test points and questions 1 to 3 (Option A each). Plan row: [plan-v2.md, PR steps, A7](../plan-v2.md). Decisions from [Change Export and maintenance][t156] (items 1 and 4) and the [Oct 28 review][t166]. v1's step: [step-5a.md](step-5a.md). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

The streaming job writes the Listing Store, and nothing sends its changes on toward serving. v1's Change Export is a delta-rs process with its own watermark file, and S2 deletes it. Freshness (plan-v2, rule 5) is measured to the export file, so the check job (A9) has nothing to measure until this step. Nine facts shape it. "Probed" means local Spark on Nov 14 (PySpark 4.1.4, delta-spark 4.4.1, JDK 21, `local[2]`), with the files in `probe/local/` on the unmerged branch [`probe/a7`][probe].

1. **The feed's rows.** Probed: a MERGE's new Listing is one `insert`; a changed one is an `update_preimage` and an `update_postimage`; a Tombstone is an `update_postimage` with `is_tombstone` true. Each row carries its commit's `_commit_version`.
2. **An `OPTIMIZE` adds no rows.** Probed: a range that holds only an `OPTIMIZE` never calls the batch function, and the checkpoint still moves past it.
3. **A stream with no checkpoint and no `startingVersion` starts with the store's current state.** Probed: every row, Tombstones included, came as an `insert` at the latest version. This is the plan's recovery after a lost checkpoint.
4. **A replay keeps its rows.** Probed: a batch that failed, then two more MERGEs, then a restart: the replay had the same batch id and the same rows, and the new commits came as the next batch.
5. **A cap splits the history between commits.** Probed with `maxFilesPerTrigger` = 1: one batch per commit. Not probed: whether a cap can end a batch inside one commit. Two such batches would have the same version range.
6. **Spark writes a directory, not a file.** Probed: `coalesce(1).write.parquet(path)` makes `path/part-….parquet` and `path/_SUCCESS`. A second write to the same path fails (`PATH_ALREADY_EXISTS`); `mode("overwrite")` replaces it. pyarrow and Spark both read the directory, and `_metadata.file_modification_time` is the part file's. An empty frame still writes a part file.
7. **Databricks commits a directory write in its own way.** It writes `_started_<id>` and `_committed_<id>` files next to the data. A failed write can leave partial files, which Databricks' Spark skips and other readers see ([ACID guarantees][acid], [uncommitted files][kb]). Locally (probed: committer version 1) the part file appears only at the write's commit.
8. **Serverless has no JVM handle** (`ingest.start` already handles this for `sparkContext`), so no Hadoop rename or delete runs there. Free Edition's `--data` is a volume, where Python's own file calls work. The paid job's `--data` is `abfss://`, where they don't.
9. **Two Spark test files read stdout** (`grep -l readouterr tests/`): `tests/spark/test_free_edition.py` and `tests/spark/test_liveness.py`. The others that read it run v1 code.

## Solution

```text
ingest.main, the new parts marked +:
  start the ingest query             (it creates the store)
  + change_export.start(spark, data, trigger)
  liveness.watch([ingest, + export])

change_export.apply_batch(batch, batch_id):
  rows = the batch without preimages
  first, last, n = min and max of _commit_version, and the count     (one Spark job)
  out  = the latest row per Listing key, plus op, minus the feed's columns
  write out as the directory export/<first>-<last>.parquet, over any earlier one
  print {"type": "export", "batch_id", "from", "to", "rows"}
  return: Spark commits the checkpoint, which is the watermark
```

## User stories

1. As a serving consumer, I read the export files in name order, upsert each row by Listing key and drop each `op=delete` key, and I end with exactly the live Listings of the Listing Store (the export oracle).
2. As a serving consumer, each file holds at most one row per Listing key: the Listing's latest state in that version range, with every Listing Store column plus `op`.
3. As the operator, a crash at any point neither loses a change nor leaves two files for one version range.
4. As the operator, an export query that fails or hangs fails the run, as the ingest query does.
5. As the developer of A9, each file has a modification time that falls inside its batch, so Freshness can be read from it.

## Failure scenarios

Seeded from lean-review's finder angles.

| Scenario | Expected |
|---|---|
| A crash after the file is written, before Spark commits the checkpoint | The replay has the same rows (fact 4), so the same name and content. It writes the directory again (question 2), and the file's time moves to the replay |
| A crash during the write | The replay's overwrite leaves one complete file under the same name |
| A crash in the first batch, with more commits before the restart | The first batch replays the same snapshot or range (fact 4); the new commits are the next file |
| Only `OPTIMIZE` commits since the last batch | No file (fact 2) |
| An idle store | No batch, no file |
| A Backfill round's reclassify | An `update_postimage`: exported as `op=upsert` |
| A physical delete in the feed (nothing writes one: Tombstones are kept) | Exported as `op=delete`, as v1 did |
| The export query fails | Its error comes out of `liveness.watch` and fails the task. The job's retries restart both queries from their checkpoints |
| The export query hangs | `Stalled` names `export` after the limit ([step-a6.md](step-a6.md)) |
| The ingest query fails while an export batch runs | `main` raises, and the process ends. The export batch replays at the restart |
| No checkpoint over a store with rows (the runbook's recovery) | One file with the current state: live rows as `op=upsert`, Tombstones as `op=delete` (fact 3). A consumer that replays from this file is exact |
| The export falls behind retention (`DELTA_CHANGE_DATA_FILE_NOT_FOUND`) | The run fails. The runbook (S3) says to delete `checkpoints/export`. Not reachable in one job that holds the writer and the VACUUM ([Change Export and maintenance][t156]) |
| Version 10 and later | Names are zero-padded to 12 digits, so name order is version order |
| A very large first batch (the 1M load, after a long stop) | One file. The collapse runs in Spark, not in Python, and no row reaches the driver |
| stdout fails | The `export` line is lost, not the batch (`ingest.say`) |

## Implementation decisions

1. **`src/ingestion/change_export.py`** (new; v1's `export.py` stays until S2): `start(spark, data, *, trigger)` and `apply_batch`. It needs pyspark, so it is not in the pure-module list.
2. **The collapse is Spark's:** drop `update_preimage`, then `max_by(struct(*), _commit_version)` per Listing key. One MERGE holds one row per key, so the version alone orders a key's rows. `op` is `delete` for a Tombstone or a physical delete, else `upsert`, v1's two values, so `replay.replay_exports` reads the files unchanged. v1's pure `collapse` is the test's reference.
3. **Every Listing Store column plus `op`,** as in v1.
4. **A file is a directory** (question 1) named by the batch's `_commit_version` range, zero-padded to 12 digits: `export/000000000041-000000000057.parquet/`, written with `coalesce(1)` and `mode("overwrite")` (question 2), so it holds one data file.
5. **No `startingVersion`.** The first start and the recovery after a lost checkpoint are then one path (fact 3). On a clean start the store is empty or one MERGE old. The ingest query pins its version because landing order needs every commit; the export needs only each key's latest row.
6. **No cap that can bind:** `maxFilesPerTrigger` = `ingest.MAX_FILES` and no byte cap, so a batch takes every commit that waits and never ends inside one (fact 5).
7. **Both queries get a name** (`queryName`: `ingest`, `export`), so `Stalled` and the progress lines name them.
8. **The `export` line** holds the batch id, the range and the row count, all from the one aggregate the name needs. The two test files of fact 9 already drop or filter lines by type; the spec's PR checks that they still pass.
9. **Free Edition gains one check:** "the export replays to the store".
10. **The job deletes no export file** (question 3). C1's Terraform adds a lifecycle rule on the `export/` prefix: delete 3 days after the last change.
11. **plan-v2 changes, in this spec's PR:** the A7 row names this spec, says a file is a directory that only a replay rewrites, and drops v1's adopt-a-file recovery. "Kept 3 days" moves to C1's row as the lifecycle rule. A9's row says its reader accepts an empty directory.

## Testing decisions

- **`tests/spark/test_change_export.py`** (new), in `make spark`, three local runs before the PR (plan-v2, Testing rules):
  1. The export oracle: after upserts, a delete, a Backfill round and an `OPTIMIZE`, `replay.replay_exports` over the files equals the live store. Red: no module.
  2. Each file equals v1's `collapse` over the same feed range: one row per key, every column plus `op`.
  3. No file for an idle store or a range of only `OPTIMIZE` commits.
  4. A crash after the write and before the checkpoint's commit, in the first batch and in a later one, with more commits before the restart: the files' names and rows equal an uninterrupted run's.
  5. A partial directory under a batch's name (what a crash during the write leaves): the replay leaves one complete file, and the oracle holds.
  6. No checkpoint over a store with rows: one file, and the oracle holds from it.
  7. Names sort in version order past version 9.
- **`tests/spark/test_liveness.py`,** through `ingest.main`:
  8. Both queries run, and the `export` line prints. An export batch that raises fails `main`. An export batch that hangs raises `Stalled`, which names `export`.
- **Free Edition:** the workflow on this PR's branch, with the new check.

## Questions

1. **Is an export "file" a directory that Spark writes, or one real file?**

   Context: the plan says "one Parquet file per batch". Spark writes a directory that holds one data file (fact 6), and a rename to one real file needs the cluster's Java file API, which serverless doesn't have (fact 8).

   - **Option A (recommended): a directory per batch.** `export/<first>-<last>.parquet/` with one data file inside. Spark, pyarrow and DuckDB read it by that path, and the same code runs locally, on Free Edition and on the paid cluster. Cost: a reader outside Databricks can see a half-written data file after a crash, until the replay replaces it (fact 7).
   - **Option B: one real file, by a rename.** Cost: about 25 more lines, no Free Edition check of the export, and two code paths.

   **Answer (Nov 19): a directory per batch.**

2. **Does a replay adopt the file a crash left, or write it again?**

   Context: a crash can come after a batch's file is written and before Spark commits the checkpoint. Spark then runs the batch again, with the same rows and so the same name (fact 4). v1 adopted the file it found, because its second attempt could cover a different version range. In v2 the range can't differ.

   - **Option A (recommended): write it again.** `mode("overwrite")`: the replay replaces the directory with the same content. No check, on any platform. Cost: the file's modification time moves to the replay, so Freshness for that one batch is measured to the second write, and a reader that opens the directory during the overwrite finds it missing for a moment. The plan's "never rewritten" becomes "rewritten only by a replay, with the same content".
   - **Option B: adopt it.** Before the write, the batch reads the directory; if it holds a complete file, the batch skips the write. Cost: about 8 more lines, one more Spark job per batch, and "complete" differs between local Spark and Databricks (fact 7), so the rule is first proved on Free Edition.

   **Answer (Nov 19): write it again.**

3. **Which step deletes export files after 3 days?**

   Context: the plan's A7 row says the files are "kept 3 days". Spark has no command that deletes a directory of plain files, so job code would need the cluster's file API (fact 8). No run lasts 3 days: a paid run ends within about 12 hours, and every exit attempt starts clean. Azure Storage has lifecycle rules: on an account with a hierarchical namespace, a delete rule removes the matching files, and then the empty directory in a later daily cycle ([policy structure][lifecycle]).

   - **Option A (recommended): a storage lifecycle rule, in C1's Terraform.** One rule on the `export/` prefix deletes files 3 days after their last change. The job holds no delete code. Cost: nothing deletes old files on local Spark or Free Edition (tests wipe their data); a directory can stay empty for up to a day, so A9's reader must accept an empty directory; the rule runs once a day, so "3 days" means 3 to 4.
   - **Option B: A8's maintenance thread.** The thread deletes directories older than 3 days, with its other upkeep. Cost: about 20 lines in A8, a file API that differs between local Spark and the cluster, and no Free Edition cover.

   **Answer (Nov 19): a storage lifecycle rule, in C1.**

## Out of scope

- The check job's freshness and the export oracle on Spark: A9.
- The runbook's line for a lost export checkpoint: S3.
- Deleting v1's `export.py` and `collapse.py`: S2.
- A consumer beyond the oracle.
- The lifecycle rule's Terraform: C1, from decision 10.
- Deleting `probe/a7`: when Phase A's probes end.

## Size

About 75 changed lines, tests left out, so one PR: `change_export.py` about 45, `ingest.py` about 8, `free_edition.py` about 15, plan-v2 about 3. About 180 of tests. A6 came in at about twice its estimate, so this may reach 150, the cap.

[t156]: https://github.com/Hien-Trinh/Shopping/issues/156
[t166]: https://github.com/Hien-Trinh/Shopping/issues/166
[probe]: https://github.com/Hien-Trinh/Shopping/tree/probe/a7
[acid]: https://docs.databricks.com/aws/en/lakehouse/acid
[kb]: https://kb.databricks.com/jobs/uncommitted-files-causing-data-duplication
[lifecycle]: https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-policy-structure
