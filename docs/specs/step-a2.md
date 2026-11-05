# Step A2: the ingest query, on `fake` (mini PRD)

Status: approved Nov 5: the decisions, the test points and all four questions. Plan row: [plan-v2.md, PR steps, A2](../plan-v2.md). Decisions from the [streaming job][t137], [Spark reads the Landing log][t131] and the [Oct 28 review][t166], which overrides the first two where they differ. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

A1 made the Landing log and an empty Outcomes table. Nothing reads the Landing log in Spark yet, and nothing writes Outcomes or a Spark Listing Store. Five facts the plan doesn't state shape this step (probed Nov 4 on PySpark 4.1.4, delta-spark 4.4.1, with Landing logs written by delta-rs 1.6.6):

- **A Delta DataFrame re-reads the table each time it runs.** After the MERGE, the same plan DataFrame read the new version, and so did a lazily persisted one. An Outcomes append computed from it would plan against the batch's own MERGE and report `already_applied` for every written Change.
- **The Spark env can't import the package.** G5's env holds Runtime 18's versions only for what it installs: `pydantic` and `deltalake` are missing, so `ingestion.plan`, `decide` and `classify` all fail to import there.
- **Pandas turns a column with nulls into floats.** A group holding 2^53 + 1 and a null read back 2^53. A plan that compares content hashes would then call a replay a `conflict`.
- **`DeltaConcurrentModificationException` doesn't catch the conflict.** In delta-spark's Python classes, `ConcurrentAppendException` isn't its subclass, so the retry has to name each class.
- **The byte cap doesn't bound a batch.** The source admits a file while it has capacity left, so a batch is the cap plus one commit, and no cap splits a commit. A restart replays a batch at its recorded offsets, even under a lower cap.

## Solution

`python -m ingestion.ingest --data DIR` runs one streaming query. Each micro-batch:

1. Reads the Listing Store at a pinned version and left-joins it to the batch's inserts.
2. Runs v1's `decide` per Listing key through `groupBy(...).applyInPandas`, in landing order, giving one Outcome row per Change and at most one Write row per key.
3. Runs one conditional MERGE of the Write rows, retried when a concurrent `OPTIMIZE` fails it.
4. Runs one Outcomes append with `txnAppId` and `txnVersion`.

The Spark checkpoint commits after that, so a crash anywhere replays the batch, and v1's replay rule makes that safe.

## User stories

1. As the operator, I want the job to apply the Landing log with v1's rules, so that the store oracle holds as it did in v1.
2. As a Merchant, I want one Outcome per Change whatever the batch boundaries were, so that a backlog doesn't change my result.
3. As the operator restarting after a crash, I want no lost Change and no duplicate Outcome row, so that a restart needs no cleanup.
4. As the operator, I want a recreated Landing log, a lost checkpoint or a lost Listing Store refused, so that a mistake can't silently skip or re-report Changes.
5. As A4, I want the batch to be one function with `fake` in one place, so that the student replaces it without touching the query.
6. As the API reading Outcomes (S1), I want the table to stay readable by delta-rs after Spark's appends, so that GET works.

## Failure scenarios

Seeded from lean-review's finder angles.

| Scenario | Expected |
|---|---|
| Crash after the plan, before the MERGE | Replays; Outcomes are as if nothing happened |
| Crash between the MERGE and the Outcomes append | Replays. A written Change reports `already_applied` or `stale`, and the Listing Store is correct (rule 3) |
| Crash after the Outcomes append, before the checkpoint commits | Replays. The MERGE changes nothing, and the append is skipped by `txnVersion`: no duplicate rows |
| Executor lost, or the persisted plan evicted, mid-batch | The plan recomputes against the pinned version, so the MERGE and the Outcomes still agree |
| A kill after the first start, before its first offset | Spark has written the checkpoint's `metadata` by then, so the job looks in `offsets` and `commits` instead. Probed: going by `metadata` alone skipped `startingVersion`, and the later commit of a tie won |
| A crash in batch 0, before it commits | Its replay has no start offset, so the job keeps `startingVersion` until `commits` holds a batch (decision 2). Without it the replay reads a snapshot: the store oracle differed on a tie, and Outcomes with it |
| Changes land in several commits before the first start | `startingVersion` keeps each commit's own version, so first-one-wins matches the store oracle. Without it the first batch would be a snapshot in no order |
| Two commits hold the same key and version with different content | The earlier commit wins and the later is `conflict`, in one batch or across two |
| The byte cap splits a backlog | Batches end on file boundaries, and one group commit is one file (probed). The same Landing log at any cap gives the same store and Outcomes |
| A batch outlives A6's watchdog | It replays at the same offsets, so a lower cap can't shrink it. The cap and the group bound (question 2) keep a batch to about 30k Changes, about 460 s at 65/s. A6's spec says what happens past that |
| A retention DELETE or a compaction lands in the Landing log | DELETE rows are `_change_type = 'delete'` and dropped; compaction adds no feed rows. A commit with no inserts makes no Outcomes commit, and a MERGE with an empty source commits nothing (probed) |
| A Change's content can't be stored | `failed` alone, as v1's `decide` does, and the rest of its key's Changes plan on |
| A Change's submission id isn't a UUID | `tables.hour` raises, so the batch fails, and so does every restart. The API mints only UUIDv7s, so nothing writes one in this step. v1's `backfill-…` and `chaos-…` ids do: see Out of scope |
| A concurrent `OPTIMIZE` fails the MERGE (local open-source Delta) | Retried in the batch, 3 attempts in all, then raised: the query fails and the batch replays. Probed: a real `OPTIMIZE` inside a MERGE raises `ConcurrentAppendException` |
| The Landing log is recreated under a checkpoint | Refused natively. Probed: `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE` |
| No checkpoint over a non-empty Outcomes table | Refused, exit nonzero, naming both paths and saying that a clean start wipes them (rule 4, question 4). A new query id would append every Outcome again: probed, 7 rows became 14 |
| A checkpoint over a missing Listing Store | Refused, exit nonzero, naming the path. Otherwise the job creates an empty store and carries on: probed, no error, and the oracle differed on 3 keys |
| The init hasn't run | `start` opens the Landing log with `DeltaTable.forPath`, which fails naming the path; `readStream` alone says only `DELTA_SCHEMA_NOT_SET`. The job creates nothing but the Listing Store |
| Two runs on one checkpoint | Outside this step: the job's one-run limit is C2's. Until then the MERGE's condition refuses the stale row and decision 8's check raises, so a refused Write is never reported `written` |

## Implementation decisions

1. **`src/ingestion/ingest.py`** (new), the query's shell: `start(spark, data, *, trigger)`, `apply_batch` and `main` through `entry.exit_with`. `--data` stays a string, since `Path` turns `abfss://c@a/x` into `abfss:/c@a/x`. `main` uses `SparkSession.builder.getOrCreate()`: the active session on Databricks, the fixture's in tests. A11 adds a local launcher when it first runs the job as a process.
2. **The source:** `readChangeFeed`, `maxBytesPerTrigger` (question 2), `maxFilesPerTrigger` = 100,000 so the byte cap binds. `startingVersion` = the lowest version in the Landing log's history (0 after a clean start), read until batch 0 commits (`{checkpoint}/commits` holds a batch): a later restart ignores the option, and the read collects every commit on the driver. Batch 0's replay doesn't ignore it: it has an offset but no start offset, so without the option Delta reads it as a snapshot, every row at one `_commit_version` (test point 6 found this). The trigger is `processingTime` 60 s in `main`; tests and A3 pass `availableNow`.
3. **The plan pins, then persists.** `apply_batch` reads the store's head version, reads with `versionAsOf`, keeps only the batch's keys with a broadcast semi join (a left join can't broadcast the batch, so it would shuffle the whole store), persists the plan and unpersists it in a `finally`, since otherwise each batch's plan stays cached (probed). The pin makes it correct and the persist makes it cheap, so eviction can't change an Outcome. This isn't the time-travel plan rule 3 dropped: that planned a replay against the version before its own MERGE, whereas this reads the head at the batch's start, so a replay still sees its earlier MERGE. Rejected: `localCheckpoint(eager=True)`, which also held in the probe, because Databricks' [serverless limits](https://docs.databricks.com/aws/en/compute/serverless/limitations) refuse `df.checkpoint()` and don't mention `localCheckpoint`, and A3 runs on serverless.
4. **Per key, it reuses v1:** rows sorted by `(_commit_version, seq)` become `Landed` through `landing._landed`, the stored row becomes `Stored` through `store._stored`, and `decide.decide(landed, stored, FakeClassifier())` does the rest, poison included. The two helpers, `store._row` and `store._APPLY_IF` lose their underscores, and `row` no longer takes `updated_at`. The classify notes and `failed`'s error text go nowhere for now: Outcomes has no column for them, and A6's spec decides whether the job logs them (rule 6). A4 splits classification out of the per-key function.
5. **Pandas stays safe by shape.** The stored columns repeat one row per group, so a group's column is all null or all not, and `to_dict("records")` gives Python ints. The output frame is `dtype=object`. A test holds this.
6. **One output schema, two kinds of row:** Outcome rows carry `submission_id` and Write rows don't. `store.SCHEMA` without `partition` and `updated_at` supplies the Write columns through `from_arrow_schema`, and then every output field is made nullable: a null in a non-nullable output column fails the task (probed), and Delta still enforces NOT NULL at the MERGE and the append. `updated_at` is `current_timestamp()` in the MERGE's source.
7. **The Listing Store** (`data/listing_store`): the job creates it if missing, with the same columns as v1's minus `partition`, `CLUSTER BY (merchant_id, merchant_product_id)`, deletion vectors and the change feed on (for A7). Probed: reader 3 / writer 7. Only Spark reads it from here on.
8. **The MERGE** keeps v1's condition (`APPLY_IF`) and v1's check: `execute()` returns `num_affected_rows`, and fewer than the Write rows raises, as `Overtaken` does, so the batch replays and reports `stale`. Probed: a refused row returns 0 and commits nothing. There is no txn on the MERGE (rule 3). The retry catches `ConcurrentAppendException`, `ConcurrentDeleteReadException` and `ConcurrentDeleteDeleteException`.
9. **Outcomes** gain no columns and one property: `delta.autoOptimize.optimizeWrite`, which the Listing Store also sets, so a batch writes one file per table (per hour, for Outcomes) instead of one per shuffle partition. Probed at Spark's default 200: 174 files a batch in each table. `hour` comes from `tables.hour`. A batch with no Outcome rows appends nothing. `txnAppId` is the query id, read from `{checkpoint}/metadata` with Spark. Probed: it equals `query.id`, and it works where a closure over the session wouldn't (Spark Connect on serverless). A read of `{checkpoint}/offsets` tells the start guard whether the query has recorded a batch.
10. **The checkpoint** is `data/checkpoints/ingest`, next to the tables (rule 4), and goes with them in a clean start.
11. **The start guard** runs in `start`, before the stream, in this order: it opens the Landing log and Outcomes with `forPath`, refuses a checkpoint with no Listing Store, refuses no checkpoint over non-empty Outcomes, and only then creates the store if missing. It goes by `offsets`, not `commits`: a crash after batch 0's append leaves offsets, Outcomes and no commit, and that must replay. There is no flag: a clean start wipes Outcomes and the checkpoint together (Oct 28 review, item 3), so it passes (question 4).
12. **The group bound:** `Appender.run` holds back a request that would take its group past 10k Changes (`envelope.MAX_BATCH`), for the next commit. No commit then holds more than one full request, so a batch is at most the cap plus 10k Changes (question 2).
13. **The env:** `pydantic` and `deltalake` go in `requirements/databricks.in`, pinned by the Runtime 18 constraints to 2.10.6 and 1.1.4 (question 1). The job never calls delta-rs; it only needs the modules that import it to load.

## Testing decisions (test points for your OK)

All on real local Delta tables, with the Landing log written by delta-rs 1.6.6 (question 1). Landing logs and Outcomes are created by `tables.init`. A query run takes 5–7 s, so `tests/spark/test_ingest.py` should take about 2.5 minutes.

- **`tests/spark/test_ingest.py`** (new), in `make spark`:
  1. Changes landed in several commits before the first start, an earlier commit's row holding a higher `seq` than a later commit's: the store matches `replay.expected_store`, with one Outcome per Change covering `written`, `stale`, `already_applied`, `conflict`, `failed` and a Tombstone. Red: no module, then no `startingVersion`. A checkpoint holding only `metadata`, as a kill before the first offset leaves it, gives the same result.
  2. The same Landing log at a tiny cap (more than one batch, from `recentProgress`) and a large one gives equal stores, `updated_at` aside, and equal Outcomes. Red: the sort reduced to `seq`.
  3. A price of 2^53 + 1 replayed unchanged is `already_applied`, and the stored price is exact.
  4. A delta-rs compaction and a retention DELETE, in a batch of their own, add no Outcome and no Outcomes commit. Red: no empty-batch check.
  5. The layouts: the store's clustering columns, deletion vectors and change feed. Outcomes, after at least 12 Spark appends so that delta-rs reads Spark's version-10 checkpoint: still reader 1 / writer 2 with no table features in the protocol (`DESCRIBE DETAIL` lists `appendOnly` and `invariants` even so), and read back by delta-rs 1.6.6 (risk B1). At conftest's 4 shuffle partitions a batch writes one file per table.
  6. The three crash points above, each in batch 0 and in a later batch (only batch 0's replay has no start offset), with a test wrapper that raises around `ingest.merge` or `ingest.append_outcomes` in a real query, then a restart from the checkpoint: the store oracle holds, with exactly one Outcome row per Change. The first crash's Outcomes equal an uncrashed run's. The second's replay holds no `written`, which pins rule 3, since a time-travel plan would report `written`. The wrappers live in the tests; the shipped job has no hooks (rule 11).
  7. A plan recomputed after its MERGE, with the persist dropped (`spark.catalog.clearCache()`), gives the same Outcomes. Red: read without the pin.
  8. The start guard, through `main` in process for the refusals: no checkpoint over non-empty Outcomes is refused, a checkpoint with no Listing Store is refused, and a missing init names the Landing log's path. No checkpoint over empty Outcomes starts.
  9. A recreated Landing log raises `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`.
  10. `ingest.merge` with a real `OPTIMIZE` run while its source waits on a file gate retries once and lands, by attempt count. The source has one partition and the gate a deadline: two gated partitions hold both of `local[2]`'s slots, so `OPTIMIZE` hangs (probed). A new small file and an `OPTIMIZE` on every attempt raises after 3. A row the condition refuses raises.
- **`tests/integration/test_appender.py`,** in `make check`: requests over the group bound, queued at once, land in more than one commit, none over 10k Changes, each request whole and in arrival order.
- **`tests/unit/test_properties.py`,** in `make check`: `plan` per key, reassembled, equals `plan` over the whole batch, so `plan`'s property test covers what the job runs.
- **Not tested here:** an earliest retained version above 0, since nothing removes Landing log history until A10 and a running stream never reads `startingVersion` again. Probed once: after delta-rs's `cleanup_metadata`, a first start at the earliest version gave every retained Change an Outcome. The 60 s trigger and the cap's real rate are P3's. Coverage isn't gated in `make spark`, as in G5.

## Questions

1. **How do the Spark env's tests get a delta-rs 1.6.6 Landing log, and how does the Spark env import the package?** (A1 left the first to this spec.)
   - Subprocess plus the two lines: `tests/spark/land.py` runs through `uv run --frozen` and writes with the API's own 1.6.6, and `databricks.in` gains `pydantic` and `deltalake` for imports only. About 30 lines. A11 runs both envs anyway. The costs: `deltalake` 1.1.4 loads on every executor for as long as `ingest` imports `landing` and `tables`, which S2 keeps, and the `spark` CI job also syncs the uv env.
   - Move the code instead: split `store`'s row mapping, `Landed` and `hour` out of the modules that import `deltalake`. About 80 moved lines in a step already over size, which S2 would otherwise do. Only `pydantic` goes in the env.
   - Test with 1.1.4 in process: no subprocess, but not the writer the API runs, so risk B1 goes unproven.

   I recommend the first. If A3 finds a serverless clash with 1.1.4, deferring the `deltalake` imports is about 32 lines in 4 modules (probed).

   **Answer (Nov 5): the subprocess plus the two lines.**

2. **What does "10k Changes' worth" of bytes mean, and what bounds a batch?** The cap sets the steady rate, and the commit size sets the bulk batch. Measured with the real API and `load`'s real texts on local disk. Bulk commits vary with timing, so a batch cell is the largest seen, and its seconds are at the 65/s the Oct 28 review expects on the student, against A6's 600 s watchdog.

   | Load | Changes per commit | Bytes per Change | Batch at 11 MB | Batch at 8 MB | Batch at 3.5 MB |
   |---|---|---|---|---|---|
   | Steady 50/s, 2 replicas, 8 processes | 4 | 1,307 | 8.4k | 6.1k | 2.7k |
   | Steady 50/s, 2 replicas, 16 processes | 8 | 857 | 12.8k | 9.3k | 4.1k |
   | Steady 50/s, 1 replica, 16 processes | 16 | 628 | 17.5k | 12.7k | 5.6k |
   | Bulk `--batch 1000`, 2 replicas | 8k | 341 | 40k, 615 s | 24k, 369 s | 16k, 246 s |
   | Bulk `--batch 1000`, 1 replica | 16k | 342 | 48k, 738 s | 32k, 492 s | 21k, 323 s |
   | Bulk `--batch 10000`, 2 replicas | 10k–40k | 346 | 70k, 1,077 s | 40k, 615 s | 40k, 615 s |
   | Bulk `--batch 10000`, 1 replica | 30k–70k | 345 | 100k, 1,538 s | 70k, 1,077 s | 70k, 1,077 s |
   | Any bulk, with the group bound | 10k | 355 | 40k, 615 s | 30k, 461 s | 10k, 154 s |

   - 8 MB and a group bound of 10k Changes. A steady batch never falls under the Oct 28 review's floor of 6k, and a bulk batch is at most the cap plus 10k: about 30k with `load`'s texts, about 460 s. The bound is about 10 lines in `Appender.run`, on the request path.
   - 8 MB, no bound, and P3's bulk is `--batch 1000`. No API change, but a Merchant's concurrent 10k requests can still make a batch the watchdog stops on every retry.
   - 11 MB, as the draft had it: over the watchdog on every bulk row, the bound included.
   - 3.5 MB: 2.7k a minute at the worst steady shape, 45/s, below the 50/s inflow.

   I recommend the first, with 8 MB as a constant P3 retunes.

   **Answer (Nov 5): 8 MB and a group bound of 10k Changes.**

3. **Does A2 stay one PR?** The estimate is about 470 changed lines against the limit of about 300.
   - Two PRs under this spec. **A2.1, about 300 lines:** the env, the query, the batch and the layouts, with test points 1–5, 7 and the property test. **A2.2, about 170 lines:** the start guard, the MERGE's retry and check, the group bound, and test points 6, 8, 9 and 10 with the Appender's. Nothing else merges between them, and the plan's row stays one row.
   - One PR of about 470 lines, reviewed in one lean-review round.

   I recommend two: the restart behaviour gets a review of its own, and it's where the concurrency finder earns its cost.

   **Answer (Nov 5): two PRs.**

4. **What does "declared clean" mean in rule 4?** A clean start wipes Outcomes and the checkpoint together, so it already passes the guard. A flag would only matter over non-empty Outcomes, where the start can't be clean.
   - Drop the flag and reword rule 4: "The job refuses to start with no checkpoint over a non-empty Outcomes table. A clean start wipes both, so it passes." A lost checkpoint then means a new attempt, as rule 7 already has it.
   - Keep `--clean-start` as "accept a second set of Outcomes". It deletes nothing, the rows double (probed), GET's best-Outcome rule hides them and the store oracle stays clean. About 10 more lines and a test, for a recovery nothing in the plan uses.

   I recommend dropping it.

   **Answer (Nov 5): drop the flag and reword rule 4.**

## Out of scope

- The student, the iterator UDF and repartitioning to the slot count: A4.
- The Backfill tail and `op=reclassify` in the job: A5, which also makes `tables.hour` read its `backfill-…` ids.
- The watchdog, the progress listener and the job's retry settings: A6.
- Change Export: A7. `OPTIMIZE`, Snapshots and `VACUUM` on the Listing Store: A8.
- The check job's Spark oracles: A9. These tests call the pure `replay` functions directly.
- The poison leg: A11, whose writer mints a UUIDv7 instead of v1's `chaos-…` id.
- Registering the Listing Store in Unity Catalog, and the job's one-run limit: C2. Its external table must carry decision 7's layout, since `CREATE TABLE IF NOT EXISTS` never checks one. The `--data` of `tables` and `api` takes a `Path`, which C2's `abfss://` paths would break.
- What only Databricks can show, for A3's first run: `delta.exceptions` importing on serverless, `execute()`'s metrics on Runtime 18's fork, `deltalake` loading in a UDF sandbox and `entry.exit_with`'s `os._exit` in a wheel task.
- S1's read of Outcomes prunes files by `hour`. At one file a batch Outcomes grows by about 1,440 files a day, and it needs no compactor over a run of at most 72 h.
- The whole-store scan each batch: the semi join (decision 3) stops the plan shuffling the store, but it and the MERGE still read every file, unless Databricks' file pruning helps. Probed on `local[2]` at 500k Listings: 4–7 s for the plan (6–10 s and 167 MB shuffled without the semi join) and 13 s for the MERGE. P3 records per-batch time, but holds only about 150k Listings; P6 is the first run at 1M.
- Deleting `store`: S2's spec keeps or moves `SCHEMA`, `row`, `stored`, `unstorable` and `APPLY_IF`, which `ingest` and `decide` use.

## Size

About 470 changed lines: `ingest.py` about 180; the renames and `databricks.in` with its compiled file about 30; `land.py` about 30; the group bound about 10; about 220 of tests.

[t131]: https://github.com/Hien-Trinh/Shopping/issues/131
[t137]: https://github.com/Hien-Trinh/Shopping/issues/137
[t166]: https://github.com/Hien-Trinh/Shopping/issues/166
