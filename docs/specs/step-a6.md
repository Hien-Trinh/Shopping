# Step A6: liveness (mini PRD)

Status: draft, Nov 11. Question 1 is answered (Option A); the test points wait for your OK. Plan row: [plan-v2.md, PR steps, A6](../plan-v2.md). Decisions from the [streaming job][t137] (item 8), [events and oracles][t160] (items 2 and 5) and the [Oct 28 review][t166] (items 2 and 13). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

v1 had a supervisor and heartbeats: a worker that stopped making progress was killed and restarted. The streaming job has neither. Spark has no task timeout, so a batch that never returns holds the job, and its cluster's bill, until the run's deadline. The job also prints nothing about its batches, and A4 and A5 left their diagnostics to this step. Seven facts shape it. "Probed" means local Spark on Nov 11 (PySpark 4.1.4, delta-spark 4.4.1, JDK 21, `local[2]`), with the files in `probe/local/` on the unmerged branch [`probe/a6`][probe].

1. **A hung batch shows in the query's status.** Probed: while a batch was stuck, `query.status` said `isTriggerActive: true` ("Processing new data") and `query.lastProgress` did not change. An idle query said `isTriggerActive: false` ("Waiting for next trigger").
2. **`stop()` ends a batch that is stuck in a Spark job.** Probed: with the batch waiting on a task that slept 120 s, `query.stop()` returned at once and the query ended with no exception.
3. **`stop()` blocks on a batch that is stuck in Python.** Probed: with the batch function waiting on a lock, `stop()` did not return for 20 s, until the lock opened, though `isActive` was already false. So the watchdog can't wait for `stop()`.
4. **An idle query reports about every 10 s, as `onQueryIdle`.** Probed: at a 2 s trigger, `lastProgress` moved at 17 s, 27 s and 39 s after the one batch, and a listener got one `onQueryProgress` and then `onQueryIdle`. Databricks says the same: `onQueryProgress` comes only at a batch's end, and during a long batch neither may come "even though the query is healthy" ([stream monitoring][monitoring]).
5. **A Python `StreamingQueryListener` gets each batch's progress as one JSON line.** Probed: `event.progress.json` holds `batchId`, `numInputRows`, `durationMs` (`addBatch` 5,500 of `triggerExecution` 5,764 ms) and each source's offsets. On Databricks the Delta source adds its backlog, `numBytesOutstanding` and `numFilesOutstanding` ([stream monitoring][monitoring]). The listener is in Python and Scala from Runtime 11.3 LTS.
6. **Serverless runs only `AvailableNow`** ([serverless limitations][limits]), and the Free Edition checks call `ingest.start`, never `ingest.main`. So a watchdog and a listener that `main` adds never run there.
7. **A replay keeps its offsets** ([step-a2.md](step-a2.md)), so a batch that the watchdog stops comes back the same size.

## Solution

```text
ingest.main, the new parts marked +:
  + add the listener: one JSON line per finished batch, one when a query ends
  start the ingest query (A7 adds the export query)
  - awaitTermination()
  + liveness.watch(queries), every 10 s, for each query:
  +   it ended           -> return, or raise its error
  +   a batch finished, or no trigger is active -> restart its timer
  +   a trigger has been active for 10 minutes with no finished batch
  +                      -> stop every query without waiting, raise Stalled
apply_batch:
  + print one "batch" JSON line: the batch id, its Write rows, its Backfill round's size
```

`Stalled` fails the task. The job's retries start it again from the checkpoint, and a run that uses them up ends with an email.

## User stories

1. As the operator, I want a batch that never returns to fail the run within about 10 minutes, so that a hung job doesn't bill until its deadline.
2. As the operator, I want one line per batch in the cluster log (its size, its duration, the backlog), so that I can see where a paid run spent its time.
3. As the developer, I want the stall rule to be a pure function with an injected clock, so that its tests need no Spark and no sleep.
4. As the developer of A7, I want the watchdog to take a list of queries, so that the export query joins it with one line.

## Failure scenarios

Seeded from lean-review's finder angles.

| Scenario | Expected |
|---|---|
| A batch stuck in a Spark job (an executor that never answers) | After the limit, `watch` stops the query, which ends the batch (fact 2), and raises `Stalled`. The task fails, and the retry replays the batch |
| A batch stuck in Python on the driver | `stop()` would block (fact 3), so `watch` calls it on a daemon thread and raises without waiting |
| An idle query, for hours | No trigger is active, so its timer restarts at every poll (fact 1). Never stalled |
| A healthy batch that takes longer than the limit | Stalled, by the plan's rule. It replays at the same offsets (fact 7), so every retry stalls too, and the run fails with its email. The cap keeps a batch near 460 s ([step-a2.md](step-a2.md)); `--watchdog` raises the limit for a rerun |
| One of two queries stalls while the other makes progress (A7) | Each query has its own timer, so the stalled one fires |
| A query fails by itself | `awaitTermination` raises its error out of `watch`, as `main` did before |
| A query is stopped from outside (the run is cancelled) | `watch` returns and `main` ends |
| `stop()` on the daemon thread never returns | The raise already happened. Locally, `entry.exit_with` ends the process. On Databricks the task fails; what the cluster does with the stuck thread is not probed, and no paid fault hangs a batch (plan-v2, section C) |
| The watchdog fires during the first batch, before any progress | Its timer starts when `watch` starts |
| The listener raises | Spark logs it and the query goes on. The listener only prints |
| The listener is slow | It only prints one line; Databricks asks for little work in a listener ([stream monitoring][monitoring]) |
| A crash between a batch's MERGE and its `batch` line | The line is lost, and the replay prints its own. Diagnostics may miss or repeat a batch (rule 6: no fact is read from them) |
| The `batch` line on Free Edition | It prints counts only, never a duration, and the workflow's log shows only PASS and FAIL lines ([step-a3.md](step-a3.md)) |

## Implementation decisions

1. **`src/ingestion/liveness.py`** (new), with no pyspark import, so it runs in `make check`:
   - `stalled(since, mark, seen, active, now, limit) -> (since, seen, expired)`, the pure rule: the timer restarts when the progress mark changes or no trigger is active; it expires when a trigger has been active for more than `limit` seconds since.
   - `watch(queries, *, limit, poll=10.0, clock=time.monotonic)`: the loop above over anything with a query's `awaitTermination`, `lastProgress`, `status`, `stop`, `name` and `id`. A query's mark is its last progress's `(batchId, timestamp)`.
   - `Stalled`, a `RuntimeError` that names the query and the limit.
2. **"Input waits" means a trigger is active** (fact 1). The plan says the watchdog fires "when no batch finishes in 10 minutes while input waits". A stuck batch has input by definition, and an idle query has no active trigger.
3. **The watchdog raises without waiting for `stop()`** (fact 3). It calls `stop()` on every query, each on a daemon thread, then raises.
4. **`ingest.Progress`, the listener:** `onQueryProgress` prints `event.progress.json`; `onQueryTerminated` prints one line with the query's id and its exception; the other two do nothing. `main` adds it before the query starts. Lines go to stdout, which cluster log delivery copies to the volume ([events and oracles][t160]).
5. **The `batch` line** (question 1, Option A), printed by `apply_batch` after the Outcomes append: `{"type": "batch", "batch_id": …, "writes": …, "round": …}`. Both counts are ones the batch already holds, so the line adds no Spark job.
6. **`main`:** `--watchdog SECONDS`, 600 by default. `awaitTermination()` becomes `liveness.watch([query], limit=a.watchdog)`.
7. **The job's settings are recorded here, not coded.** C2's Terraform applies them: `max_retries` = 3 (fault-1 firings + 3 in a paid run), `min_retry_interval_millis` = 30,000, an email on run failure, and a `STREAMING_BACKLOG_BYTES` health rule above 12 MB (1.5 × `ingest.MAX_BYTES`), a notice and not a gate.
8. **plan-v2 changes, in this spec's PR:** the A6 row names this spec.

## Testing decisions (test points for your OK)

- **`tests/unit/test_liveness.py`** (new), in `make check`, with fake queries and an injected clock, no sleep:
  1. A query whose trigger stays active with no new progress past the limit: `watch` raises `Stalled`, which names the query. Its `stop()` was called, and a `stop()` that never returns does not hold the raise. Red: no module.
  2. Never stalled: a query that finishes a batch at every poll, and an idle query, each for ten times the limit. `watch` returns when a query ends.
  3. A batch that finishes just under the limit restarts the timer, so the next batch gets the whole limit.
  4. Two queries: one stalls while the other finishes batches, and `Stalled` names the first.
  5. A query that fails: its error comes out of `watch`.
- **`tests/spark/test_liveness.py`** (new), in `make spark`, through `ingest.main`:
  6. A real hung batch: a test wrapper holds `apply_batch` on a lock, with a 2 s trigger and `--watchdog 3`. `main` raises `Stalled` within 15 s, with nothing committed. With the lock open, a restart from the checkpoint lands the batch: the store and Outcomes equal an unhung run's.
  7. The lines: an `AvailableNow` run through `main` prints one progress line per batch that parses as JSON with `batchId`, `numInputRows` and `durationMs`, one `batch` line with the batch's Write count and round size, and one line when the query ends.
- **A2's, A4's and A5's tests,** unchanged.
- **Free Edition:** A5's owed run on `main` comes first (plan-v2, Testing rules). Then the workflow on this PR's branch: the existing checks, through a batch that prints its line.

## Questions

1. **What does the job log about classifying and the Backfill?**

   Context: v1 emitted a `classify` event (batch size, time), a `classify_failed` event (how many Listings got no answer, and why) and a `backfill` event (the round's size, plus the totals of flagged and outdated Listings). A4 and A5 left these to this step. Rule 6 says facts come from tables, and diagnostics are only for a person reading the log. In the job the classifier runs inside the MERGE on the executors, so the driver doesn't see its failures: counting them needs one more read of the store per batch. The totals need a scan of every row per batch.

   - **Option A (recommended): only what the driver already holds.** Spark's progress line gives the batch's size, duration and backlog. The `batch` line adds the Write count and the round's size. Classifier failures show in the table: the check job (A9) computes the Uncategorized rate from the Listing Store. Cost: the log doesn't say which batch a classifier outage started in.
   - **Option B: also count failures and totals.** After the MERGE, the batch reads its keys back and counts the rows left flagged, and one aggregate counts every flagged and outdated row. Cost: two more Spark jobs per batch, about 25 more lines, and the totals scan grows with the store.

   **Answer (Nov 11): only what the driver holds.**

## Out of scope

- The export query in the watchdog: A7 adds it to `main`'s list.
- The maintenance thread's lines (`OPTIMIZE`, Snapshots, `VACUUM`): A8.
- Terraform for the retries, the email and the backlog alert: C2, from decision 7.
- A hung batch on Databricks: no paid fault makes one (plan-v2, section C). P3's fault 1 shows that a failed task retries.
- The listener on serverless: `main` never runs there (fact 6).
- Retuning the 10-minute limit: the paid smoke check (P3), with the trigger and the cap.
- Deleting `probe/a6`: when Phase A's probes end.

## Size

About 85 changed lines, tests left out, so one PR: `liveness.py` about 45, `ingest.py` about 35 (the listener, the `batch` line, `main`), plan-v2 about 2. About 170 of tests. A1 to A4 came in 22–109% over their estimates and A5 on its estimate, so this may reach about 180, under the cap.

[t137]: https://github.com/Hien-Trinh/Shopping/issues/137
[t160]: https://github.com/Hien-Trinh/Shopping/issues/160
[t166]: https://github.com/Hien-Trinh/Shopping/issues/166
[probe]: https://github.com/Hien-Trinh/Shopping/tree/probe/a6
[monitoring]: https://docs.databricks.com/aws/en/structured-streaming/stream-monitoring
[limits]: https://docs.databricks.com/aws/en/compute/serverless/limitations
