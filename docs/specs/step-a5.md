# Step A5: the Backfill as each micro-batch's tail (mini PRD)

Status: approved Nov 11: the decisions, the test points and both questions (Option A for both). Plan row: [plan-v2.md, PR steps, A5](../plan-v2.md). Decisions from the [streaming job][t137] (item 5) and v1's Backfill ([step-6c.md](step-6c.md)). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

A4 leaves a Listing flagged `needs_reclassify` when the classifier can't answer, and a new student version leaves every stored Listing on the old one. v1's Backfill fixed both: a separate process that appended `op=reclassify` Changes to the Landing log. In v2 only the API writes the Landing log (rule 1), so the Backfill runs inside the Ingestion job. Five facts shape this step. "Probed" means local Spark on Nov 11 (PySpark 4.1.4, delta-spark 4.4.1, JDK 21, `local[2]`), with the file in `probe/local/` on the unmerged branch [`probe/a5`][probe].

1. **A trigger with no new input runs no batch.** Probed: with one landed Change and a 2 s `processingTime` trigger, `apply_batch` ran once in 14 s. Spark's guide says the same: "If no new data is available, then no micro-batch will be kicked off" ([triggers][triggers]). So a tail runs only when a Merchant Change arrives (question 1).
2. **A `rate` source unioned to the change feed makes every trigger run a batch.** Probed: 8 batches in 14 s at a 2 s trigger, the one Change in batch 0 only, and an `AvailableNow` run on the same checkpoint ended after one batch.
3. **The plan already knows `op=reclassify`.** `plan.plan` marks a live Listing for classifying and reports `reclassified`, or `skipped` for a Tombstone or a missing row. `decide.provisional` keeps the stored answer, flagged, when the classifier fails. A reclassify Write keeps its source version and content hash, so the MERGE's condition (`store.APPLY_IF`) accepts it.
4. **`tables.hour` raises on a `backfill-…` id,** so one Backfill Outcome would fail the batch and every restart ([step-a2.md](step-a2.md), Failure scenarios).
5. **The job can't import `api.uuid7_at`.** `api` imports FastAPI, which the Spark env and Runtime 18 don't hold.

## Solution

```text
each micro-batch (ingest.apply_batch), the new parts marked +:
  read the Listing Store at the pinned version
  + pick up to LIMIT pending Listings from that read, oldest updated_at first:
  +   live, and flagged needs_reclassify or on another version than the classifier's
  + turn each pick into an op=reclassify row, placed after every Merchant Change
  +   of its key, under one backfill-<uuid7> submission id
  plan per Listing key, over the Merchant Changes + and the picks
  classify the marked Writes, MERGE, append the Outcomes, as in A4
```

One plan, one MERGE and one Outcomes append per batch, as today (question 2).

## User stories

1. As the operator, I want Listings that an outage left Uncategorized to get a real Category once the classifier is back, with no Merchant action on those Listings.
2. As the operator, I want every live Listing to move to a new student version without a reload.
3. As a Merchant, I want my Changes to stay fresh during a big Backfill: a batch never holds more than `LIMIT` reclassifies.
4. As the developer, I want the Backfill to use the plan's existing `reclassify` rule, so that its property test still covers what the job decides.

## Failure scenarios

Seeded from lean-review's finder angles.

| Scenario | Expected |
|---|---|
| Nothing is pending | The batch is A4's: no extra Outcome row, and the same commits |
| A Listing flagged by an outage, the classifier back | The next batch that runs picks it. It gets the classifier's answer, unflagged, and a `reclassified` Outcome |
| The classifier is still down | The pick keeps its stored answer, still flagged, with a new `updated_at`, so it goes to the back of the queue. Its Outcome is `reclassified`, as in v1. One round per batch, so no hot loop |
| A new student version, 1M Listings on the old one | `LIMIT` per batch: about 17 h at 1,000 per 60 s (v1: about 3 h at 1,000 per 10 s). P4 retunes `LIMIT` with the trigger |
| No Merchant Change arrives | No batch runs (fact 1), so pending Listings wait for the next Change from any Merchant (question 1, an accepted gap) |
| A Merchant Change for a picked key in the same batch | The pick sorts after it. An upsert: one Write, classified once, Outcomes `written` and `reclassified`. A delete: a Tombstone, and the pick is `skipped` |
| A picked row is a Tombstone or on the classifier's version, unflagged | Never picked |
| More pending than `LIMIT` | The oldest `LIMIT` by `updated_at`, ties by Listing key. The rest wait for later batches |
| The persisted plan is evicted | The picks were collected on the driver once, so the recomputed plan holds the same picks |
| A crash before the MERGE | Nothing committed. The replay picks the same rows from the same store version, under a new id |
| A crash between the MERGE and the Outcomes append | The replay finds those picks no longer pending and picks the next ones. The first picks are reclassified in the store, with no Outcome row. No Merchant reads a Backfill Outcome, and the oracles don't count them (rule 3's cost, for the Backfill) |
| A crash after the Outcomes append | The replay's append is skipped by `txnVersion`. Its picks, if any, are reclassified with no Outcome row, as above |
| A batch that holds only a retention DELETE's rows | The tail still runs. With nothing pending, the batch commits nothing, as today |
| The job starts with another classifier than the stored rows' | Every row is pending at once. That is the version bump above, not v1's endless churn: the tail and the plan share one `Spec` |
| GET `/submissions/backfill-…` | 404: not a UUIDv7 (v1's rule). S1 keeps it |
| The pending scan on a big store | It reads six columns of every row, each batch. P3 measures it at 100k Listings and an exit attempt at 1M |

## Implementation decisions

1. **`ingest.pending(stored, version, limit)`:** from the batch's pinned read, before the key semi join: rows with `NOT is_tombstone` and (`needs_reclassify`, or `taxonomy_version` null or not `version`), ordered by `updated_at`, then the Listing key, cut to `limit`, collected on the driver as `(key, source_version)`. `LIMIT = 1000` sits in `ingest`, since `backfill.py` imports `api` (fact 5) and S2 deletes it.
2. **The picks join the batch's Changes** (question 2): one row each, in the change feed's columns, with `op = reclassify`, the stored `source_version` (v1's decision 4), no Listing, `submission_id = backfill-<uuid7>`, `change_index` = its place in the round, and a `_commit_version` above any real one, so a key's pick sorts after its Merchant Changes. The key semi join, the plan, the classifier, the MERGE and the Outcomes append then run unchanged.
3. **One id per round,** made on the driver from a clock that `start` takes (`time.time` by default; tests inject one). A replay makes a new id.
4. **`tables.hour` reads `backfill-…` ids** by dropping the prefix. `uuid7_at` moves from `api` to `tables`, beside `hour`, which reads what it writes. `api` imports it from there, so `api.uuid7_at` still names it for v1's `backfill` and the tests.
5. **No totals.** v1's `backfill` event counted every flagged and outdated row. That is a diagnostic, and A6's spec decides what the job logs (rule 6), as A4 left its `classify` events.
6. **No Free Edition check is added.** The tail is plain Spark (a filter, a sort, a union), and the A3 and A4 checks run it on every batch with nothing pending. The PR's branch runs the workflow and cites it in its Evidence, as the plan asks.
7. **plan-v2 changes, in this spec's PR:** the A5 row names this spec; section C gains question 1's gap.

## Testing decisions

- **`tests/spark/test_backfill_tail.py`** (new), in `make spark`, through the real query on `fake`:
  1. An outage, then the classifier back: a batch on `down` flags two Listings; a batch on `fake` with one unrelated Change gives both fake's answer, unflagged. Outcomes gain two `reclassified` rows under one `backfill-…` id, `change_index` 0 and 1 in oldest-first order, in the injected clock's hour. Red: no tail.
  2. A version bump: Listings stored by a `fake` Spec at version `fake-0`, then a batch at `fake-1`: every live Listing moves to `fake-1`. A Tombstone and a current, unflagged Listing are not picked: no Outcome row, and the same `updated_at`.
  3. The limit, oldest first: `LIMIT` patched to 2, three pending Listings from two earlier batches. The first batch picks the two oldest (a tie broken by key), the next batch the third.
  4. The classifier still down: the picks keep their stored answer, flagged, with a later `updated_at`, and Outcomes say `reclassified`.
  5. A Merchant Change for a picked key in the same batch. A changed title: one stored row, classified once, Outcomes `written` and `reclassified`. A delete: a Tombstone, and `skipped`.
  6. A crash between the MERGE and the Outcomes append, with A2's test wrapper, then a restart: the store oracle holds, every Merchant Change has one Outcome row, the first picks are unflagged, and the only Backfill Outcomes are the replay's.
  7. A plan recomputed after its MERGE (A2's test 7, with pending rows): the same picks and the same Outcomes.
- **`tests/integration/test_tables.py`,** in `make check`: `hour("backfill-<uuid7>")` is the UUIDv7's hour; an id that is neither still raises. `uuid7_at`'s tests stay where they are and pass through `api`'s import.
- **A2's and A4's tests,** unchanged: with nothing pending, the same Outcomes and commits, "a compaction and a retention delete add no outcome and no commit" among them.
- **The Free Edition workflow on the PR's branch:** the existing checks, through the new batch.

## Questions

1. **Does the Backfill run when no Merchant Change arrives?**

   Context: the plan makes the Backfill "each micro-batch's tail". Spark starts a micro-batch only when the Landing log has new rows (fact 1). v1's Backfill ran every 10 s on its own. So if every Merchant is quiet, flagged Listings stay Uncategorized until the next Change from any Merchant. At the SLO's 50 Changes/s every trigger has input, and so does every stress window while the load generator runs.

   - **Option A (recommended): only with a batch.** The step is the plan's row as written. Cost: a quiet system doesn't reclassify, which plan-v2 section C records as an accepted gap. A11's outage leg gates on the oracles, not on the flagged count, so it still passes.
   - **Option B: a `rate` source ticks the stream.** The reader unions a one-row-per-second `rate` source to the change feed, and the batch drops its rows, so every trigger runs a batch and its tail (fact 2). Cost: about 15 more lines; an idle job scans the store and writes two checkpoint files every 60 s; the checkpoint holds two sources, so A6's "input waits" check and the `STREAMING_BACKLOG_BYTES` alert must ignore the tick; and no test has run `rate` on serverless.

   **Answer (Nov 11): only with a batch.**

2. **Do the picks join the batch's plan, or run as a second MERGE?**

   Context: the plan says the picks "run as `op=reclassify` after the batch's Merchant Changes". Two designs give that order.

   - **Option A (recommended): one plan.** The picks come from the store version the batch pinned and enter the plan as rows that sort last per key (decision 2). One MERGE and one Outcomes append, so the Backfill adds no commit and no crash point, and "one round in flight" holds with no gate. It equals v1: there, a reclassify also landed after the scan and met whatever the Listing held by then. Cost: a Listing flagged in this batch waits one batch.
   - **Option B: a second plan and MERGE after the first.** The picks see the store after the Merchant Changes. Cost: two more commits per batch with pending rows, a new crash point between the two MERGEs, a second classifier pass, and about 40 more lines.

   **Answer (Nov 11): one plan.**

## Out of scope

- The Backfill's diagnostics (round size, flagged and outdated totals): A6.
- Retuning `LIMIT` and the trigger for a version bump on a big store: P4.
- A per-Listing retry limit for a Listing the classifier always fails, and ordering by Merchant: v1 left both out ([step-6c.md](step-6c.md)).
- Deleting v1's `backfill.py` and its tests: S2.
- GET /submissions on the new tables: S1.
- Deleting `probe/a5`: when Phase A's probes end.

## Size

About 70 changed lines, tests left out, so one PR: `ingest.py` about 45, `tables.py` about 12, `api.py` about 8 (the moved `uuid7_at`), plan-v2 about 5. About 200 of tests. A1 to A4 came in 22–109% over their estimates, so this may reach about 160, under the cap. Option B of question 2 adds about 40.

[t137]: https://github.com/Hien-Trinh/Shopping/issues/137
[probe]: https://github.com/Hien-Trinh/Shopping/tree/probe/a5
[triggers]: https://spark.apache.org/docs/4.1.0/streaming/apis-on-dataframes-and-datasets.html#triggers
