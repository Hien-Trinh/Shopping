# Step 5e: Retention-horizon bootstrap (mini PRD)

Status: approved Oct 3. Confirmed: the horizon file saved by 5d (decision 1), the startup-only check (decision 2), the Landing log as the retained snapshot (decision 3), the replay order (decision 4), one partition at a time (decision 5), and the test points. Plan row: [plan-v1.md, PR steps, 5e](../plan-v1.md), and Phase 5's "Retention horizon (from the Phase 2 review)": "a worker below the horizon bootstraps from the retained snapshot (pinned version, in `seq` order), then follows the change feed". Design: offsets in the [design doc's state section](../design-commerce-ingestion-pipeline.md) (an offset is the next `(commit version, seq)` to read), A3, A6, A9 and A13 in [plan-v1.md](../plan-v1.md), and [ADR-0002](../adr/0002-local-first-delta-no-queue.md) (workers read the Landing log by offset). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Workers read the Landing log's change feed from their offsets. Once Landing log retention (5d) deletes old rows, compacts and vacuums, the change feed below some version can't be read any more: `load_cdf` from a vacuumed version fails with a generic `Exception` ("Failed to fetch metadata for file …", checked on delta-rs 1.6.6), on every retry. A worker whose offset is below that version crash-loops forever. Two cases reach it:

- a fresh `START` (no offset file, so `(0, 0)`) after retention has vacuumed version 0, for example a new state directory next to an old Landing log;
- a worker so far behind that retention passed its offset anyway (5d never vacuums past the slowest offset, but its disk guard can).

5d keeps retention behind the offsets; this step handles the worker that is below the horizon all the same.

## Solution

1. `state.py`: the Landing log horizon file, `state/landing_horizon.json` (`{table, version}`): "the change feed is readable from `version` on". `load_horizon(state, table)` returns 0 when the file is missing and refuses one saved against another Landing log (`OffsetsMismatch`). `save_horizon` is for 5d, which saves it before vacuum deletes any file.
2. `landing.py`: `retained(dt, version, partition)`: every Change row of one partition in the table as of `version` (what retention kept), in replay order (decision 4).
3. `worker.py`: at startup, after loading the offsets, every owned partition whose offset version is below the horizon is bootstrapped: pin `P` = the Landing log's current version, re-apply that partition's retained rows through the normal batch path in chunks of `limit`, then save its offset as `(P + 1, 0)`. The poll loop then follows the change feed as usual.

## User stories

1. As the operator, a worker whose offset is below the horizon recovers on its own and catches up, instead of crash-looping.
2. As the operator, starting with a fresh state directory next to a Landing log that retention has trimmed works: the retained Changes are applied and the worker follows the change feed.
3. As a Merchant, a Change that was retained but never applied gets applied, with the usual Outcome event, so its Submission completes.
4. As the operator, Changes that were already applied are not applied twice: the Listing Store is unchanged by them, they cost no classifier calls, and their Submissions keep their Outcomes.
5. As the operator, I can see each bootstrap in the events (`bootstrap`: worker, partition, old offset, horizon, pinned version, changes, ms).
6. As the operator, a bootstrap that is killed partway resumes at the next start without redoing the partitions it finished.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| No horizon file | Horizon 0: no bootstrap, the worker behaves exactly as today |
| Every owned offset at or above the horizon | No bootstrap, no `bootstrap` event |
| No offset file (`START`) and horizon > 0 | That partition is bootstrapped; the others are untouched |
| Offset below the horizon (far behind, disk guard) | That partition is bootstrapped from its retained rows, then its offset is `(P + 1, 0)` |
| A worker owning partitions both below and above the horizon | Only the ones below are bootstrapped; the others keep their offsets |
| Retained rows that were already applied | `already_applied` or `stale` Outcomes; Listing Store unchanged; no classifier call for them |
| Retained rows never applied | `written` Outcomes, classified as usual; the Listing Store then matches the replay oracle over every Change applied so far |
| Two retained Changes for one key, same source version, different content | The first in replay order wins; the other is `conflict` (A3) |
| A retained row that can't be stored | `failed` Outcome for it alone, as in a normal batch (3b) |
| `kill -9` during a bootstrap | Partitions already finished keep `(P + 1, 0)`; the unfinished one still has its old offset and is bootstrapped again at the next start, with a new pin. Re-applying is safe (A3) |
| SIGTERM during a bootstrap | Stops after the current chunk; the unfinished partition is redone at the next start |
| A bootstrap longer than the watchdog's 60 s | A heartbeat after every chunk, so the supervisor doesn't kill it |
| Horizon file saved against another Landing log | `OffsetsMismatch` at startup (exit 4, fatal), as for offsets |
| Horizon file torn or not JSON | `CorruptState` (exit 5, fatal), as for offsets |
| Reading the retained rows fails (I/O, a vacuum during the read) | Error out of startup, no offset moves; exit 1, the supervisor restarts it |
| Disk full writing events or an offset during a bootstrap | As above: exit 1, that partition is redone after the restart |
| The horizon passes a worker that is already running | Its reads fail; after `ATTEMPTS` failed ticks it exits 1 (A6); the restart bootstraps (decision 2) |
| Changes retention deleted before any worker applied them | Out of scope: they are gone, and their Submissions stay pending. Only 5d's disk guard can cause it; the `bootstrap` event's old offset and horizon make it visible |

## Implementation decisions

1. **Maintenance publishes the horizon; workers don't infer it.** 5d is the only thing that makes versions unreadable, so it saves `state/landing_horizon.json` before vacuum deletes any file. Alternatives rejected: reading the Delta log to find the oldest readable version (the Landing log commits up to 10 times a second, about 860k commits a day, and a `remove` doesn't say whether vacuum has deleted the file yet), and treating a read error as "below the horizon" (delta-rs raises a generic `Exception`, the same as for a corrupt file, which must crash, not bootstrap). **This adds one duty to 5d:** save the horizon before each vacuum.
2. **Checked at startup only**, as A18 does for Change Export. A horizon that moves under a running worker costs one crash and restart (about 15 s of backoff plus the restart), and only the disk guard can cause it. A check every tick would read a file 5 times a second for a case that should never happen.
3. **"The retained snapshot" is the Landing log itself as of a pinned version**, not a Catalog Snapshot (those are copies of the Listing Store). Bootstrapping re-applies every retained row of the partition. Some of them were applied before; A3 makes that safe (`already_applied` or `stale`, and Submission status keeps each Change's best Outcome). This avoids having to know which retained rows the worker already saw, which compaction makes impossible (below).
4. **Replay order is `(received_at, submission_id, change_index)`**, not landing order. Compaction rewrites rows into new files, so a row's commit version is lost, and `seq` is only the position within one commit. The new order matches landing order except for two requests racing within a millisecond, and order only decides which of two same-version, different-content Changes for one key wins (A3). `ponytail:` comment naming it; if it matters, the Landing log needs a column written at append time.
5. **One partition at a time, its offset saved when it finishes.** Order only matters per Listing key, and a key lives in one partition, so sorting per partition is enough. It bounds memory to one partition and makes a killed bootstrap resume per partition. Ceiling: at the design's 50 changes/s, 7 days is about 470k rows per partition, read and sorted in memory. `ponytail:` comment; if it matters, sort in DuckDB, which spills, or bootstrap a day at a time.
6. **The same apply path as a batch.** The storability check, `plan`, classification, MERGE and Outcome events move out of `process_batch` into one helper both use, so a bootstrap can't drift from normal processing. A heartbeat after every chunk; `stop` checked between chunks.
7. **A `reclassify` Change re-applied by a bootstrap reclassifies again.** `plan` can't tell an old reclassify from a new one. It costs classifier calls and may change a Category, which classification timeouts already make nondeterministic (A4). Accepted.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`landing.retained`** (new, red first, `tests/integration/test_landing.py`, real Delta in `tmp_path`): returns only that partition's rows, as of the given version, in `(received_at, submission_id, change_index)` order, after a compaction has rewritten the files.
  2. **`state.load_horizon`** (new, red first, `tests/integration/test_state.py`): missing file is 0; another Landing log's horizon raises `OffsetsMismatch`; a torn file raises `CorruptState`.
  3. **`worker.run` with an offset below the horizon** (new, red first, `tests/integration/test_worker.py`, real Delta, FakeClassifier): retention simulated by a test helper (delete old rows, compact, vacuum with zero retention, save the horizon). Covered: a partition with no offset file bootstraps and its store rows match the replay oracle; already-applied rows leave the Listing Store unchanged and make no classifier calls; a partition above the horizon keeps its offset; the offset becomes `(P + 1, 0)` and a later append is read from the change feed; the `bootstrap` event; a crash between partitions (an apply that raises on the second partition) leaves the first one's offset saved and the second redone on the next run; `stop` set during a bootstrap ends it without saving the unfinished partition.
  4. **`worker.process_batch`** (unchanged tests): they must still pass after the apply helper is extracted, which is the characterization check for decision 6.
- **No sleeps tuned to the machine.** `run` is driven with `stop` and the FakeClassifier, as in the existing worker tests.
- **Coverage:** `worker.py`, `landing.py` and `state.py` stay under the 90% package gate.

## Out of scope

- Writing the horizon, retention, compaction and vacuum of the Landing log (5d; decision 1 adds the horizon save to it).
- Change Export's watermark below the Listing Store's horizon (5b, A18).
- Recovering Changes retention deleted before they were applied: they are gone.
- Checking the horizon on every tick (decision 2).
- Exact landing order within a bootstrap (decision 4).
- Power loss: as everywhere, process crashes only (ADR-0002).

## Size

About 70 lines of production code (about 45 in `worker.py`, 15 in `landing.py`, 10 in `state.py`) and about 170 of tests, so under 300.
