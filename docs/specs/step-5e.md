# Step 5e: Retention-horizon bootstrap (mini PRD)

Status: approved Oct 3, then revised the same day to follow 5b: the gap is detected with 5b's `delta.history_gone` at every tick (decision 1, chosen Oct 3). Confirmed: bootstrapping the partitions at the lowest offset version (decision 2), the revised test points, and, unchanged, the Landing log as the retained snapshot (decision 3), the replay order (decision 4), one partition at a time (decision 5). Plan row: [plan-v1.md, PR steps, 5e](../plan-v1.md), and Phase 5's "Retention horizon (from the Phase 2 review)": "a worker below the horizon bootstraps from the retained snapshot (pinned version, in `seq` order), then follows the change feed". Design: offsets in the [design doc's state section](../design-commerce-ingestion-pipeline.md) (an offset is the next `(commit version, seq)` to read), A3, A6, A9 and A13 in [plan-v1.md](../plan-v1.md), and [ADR-0002](../adr/0002-local-first-delta-no-queue.md) (workers read the Landing log by offset). Builds on [step-5b.md](step-5b.md), whose spike and `history_gone` this reuses. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

Workers read the Landing log's change feed from their offsets. Once Landing log retention (5d) deletes old rows, compacts and vacuums, the change feed below some version can't be read any more, and every retry fails the same way (5b's spike: a bare `Exception` once VACUUM removed a file, a `DeltaError` once log cleanup removed a commit). A worker whose offset is below that horizon crash-loops forever. Two cases reach it:

- a fresh `START` (no offset file, so `(0, 0)`) after retention has vacuumed version 0, for example a new state directory next to an old Landing log;
- a worker so far behind that retention passed its offset anyway (5d never vacuums past the slowest offset, but its disk guard can).

5d keeps retention behind the offsets; this step handles the worker that is below the horizon all the same.

## Solution

1. `delta.py`: `history_gone(error)`, as 5b specifies it. Whichever of 5b and 5e lands first adds it; the other reuses it.
2. `landing.py`: `retained(dt, version, partition)`: every Change row of one partition in the table as of `version` (what retention kept), in replay order (decision 4).
3. `worker.py`: when a tick's read fails and `history_gone` is true, the next tick bootstraps instead of reading. A bootstrap pins `P` = the Landing log's current version and, for each owned partition at the lowest offset version, re-applies its retained rows through the normal batch path in chunks of `limit`, then saves its offset as `(P + 1, 0)`. The poll loop then follows the change feed as usual.

## User stories

1. As the operator, a worker whose offset is below the horizon recovers on its own and catches up, instead of crash-looping.
2. As the operator, starting with a fresh state directory next to a Landing log that retention has trimmed works: the retained Changes are applied and the worker follows the change feed.
3. As a Merchant, a Change that was retained but never applied gets applied, with the usual Outcome event, so its Submission completes.
4. As the operator, Changes that were already applied are not applied twice: the Listing Store is unchanged by them, they cost no classifier calls, and their Submissions keep their Outcomes.
5. As the operator, I can see each bootstrap in the events (`bootstrap`: worker, partition, old offset, pinned version, changes, ms), after the `tick_failed` event holding the gap's error.
6. As the operator, a bootstrap that is killed partway resumes at the next start without redoing the partitions it finished.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| History intact | No bootstrap: the worker behaves exactly as today |
| No offset files (`START`) after VACUUM removed version 0's files | The first read fails with gone history (`tick_failed`); the next tick bootstraps every owned partition |
| An offset older than what log cleanup kept | Same, through the `DeltaError` case of `history_gone` |
| Offset below the horizon (far behind, disk guard) | The partitions at the lowest offset version are bootstrapped, then their offsets are `(P + 1, 0)` |
| VACUUM overtakes a worker that is already running | Its next read fails with gone history and it bootstraps at the next tick, without exiting |
| Owned partitions at different offset versions | Only those at the lowest version are bootstrapped; if the next read still finds a gap, the next lowest are |
| Retained rows that were already applied | `already_applied` or `stale` Outcomes; Listing Store unchanged; no classifier call for them |
| Retained rows never applied | `written` Outcomes, classified as usual; the Listing Store then matches the replay oracle over every Change applied so far |
| Two retained Changes for one key, same source version, different content | The first in replay order wins; the other is `conflict` (A3) |
| A retained row that can't be stored | `failed` Outcome for it alone, as in a normal batch (3b) |
| `kill -9` during a bootstrap | Finished partitions keep `(P + 1, 0)`; the unfinished one still has its old offset, so it alone is at the lowest version and is bootstrapped after the restart, with a new pin. Re-applying is safe (A3) |
| SIGTERM during a bootstrap | Stops after the current chunk; the unfinished partition is redone at the next start |
| A bootstrap longer than the watchdog's 60 s | A heartbeat after every chunk, so the supervisor doesn't kill it |
| Reading the retained rows fails (I/O, a VACUUM during the read) | A failed tick (A6): backoff, no offset moves, the next tick bootstraps again |
| Disk full writing events or an offset during a bootstrap | As above |
| `history_gone` true for something that isn't a gap (a false positive) | A bootstrap, which is harmless (A3). If the error keeps coming back, a gap counts as a failed tick and only a successful read resets the count, so the worker exits 1 after `ATTEMPTS` (decision 1) |
| Any other read error | Unchanged: a failed tick, then exit 1 after `ATTEMPTS` |
| Changes retention deleted before any worker applied them | Out of scope: they are gone, and their Submissions stay pending. Only 5d's disk guard can cause it; the `bootstrap` event's old offset makes it visible |

## Implementation decisions

1. **The gap is detected from the read error with 5b's `delta.history_gone`, at every tick**, not from a horizon file 5d would write. Same reasons as 5b's decision 1: no coupling to 5d, it covers a VACUUM that overtakes a running worker, and both ways it can be wrong are safe (a false positive bootstraps, which re-applies harmlessly; a false negative crash-loops loudly, as today). The gap tick counts as a failed tick (logged as `tick_failed` with the error, with its backoff), and a bootstrap doesn't reset the count; only a successful read does. So an error that `history_gone` matches but a bootstrap can't cure still ends in exit 1 after `ATTEMPTS`.
2. **A gap bootstraps the owned partitions at the lowest offset version**, the version the failed read started from. delta-rs can't say where history starts, so the worker can't tell which partitions are below it. A worker's partitions usually share one version (every commit moves them all), so this is usually all of them. It also makes a killed bootstrap resume: the partitions it finished are already at `P + 1`. If the next read still finds a gap, the next lowest version is bootstrapped then. Each of those rounds counts as a failed tick, so more than `ATTEMPTS` distinct versions below the horizon would exit 1 and resume after the restart. Accepted: it needs partitions far apart in one worker.
3. **"The retained snapshot" is the Landing log itself as of a pinned version**, not a Catalog Snapshot (those are copies of the Listing Store). Bootstrapping re-applies every retained row of the partition. Some of them were applied before; A3 makes that safe (`already_applied` or `stale`, and Submission status keeps each Change's best Outcome). This avoids having to know which retained rows the worker already saw, which compaction makes impossible (below).
4. **Replay order is `(received_at, submission_id, change_index)`**, not landing order. Compaction rewrites rows into new files, so a row's commit version is lost, and `seq` is only the position within one commit. The new order matches landing order except for two requests racing within a millisecond, and order only decides which of two same-version, different-content Changes for one key wins (A3). `ponytail:` comment naming it; if it matters, the Landing log needs a column written at append time.
5. **One partition at a time, its offset saved when it finishes.** Order only matters per Listing key, and a key lives in one partition, so sorting per partition is enough. It bounds memory to one partition and makes a killed bootstrap resume per partition. Ceiling: at the design's 50 changes/s, 7 days is about 470k rows per partition, read and sorted in memory. `ponytail:` comment; if it matters, sort in DuckDB, which spills, or bootstrap a day at a time.
6. **The same apply path as a batch.** The storability check, `plan`, classification, MERGE and Outcome events move out of `process_batch` into one helper both use, so a bootstrap can't drift from normal processing. A heartbeat after every chunk; `stop` checked between chunks.
7. **A `reclassify` Change re-applied by a bootstrap reclassifies again.** `plan` can't tell an old reclassify from a new one. It costs classifier calls and may change a Category, which classification timeouts already make nondeterministic (A4). Accepted.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`landing.retained`** (new, red first, `tests/integration/test_landing.py`, real Delta in `tmp_path`): returns only that partition's rows, as of the given version, in `(received_at, submission_id, change_index)` order, after a compaction has rewritten the files.
  2. **`worker.run` after a real gap** (new, red first, `tests/integration/test_worker.py`, real Delta, FakeClassifier): retention simulated by a test helper (delete old rows, compact, VACUUM with zero retention). Covered: a partition with no offset file bootstraps and its store rows match the replay oracle; already-applied rows leave the Listing Store unchanged and make no classifier calls; a partition at a higher offset version is not bootstrapped; the offset becomes `(P + 1, 0)` and a later append is read from the change feed; the `tick_failed` and `bootstrap` events; a VACUUM between two ticks of a running worker; a crash between partitions (an apply that raises on the second partition) leaves the first one's offset saved and only the second redone; `stop` set during a bootstrap ends it without saving the unfinished partition; a gone-history error that keeps coming back (fault injected by `monkeypatch`, as the worker tests do) exits after `ATTEMPTS`.
  3. **`worker.process_batch`** (unchanged tests): they must still pass after the apply helper is extracted, which is the characterization check for decision 6.
  4. **`delta.history_gone`**: through 5b's tests if 5b lands first; otherwise through test point 2's real VACUUM, and 5b adds the log-cleanup case.
- **No sleeps tuned to the machine.** `run` is driven with `stop` and the FakeClassifier, as in the existing worker tests.
- **Coverage:** `worker.py`, `landing.py` and `delta.py` stay under the 90% package gate.

## Out of scope

- Landing log retention, compaction and VACUUM (5d).
- Change Export's own gap (5b, A18).
- Recovering Changes retention deleted before they were applied: they are gone.
- Exact landing order within a bootstrap (decision 4).
- Power loss: as everywhere, process crashes only (ADR-0002).

## Size

About 65 lines of production code (about 45 in `worker.py`, 15 in `landing.py`, 5 in `delta.py` if 5b hasn't added it) and about 170 of tests, so under 300.
