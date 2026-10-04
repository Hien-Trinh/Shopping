# Step 5b: Export gap recovery (mini PRD)

Status: approved Oct 3. Confirmed: detecting the gap from the read error at every tick (decision 1), the gap file as an ordinary export file (decision 2), building it in Arrow (decision 3), the `gap_recovered` event (decision 4), and the test points. Plan row: [plan-v1.md, PR steps, 5b](../plan-v1.md), and Phase 5's test "gap recovery (A18)". Design: A18 and A17 in [plan-v1.md](../plan-v1.md), the [Change Export row](../design-commerce-ingestion-pipeline.md) ("if the watermark is older than what cleanup kept, it exports a full snapshot instead"), and [ADR-0002](../adr/0002-local-first-delta-no-queue.md). Builds on [step-5a.md](step-5a.md). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Change Export reads the Listing Store's change feed from its watermark + 1. If the exporter is down longer than cleanup keeps history, the versions it needs are gone, and 5a's exporter then crash-loops on every tick: the store keeps changing, but nothing reaches serving again until someone resets it by hand. A18 asks for the fix: notice the gap, export a full copy of the Listing Store instead, log `gap_recovered`, and carry on from there.

## What the spike showed (delta-rs 1.6.6)

Two kinds of cleanup break `load_cdf`, with two different errors:

| Cleanup | When it fails | Error |
|---|---|---|
| A file the change feed needs is deleted (VACUUM did this for a plain append; for the Listing Store only 5d's retention can, see below) | Lazily, on `read_all()` | Bare `Exception`: `... Object at location <path> not found: No such file or directory` |
| Log cleanup removed the old commit JSONs (after a checkpoint, once `delta.logRetentionDuration` has passed; 30 days by default) | At once, on `load_cdf()` | `DeltaError`: `Invalid table version: <n>` |

Versions after the vacuumed ones still read fine. So the gap shows up only as a failed read; delta-rs has no call that says how far back history is complete.

Corrected while implementing: every Listing Store MERGE writes `_change_data` files, inserts included, and delta-rs VACUUM never deletes them. So VACUUM alone can't open a gap in the Listing Store: today only log cleanup can. Those files grow without bound, so 5d's retention has to delete them itself, and that deletion is what makes the first error appear. The tests simulate it by deleting them.

## Solution

1. `catalog/delta.py`: `history_gone(error) -> bool`, true for exactly the two errors above. 5e's Landing log bootstrap needs the same test, so it sits with the other shared Delta helpers.
2. `catalog/export.py`, in `tick`: the change-feed read (both `load_cdf` and `read_all`) is wrapped; when `history_gone` is true, the tick writes a **gap file** instead: every row of the Listing Store at `head`, Tombstones as `op=delete`, named `<w+1>-<head>.parquet`. It logs `gap_recovered` and saves the watermark at `head`. Any other error still propagates and exits the process, as in 5a.

## User stories

1. As a serving consumer, I apply a gap file exactly like any other export file (upsert each row, drop each `op=delete` key) and end up with the live Listings at the watermark. Nothing about the file format changes.
2. As the operator, an exporter that was down past cleanup recovers on its own at the next tick, instead of crash-looping.
3. As the operator, I see each recovery in the events (`gap_recovered`: version range, rows, deletes, ms, head), so a gap is never silent.
4. As the operator, a crash in the middle of a recovery neither loses changes nor writes a second file for the same versions.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| `_change_data` files the change feed from watermark + 1 needs were deleted | One gap file `<w+1>-<head>`; `gap_recovered` event; watermark = head; the export oracle holds over all files, the earlier ones included |
| Log cleanup removed commits from watermark + 1 | Same |
| A Listing deleted during the gap | Its Tombstone is in the gap file as `op=delete`, so the consumer drops it (Tombstones are kept forever, A17, so every key ever exported is still in the store) |
| The gap range is more than `max_versions` versions | One gap file to `head` anyway: its size is bounded by the store, not the range |
| Crash after the gap file is written, before the watermark | Next tick adopts it (5a's decision 1, unchanged); logged as an adopted `export` |
| Crash while writing the gap file | Hidden temp file only (5a's `_write`); the next tick hits the same gap and writes it again |
| Any other read error (permission, I/O, disk full) | Propagates: exit 1, watermark unchanged, the supervisor restarts it (5a's decision 5) |
| A delta-rs upgrade changes either message | `history_gone` turns false, so the exporter crash-loops visibly rather than skipping data; the tests that delete change-feed files and clean the log for real fail in CI first |
| Cleanup runs while a normal tick is reading | The read fails with the same error, so it is handled as a gap at that tick |
| An empty store (head 0, only the CREATE) in a gap | No file, watermark = head, as for an empty range in 5a |

## Implementation decisions

1. **The gap is detected from the read error, at every tick, not from a recorded horizon.** Alternatives: a horizon file written by 5d's VACUUM (couples 5b to 5d and races with a VACUUM that runs mid-read), or reading the Delta log ourselves to check every needed file exists (reimplements change-feed rules). Matching the error is the smallest, and both ways it can be wrong are safe: a false positive writes a full copy, which is still correct; a false negative crash-loops loudly, as today. `ponytail:` comment on the message match, naming the upgrade path (check the log and files directly). A18 says "on startup"; checking every tick covers that and a VACUUM that overtakes a running exporter.
2. **A gap file is an ordinary export file holding the whole store at `head`**, Tombstones as `op=delete`, not a separate format or a "reset" marker. Because Tombstones are kept forever (A17), upserting the live rows and deleting the Tombstones already gives the consumer exactly the live Listings, whatever it held before. If A17 ever changes, this decision has to change with it.
3. **The gap file is built in Arrow from the table at `head`** (`op` from `is_tombstone`, in the export file's column order), not through `collapse`'s Python rows, so 1M Listings never become Python dicts. It ignores `max_versions`.
4. **The event is `gap_recovered`** with the same fields as `export` (A18 names it). Phase 7's lag metrics must count both types. An adopted gap file logs `export` with `adopted: true`, as 5a does, because its type isn't recoverable from the file; acceptable since the original `gap_recovered` is usually there too.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`export.tick`** (red first, `tests/integration/test_export.py`, real Delta in `tmp_path`, writes through `store.merge`): export, merge more (including a delete and an update of an exported Listing), VACUUM with zero retention and delete the `_change_data` files, tick: one gap file `<w+1>-<head>`, a `gap_recovered` event, and the export oracle over every file holds at `head`. The same after log cleanup (set `delta.logRetentionDuration` to 0, checkpoint, `cleanup_metadata`). A gap longer than `max_versions` is still one file. A crash between the gap file and the watermark is adopted with no second file. A non-gap read error (fault injected by `monkeypatch`, as 5a's tests do) propagates with the watermark unchanged.
  2. **`delta.history_gone`**: covered through test point 1 with real errors; no unit test on hand-made messages, which would only pin the strings.
- **The end-to-end test** is unchanged: it never vacuums.
- **No sleeps.** VACUUM, the file deletion and log cleanup are called directly.
- **Coverage:** `export.py` and `delta.py` stay under the 90% package gate.

## Out of scope

- Running VACUUM, log cleanup or any retention (5d), and whether retention waits for the watermark. 5b only recovers once history is gone.
- Deleting the `_change_data` files VACUUM leaves behind (5d, see the spike).
- The same recovery for workers below the Landing log's horizon (5e); it reuses `delta.history_gone`.
- Catalog Snapshots (5c): the gap file is read straight from the Listing Store, not from a snapshot.
- Power loss: process crashes only (ADR-0002).

## Size

About 30 lines of production code (about 25 in `export.py`, 5 in `delta.py`) and about 100 of tests.
