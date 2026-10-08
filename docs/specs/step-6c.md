# Step 6c: Backfill (mini PRD)

Status: approved Oct 4, with the test points and decisions 1 to 6 as written (one round in flight, 1,000 rows per round; the version from the same `--classifier` flag). Plan row: [plan-v1.md, PR steps, 6c](../plan-v1.md) ("Backfill job (`op=reclassify` for flagged rows and taxonomy bumps)"), A5 ("Backfill appends `op=reclassify` changes (carrying the current `source_version`) to the Landing log, and the partition's owning worker applies them. There's still only one writer"), and Phase 6 ("a taxonomy version bump reclassifies through the Landing log"). Design: [Components](../design-commerce-ingestion-pipeline.md) ("Backfill: appends `op=reclassify` changes for `needs_reclassify` rows and after taxonomy version bumps"; reads the Listing Store, writes the Landing log) and lifecycle step 4 (`op=reclassify` ignores `source_version`). ADR-0001 (single writer per partition). Builds on [step-6b.md](step-6b.md) (`--classifier`, `taxonomy_version` naming the model, `None` answers), which lands first. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

A Listing reclassifies only when a Change touches it. So a Listing left Uncategorized by an outage or 6b's budget (`needs_reclassify = true`), or classified under an older taxonomy or model, stays that way until its Merchant happens to send another Change, which may be never. The worker side already exists: plan turns `op=reclassify` into `reclassified` (or `skipped` for a missing or deleted Listing), keeps the stored answer if classifying fails, and the MERGE accepts the same source version with the same content. What's missing is the job that finds those rows and appends the Changes, without becoming a second writer to the Listing Store, without flooding the Landing log ahead of Merchant traffic, and without re-appending rows the workers haven't reached yet.

## Solution

1. `src/catalog/backfill.py`, a long-running process like maintenance:
   - `pending(store_dt, version, limit) -> Pending`: scans the Listing Store's live rows (`NOT is_tombstone`) for `needs_reclassify` or `taxonomy_version != version`, oldest `updated_at` first, and returns up to `limit` `(key, source_version)` pairs plus the total counts of each kind (flagged, outdated).
   - `tick(landing_dt, store_dt, state_dir, events, version, now, *, limit, min_free) -> int | None`: the commit version it appended, or None. In order:
     1. **Gate:** if the last Backfill commit (saved in `state/backfill.json`, with the Landing log's table id) is at or past the lowest worker offset version, the workers haven't applied it yet: return None.
     2. **Disk guard:** below `min_free` (the API's 5 GiB) return None, as the API refuses writes.
     3. `pending(...)`; nothing pending: return None.
     4. One `landing.append` of `op=reclassify` Changes, one per row, carrying the stored `source_version`, `listing` null, submission id `backfill-<uuid7>`, `change_index` 0..n-1, `received_at = now`.
     5. Save the commit version to `state/backfill.json`, then emit one `backfill` event: `appended`, `flagged`, `outdated`, `version`, `landing_version`, `ms`.
   - `run(data, state_dir, version, *, stop, interval=10.0, limit=1000)`: `state.claim_backfill` first, then `tick` every `interval` seconds until `stop`. Any error propagates and the supervisor restarts it, as maintenance does.
   - `main`: `python -m catalog.backfill --classifier {fake,embedding}`, plus `--data`, `--state`, `--interval`, `--limit`; watches the supervisor and stops on signals like the others.
2. `classify.taxonomy_version(kind)`: the version a `--classifier` kind stamps, without loading the model (`fake-1`, or the taxonomy's version plus the model name). 6b's `EmbeddingClassifier` and `worker.main` use the same function, so the two can't spell it differently.
3. `state.claim_backfill(state_dir)`: one Backfill per state directory.
4. `Procfile`: `backfill: python -m catalog.backfill --classifier fake`, next to the workers' (same flag; 6e switches both).

The worker doesn't change.

## User stories

1. As the operator, Listings left Uncategorized by an outage get a real Category once the classifier is back, with no Merchant action.
2. As the operator, after a taxonomy or model bump every live Listing moves to the new version within hours, and the `backfill` events show the backlog (`flagged`, `outdated`) draining.
3. As a Merchant, my own Changes stay fresh during a big Backfill: it never puts more than `limit` reclassifies ahead of me at a time.
4. As ADR-0001, the Listing Store still has one writer per partition: Backfill only reads it and appends to the Landing log.
5. As a worker on 6b's budget, a reclassify I couldn't answer in time stays flagged and comes back in a later round, at the back of the queue.

## Failure scenarios

| Scenario | Expected |
|---|---|
| Nothing flagged or outdated | No commit, no event; one Listing Store scan per tick (about 10 s) |
| A row flagged after an outage | Appended next tick; the worker reclassifies it; flag cleared, `reclassified` event |
| The classifier is still down | The worker keeps the stored answer, still flagged, with a new `updated_at`; Backfill re-appends it next round, at most every `interval`. No hot loop |
| A taxonomy or model bump (1M outdated rows) | `limit` rows per round, one round in flight at a time; about 3 h at 1,000 per 10 s, or slower if workers are the bottleneck |
| A 6b budget leaves part of a round unanswered | Those rows keep their stored answer, flagged, and return in a later round; every batch answers at least one chunk, so the backlog shrinks |
| The workers haven't reached the last Backfill commit | No append (the gate), so a slow or stopped worker never gets the same rows twice |
| Backfill appended, then crashed before saving `state/backfill.json` | The next tick may append the same rows again: harmless (a reclassify is idempotent), costs one round of classifying |
| `kill -9` mid-append | Delta commits atomically: the rows landed or didn't; then as above |
| A Merchant Change lands for the row between the scan and the worker | The reclassify classifies whatever the Listing holds then; a delete makes it `skipped` |
| The same row is also flagged by a Merchant Change in the same batch | plan classifies it once per batch |
| Two Backfills on one state directory | The second fails `claim_backfill` and exits 1, as a second maintenance does |
| Low disk | No append (the API refuses too); maintenance's cleanup frees space |
| `state/backfill.json` from another Landing log (a reset data directory) | `OffsetsMismatch`, as for worker offsets: exit, fix by resetting state and data together |
| Landing log commit conflict with the API's append | `landing.append` retries once; a second conflict raises and the supervisor restarts Backfill |
| Backfill and workers started with different `--classifier` | Rows the workers reclassify stay "outdated" to Backfill, so it re-appends them forever (one round in flight, so not a flood). The `outdated` count in the `backfill` events never drops; fix the Procfile (decision 5) |
| Maintenance's 7-day DELETE | Removes old Backfill rows like any other; the gate only looks at offsets |
| A worker bootstrapping (5e) after cleanup | Replays retained reclassify rows: `reclassified` again, harmless |
| GET `/submissions/backfill-…` | 404: not a UUIDv7, so the API's existing check refuses it |

## Implementation decisions

1. **One round in flight, `limit` rows per round (recommended).** Backfill appends again only after every worker's offset has passed its last commit. This dedupes without per-row tracking (a row can't be appended twice while unprocessed), paces Backfill to the workers' real throughput, and caps how much reclassify work sits ahead of Merchant Changes at `limit` (1,000, spread over 4 workers). Alternatives: (a) append everything pending each tick: a bump puts 1M rows in front of Merchant traffic for hours and breaks the 5-minute freshness SLO; (b) a fixed rate with no gate: re-appends rows still in flight, and a stopped worker gets a growing pile. Cost: one stalled partition stalls Backfill, which is right, since nothing would apply anyway.
2. **Oldest `updated_at` first.** A row that fails again gets a new `updated_at` from its MERGE and moves to the back, so a Listing the classifier keeps failing can't starve the rest. Flagged and outdated rows share the queue.
3. **Submission id `backfill-<uuid7>`.** The plan's metrics count Changes by `(submission_id, change_index)`; the prefix lets Phase 7 count Backfill apart from Merchant traffic, and the API's UUIDv7 check already 404s it. No `accepted` events are emitted: there is no Merchant to report to.
4. **Carry the stored `source_version`** (A5), though plan ignores it for a reclassify: it shows in the Landing log what the row was when Backfill saw it.
5. **The target version comes from the same `--classifier` flag as the workers'**, via `classify.taxonomy_version(kind)`, without loading the model (Backfill needs no model in memory). A mismatch churns but doesn't flood (decision 1), and the `outdated` count shows it. Alternative: workers publish their version (in heartbeats) and Backfill refuses to run on a mismatch; more code for a Procfile typo, so not now.
6. **A long-running Procfile process, not a one-shot command.** Outage flags appear at any time, so someone would have to remember to run it. A 10 s tick with the gate costs one offsets read, plus a scan when the gate is open.
7. **The scan reads only the columns it needs** (key, `source_version`, `is_tombstone`, `needs_reclassify`, `taxonomy_version`, `updated_at`), filtered in Arrow (delta-rs string_view columns can't be filtered at scan time, as in `store.read`). Phase 7 measures it at 1M rows.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`backfill.pending`** (new, red first, `tests/integration/test_backfill.py`, a real Delta Listing Store in `tmp_path` written through `store.merge`): picks flagged live rows and rows on another version; skips Tombstones and current unflagged rows; oldest `updated_at` first; capped at `limit`; counts are totals, not capped.
  2. **`backfill.tick`** (new, red first, same file): appends one commit of `op=reclassify` rows (stored `source_version`, `backfill-` id, indexes 0..n-1) and saves its version; then appends nothing while any worker offset is at or below that version, and appends again once all are past; nothing pending or low disk (an injected `min_free`) appends nothing and saves nothing; a saved file from another Landing log raises `OffsetsMismatch`.
  3. **Through the worker** (new, red first, same file; the plan's Phase 6 test): Listings classified under `fake-1`; `tick` with `fake-2`; `worker.process_batch` with `FakeClassifier(taxonomy_version="fake-2")` → every live Listing on `fake-2`, `reclassified` events, Tombstone untouched, and the next `tick` appends nothing. Same shape for an outage: rows flagged by `FakeClassifier(fail=True)` are fixed by a tick plus a batch with a working classifier.
  4. **The process** (new): a second `run` on the same state directory raises `PartitionTaken`; the Procfile's `backfill` line parses and uses the same `--classifier` as the workers'. `test_e2e` already runs the whole Procfile, so Backfill starts and stops with the rest.
- **No model in any test:** `--classifier fake` everywhere; `classify.taxonomy_version("embedding")` reads only the taxonomy file.
- **Coverage:** `backfill.py` stays under the 90% package gate; it isn't pure (it reads Delta and the clock), so not on the 100% list.

## Out of scope

- Worker changes: plan and the MERGE already handle `op=reclassify`.
- Prioritizing by Merchant, or a per-Listing retry limit for Listings that always fail.
- Workers publishing their classifier version (decision 5).
- Measuring the scan and the bump drain time at 1M Listings (Phase 7).
- Switching the Procfile to the embedding classifier (6e).

## Size

About 80 lines in `backfill.py`, 5 in `classify.py` and `state.py`, 1 in the `Procfile`, about 140 of tests.
