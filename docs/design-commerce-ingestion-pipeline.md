# Commerce Ingestion Pipeline — Design Doc

Sep 30, 2026 · David

Terms are defined in [GLOSSARY.md](../GLOSSARY.md). Key decisions: [ADR-0001](adr/0001-partition-by-listing-key.md) (partition by Listing key) and [ADR-0002](adr/0002-local-first-delta-no-queue.md) (local-first on Delta, no separate queue). The build order and risk review are in [plan-v1.md](plan-v1.md).

## Overview and goals

Merchants push Listing changes through one Ingestion API. Ingestion workers validate and classify each change, then apply it to the Listing Store, which is the source of truth for current state. Change Export sends recent changes toward serving every minute, and Catalog Snapshots copy the Listing Store a few times a day.

- **Single entry point.** Every change enters through the Ingestion API, so auth and envelope validation live in one place.
- **Correct under chaos.** Out-of-order, duplicate, late and replayed-after-crash changes always converge to the same Listing Store state, and three oracles prove it.
- **Categorized catalog.** Every Listing gets exactly one Primary Category. Uncategorized is the fallback, and categorization never blocks ingestion.
- **Fresh export.** A change is exported within 5 minutes at p99.
- **Stress-testable for free.** Runs entirely on one Mac, with a load generator, full event logging and replay-correctness checks.

## Non-goals

- **Serving, search, ranking, recommendations.** This doc ends at the Change Export output.
- **Product matching** (grouping Listings from different Merchants into one Product). This is future work.
- **Multi-market Merchants.** One Merchant is one storefront, one market, one currency.
- **Per-Category validation rules.** v1 validates the same way for every Category. Rules per Category arrive with Category-specific workers.
- **Long-term history.** The Landing log and snapshots each cover 7 days, and nothing older is kept.
- **Listing expiry.** A Listing lives until it's explicitly deleted, even if a full-catalog feed stops sending it.
- **Per-merchant rate limits, a merchant read API, TLS.** The API binds to `127.0.0.1`.
- **Merchant UI.** Merchants call the Ingestion API directly.
- **Large-company scale.** The target is about 1M Listings, about 50 changes/s at peak, and batches of up to 10k.

## Architecture

```mermaid
flowchart LR
    M[Merchant] -->|POST /listings:batch| A[Ingestion API<br/>auth + envelope<br/>group commit ≤100 ms<br/>202 + submission_id]
    subgraph Catalog
        L[(Landing log<br/>every accepted change<br/>64 hash partitions · 7 d)]
        W[Ingestion workers ×N<br/>own partition blocks<br/>plan → classify → conditional MERGE]
        S[(Listing Store<br/>current state, source of truth<br/>tombstones kept forever)]
        X[Change Export<br/>every 1 min, change feed]
        C[(Catalog Snapshots<br/>every 6 h, keep 7 d)]
        B[Backfill<br/>op=reclassify]
        A --> L
        B --> L
        L -->|per-partition offset| W
        W --> S
        S --> X
        S --> C
    end
    X -.-> SV[Serving<br/>out of scope]
    A & W & X --> E[(events/*.jsonl)]
```

Only the Ingestion API and the backfill job write to the Landing log, both append-only. Only a partition's owning worker writes that partition of the Listing Store, and everything else reads.

## Components

| Component | Responsibility | Reads | Writes |
| --- | --- | --- | --- |
| Ingestion API | Authenticates the Merchant, checks the envelope, group-commits accepted changes to the Landing log, returns 202 + `submission_id`. Serves `GET /submissions/{id}`. Rejects bodies over 32 MB (413) and returns 503 when free disk is under 5 GB. | Merchant requests, merchant registry, events | Landing log, events |
| Merchant registry | SQLite: `merchant_id`, `currency`, `key_hash`, `status`. An admin CLI creates Merchants and rotates keys. | — | — |
| Landing log | Append-only Delta table (change feed enabled), partitioned by `partition`. It doubles as the work queue. Landing order is `(commit version, seq)`, where `seq` is a row's position in its commit. The content is stored as JSON (`listing`). Retained 7 days. | — | — |
| Ingestion worker | Owns partitions where `partition * N // 64 == index`, holding an `flock` per partition. Loop: read up to 1,000 changes or 200 ms from each partition's next position → plan → classify the batch → one conditional MERGE → events → offsets → heartbeat. Compacts its own partitions every K batches. | Landing log, Listing Store | Listing Store, offsets, events |
| Listing Store | Delta table (change feed enabled), partitioned by `partition`. Current state of every Listing, including Tombstones. | — | — |
| Change Export | Every minute: reads the change feed since its watermark and writes one Parquet file (latest row per changed Listing key; tombstone → `op=delete`), named by version range, then advances the watermark. If the watermark is older than what cleanup kept, it exports a full snapshot instead. Files kept 3 days. | Listing Store | Export files, watermark, events |
| Catalog Snapshots | Every 6 h: copies the Listing Store at a pinned version. Keeps 7 days. Never read to serve shoppers. | Listing Store | Snapshot folders |
| Backfill | Appends `op=reclassify` changes for `needs_reclassify` rows and after taxonomy version bumps. | Listing Store | Landing log |
| Supervisor | `python -m catalog.supervisor` starts the `Procfile`'s processes, restarts one that exits, and kills and restarts a `worker-*` whose last sign of life is more than 60 s old. That is its latest heartbeat, or its start if it has not beaten yet: a beat from an earlier run never counts, and a torn heartbeat file counts as none. Workers beat once at startup, before their first batch. A process that exits within 10 s of starting restarts after 5 s, so a crash loop can't spin; a restart that can't spawn is retried the same way. Only a worker's exit code can be fatal (2 for a bad flag, 3 `PartitionTaken`, 4 `OffsetsMismatch`, 5 `CorruptState`, 6 a model or the student's index missing, 7 `TYPESAFE_API_KEY` missing under `--classifier jev`): a restart can't fix it, so everything is stopped with that code. Children get their own session and are signalled as a whole process group, so a Ctrl-C or a closed terminal reaches only the supervisor, which stops them all (SIGTERM, then SIGKILL after one shared 10 s grace), wrappers' children included. Their environment carries its pid (`CATALOG_SUPERVISOR`), and a worker checks every second that it still runs: if the supervisor dies without stopping them (`kill -9`), the worker stops after its current tick, or exits hard 30 s later if stuck in a native call, so it doesn't keep its partition locks. A dead supervisor counts as running until its parent reaps it, which a shell or `uv run` does at once. A new supervisor started before the workers have gone gets exit 3 (`PartitionTaken`). One supervisor per state directory: a second exits 3 at once (`state/supervisor.lock`). A bare `python` in the `Procfile` is the supervisor's own interpreter. Heartbeats use `time.monotonic()`, so a laptop's sleep or a clock step never looks like a hang. | Heartbeat files, exit codes | `process_exit` and `supervisor_*` events |

### Change lifecycle

1. The Merchant sends `POST /listings:batch` with 1–10k changes.
2. The Ingestion API authenticates the Merchant and checks each change's envelope (see Data model). Invalid changes are rejected in the response with their index and reasons. The valid ones go through the group commit to the Landing log, each with `partition`, `submission_id` (UUIDv7) and its index. The API returns 202 once the commit succeeds.
3. The partition's owning worker reads the change past that partition's saved offset.
4. **Plan (pure):** compare against the stored row:
   - `source_version` < stored: **stale**
   - `==` stored with the same content hash: **already applied**, which counts as success
   - `==` stored with a different content hash: **conflict**, rejected, and the first write wins
   - `>` stored, or no stored row: **apply**

   A batch behaves exactly like applying its changes one at a time in landing order. Each change is compared with the state left by the changes before it, so batching only decides when writes are flushed, never what they are. A property test checks that outcomes are the same for any batching. A delete of an unknown Listing still writes a Tombstone, so an older upsert that arrives later can't bring it back.

   `op=reclassify` ignores `source_version`: it re-classifies whatever the Listing holds now (`reclassified`), or does nothing if the Listing is missing or deleted (`skipped`). Tying it to a version would let a price-only update make it skip, leaving the Listing on the old taxonomy.
5. **Classify** the Listings the batch leaves live that are new, re-created after a delete, have a changed title, description or attributes, are flagged `needs_reclassify`, were classified under an older taxonomy version, or got `op=reclassify`. A price or stock change never reclassifies. The timeout covers the whole batch: 200 ms for the embedding, 10 s for Jev, since one Jev call takes about 250 ms (step 6f); the student has no budget, since 1,000 Listings take it a few seconds at a few ms each, against the 60 s heartbeat (step 6m). On a timeout or error, those Listings get Primary Category = Uncategorized and `needs_reclassify = true`, except a Listing whose title, description and attributes are unchanged (a reclassify or a taxonomy bump): it keeps its stored category, flagged `needs_reclassify = true`, so an outage never downgrades a good answer.
6. **MERGE** the batch in one commit: update only if `s.source_version > t.source_version`, or `s.source_version = t.source_version` with an equal content hash (the same Listing state, which is what a reclassify writes). An upsert replaces the whole Listing. A delete writes a Tombstone (merchant content cleared, key and `source_version` kept).
7. Emit one event per change (`written`, `stale`, `already_applied`, `conflict`, `failed`, or the internal `reclassified` / `skipped`), then write each partition's offset atomically, then the heartbeat.
8. **Errors:**
   - A change whose data can't be stored (a value that slipped past validation): marked `failed` and skipped. Before planning, each change goes through the same conversion the MERGE uses, so a bad change fails alone, the same way on every replay, whatever the batch boundaries. The check is deterministic, so it isn't retried.
   - Any other error, whether storage (Landing log read, Listing Store read, MERGE commit, offset write) or a bug in our code: back off. The offset is never advanced, and the worker crashes after N attempts so the supervisor restarts it. Nothing is marked `failed`, so a full disk or a bug can never skip valid changes.
9. On its next tick, Change Export exports the changes, then records the new watermark.

Delivery is at least once throughout. Reprocessing is safe because of step 4's rules and because export consumers upsert by Listing key.

## Data model

A Listing is one sellable variant (the medium red t-shirt), identified by its **Listing key** = `merchant_id` + `merchant_product_id`.

| Field | Type | Set by | Rule |
| --- | --- | --- | --- |
| merchant_id | string | Ingestion API (from API key) | Generated by the server, `m_[a-z0-9]+`. Never read from the payload |
| merchant_product_id | string | Merchant | 1–128 printable characters, case-sensitive, no leading or trailing whitespace. Trusted as-is: no dedupe, a recycled ID is an update, and re-keying is a delete plus a create |
| source_version | int64 | Merchant | Required, > 0, and ≤ now_ms + 24 h. Monotonic per Listing; a millisecond epoch `updated_at` is fine |
| op | enum | Merchant | `upsert` or `delete` (`reclassify` is internal only) |
| title | string | Merchant | Required for upserts, 1–150 characters |
| description | string | Merchant | Optional, ≤ 5,000 characters |
| price_micros | int64 | Merchant | Required for upserts, > 0 |
| currency | string | Merchant | Required for upserts. Must equal the Merchant's currency |
| availability | enum | Merchant | Required for upserts: `in_stock`, `out_of_stock` or `preorder` |
| attributes | map<string,string> | Merchant | ≤ 100 keys; names 1–100 characters, values ≤ 1,000. Includes `gtin`, `brand` and `mpn` (for future Product matching) and `group_id` (Variant group). Stored and exported as canonical JSON text (sorted keys) |
| content_hash | string | Ingestion worker | SHA-256 of the canonical JSON (sorted keys) of the merchant content; a Tombstone hashes `null`. Drives step 4's rules |

All merchant fields are validated in strict mode: `"5"`, `5.0` and `true` are not integers, and unknown fields are rejected, both at the top level and inside `listing`. The request shape is `{"changes": [{"op", "merchant_product_id", "source_version", "listing": {…}}]}`. A delete carries no `listing`.
| primary_category | string | Ingestion worker | Taxonomy node, or Uncategorized |
| taxonomy_version, classify_confidence, needs_reclassify | string, float, bool | Ingestion worker | |
| is_tombstone | bool | Ingestion worker | Kept forever |
| partition | int (0–63) | Ingestion API | `int.from_bytes(sha256(f"{merchant_id}/{merchant_product_id}")[:8], "big") % 64`, which DuckDB and Spark SQL can reproduce |
| submission_id, change_index, received_at | UUIDv7, int, timestamp (UTC) | Ingestion API | Landing log and events. Not used for ordering |

**Submission status** is derived from events, read only from event hours starting at the UUIDv7 timestamp. A Merchant can only read its own Submissions. Each change's status is the **best** outcome any event reported for it, in the order written, reclassified, already_applied, conflict, stale, skipped, failed, rejected. It isn't the latest, because a crash replay can only make a merchant change look worse (a written change replays as `already_applied` or `stale`). A change with only `accepted` is `pending`.

## Categorization

- **Taxonomy:** Shopify's open-source product taxonomy, cut to 3 levels. Deeper nodes map to their ancestor.
- **Interface:** `classify(batch) → [(category, confidence)]`. Below the confidence threshold the result is Uncategorized. The threshold is chosen from the eval.
- **v1 classifier:** embedding similarity (fastembed) against Category paths, with the batch embedded in one call. The eval (step 6e, 198 hand-labeled Listings) chose instead an embedding shortlist of 50 paths followed by one TypeSafe Jev `Choice`, at threshold 0.40, with confidence = the chosen option's probability: 54.0% exact, 57.2% precision, 5.6% Uncategorized, against 23.7% exact for the embedding alone. Step 6f moves the pipeline onto it, with a 10 s batch budget, each worker starting at most 80 ÷ workers Jev calls per second, and no retries inside a batch (a failed call leaves the rest of the batch to the Backfill). The shipped `Procfile` ran it (`--classifier jev`) until step 6m; the embedding alone stays available as `--classifier embedding`, at its provisional threshold, 0.5. Step 6m replaces Jev with the fine-tuned student (`--classifier student`, [step-6m.md](specs/step-6m.md)): kNN, k = 5, over the training rows' vectors from a bge-small fine-tuned on them (step 6l.3), 73.4% exact on the 1,020 against Jev's 66.9% and 62.9% on Shopify's 2,000 against 61.8%, a few ms a Listing in the worker for nothing, with no key, no rate limit and no budget; every Listing gets an answer, none goes Uncategorized by the student. The `jev` kind stays for comparison and for a later cascade step (the student first, Jev below a threshold).
- **Reclassification:** only in the cases listed in lifecycle step 5, and only through the Landing log.

## State on disk

```
data/landing_log/  data/listing_store/  data/snapshots/<ts>/  data/export/<v1>-<v2>.parquet
data/events/<yyyy-mm-ddThh>/<process>-<pid>-<nonce>.jsonl   data/merchants.sqlite
state/offsets/pNN.json ({table, next: [version, seq]})  state/export_watermark.json  state/locks/pNN.lock  state/heartbeat/<worker>.json
```

Offsets and watermarks are written atomically (write to a temp file, then `os.replace`). An offset is the **next** Landing log position to read, `(commit version, seq)`, plus the id of the Landing log it belongs to. A worker refuses offsets saved against another Landing log (one recreated since), because trusting them would silently skip every row below them. Retention: Landing log 7 days, events and export files 3 days, snapshots 7 days. All are configurable.

Durability: everything survives a process crash or `kill -9` (atomic Delta commits, atomic state files). **Power loss is not covered:** delta-rs doesn't fsync local commits, so a commit and the offset after it can be lost or reordered. This is a deliberate v1 ceiling for a laptop. The upgrade path is to fsync new Delta log and data files after each commit, then use `F_FULLFSYNC` for state files.

Tables are created by whichever process opens them first. A lock file next to each table serializes creation: without it, processes started together race, and the losers crash or land a second CREATE with a new table id. Every timestamp passed in must be timezone-aware; naive datetimes are refused.

The Listing Store is compacted to about 1 MiB files. A copy-on-write MERGE rewrites every file holding a matched row, so a 1 MiB file bounds the rewrite to roughly 1–3k Listings per change, instead of a whole partition (measured: one changed key across 10 small files rewrote 1 file).

## Observability and testing

- **Events:** one JSONL line per stage per change, carrying `submission_id`, `change_index`, the Listing key, `partition`, the Listing Store version where relevant, and a timestamp. Each process writes its own files. Workers also write two batch-level events, both only after their MERGE commits, so a retried batch doesn't repeat them:
  - `classify_failed`: `listings`, `partitions`, `error`.
  - `batch`: `worker`, `changes`, `ms` (the tick, including the MERGE), `head` (the Landing log version read up to), and `next` (`{partition: [version, seq]}` for each partition whose offset moved). A partition's lag in commits is `head + 1 - next[p][0]`. A tick that moves no offset writes nothing.
- **Process events**, written as they happen:
  - Worker: `worker_start` (`worker`, `workers`, `pid`); `tick_failed` (`worker`, `attempt`, `error`) for each failed tick before the retry; `worker_stop` (`worker`, and `error` if it gave up).
  - Supervisor: `supervisor_start` (`processes`, `pid`) and `supervisor_stop` (`code`); `process_exit` (`process`, `reason` exit, stale, fatal or spawn, the exit `code` and old `pid`, or the spawn `error`). Only a fatal one isn't followed by a restart. The supervisor's own events are best effort: a full disk never stops it.
- **Reading events:** each process run writes its own file (`<process>-<pid>-<nonce>.jsonl`), so a restarted process never appends after its predecessor's torn last line. The reader streams files and trusts only complete lines, skipping a line still being written or one torn by a crash. (DuckDB's `read_json` with `ignore_errors` returns a *partial* event instead, which could look real, so it isn't used for events that drive status.)
- **DuckDB is a read-only query engine** for metrics and ad-hoc SQL. Each query uses a throwaway in-memory connection over Delta (via `to_pyarrow_dataset()`) and Parquet. The pipeline never writes to DuckDB.
- **Metrics (saved SQL):** freshness p50/p99, worker lag per partition, classify latency, rates of stale, conflict, failed and Uncategorized changes, worker utilization.
- **Three oracles:**
  1. **Store:** the Landing log (excluding rejected, conflicting and failed changes) → highest `source_version` per key → full replace → tombstones must equal the Listing Store, compared on merchant fields, `source_version` and the tombstone flag. It runs from an empty store.
  2. **Export:** replaying the export files in order must equal the Listing Store at the watermark.
  3. **Snapshot:** each snapshot must equal the Listing Store at its pinned version.
- **Tests:** a functional core with an imperative shell. Pure modules have 100% branch coverage, and the whole package at least 90%. A Hypothesis property test checks plan against the store oracle. Integration tests run on real Delta tables. Details are in the plan.
- **CI:** GitHub Actions on every PR, and checks are required before merging to `main`.

## Runtime

Local-first on one Mac (ADR-0002), Python 3.14:
- Delta tables through `deltalake` (delta-rs)
- FastAPI and uvicorn, in a single process
- worker processes, run from a `Procfile` by our own supervisor, not `honcho`: it stops everything when one process exits and never restarts
- fastembed running the fine-tuned student (step 6m); Jev, the shortlist and Laya were the earlier choices

No Docker, because MLX can't use the GPU inside it. Stress runs go under `caffeinate`. A later move to Databricks keeps the same tables.

## Future work

- **Classifier experiment.** On a 200-Listing eval set, compare (a) Laya hierarchical choice, (b) an embedding shortlist of 10 followed by a Laya choice, (c) embeddings only, and (d) an embedding shortlist of 10 followed by a TypeSafe Jev `Choice` call, on accuracy, latency and cost. Jev is a paid API at $0.042 per million input tokens (output free), about $17 to classify 1M Listings once by the estimate made before the eval (measured: $24 with a shortlist of 10, $62 with 50, since every option's path is input). Its limit, 40 requests/s when this was written and 80/s now, sits above the 50 changes/s peak, but a bulk upload still overflows it and takes the timeout → Uncategorized path. If Laya wins, it runs as one shared classifier process, because 16 GB of RAM can't hold one model per worker. **Result (step 6e, [eval/report.md](../eval/report.md)):** (d) won and (a) lost to (c): (a) 14.6% exact at 5.7 Listings/s, (b) 17.2%, (c) 23.7% at 116/s, (d) 48.5% at about $24 per 1M Listings, and (d) with a shortlist of 50 54.5% at about $62 per 1M. The shortlist caps (d): the embedding's top 10 holds the right path for 53.5% of Listings, its top 50 for 69.7% (measured with `EmbeddingClassifier.top` over the labeled set; not in the results files).
- **Category memberships.** Zero or more extra Categories per Listing for browsing and recommendations. They never drive processing.
- **Category-specific workers and rules.** Dedicated pools and validation for Categories that need their own code or much more capacity. These sit on top of Listing-key partitioning (ADR-0001), not in place of it.
- **Product matching.** Group Listings from different Merchants into one canonical Product. This is a separate step after Categorization: use the Primary Category plus GTIN, brand and MPN to narrow candidates, then match. It's the most important next step and the hardest.
- **Multi-market Merchants**, **Listing expiry**, **per-merchant rate limits**, **merchant read API**, **webhooks**, **long-term history**.
- **Serving scale.** Heavy serving read load may need fan-out or a dedicated serving store.
