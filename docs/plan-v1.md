# v1 Build Plan — Commerce Ingestion

Sep 29, 2026 · Architecture review of [design-commerce-ingestion-pipeline.md](design-commerce-ingestion-pipeline.md), [ADR-0001](adr/0001-partition-by-listing-key.md), [ADR-0002](adr/0002-local-first-delta-no-queue.md). Terms are defined in [CONTEXT.md](../CONTEXT.md).

## Verdict

The shape is sound: single-writer partitions, a conditional write, and a replay check that acts as the correctness oracle. **The doc has 19 holes, and 8 of them would ship bugs** (section A). Section B lists risks to prove early, and section C lists scope gaps we accept for v1.

**Status (Sep 30):** every decision is resolved, and A1–A19 are folded into the design doc, CONTEXT.md and both ADRs.

## Is it Python only?

Yes. Everything is written in **Python 3.14**: the API, workers, export, snapshots, load generator and eval harness. On this Mac (M4, 16 GB RAM, 10 cores) I checked that these install on 3.14: `deltalake` 1.6.6, `duckdb` 1.5.6, `mlx` 0.32.3, `fastembed` 0.8.1, `fastapi`, `hypothesis`, `pytest-cov`, and `uuid.uuid7` from the standard library.

Things that aren't Python, none of which you write as code:
- SQL, for DuckDB metric queries
- TOML, for config
- a `Procfile`, to start the processes
- Rust inside delta-rs, which you never touch

Two places where Python could become the bottleneck, and neither needs another language at this scale:
- **The load generator** (the GIL limits it): run it as several processes, and check that it isn't the bottleneck before trusting any stress number.
- **Parsing a 10k-item JSON body in the API**: it blocks the event loop for up to about a second. Measure it and cap the body size.

## What I verified (spike, delta-rs 1.6.6)

| Assumption | Result |
| --- | --- |
| Conditional MERGE (`s.sv > t.sv`) skips stale writes | ✅ The stale write left the row unchanged |
| MERGE writes the change feed (CDF) | ✅ It produced `insert`, `update_preimage` and `update_postimage` rows |
| Separate processes can MERGE into disjoint partitions | ✅ 8 processes × 20 MERGEs: **0 conflicts**, 1.3 s |
| Log checkpoints happen automatically | ✅ A checkpoint appeared at version 99 |
| One commit per MERGE | ⚠️ 160 MERGEs made 160 log files, which confirms hole A10 |
| MERGE never hangs | ❌ **One run hung on the first MERGE for over 5 minutes** and never reproduced. See B1 |

## A. Holes in the design (fix before coding)

**A1. Validation runs before classification, but category-specific validation needs the Category.** Lifecycle steps 4 and 5 are in an impossible order. Timeouts also send Listings to Uncategorized, which skips category rules entirely. And reclassification could later invalidate a Listing that was already accepted. Also, no per-Category rules are actually defined.
→ **Drop per-Category validation from v1.** Workers run generic validation only. Category rules come back together with the Category-specific workers (future work).

**A2. Upsert semantics are undefined.** If a merchant sends only a new price, does the description get cleared? Partial updates applied out of order would need a version per field.
→ **An upsert is a full replace.** The change is the Listing's complete state. That also keeps the replay check trivial.

**A3. After a crash, changes that were applied get reported as "stale".** Say a worker MERGEs a batch, then crashes before writing events. On restart it reprocesses the batch, finds `source_version == stored`, and reports `stale` for changes that are actually live.
→ Three rules:
- `<` stored: `stale`
- `==` stored, same content hash: `already_applied`, which counts as success
- `==` stored, different content: `conflict`, rejected, and the first write wins

This replaces the Round 6 decision that retries show up as "stale, ignored".

**A4. The replay check as written would fail on a correct system.** Fix it to:
- exclude changes that were rejected, conflicting or failed (join against events)
- use the A3 tie rule
- compare merchant fields, `source_version` and the tombstone flag, but not the Category, because timeouts make classification nondeterministic
- only run from an empty store, since Landing log retention (A13) drops old history

→ Add two more oracles: replaying the export files in order must equal the Listing Store at the watermark, and every snapshot must equal the Listing Store at its pinned version.

**A5. The backfill/reclassify job would be a second writer to the Listing Store.** That breaks ADR-0001's single writer per partition.
→ Backfill appends `op=reclassify` changes (carrying the current `source_version`) to the Landing log, and the partition's owning worker applies them. There's still only one writer.

**A6. "Retry 3 times, then skip" loses data on infrastructure errors.** With a full disk, thousands of valid changes would be skipped and marked `failed`.
→ Treat the two kinds of error differently:
- **A Change whose data can't be stored** (a value that slipped past validation): mark `failed` and skip. It is found per Change before planning, by the same conversion the MERGE uses, so the check is deterministic and isn't retried.
- **Every other error**, whether storage (Landing log read, Listing Store read, MERGE, offset write) or a bug in our code: back off, never advance the offset, and crash loudly after N attempts.

Step 3b first bisected a failing MERGE. Its review (Oct 2) replaced that with the per-Change check, for three reasons. Bisecting blamed a corrupt store file's read error on the data and mass-failed valid Changes. It reported a bad Change as `written` when a later Change in the same batch superseded it. And it reran reads, the classifier and MERGEs at every level.

**A7. Python's `hash()` is randomized per process.** Different processes would put the same Listing key in different partitions, and ordering silently breaks. When I ran `hash('m_42/SKU-123') % 64` in fresh processes I got 21, 42 and 35. One earlier pair happened to match (55, 55), which is exactly why a quick check can make it look stable.
→ Use **SHA-256**:

```python
def partition(merchant_id: str, merchant_product_id: str) -> int:
    digest = hashlib.sha256(f"{merchant_id}/{merchant_product_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % 64
```

- **Why SHA-256 rather than blake2b or xxhash:** it's in the Python standard library, in DuckDB (`sha256`) and in Spark SQL (`sha2`). Metric queries, the replay check and a later Databricks port can all recompute the partition inside SQL. I checked that Python and DuckDB (`('0x' || substr(sha256(k), 1, 16))::UBIGINT % 64`) agree on 20,000 random keys, including Unicode ones, and the keys spread evenly across the 64 buckets. Speed doesn't matter at 50/s.
- **Why a `/` separator is enough:** `merchant_id` is generated by the server from `[a-z0-9_]` and never contains `/`, so the first `/` always splits the key correctly. That's true no matter what the merchant product ID contains, so no length prefix is needed.
- **Test:** pin known key → partition values, plus the Python/DuckDB equivalence check.

**A8. ADR-0001's `partition // (64/N)` uses float division when N doesn't divide 64, and nothing stops two workers from claiming the same partition.**
→ Use `owner = partition * N // 64`. Each worker takes an `flock` on `state/locks/pNN.lock` for every partition it owns and refuses to start if one is taken.

**A9. Offsets need to be stored per partition, and the design never says where.** A per-worker offset can't move with a partition when workers are rescaled.
→ One JSON file per partition, `state/offsets/pNN.json`, holding the **next** position to read, `(commit version, seq)`, and the id of the Landing log it belongs to. It's written atomically (write to a temp file, then `os.replace`). Only the owner writes it, so there's no contention. The Change Export watermark is stored the same way.

**A10. One MERGE per change means one Delta commit per change.** At 50 changes/s that's 4.3M log files and small data files a day. The API has the same problem when it appends to the Landing log.
→ Batch on both sides:
- **Workers:** read up to 1,000 changes or 200 ms, dedupe per Listing key, then do one MERGE per batch.
- **API:** group commit. A single appender collects requests for up to 100 ms, commits once, then answers all of them with 202.
- **Compaction:** each worker compacts its own partitions every K batches. It's the only writer there, so compaction can't conflict with another worker's writes to the same partition. Delta can still report a `CommitFailedError` across partitions (see Phase 7), which the worker loop retries.

**A11. Events can't go into a DuckDB table.** DuckDB allows only one process to hold a database open for writing, and the API, N workers and Change Export all write events.
→ Each process run appends to its own JSONL file under `events/<hour>/<process>-<pid>.jsonl`. They're read by a streaming reader that trusts only complete lines. (DuckDB's `read_json(ignore_errors=true)` returns a half-written line as a *partial* event, so it's never used on event files.) See "DuckDB's role" below.

**A12. Deriving Submission status from events means scanning every event on each lookup, which gets slower over time.**
→ Make `submission_id` a UUIDv7, which embeds a timestamp. A lookup then only reads event hours from that timestamp onward.

**A13. The disk won't fit 30 days of Landing log.** There's 27 GB free. My estimate: sustained 50 changes/s × about 500 B compressed ≈ 2 GB/day of Landing log, plus about 4 GB/day of events, which is roughly 60 GB+ over 30 days.
→ Keep 7 days of Landing log and 3 days of events and exports, all configurable. The API returns 503 when free disk drops below 5 GB.

**A14. A single bad `source_version` freezes a Listing forever.** If a merchant sends `9e18` by mistake, no later write can ever be higher.
→ Reject `source_version > now_ms + 24h`. This works for both kinds of versions merchants send: plain counters stay tiny, and epoch milliseconds stay close to now.

**A15. Price has no defined type.** A float price rounds money.
→ Store `price_micros` (int64, > 0). `currency` must equal the Merchant's single currency, per the one-market rule.

**A16. There's no design for storing merchants (IDs, keys, currency).**
→ A SQLite `merchants` table (`merchant_id`, `currency`, `key_hash`, `status`), plus a CLI to create a merchant and rotate its key:
- keys come from `secrets.token_urlsafe(32)` and are shown once
- only the SHA-256 is stored, compared with `hmac.compare_digest`

**A17. Compacting tombstones after N days lets old changes bring deleted Listings back.**
→ Keep tombstones forever in v1. 1M rows is nothing.

**A18. Change Export can fall behind cleanup and lose data without noticing.** If export is down longer than VACUUM's retention, the change-feed files it still needs are gone.
→ On startup, if the watermark is behind the oldest readable version, export a full snapshot and log a `gap_recovered` event. Export files are kept 3 days (A13).

**A19. "Retries create a Submission that reports stale, ignored" (Round 6).** Superseded by A3.

## B. Risks to prove early

| # | Risk | Mitigation / proof |
| --- | --- | --- |
| B1 | MERGE hung once in the spike, with no timeout available | Each worker writes a heartbeat file every batch, and the supervisor kills and restarts a worker whose heartbeat is older than 60 s. At-least-once delivery plus A3 make the restart safe. Phase 0 tries to reproduce the hang. |
| B2 | OPTIMIZE running at the same time as a MERGE or append | Phase 0 spike. Workers compact only their own partitions. The Landing log is compacted by a maintenance job, and appends (which never read) shouldn't conflict with it. |
| B3 | Reading the Landing log change feed per partition without reading every partition | Phase 0 spike: `load_cdf` with a partition predicate on a table partitioned by `partition`. |
| B4 | Classifier throughput on bulk uploads | Batch-embedding a whole worker batch is fine. Laya does about 13 ms per decision, one at a time, so roughly 75/s per process. Jev is capped at 40 requests/s, so a 1M-Listing initial load through Jev takes about 7 h, and most of it would time out into Uncategorized. The **timeout applies to the whole batch**, not to each change. |
| B5 | Memory with 16 GB | Laya at FP16 is about 0.85 GB per process, so 8 workers use about 7 GB. If Laya wins the eval, run **one shared classifier process** instead of loading the model into every worker. |
| B6 | Docker | Ruled out: MLX can't use the GPU inside Docker on macOS. Processes run natively from a `Procfile`, under our own supervisor (honcho stops everything when one process exits and never restarts). |
| B7 | Laptop sleep pauses every process and skews freshness numbers | Run stress tests under `caffeinate -dims`. |
| B8 | Unbounded request bodies | FastAPI and uvicorn don't limit body size. Add middleware that caps bodies at 32 MB and returns 413 above that. |
| B9 | The load generator becomes the bottleneck | Run the load generator as several processes. Its own send rate goes in the report, and a run is invalid if the generator is at 100% CPU. |

## C. Scope gaps accepted for v1 (say if any is wrong)

- **No long-term history.** Snapshots and the Landing log each cover only 7 days, so the original "every version of every listing" promise is gone.
- **No `availability`/stock field.** I recommend adding it: stock changes are the most frequent real update, and they don't trigger reclassification, which makes stress tests realistic. There's no image URL either (put it in `attributes`).
- Listings never expire. A full-catalog feed that silently drops an item doesn't delete that item.
- No per-merchant rate limit. One merchant hammering a single key slows its partition (1/64 of the catalog).
- No merchant read API (`GET /listings/{key}`). You inspect data through DuckDB.
- The API binds to `127.0.0.1` without TLS. Before exposing it anywhere, add TLS and rate limits.
- Jev sends merchant data to a third party. Prompt injection in a title can at worst cause a misclassification, because `Choice` can only answer from the given options.
- The Amazon Reviews '23 dataset is for research use. Fine for learning, not for a commercial product.
- "Catalog" will clash with Databricks' Unity Catalog if you port. Minor.
- `laya-mlx` installs from a GitHub repo. Review it before installing. I won't install it without your OK.

## DuckDB's role: read-only query engine

The pipeline never writes to DuckDB. Every DuckDB use opens a throwaway **in-memory** connection and reads files that other components own. That makes the single-writer limit irrelevant, and any number of processes can query at the same time.

| DuckDB reads | How |
| --- | --- |
| Listing Store, Landing log, snapshots | Through `DeltaTable(path).to_pyarrow_dataset()`, which DuckDB queries directly as Arrow (verified). This avoids DuckDB's `delta` extension, which is a separate Delta reader downloaded at runtime. The pipeline then has one Delta implementation (delta-rs), and neither CI nor tests need network access. |
| Events | `events.read()` (complete lines only) loaded into DuckDB as Arrow, never `read_json` on live files |
| Export files | `read_parquet('export/*.parquet')` |

What DuckDB is used for:
- saved metric queries (freshness, lag, rates)
- the SQL half of the replay oracles

The `GET /submissions/{id}` status fold uses `events.read(submission_id=…)` and `status.fold` directly; no DuckDB is involved.

Merchants stay in SQLite. It handles several processes writing (in WAL mode), and only the API and the admin CLI write to it.

## CI

**Every PR runs the whole suite on GitHub Actions and must be green before merging.** The repo is public, and `main` requires a PR with passing checks, even for admins. One workflow, `.github/workflows/ci.yml`, gains a job as each phase makes it meaningful:

| Job | Runner | Runs | When |
| --- | --- | --- | --- |
| `check` | `ubuntu-latest` | `make check`: ruff, unit, integration, both coverage gates, and the Python/DuckDB hash equivalence test | From Phase 0: every PR and every push to `main` (required) |
| `stress-smoke` | `ubuntu-latest` | 60 s of every chaos scenario at low rate, ending with the three oracles | From Phase 7: every PR (required) |
| `model` | `macos-latest` (Apple Silicon) | Tests marked `model` (MLX/Laya and the real embedding model), with the model cached | From Phase 6: PRs that touch `classify*`, `taxonomy*`, `eval/` or dependencies (required, skipped otherwise) |
| full stress / eval | local Mac | Full Phase 7 scenarios and the classifier eval | Before a release, run by hand. CI runners are too small to mean anything |

Details that matter:
- **The `model` job filters by path with a job-level `if`, not the workflow-level `paths:` filter.** A required check whose workflow never runs stays "Expected" forever and blocks the merge. A job skipped through `if` counts as passed. A first step runs `git diff --name-only origin/main...HEAD`, so no third-party action is needed.
- **Cost on this private repo:** the Free plan includes 2,000 Actions minutes a month, and GitHub's price list has macOS at about 10× Linux ($0.062 vs $0.006/min). That's why `model` only runs when relevant. Public repos run for free.
- **No secrets in CI.** Jev is never called in CI, and the `model` job uses only local models.
- **Why the repo is public:** private repos on the Free plan can't require status checks. Making it public was chosen over paying for GitHub Pro, and public repos also run Actions (including macOS) for free.
- **Why jobs arrive by phase:** a job can't be required before it has something to run. A placeholder job that always passes would be a check that proves nothing.
- The local pre-push hook stays, so failures show up before you push.

## Architecture after fixes

```
Merchant ─HTTP─▶ Ingestion API (1 process, async, group commit ≤100 ms)
                   │ append (partitioned by hash % 64)        events/*.jsonl
                   ▼
             Landing log (Delta, CDF, 7 d) ◀── backfill appends op=reclassify
                   │ load_cdf(since per-partition offset)
                   ▼
  Ingestion worker ×N (flock per partition, heartbeat, batch ≤1000/200 ms)
    plan (pure) → classify batch (timeout) → conditional MERGE → events → offsets
                   ▼
             Listing Store (Delta, partitioned, CDF, tombstones forever)
               │ change feed since watermark          │ pinned-version copy
               ▼                                      ▼
        Change Export (1 min, files 3 d)       Catalog Snapshots (6 h, 7 d)
```

## Code layout

A **functional core with an imperative shell**: every decision lives in pure functions that are tested without touching disk, and the I/O modules stay thin.

```
src/catalog/
  keys.py         Listing key encoding, stable hash, partition, owner(p, N)            [pure]
  envelope.py     pydantic models, limits, source_version ceiling, currency rule        [pure]
  plan.py         batch + stored rows → outcomes, merge rows, needs-classify set       [pure]  ← the heart
  replay.py       the three oracles: store, export, snapshot                           [pure]
  status.py       events → Submission status fold                                     [pure]
  collapse.py     change-feed rows → latest row per key, tombstone → delete            [pure]
  classify.py     Classifier protocol, threshold, EmbeddingClassifier, FakeClassifier
  taxonomy.py     load Shopify taxonomy, trim to depth 3, version string
  merchants.py    SQLite registry, key issue/verify, CLI
  landing.py      schema, group-commit appender, read-since per partition
  store.py        schema, read rows by keys, conditional MERGE, compact own partitions
  state.py        atomic offset/watermark files, partition flocks, heartbeat
  events.py       JSONL writer, event types
  worker.py       loop + error policy (A6)
  supervisor.py   runs the Procfile: restarts, heartbeat watchdog, fatal exits
  entry.py        how every standalone entry point exits (flush, os._exit)
  api.py          FastAPI app, auth, body cap, disk guard
  export.py       Change Export + gap recovery
  snapshots.py    pinned copy + pruning
  maintenance.py  Landing log compaction, retention, vacuum
loadgen/          scenarios, multiprocess sender
eval/             labeled set, classifier experiment
tests/unit  tests/integration  tests/stress
```

## Testing rules

- **Every module ships with its tests in the same commit.** TDD is recommended (red, green, refactor).
- **Coverage gate:** `pytest --cov=catalog --cov-branch --cov-fail-under=90` on every run. The pure modules (`keys`, `envelope`, `plan`, `replay`, `status`, `collapse`) must reach **100% branch coverage**, enforced by a second `coverage report --include=… --fail-under=100`. The only coverage exclusions are `if __name__ == "__main__":` lines.
- **Coverage doesn't prove correctness; property tests do.** A Hypothesis test generates random change sequences (with shuffles, duplicates, deletes, stale versions, crash-and-replay), runs them through `plan` against an in-memory store, and asserts the result equals the `replay` oracle. That single test covers A2, A3, A4 and the tombstone rules.
- **Integration tests** run against real Delta tables in `tmp_path`. No mocks of delta-rs: the spike showed its behavior is exactly what needs testing.
- **Inject everything that varies:** clock, classifier and paths. There's no `sleep` in unit tests.
- **The stress suite** (`tests/stress`, marked `slow`) is excluded from the coverage gate and ends with all three oracles.
- **Tooling:** `uv`, `ruff` (lint and format), `pytest`, `pytest-cov`, `hypothesis`. `make check` = ruff + unit + integration + coverage gates, run by a pre-push hook.

## Review process (from Oct 1)

Every step PR (under about 300 changed lines) goes through `/lean-review` (`.claude/skills/lean-review`): deterministic gates first (`make check`, plus `make mutate` for pure modules), then Sonnet finder agents scaled to the diff (`.claude/agents/reviewer-*.md`, run by `.claude/workflows/lean-review.js`): 1 for docs or tooling, 4 up to 50 changed lines, 6 up to 150, all 11 above that. A spec lane runs at every size and a standards lane from 51 lines, both after Matt Pocock's two-axis `/code-review` (compared on PR #15: it found the same top three bugs for a quarter of the tokens, plus rule breaches and scope decisions the bug finders never ask about). Their scope decisions and smell judgement calls are reported apart from the defects. Candidates are deduped, at most 5 unreproduced medium- or high-severity ones go to a Sonnet verifier, and I verify every candidate in-context. One round, with a second in-context round only if correctness bugs were found. An earlier max-effort fan-out (11 Opus agents, about 1.7M tokens for one PR) hit the usage limit. A fixed 10-finder pass with 3 verifiers per candidate spent 2.4M tokens on a 21-line PR with no code findings, hence the scaling. The deterministic gates found most of the real bugs anyway. Reuse, simplification and altitude reviews run once at the end of the project.

**Mutation baseline** (`make mutate`, after step 2r): 373 of 382 mutants killed. The 9 survivors are equivalent mutants, so a new survivor outside this list is a real gap:
- `keys.partition`: `from_bytes(..., "big")` without the byte order (big is the default)
- `content_hash`: `model_dump` mode `None`, `"XXjsonXX"` or `"JSON"`, since any mode other than `"json"` means python, and every `Content` field is already JSON-native; and `ensure_ascii=None`, which is falsy like `False`. The exact bytes are pinned by `test_content_hash_bytes_are_pinned`.
- `plan`: `touched[k] = ""` (twice), since the dict is an ordered set and its values are never read
- `status.fold`: `<=` for `<`, since equal rank means the same Outcome
- `collapse`: `>=` for `>`, since a change feed has at most one row per key per commit once pre-images are skipped

## PR steps (from Oct 1)

Each step is one PR of **under about 300 changed lines, tests included**, merged before the next one starts. A step that grows past that gets split rather than squeezed. ⏸ means the loop stops for you.

| Phase | Step | PR |
| --- | --- | --- |
| 2 | 2r ✅ | Mutation-survivor triage: pin `content_hash` bytes, kill or justify the rest |
| 3 | 3a ✅ | `worker.process_batch`: one batch from read to plan, classify (FakeClassifier), merge, events and offsets, plus the replay oracle test |
| 3 | 3b ✅ | Error policy: per-change failure isolation (a storability check before planning); any other error never advances the offset |
| 3 | 3c ✅ | Worker process: claim (a second worker on a claimed partition gets `PartitionTaken`), startup beat, poll loop with backoff on any error `process_batch` raises (crash after N attempts), owner compaction cadence, a per-batch event (offsets, duration, changes) for lag and utilization, CLI entry, `Procfile` |
| 3 | 3d ✅ | Supervisor (`python -m catalog.supervisor`, no `honcho`): heartbeat watchdog (start time counts as a beat), restart. From the 3c review: distinct worker exit codes for fatal startup errors (`PartitionTaken`, `OffsetsMismatch`, `CorruptState`) versus giving up after N failed ticks, worker start/stop events, and whether heartbeats use a monotonic clock |
| 3 | 3e ✅ | Spec: [step-3e.md](specs/step-3e.md). Chaos tests: `kill -9` mid-batch, rescale from 4 to 3 workers, and a `kill -9`ed supervisor: its workers keep running and keep their locks (the next supervisor's workers exit 3), so workers should watch their parent |
| 4 | 4a ✅ | Spec: [step-4a.md](specs/step-4a.md). Merchant registry (SQLite) and admin CLI: create a merchant, rotate (and revoke) a key, plus `verify` for 4b's auth |
| 4 | 4b | Spec: [step-4b.md](specs/step-4b.md). `POST /listings:batch`: auth, envelope, 32 MB cap, disk guard, direct append. From the 4a review: `merchants.verify` raises on a missing registry rather than refusing every key, so the API checks the registry at startup; count auth failures by reason (unknown key or revoked Merchant) without ever logging the key |
| 4 | 4c | Spec: [step-4c.md](specs/step-4c.md). Group-commit appender (≤100 ms window) |
| 4 | 4d | Spec: [step-4d.md](specs/step-4d.md). `GET /submissions/{id}`: status from events, IDOR check |
| 4 | 4e | Spec: [step-4e.md](specs/step-4e.md). End-to-end test: HTTP → Landing log → worker → Listing Store, under the supervisor and the shipped `Procfile`. The API watches its supervisor like the workers, so a `kill -9`ed supervisor no longer leaves it holding the port |
| 5 | 5a | Change Export: change feed since the watermark, export files, export oracle |
| 5 | 5b | Export gap recovery (A18) |
| 5 | 5c | Catalog Snapshots and pruning |
| 5 | 5d | Landing log retention and compaction (never past the slowest offset). From the 4d review: the 3-day events retention (A13), which no step had, and `GET /submissions/{id}` answering 404 before any read for an id dated before that horizon, so a forged old id can't scan every event hour |
| 5 | 5e | Retention-horizon bootstrap for workers below the horizon |
| 6 | 6a ⏸ | Taxonomy loader (asks before downloading the Shopify taxonomy) |
| 6 | 6b ⏸ | Embedding classifier with a batch timeout (asks before downloading the model). From 3e: its native calls must release the GIL, or the supervisor watch can't exit a worker stuck in one |
| 6 | 6c | Backfill job (`op=reclassify` for flagged rows and taxonomy bumps) |
| 6 | 6d | Eval harness and report format |
| 6 | 6e ⏸ | Labeled set and classifier experiment: you verify the labels, laya-mlx and Jev need your OK |
| 7 | 7a | Load generator (multiprocess) |
| 7 | 7b | Chaos scenario runner and the three oracles. From 3e: a runner that kills the supervisor must reap it (`wait`), or its workers keep running until it does |
| 7 | 7c | Metrics SQL and runbook. From the 4a review: an audit event (Merchant, action, time) for each create, rotate and revoke. From the 3d review: `process_exit` events for processes stopped at shutdown, and for every exit seen in the pass that hit a fatal one. From 3e: after a supervisor is killed, wait a few seconds for its workers to stop before starting a new one; a reason on `worker_stop` when the supervisor watch stopped it, and a trace of the watch's hard exit, written so they can't block that exit. From the 4e review: the API's watch too: an event when it stops the API, and the same trace of its hard exit |
| 7 | 7d ⏸ | `stress-smoke` CI job; then full stress runs on your Mac |

## Phases

Each phase ends green on `make check`, and its exit criteria are the tests.

**Phase 0 — Skeleton and spikes** ✅
- `uv` project on Python 3.14, ruff, pytest with the coverage gates, pre-push hook (`make hooks`), README.
- `.github/workflows/ci.yml` with the `check` job, which is required on `main`.
- `keys.py` pulled forward from Phase 1, so CI has real code to gate: stable SHA-256 partition, `owner`/`owned`, 100% branch coverage, and property tests including Python/DuckDB equivalence.
- Spikes B1–B3 plus sizing, with results in [spikes/NOTES.md](../spikes/NOTES.md). All passed, so no fallback is needed.
- The `Procfile` moves to Phase 3, when there are processes to start.

**Phase 1 — Pure core** (`envelope`, `plan`, `replay`, `status`, `collapse`) ✅
- Tests: a test for every envelope limit, a test for each of A3's three rules, and the Hypothesis replay property (10k examples).
- **Exit:** 100% branch coverage on these modules, and the property test passes.
- Result: 118 tests in about 17 s, and all six pure modules at 100% branch coverage. Three properties hold:
  - the store matches the oracle for any batching (10k examples)
  - outcomes don't depend on batching
  - a crash replay changes neither the store nor any merchant Change's Submission status

  The property test caught one real subtlety on its first run: an internal reclassify can replay as `reclassified` after `skipped`. That's harmless and now documented. Two decisions made along the way: reclassify ignores the source version, and Submission status uses the best outcome, not the latest (both in the design doc).

**Phase 2 — Storage shell** (`landing`, `store`, `state`, `events`) ✅
- Integration tests: an append then a per-partition read, conditional MERGE outcomes, atomic offset writes surviving a simulated crash, a second process failing to lock a partition that's already taken, events readable while being written.
- **Exit:** integration tests pass against real Delta.
- Result: 148 tests in about 17 s, 100% branch coverage across the whole package. The integration tests caught three real bugs before any commit:
  - a worker would stall forever on a stretch of commits with nothing for its partitions
  - after a batch limit cut a commit, the rest of that commit was skipped
  - DuckDB's `ignore_errors` turns a half-written event into a partial event that looks real

  All three are fixed, and each has a test that fails without the fix. Also verified: concurrent MERGEs from two processes each get their own commit version, a killed lock holder releases its partitions, and a crash in the middle of saving an offset leaves the old value.
- **Strict review, round 1** (PR #3 was merged before the review ran, so the fixes landed in a follow-up PR). 10 reviewer angles plus a gap sweep produced 15 reported findings, 12 of them reproduced, all fixed or explicitly deferred:
  - **Bugs:**
    - `ensure` raced when processes started together; fixed with a lock file.
    - Offsets from a recreated Landing log silently skipped rows; offsets now carry the table id.
    - `needs_classify` was overloaded, a reclassify-loop trap; the flag now lives on `Classification`.
    - Strict re-validation of stored rows would strand them after a limit change; stored rows now decode without validation.
    - Key columns were nullable; they're now non-nullable.
    - Naive datetimes were accepted; they're now refused.
    - A corrupt state file gave an unclear crash; it now raises a clear `CorruptState`.
    - `test_compact_keeps_data` was vacuous; it now really compacts.
    - `append([])` made an empty commit.
    - An event's own `ts` overrode the shared stamp.
  - **Performance:**
    - `store.read` turned whole partitions into Python.
    - `landing.read` turned the whole version window into Python and pinned idle partitions; offsets are now half-open cursors with one high-water mark.
    - Compaction to 100 MB files made each MERGE rewrite a whole partition; compaction now targets 1 MiB.
    - `events.read` parsed every line.
    - Tables were re-opened on every call; handles are now kept and refreshed with `update_incremental`.
  - **Docs:** A9, A11 and "DuckDB's role" were stale.
  - **Simplified:** the MERGE guard no longer needs the `_reclassify_only` column or `Write.content_changed`. `ensure` is shared in `catalog/delta.py`. One events file per process run removes the torn-line repair.
  - **Deferred, now written into later phases:** the retention horizon (Phase 5) and heartbeat startup gaps (Phase 3).
  - **Accepted ceiling, documented:** power-loss durability.
  - **Planted-bug check:** reverting each fix fails its regression test. Two tests had to be strengthened before they caught their bug: one now uses a barrier-released race, the other checks the idle partition after every cut-off read.

**Phase 3 — Worker end to end** (with `FakeClassifier`) ✅
- Tests:
  - Poison change: isolated and skipped, and the partition keeps moving.
  - Storage error: the offset does not advance.
  - `kill -9` mid-batch, then restart: the replay oracle holds and outcomes are `already_applied`.
  - Rescale from 4 to 3 workers: every partition keeps its offset.
  - Stale heartbeat: the supervisor restarts the worker.
  - The worker beats once at startup, before its first batch. A worker that hangs before ever beating is still restarted, because the supervisor measures age from its start time. A restarted worker isn't killed for its predecessor's old heartbeat.
- **Rules carried from the Phase 2 review:**
  - keep one table handle per table per process
  - never fork while holding partition claims; use spawn
  - mark a classifier failure with `Classification(..., needs_reclassify=True)`, never by leaving `needs_classify` set
  - every standalone entry point (the worker and supervisor CLIs, and later export, snapshot and chaos runners) exits through `entry.exit_with`, which flushes and calls `os._exit`: Arrow can hang at process exit after a Delta scan ([spikes/NOTES.md](../spikes/NOTES.md), "Exit hang")
- **Exit:** all of the above pass.
- Result: 295 tests in about 45 s, 99.6% branch coverage across the package and the six pure modules at 100%. The chaos tests run real processes. A worker killed with `kill -9` at each step of a batch, during compaction or at a random moment leaves the store equal to the replay oracle, with no Change pending. A rescale from 4 to 3 workers rereads nothing. The workers of a `kill -9`ed supervisor stop on their own. Reviews caught real bugs before merge, each now with a test:
  - bisecting a batch on a data error also caught a corrupt store file's error, failing valid Changes and advancing the offset (3b); a storability check before planning replaced it
  - a classifier outage downgraded good categories to Uncategorized (3a); the stored answer is now kept, flagged
  - a worker still behind after a capped read waited 200 ms anyway, and a failed compaction dropped its batch event (3c)
  - a `kill -9`ed supervisor left its workers running with their locks (found in 3d's review, fixed in 3e)

  Also: pruning partitions before the scan took `store.read` from 1.7 s to 33 ms at 1,280 files, and an Arrow hang at process exit is why every entry point leaves through `os._exit`. Deferred, now written into later steps: 7b's runner must reap a supervisor it kills, 7c adds a reason to the watch's stops and a trace of its hard exit, and 6b's native calls must release the GIL.

**Phase 4 — Ingestion API** (`api`, `merchants`, `status`)
- Tests:
  - Auth: no key, a wrong key, a revoked key.
  - A merchant can't read another merchant's Submission (IDOR).
  - Partial batch: per-item errors come back and the valid items are accepted.
  - A 33 MB body gets 413.
  - Low disk gets 503.
  - Group commit makes one Delta commit for concurrent requests.
  - Submission status moves through pending, then done, then a mix of outcomes.
- **Exit:** tests pass, and an end-to-end request travels HTTP → Landing log → worker → Listing Store.

**Phase 5 — Change Export, Snapshots, maintenance**
- Tests:
  - The export oracle.
  - Re-running after a crash between writing the file and the watermark produces identical output.
  - Gap recovery (A18).
  - Snapshots equal the pinned version.
  - Pruning at 7 days (with an injected clock).
  - Landing log retention and compaction don't break worker reads.
  - **Retention horizon (from the Phase 2 review):** an offset older than the Landing log's oldest readable version must not crash-loop. That happens for a fresh `START` after retention has vacuumed version 0, or for a worker more than 7 days behind; reading vacuumed versions fails forever. Two measures:
    - retention never deletes or vacuums past the slowest partition's offset, with the disk guard as the backstop
    - a worker below the horizon bootstraps from the retained snapshot (pinned version, in `seq` order), then follows the change feed

    The same guard applies to Change Export's watermark (A18).
- **Exit:** all three oracles pass on a generated run.

**Phase 6 — Categorization**
- Load and trim the Shopify taxonomy. `EmbeddingClassifier` (fastembed) with batch classification and a batch timeout. Backfill through `op=reclassify`.
- The eval harness scores all 4 options, then accuracy, p50/p99 latency and cost go into a report.
- Label set: sample about 200 items from Amazon Reviews '23. An LLM can pre-label them, but you verify every label by hand.
- Tests:
  - Threshold boundaries.
  - A timeout produces Uncategorized plus `needs_reclassify`.
  - A price-only change doesn't reclassify.
  - A taxonomy version bump reclassifies through the Landing log.
  - `FakeClassifier` is used everywhere except `tests/model` (marked `model`, with the model cached).
- **Exit:** the eval report is committed and the threshold is chosen from it.

**Phase 7 — Load and chaos**
- Multi-process load generator. Scenarios: steady 50/s, a 10k-item bulk batch, a 1M initial load, out-of-order changes, duplicate retries, a delete followed by a late update, a poison change, a classifier outage, killing a worker, rescaling, and a disk-guard trip.
- Metrics as saved DuckDB SQL: freshness p50/p99, lag per partition, classify latency, stale/conflict/failed rates, Uncategorized rate, worker utilization.
- A runbook covers start, stop, rescale, reset and reading results.
- Measure three costs from the PR #4 review at full batch size (1,000 random keys, 1M Listings) before the SLO run, and fix only the ones that break it:
  - `store.read` decodes every column of the touched partitions. Fix: scan the key columns first, then `take` the matching rows.
  - A batch MERGE touches about 63% of 1 MiB files, because keys are hash-scattered. Fix: key-sorted (Z-order) compaction, or a bucket column.
  - Every Landing log commit moves the offsets of all a worker's partitions, so a tick rewrites (and fsyncs) up to 16 offset files: 2.7 ms on the Mac. Measure on the stress run; if it matters, save offsets that moved only past other partitions' commits less often.
  - Workers on different partitions sometimes hit Delta `CommitFailedError`: about 1 in 412 commits in the 3c design panel's 4-worker run. ADR-0002's spike saw none. The loop's backoff absorbs it; measure the rate under load.
  - `landing.read` re-reads a bulk commit once for every `limit` slice. Fix: end the change-feed range once `limit` pending rows are in hand, or cache the remainder in the worker.
- **Exit:** every scenario ends with the three oracles passing, and the p99 freshness SLO (under 5 minutes) holds at 50/s.

## Decisions (resolved Sep 30)

1. **A1:** drop per-Category validation from v1. ✅
2. **A2:** an upsert is a full replace. ✅
3. **A13 / C:** keep 7 days of Landing log, with no long-term history. ✅
4. **C:** add `availability` as a first-class field. ✅
5. **CI enforcement:** the repo is public, and `main` requires passing checks. ✅
6. **Branch workflow:** every change goes through a PR, and there are no direct pushes to `main`. ✅
