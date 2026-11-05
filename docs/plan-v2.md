# v2 Build Plan — Azure and Databricks

Nov 1, 2026 · Decided on the [v2 map][map] in 28 decision tickets (Oct 12 – Nov 1). Builds on [plan-v1.md](plan-v1.md), [design-commerce-ingestion-pipeline.md](design-commerce-ingestion-pipeline.md) and two new ADRs, [ADR-0003](adr/0003-azure-databricks-split-at-landing-log.md) and [ADR-0004](adr/0004-one-streaming-job.md), both `proposed` until the swap. Terms are defined in [GLOSSARY.md](../GLOSSARY.md). Each decision's reasons and rejected options live in its ticket; this plan orders the decisions into steps.

## Summary

v2 moves the Listing pipeline to Azure and Databricks and runs v1's stress exit there, inside a $50 cash cap beyond the free credits. The Ingestion API stays Python on Container Apps, and everything after the Landing log runs in one Databricks streaming job ([ADR-0003](adr/0003-azure-databricks-split-at-landing-log.md), [ADR-0004](adr/0004-one-streaming-job.md)). Everything before the paid runs costs $0 and happens before the Azure sign-up.

**Status (Nov 1):** every decision is made and every ticket on the map is closed. Nothing is built yet. The 36 steps below run through the phase loop (mini PRD → TDD → lean-review → retro), in six phases: groundwork, A (the Spark pipeline on local Spark), the swap, B (the API for the cloud), C (cloud code, offline), then the paid runs.

## What runs where

| Part | Runs on | Written in |
| --- | --- | --- |
| Ingestion API | Container Apps, 2 fixed replicas of 1 vCPU and 2 GiB | Python 3.12, delta-rs |
| Landing log upkeep | a scheduled Container Apps Job, from the API's image | Python 3.12, delta-rs |
| Streaming job: the Ingestion job, Change Export, the maintenance thread | Databricks Runtime 18 (Spark 4.1.0, JDK 21) on classic jobs compute, D4as_v5, a driver + 7 workers | PySpark on Python 3.12 |
| Check job: freshness, rates, the three oracles | a single-node classic job cluster | PySpark |
| Deadline watchdog | a Container Apps Job in `base`, every 15 minutes | Microsoft's `azure-cli` image, no code of ours |
| Terraform, the load generator, the chaos runner, the models step | GitHub Actions runners, logged in to Azure by OIDC | HCL, Python, bash |
| The laptop | `gh workflow run`; ruff on push; `make check` and `make spark` by hand | — |

Other languages, none of them a new service: Spark SQL for `OPTIMIZE`, `SHALLOW CLONE` and `VACUUM`; GitHub Actions YAML; a bash sign-up wizard.

## What was verified before planning

| Check | Result | Ticket |
| --- | --- | --- |
| Spark streams a Landing log that delta-rs 1.6.6 writes | ✅ reader 1 / writer 4 with no table features, classic checkpoints every 100 commits; every runtime and delta-spark reads it | [Spark reads the Landing log][t131] |
| delta-rs's retention DELETE and `OPTIMIZE` against the change feed | ✅ the DELETE writes `cdc` files and the table stays at reader 1 / writer 4; `OPTIMIZE` adds no change-feed rows | [streaming job][t137] |
| delta-rs commits on Azure | ✅ one conditional Put Blob per commit and never a rename, read from the pinned code | [commit cost][t130] |
| The repo on Python 3.12 | ✅ 963 of 963 tests pass on 3.12.11 after three kinds of fix. With Runtime libraries, unit and student tests pass 214 of 214; the 72 failures are all delta-rs paths that stay on Container Apps. That trial used deltalake 1.5.1, and 18 LTS ships 1.1.4 | [3.12 packaging][t141], [Oct 28 review][t166] |
| Two API replicas appending to one table | ✅ 4 writers × 150 appends on stale handles: 599 commits, 1 `CommitFailedError`, 0 lost rows (local disk) | [API on Container Apps][t161] |
| Local Spark in tests | ✅ a session starts in 4–5 s warm (31 s the first time, for 85 MB of jars) and holds about 850 MB; a write, MERGE and change-feed stream take 1.3 s with `snapshotPartitions=1` | [CI][t162] |
| A 60 s trigger with a 1,000-Change cap | ❌ it holds the job near 17 Changes/s: locally, with 1,000 rows waiting, a 5 s trigger ran batches of about 105 rows. Hence the 10k cap | [Oct 28 review][t166] |
| A checkpoint reset with a fixed `txnAppId` | ❌ it lost 40 of 100 rows with no error. Hence `txnAppId` = the query's id | [Oct 28 review][t166] |
| A change-feed stream's first start with no checkpoint | ❌ every row carried the latest `_commit_version` (15 rows from 5 commits, all version 4). Hence the pinned `startingVersion` | [Oct 28 review][t166] |
| MERGE txn options set outside `foreachBatch` | ❌ confs on the outer session stamped stale tags on `OPTIMIZE` commits, and a `DeltaTable` bound outside `foreachBatch` ignored them. Hence v1's replay rule | [Oct 28 review][t166] |
| API memory for a near-max body | ✅ a 27.5 MiB body peaks at about 66 MiB once parsed (about 100 MiB with its bytes); the load generator's 6 MiB batches at about 26 MiB | [API on Container Apps][t161] |

Inferred, not measured, so checked later: the student's rate per vCPU (5.7–11.4 Listings/s, so 5–9 vCPUs for 50/s), that Databricks renames once per commit, and that executors read `/Volumes` from a pandas UDF ([student on Spark][t132], [commit cost][t130]). The Free Edition runs and the paid smoke run check them.

## A. Rules that hold across steps

Every step keeps these. Each ticket holds the reasons.

1. **Each table's writers are fixed.** Only delta-rs writes the Landing log: the API's appends, and the upkeep job as its only maintainer. Only the API writes the Submission log. Only the streaming job writes the Listing Store (the Ingestion job's MERGE, the maintenance thread's `OPTIMIZE` and `VACUUM`), and only the Ingestion job writes Outcomes. Anything else that changes a Listing appends to the Landing log or runs inside the job ([ADR-0004](adr/0004-one-streaming-job.md), [Landing log and Outcomes][t136], [off the laptop][t165]).
2. **Landing order is `(_commit_version, seq)`** from the Landing log's change feed, and the highest source version per key wins. The stream's first start pins `startingVersion` to the earliest retained version, which is 0 after a clean start ([streaming job][t137], [Oct 28 review][t166]).
3. **v1's replay rule:** a written Change replays as `already_applied` or `stale`. There is no `batchId` tag, no time-travel plan and no MERGE txn. Only the Outcomes append carries `txnAppId` (the streaming query's id, kept across restarts from one checkpoint) and `txnVersion` (the batch id) ([Oct 28 review][t166]).
4. **Checkpoints live next to the tables, in `base`.** The job refuses to start with no checkpoint over a non-empty Outcomes table. A clean start wipes both, so it passes ([Oct 28 review][t166], [A2's spec](specs/step-a2.md)).
5. **Freshness is accepted → exported, per Change,** counting only Changes whose best Outcome is `written`. v1's `freshness_s` meant accepted → first Outcome, so every report says which definition a number uses ([events and oracles][t160]).
6. **Facts in tables, diagnostics in logs.** Freshness, the rates and the oracles read only tables and export files, never a diagnostic line. Diagnostics are JSON lines: the API's on stdout to Log Analytics, the job's on the driver to cluster logs on a Unity Catalog volume ([events and oracles][t160]).
7. **Every exit attempt starts clean** and runs its own 1M load. Between runs, `base` keeps only infrastructure. The smoke run loads on the student; debug reruns may use `fake` ([Oct 28 review][t166], [smoke check][t163]).
8. **PR jobs hold no secrets and no cloud identity.** Only hand-started workflows (Free Edition, `stress.yml`, the `base` apply) do, and nothing secret prints: the Merchant key is masked, Terraform prints only its summary line, and no plan file is uploaded ([CI][t162], [off the laptop][t165]).
9. **Cash spend never passes $50.** The preflight refuses a run it can't afford, and the subscription budget only alerts ([Oct 28 review][t166], [smoke check][t163]).
10. **Timings:** paid-trial and Azure numbers are published with their method line. Free Edition prints pass or fail only, and so does any number that rests on a Beta, Private Preview or Experimental feature ([Free Edition][t129], [publishing timings][t167]).
11. **The shipped job carries no fault hooks.** Faults come from outside, through control-plane calls, or from local step-precise tests ([chaos][t138]).

## B. Risks to prove early

| # | Risk | Where it is proven |
| --- | --- | --- |
| B1 | Spark reading the delta-rs Landing log's change feed on real ADLS, and delta-rs reading the Outcomes table Databricks writes | Locally on open-source Delta from A1 and A2; on ADLS first in the smoke run (P3) |
| B2 | The job draining 50/s on the student under the 10k cap | P3's exit gate: the backlog doesn't grow at 50/s over 3 or more intervals. P4 retunes the cap |
| B3 | Runtime 18's own libraries under our `--no-deps` wheel | The `spark` and `model` jobs on the compiled file (G5, A4), and the Free Edition runs on the real runtime (from A3) |
| B4 | The student's real rate per core | P3's bulk drain rate. Whether P3's SLO window held 50/s picks the exit classifier (P4) |
| B5 | Two replicas' lost commit races on ADLS: 409 or 412 (object_store retries only 409), and the `append_retry` rate | P3's probe and record; one replica if over 5% of appends retry (P4) |
| B6 | Unity Catalog objects created and dropped through workspace-admin grants in per-run workspaces | P3's apply and P6's opening steps. Fallback: the account admin role, set by hand |
| B7 | A deleted job cluster ends the run FAILED, so the retry fires | P3's fault 1 |
| B8 | A Free Edition personal access token works with the Runs Submit API | A3's first run |
| B9 | The Azure CLI's token still working an hour into a phase job | P1's drill: one `az` call 61 minutes after login |

## C. Scope gaps accepted for v2 (say if any is wrong)

- **Fault 2 (one executor lost) is dropped.** No documented outside path kills one executor, and a hook would be test code inside the job ([Oct 28 review][t166]).
- **No hung-batch or ADLS-throttling fault.** The first costs 10+ paid minutes for one setting, and the second has no cheap injection ([chaos][t138]).
- **"Maintenance races the MERGE" runs only in the paid chaos window.** Open-source Delta can conflict where Databricks can't, so a CI runner would fail for the wrong reason ([CI][t162]).
- **No GPU path.** A T4 would sit 90–95% idle at 50/s and save $3–6, once, on the 1M load ([GPU][t155]).
- **Revoking or rotating a Merchant key takes effect at the next `terraform apply`,** about a minute later, not at once as in v1 ([API on Container Apps][t161]).
- **A hard kill between the API's two commits** leaves Landing log rows with no Submission log rows. That's harmless: the oracles read the Landing log, and freshness counts only Submission log Changes ([API on Container Apps][t161]).
- **One region (East US) and one 14-day trial** ([provisioning][t139], [smoke check][t163]).
- **A 1M-row store's effects first show in an exit attempt.** The smoke run's store is a tenth of that size ([smoke check][t163]).
- **A dying cluster can lose its last ~5 minutes of diagnostics,** since cluster logs are delivered about every 5 minutes ([events and oracles][t160]).
- **`REORG … APPLY (PURGE)` is left out** until a smoke run shows deletion-vector rows piling up ([Change Export and maintenance][t156]).
- **v1's local data isn't migrated.** v2 starts empty, and the 1M initial load fills it (the map's Out of scope).
- **Nothing in the cloud is kept after v2's exit.** `base` and `tfstate` are destroyed and the subscription is cancelled ([phase order][t164]).
- **v1's open steps 5b, 5e and 7e don't run.** v2 replaces the code they would change: the export checkpoint rule (A7) replaces 5b's gap recovery, fixed retention windows (A1) replace 5e's horizon bootstrap, and the check job (A9) replaces 7e's DuckDB metrics.

## CI

**Every PR runs on GitHub Actions and must be green before merging, as in v1. CI is the gate: the Mac runs only ruff when you push.**

| Job or workflow | Runs | Arrives with |
| --- | --- | --- |
| `check` | `make check`: ruff, unit and integration tests, both coverage gates; from B1, the API's ADLS-path tests against Azurite (a pinned Azurite started through `npx` on a free port by a pytest fixture) | required already |
| `spark` | `make spark` in the Spark env, on every PR and push to `main`, with no path filter | G5, required |
| `stress-smoke` | v1's ten legs until S2, then the seven local Spark legs | A11; the matrix swaps in S2 |
| `model` | Linux in the Spark env: the bge-small tests and one real-student test, path-filtered by a job-level `if` | A4, which makes it required |
| `image` | builds `linux/amd64` and smoke-runs one authenticated POST on every PR; pushes `ghcr.io/hien-trinh/shopping:<sha>` on `main` | B5, required |
| `terraform` | `fmt -check`, `init -backend=false -lockfile=readonly` and `validate` on `base` and `run`, with no `plan` and no Azure identity | C1, required |
| Free Edition smoke (`workflow_dispatch`) | our wheel on Free Edition's serverless compute; prints pass or fail only | A3 |
| `stress.yml` and the `base` apply (`workflow_dispatch`, OIDC) | the paid runs | C3 |

Details that matter:
- **Required once every job has arrived:** `check`, `spark`, `stress-smoke`, `image`, `model`, `terraform`. Each joins `main`'s protection when its job arrives. Today the protection lists only `check` and `stress-smoke`, although plan-v1 called `model` required too.
- **A stale lock fails its job:** the compiled requirements file and `.terraform.lock.hcl` are checked as `UV_LOCKED` checks `uv.lock`.
- **The Free Edition workflow never runs on PRs.** Its quota is unpublished, and going over stops compute for the day.

## Architecture after v2

```
Merchant ─HTTPS─▶ Ingestion API ×2 (Container Apps: group commit, per-Merchant rate limit)
                    │ append (delta-rs)                  │ append (delta-rs), before the 202
                    ▼                                    ▼
      Landing log (Delta on ADLS, change feed, 7 d)    Submission log (Delta on ADLS)
        ▲ upkeep job (delta-rs): 7-day DELETE, OPTIMIZE, VACUUM
                    │ readChangeFeed in (_commit_version, seq) order, every 60 s
                    ▼
  Streaming job (Databricks, classic jobs compute, one driver)
    Ingestion job:  plan (v1's fold) → classify (student UDF) → MERGE → Outcomes append
                    then the Backfill tail (op=reclassify)
    Change Export:  the Listing Store's change feed → one Parquet file per batch (3 d)
    maintenance:    OPTIMIZE hourly · shallow-clone Snapshots every 6 h, kept 7 d · VACUUM daily
                    │ MERGE                               │ append
                    ▼                                     ▼
      Listing Store (Delta, liquid clustering,         Outcomes (Delta, append-only,
                     deletion vectors)                           no deletion vectors)
```

GET /submissions/{id} reads the Submission log and Outcomes. The check job reads every table and the export files.

## Code layout

After G4 the package is `src/ingestion/`, with v1's **functional core and imperative shell**:

- **Kept:** the pure core (`keys`, `envelope`, `plan`, `replay`, `status`, `collapse`), unchanged and at 100% branch coverage, and categorization and its eval (`classify`, `classifiers`, `student`, `taxonomy`, `evaluate`, `benchmark`, `jev`, `laya`). The Ingestion job calls `plan` per Listing key through `applyInPandas`, so `plan`'s property test still covers the job's decisions.
- **Kept outside Databricks:** `api`, `merchants` and `landing` on Container Apps, and `load` on the runners; A1 and Phase B change them. The upkeep CLI (A10) reuses `maintenance`'s Landing log code without its offset gate.
- **New in Phase A:** the streaming job's two queries, its maintenance thread and the check job, run from the Spark env. Each step's spec names their modules.
- **Deleted in S2:** `worker`, `supervisor`, `store`, `export`, `snapshots`, `metrics`, `chaos`, the Outcome events in `events`, the Listing Store half of `maintenance`, and the `Procfile`. S2's spec sorts out the shared helpers (`state`, `entry`, `decide`, `backfill`, and the `SCHEMA`, `row`, `stored`, `unstorable` and `_APPLY_IF` of `store`, which `ingest` and `decide` use), since `student` imports `state` and `entry`.
- **New in Phases B and C:** the `Dockerfile`; Terraform's `base` and `run` root modules, plus one for the workspace-level resources; the workflows; the sign-up wizard.

## Testing rules

v1's [testing rules](plan-v1.md#testing-rules) still hold: the coverage gates, the 100% pure list, property tests, real Delta tables, an injected clock, classifier and paths, no `sleep` in unit tests, asserted times. v2 adds:

- **Spark tests run in `make spark`,** not `make check`: the Spark env, one shared session with `spark.databricks.delta.snapshotPartitions=1`, and no xdist, since each JVM holds about 850 MB.
- **No mocks of delta-spark or delta-rs.** Spark tests run against real local Delta tables, as v1's integration tests do.
- **The ADLS path is tested against Azurite,** never against a mock of object_store.
- **Crash points are local Spark tests,** step-precise, including the one between the MERGE and the Outcomes append. The shipped job carries no fault hooks; the paid chaos window injects faults from outside.
- **The `fake` classifier everywhere** except the `model` job's bge-small tests and its one real-student test.
- **Timings come from the paid trial,** each with a method line: commit, workflow run, region, node type × count, runtime, store size, classifier and profile. Free Edition prints pass or fail, never a duration.

## Review process

As in [plan-v1.md](plan-v1.md#review-process-from-oct-1): `/lean-review` on every step PR, PR bodies through the `pr` skill, `/retro` after each round, and a measured ceiling gets a step. Two changes for v2:

- Rename-only and delete-only PRs (G4, S2) are exempt from the ~300-line cap.
- G2 adds a v2 step's row in this plan to `.claude/lean-review.md`'s spec sources, so the first reviewed v2 step finds its spec. S3 points the profile's other v1 references at the v2 docs.

## PR steps

Each step is one PR of **under about 300 changed lines, tests included**, merged before the next one starts, as in v1. Code steps start with a mini PRD in `docs/specs/step-<id>.md` (`step-a2.md`, say). ⏸ means the loop stops for you, and ✅ marks a finished step.

| Phase | Step | PR |
| --- | --- | --- |
| G | G1 ✅ | Tag `v1` on `main` (no tags exist yet), so v1's docs and their `src/catalog/` paths keep a fixed target. A tag push, not a PR ([phase order][t164]) |
| G | G2 ✅ | The pre-push hook runs ruff only (lint and format check, under 1 s). CI's `check` job is the gate, and `make check` stays for hand runs. The "Same gate as CI" line in `.githooks/pre-push` changes; `plan-v1.md` keeps its words. Plus one line in `.claude/lean-review.md`'s spec sources: a v2 step's row in this plan ([CI][t162]) |
| G | G3 ✅ | The Python 3.12 port: the 7 unparenthesized `except A, B:` clauses (api ×2, jev, load, state ×2, student), `from __future__ import annotations` in `student.py` (a PEP 649 forward reference), and a small `uuid7` stand-in for `api.py` and `backfill.py` (a 48-bit millisecond timestamp, version 7, variant bits) with its test; `requires-python >=3.12`, `.python-version` and CI on 3.12. The pins stay (pyarrow ≥25, deltalake ≥1.6.6, numpy ≥2.3). The Oct 23 trial passed 963 of 963 tests on 3.12.11 ([3.12 packaging][t141]) |
| G | G4 ✅ | The rename `catalog` → `ingestion`, package and dist name, as one rename-only PR whose SHA goes in `.git-blame-ignore-revs`: the imports, `pyproject.toml`, the Makefile's `PURE` list, `mutation.yml`, `ci.yml` and the `Procfile`. `ingestion` because the package holds the whole context: the Ingestion API, the Ingestion job, categorization and the eval. Words change only in living docs (README, GLOSSARY.md, `.claude/lean-review.md`). v1's docs keep v1's words and their `src/catalog/` paths, which exist at the `v1` tag; `docs/specs/step-7f.md:28`, their one real code link, becomes a link to the tag ([phase order][t164], [glossary][t135]) |
| G | G5 ✅ | The compiled requirements file (`uv pip compile` with Runtime 18's versions as constraints, plus `fastembed==0.9.0` and its exact transitive pins); the Spark env (that file plus pyspark 4.1 and delta-spark 4.4); `make spark` (a second pytest run: one shared session with `snapshotPartitions=1`, no xdist); and the required `spark` job: every PR and push to `main`, no path filter, JDK 21 from the runner (`JAVA_HOME_21_X64`), a step that fails on a stale compiled file, the Delta jars cached by the pyspark and delta-spark versions, under 5 minutes with a 10-minute timeout. A first Spark test (a write, a MERGE, a change-feed read) gives the job something to run ([3.12 packaging][t141], [CI][t162]) |
| A | A1 ✅ | Spec: [step-a1.md](specs/step-a1.md). The three cross-engine tables, and one init step that creates them, since two API replicas starting on an empty store would both try. **Landing log:** unpartitioned (the `partition` column stays); rows, removed files (`delta.deletedFileRetentionDuration`) and history (`delta.logRetentionDuration`) all kept 7 days; written only by delta-rs. **Submission log:** the API's record of accepted and rejected Changes, written with delta-rs in the same group-commit window, after the Landing log commit and before the 202. **Outcomes:** append-only, `delta.enableDeletionVectors=false`, classic checkpoints. The Submission log and Outcomes are partitioned by the hour of the submission's UUIDv7. `landing.retained` gains one row filter and a test: it picks a partition with `file_pruning_predicate`, which prunes files, not rows, so on an unpartitioned log it would hand a v1 worker other partitions' Changes (`landing.read` is already safe: `load_cdf`'s predicate filters rows). v2 is then tested on its final layout from the first day ([Landing log and Outcomes][t136], [Oct 28 review][t166]) |
| A | A2 | Spec: [step-a2.md](specs/step-a2.md). The ingest query, on `fake`: `readChangeFeed` on the Landing log, inserts only, each key's Changes ordered by `(_commit_version, seq)`; a 60 s `processingTime` trigger; `maxBytesPerTrigger` = 8 MB, with `maxFilesPerTrigger` raised so the byte cap binds, and the API's group commit holds a commit to 10k Changes. The plan is v1's pure function per Listing key (`groupBy(...).applyInPandas`), cut to one row per key before one MERGE per micro-batch into the Listing Store (unpartitioned, liquid clustering on `(merchant_id, merchant_product_id)`, deletion vectors on). Then one Outcomes append with `txnAppId` and `txnVersion`. The replay rule, `startingVersion`, the checkpoints and the start guard follow rules 2–4 above; a recreated Landing log is refused natively (`DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`); the MERGE is retried inside the batch when a concurrent `OPTIMIZE` fails it, as local open-source Delta can ([streaming job][t137], [Spark reads the Landing log][t131], [Oct 28 review][t166]) |
| A | A3 ⏸ | The Free Edition workflow and its first run: `workflow_dispatch`, started with `gh workflow run`. It builds the wheel, copies it to a Unity Catalog volume, makes one one-time serverless run through the Runs Submit API on Free Edition's own storage (`AvailableNow`), and prints each check's pass or fail, never a duration. It authenticates with a personal access token in a repository secret. ⏸ You make the Free Edition account. The workflow runs again as each later piece of Phase A lands ([Free Edition][t129], [CI][t162]) |
| A | A4 | Classification on executors: the student as an iterator pandas UDF that loads the encoder and the index once per Python worker and classifies only the rows the plan marks; each micro-batch repartitioned to the slot count; the driver computes `student.version()` and `index_path()` and broadcasts them, since executors have no checkout; the files on a Unity Catalog volume, loaded with `local_files_only`; the UDF catches per-row errors and never raises, so v1's Uncategorized path holds. The `model` job moves to Linux in the Spark env (Python 3.12, Runtime 18's libraries, fastembed 0.9.0): it keeps the bge-small tests, adds one real-student test through the UDF in local Spark, caches `models/` by the student's version (the Hub revision plus the `train/` hash), adds `student`, the UDF module and the compiled file to its path filter, and becomes required. The macOS leg goes ([streaming job][t137], [student on Spark][t132], [CI][t162]) |
| A | A5 | The Backfill as each micro-batch's tail: up to v1's `LIMIT` pending Listings (`needs_reclassify` or an older taxonomy version, oldest first), read from the Listing Store with Spark and run as `op=reclassify` after the batch's Merchant Changes, with Outcomes `reclassified` or `skipped` under a `backfill-…` submission id. Only one round is ever in flight ([streaming job][t137]) |
| A | A6 | Liveness. A driver watchdog stops the query and raises when no batch finishes in 10 minutes while input waits, on both queries; a `StreamingQueryListener` writes each batch's progress as JSON lines on the driver. The job's settings, which Terraform `run` applies in C2: 3 task retries 30 s apart (fault-1 firings + 3 in a paid run), an email on run failure, and a `STREAMING_BACKLOG_BYTES` alert above 1.5 × `maxBytesPerTrigger`, a notice, not a gate ([streaming job][t137], [events and oracles][t160], [Oct 28 review][t166]) |
| A | A7 | Change Export: a second streaming query in the same job, `readChangeFeed` on the Listing Store every 60 s. Its checkpoint is the watermark: it commits only after `foreachBatch` has written the file. One Parquet file per batch, named by the batch's `_commit_version` range, holding the latest row per Listing key (an insert or an `update_postimage`; preimages dropped; a tombstone becomes `op=delete`). Files are written once, never rewritten, kept 3 days, with v1's adopt-a-file recovery (step 5a). A failed export query fails the run. If Change Export ever falls behind retention (`DELTA_CHANGE_DATA_FILE_NOT_FOUND`), the runbook says to delete its checkpoint: the restarted stream exports the table's current state as inserts, which is v1's full-snapshot export ([Change Export and maintenance][t156], [Oct 28 review][t166]) |
| A | A8 | The maintenance thread, in the streaming task's driver: incremental `OPTIMIZE` (liquid clustering) every hour, in its own `FAIR` pool so it can't starve a micro-batch; Listing Store Snapshots as Unity Catalog external shallow clones named `snapshot_v<version>` (`SHALLOW CLONE … VERSION AS OF v LOCATION …`), every 6 h (30 min in stress runs, a config value) and dropped after 7 days; `VACUUM` once at start and then every 24 h on Delta's defaults (removed files 7 days, history 30 days). The VACUUM retention stays at least the age of the oldest Snapshot, since VACUUM on the source breaks a shallow clone ([Change Export and maintenance][t156], [Oct 28 review][t166]) |
| A | A9 | The check job. Freshness per Change: the `_metadata.file_modification_time` of the first export file holding a row with the same Listing key and a `source_version` at least the Change's. Then the rates, and v1's three oracles ported to Spark: the store oracle from an empty store over the Landing log, the export oracle over every export file, and the snapshot oracle comparing each clone with the export replay up to its pin. The snapshot oracle keeps v1's guard by content: it fails unless at least one clone is non-empty and pinned after the first MERGE. It runs over a stopped system after each window, prints v1's one-line JSON summary and exits nonzero on a failed oracle ([events and oracles][t160], [Oct 28 review][t166]) |
| A | A10 | The Landing log upkeep CLI: the 7-day `DELETE`, `OPTIMIZE` and `VACUUM` with delta-rs, from v1's maintenance code without its offset gate (v1 gated the DELETE on worker offsets). Its DELETE and VACUUM against a running stream are tested with an injected clock, as `test_maintenance.py` does. In the cloud it runs as a scheduled Container Apps Job from the API's image (C2); this step sets its cadence ([Landing log and Outcomes][t136], [Oct 28 review][t166]) |
| A | A11 | The seven `stress-smoke` legs, under the same required name: steady, bulk, out-of-order, duplicates, poison and classifier outage, plus Landing log upkeep mid-stream (a compact, the retention DELETE and a vacuum while the stream reads). Each leg runs the API and the upkeep from the uv env, and the streaming job, the export and the check job's oracles from the Spark env, with the `fake` classifier, a 5 s trigger and an ingest cap that drains inside the timeout. The gate is the three oracles; latency is not gated. v1's ten legs run beside them until S2 ([chaos][t138], [CI][t162]) |
| S | S1 | GET /submissions/{id} reads the Submission log and Outcomes with v1's best-Outcome rule, so `rejected`, `pending` and 404 behave as in v1. Until this step it folds v1's JSONL events, which S2 deletes ([Landing log and Outcomes][t136], [phase order][t164]) |
| S | S2 | One delete-only PR: v1's workers and supervisor, the delta-rs Listing Store, export and Snapshot code, the Listing Store's maintenance, the Outcome events, the DuckDB metrics, the v1 chaos runner, the `Procfile` and v1's ten legs. The `stress-smoke` matrix keeps only A11's seven ([phase order][t164], [CI][t162]) |
| S | S3 | One docs PR. The design doc, `runbook.md` and `stress.md` stay in place, frozen, each with a one-line banner: it describes the `v1` tag, and the current version is the `-v2` file. Freeze, not rewrite, because v1's specs and `plan-v1.md` link the design doc by file, with no anchor. New: `design-v2.md` (starting from a copy of the goals, non-goals, data model and categorization), `runbook-v2.md` and a `stress-v2.md` skeleton. README and `.claude/lean-review.md` point at the v2 docs. GLOSSARY.md gains the Oct 28 review's lines: Freshness, and that the streaming job runs the Ingestion job, Change Export and the Listing Store's maintenance. ADR-0003 and ADR-0004 become `accepted`, ADR-0002 `superseded by ADR-0003` and ADR-0001 `superseded by ADR-0004` ([phase order][t164], [Oct 28 review][t166]) |
| B | B1 | The API on ADLS: the ADLS path, tested in `check` against Azurite; the managed identity (`AZURE_MSI_ENDPOINT` set from `IDENTITY_ENDPOINT`, since delta-rs copies only `AZURE_*` variables); diagnostics as JSON lines on stdout (request logs, process start, append retries, refusals); `--min-free 0`, since the API writes nothing that matters to local disk. v1's single retry in `landing.append` stays for two replicas ([API on Container Apps][t161], [events and oracles][t160], [CI][t162]) |
| B | B2 | The registry import at startup. The API's Container Apps secret holds the Merchant rows (`merchant_id`, `currency`, `key_hash`, `status`, rate limit), and an environment variable holds their SHA-256, so every registry change rolls out a new revision. Each replica writes the rows into a local read-only SQLite before `create_app`, so `merchants.verify` and its tests stay as they are. v1's CLI stays for local runs ([API on Container Apps][t161]) |
| B | B3 | The per-Merchant rate limiter: a token bucket in Changes/s in each replica's memory, at the limit ÷ 2. Over the limit, the API answers 429 with `Retry-After` and logs a `refused` line with reason `rate_limited`. Each Merchant's limit is a field in its registry row; the stress Merchant's sits above the 1M load's rate. A CI test with two Merchants, one over its limit, proves it. This step sets the numbers ([API on Container Apps][t161]) |
| B | B4 | The load generator re-sends the same body on a connection error, a 5xx or a 429 (honouring `Retry-After`), with backoff, and counts retries in its summary. It takes `https://` only, since it refuses redirects. Its one-line JSON summary labels request latency as measured from outside Azure ([API on Container Apps][t161], [off the laptop][t165]) |
| B | B5 ⏸ | The `Dockerfile` and the required `image` job. The image is `linux/amd64`, copies only `pyproject.toml`, `uv.lock`, `README.md` and `src/`, and serves both the API and the upkeep job with different commands. The job builds with plain Docker on every PR (no third-party actions, no layer cache) and smoke-runs one authenticated POST that must return 202, against a temporary Landing log and a test Merchant. On `main` only, it pushes `ghcr.io/hien-trinh/shopping:<sha>`, with `packages: write` on that job alone. ⏸ You switch the package to public once ([API on Container Apps][t161], [CI][t162]) |
| C | C1 | Terraform `base`, East US, kept between paid runs. It holds: the ADLS account (StorageV2, hierarchical namespace, Hot, LRS, blob soft delete off) with the tables, the checkpoints, the export files and the volumes' files; Key Vault with the Merchant keys (`random_password`, `random_id`); a subscription-scope budget alerting at $200, $225 and $250 gross, with its action group; Log Analytics with a daily cap; a Consumption-only Container Apps environment with the deadline watchdog (a Container Apps Job every 15 minutes on Microsoft's `azure-cli` image, with its own managed identity and no standing roles) and an activity-log alert that emails when it deletes a `run` group; and the workflow identity's Key Vault Secrets Officer and Storage Blob Data Reader roles. Plus the required `terraform` job, and `.gitignore` gains `*.tfstate*`, `*.tfvars` and `.terraform/` ([provisioning][t139], [off the laptop][t165], [Oct 28 review][t166], [CI][t162]) |
| C | C2 | Terraform `run`, applied before each paid run and destroyed after it. Its first resources are its resource group, tagged `deadline` (apply time plus the deadline input), and the watchdog's Contributor role on it. Then: VNet injection (two delegated /26 subnets, one NSG, a NAT gateway, a `Microsoft.Storage` service endpoint on both subnets); the classic workspace (Trial tier) and its access connector; the Unity Catalog objects (the three cross-engine tables and the Listing Store as external tables, the external model volume and the cluster-log volume, all on `base`'s account); the compute policy (D4as_v5 only, Photon off, the Ingestion job's cluster a fixed driver + 7 workers, or + 4 if the quota is refused, the check job on a single node, `dbus_per_hour` ≤ 9); the jobs as `databricks_job` resources (dedicated access mode, run as a service principal, `max_concurrent_runs` = 1, `timeout_seconds` = the deadline, A6's settings), with `SELECT` only on the Landing log and the Submission log; the API (2 fixed replicas, public HTTPS ingress, the image by SHA, the registry secret) and the upkeep job. Workspace-level resources sit in their own root module, whose state is discarded after a watchdog fire. A run's opening drops the Unity Catalog objects an older run left, through workspace-admin grants ([provisioning][t139], [workspace kind][t142], [smoke check][t163], [Oct 28 review][t166]) |
| C | C3 | `stress.yml` and the `base`-apply workflow, both `workflow_dispatch` under the managed identity. `stress.yml` takes a `profile` (`smoke` or `exit`) and a deadline; a profile changes numbers (load size, window lengths, fault firings, deadline), not steps, apart from two smoke-only steps: the 409-or-412 probe and the NAT data read. Its jobs: the preflight; the phase jobs, handing over through the remote state (the models step, the 1k canary, the load generator, the SLO window, the chaos window with its chaos runner and recovery poll, the check job after each window); teardown, `if: always()`; and the cost job behind the `cost` environment's 72 h wait timer. The models step lands here, after A4 has given CI's `model` job the cache it restores. The rules are under Phase C below ([off the laptop][t165], [smoke check][t163], [chaos][t138], [Oct 28 review][t166]) |
| C | C4 | The sign-up wizard, an interactive bash script for the steps only you can do: make the Azure account (the 30-day $200 credit starts); upgrade to pay-as-you-go and remove the spending limit; request 32 DASv5 vCPUs and 32 regional vCPUs in East US, before any workspace; register the resource providers; create the `tfstate` group with az (a storage account with shared keys off and versioning on, the managed identity, federated credentials for the GitHub environments `azure` (deployments from `main` only) and `cost`, Contributor plus an RBAC Administrator role conditioned against admin roles on the subscription, and Storage Blob Data Contributor on the state container) and the two GitHub environments with their secrets; apply `base` by workflow; copy the Databricks account ID, unless P3 shows workspace-admin grants are enough. It also walks you through P1's drills ([provisioning][t139], [off the laptop][t165], [smoke check][t163], [Oct 28 review][t166]) |
| C | C5 | `runbook-v2.md`'s cloud sections; the list of Databricks features v2 times, with their release status; and `stress-v2.md`'s smoke section, ready for P3's numbers ([smoke check][t163], [publishing timings][t167]) |
| P | P1 ⏸ | Sign-up and the $0 drills, before the trial. You run the wizard. Then: record the quota answer; the first `base` apply by workflow, which proves the `azure` OIDC subject and the state backend; one `az` call 61 minutes after login, for the token's lifetime; and the watchdog fired once on a throwaway group whose `deadline` tag has passed, which proves `az login --identity` in a Container Apps Job and the alert email. Stop gate: the watchdog deletes the group within 15 minutes past its tag, and the email arrives ([smoke check][t163], [off the laptop][t165]) |
| P | P2 ⏸ | The cancel drill: dispatch the smoke profile, then `gh run cancel` while the workspace is created. This starts the 14-day DBU trial. Stop gate: no `run` group and no managed group are left ([smoke check][t163]) |
| P | P3 ⏸ | The smoke run, right after, on the student throughout: the 1k canary, a 100k bulk (its drain rate is the student's capacity), a 15-minute SLO window at 50/s, and faults 1, 3, 4 and 5 once each; then the drain, the check job and teardown. About 2–2.5 h, with a 4 h deadline; worst case about $18, usually $3–5 from the credit and $0 of cash. Its gates and records are under the paid phase below ([smoke check][t163]) |
| P | P4 ⏸ | One settings PR, with `stress-v2.md`'s smoke section, showing each number and its rule. **Ingest cap:** keep A2's 8 MB unless a full batch takes over 200 s; then 200 s × the measured drain rate, never under 6k. **Backlog alert:** 1.5 × the cap, in bytes. **`OPTIMIZE`:** hourly, unless the batches beside it take over 1.5× the median; then every 2 h. **API replicas:** 1 if over 5% of appends retry, or any append still fails after its retry. **`limit_concurrency`:** unset unless a replica's peak passes 1.5 GiB; then 15 per replica. **Exit classifier:** the student if the SLO window held 50/s; else `fake`, and the report says so. An image pull over 60 s makes a lean API image a follow-up, not a blocker ([smoke check][t163]) |
| P | P5 | The smoke run's cost job, 72 h after its teardown; it runs by itself. It records the ledger, how trial DBUs appear in Cost analysis by meter, the rename meter, `base`'s idle Container Apps environment, the day the data looked complete, and the `cost` OIDC subject. Exit attempts don't wait for it ([smoke check][t163], [Oct 28 review][t166]) |
| P | P6 ⏸ | Exit attempts, only once the smoke run's exit gates hold. Each opens by dropping the leftover Unity Catalog objects and reusing the model files, both before any load; starts clean, wiping `base`'s data (the Landing log, the Listing Store, Outcomes, the Submission log, the export files and the checkpoints); and runs v2's exit (under the paid phase below), with a deadline of about 12 h. The phase ends at the first passing attempt or the first preflight refusal. The credit lasts 30 days from the account's creation, and an attempt's worst case (about 12 h, about $54 at list prices) is more than the $50 of cash, so the preflight refuses from about day 29. `docs/stress-v2.md` records the verdict either way; a fail also records what failed and the next fix ([phase order][t164], [exit][t127], [Oct 28 review][t166]) |
| P | P7 ⏸ | The wind-down, in this order: the last cost job (72 h); `base` destroyed by workflow; the `tfstate` group deleted with az, by you; the subscription cancelled, by you. Every job logs in as the managed identity in `tfstate`, so `tfstate` goes last ([phase order][t164]) |

## Phases

Each phase ends green on CI, and its exit criteria are the tests.

**Groundwork — G1–G5** ($0)
- **Exit:** `check` and `spark` pass on Python 3.12, the package is `ingestion`, and the `v1` tag exists.

**Phase A — the Spark pipeline on local Spark, A1–A11** ($0, before sign-up)
- Phase A comes first because it holds v2's unknowns: v1's pure plan inside `applyInPandas`, the change feed's order, `txnAppId` on the Outcomes append, MERGE with deletion vectors, and Runtime 18's libraries under `--no-deps`. A surprise here can change the tables Phase B writes and the jobs Phase C deploys.
- Tests, beyond each step's own:
  - Changes landed in several commits before the stream's first start: first-one-wins matches the store oracle.
  - Crash points, including the one between the MERGE and the Outcomes append, replay to the three oracles. A written Change may then report `already_applied`; the Listing Store is correct either way.
  - A recreated Landing log, and a missing checkpoint over a non-empty Outcomes table, both refuse to start.
  - A concurrent `OPTIMIZE` that fails the MERGE locally is retried inside the batch.
  - The upkeep's DELETE and VACUUM against a running stream, with an injected clock.
- **Exit:** the seven `stress-smoke` legs pass with the three oracles, and the Free Edition workflow passes on Phase A's last code.

**The swap — S1–S3** ($0, at the end of Phase A, once the seven legs pass)
- **Exit:** `main` holds only v2's pipeline code, with the seven legs as `stress-smoke`; the v1 docs carry their banners; the ADR statuses have changed.

**Phase B — the API for the cloud, B1–B5** ($0)
- Tests: the ADLS path against Azurite; the two-Merchant rate-limit test; the load generator's retries and its refusal of `http://`; the image's smoke POST.
- **Exit:** `image` is required and green, and `check` runs the Azurite tests.

**Phase C — cloud code, offline, C1–C5** ($0; still no Azure account, so `terraform validate` is the only check)
- The rules `stress.yml` carries:
  - **Preflight,** the workflow's first job. It prints trial days left (from the sign-up date in config), credit left (typed in from the portal) and cash left. It refuses the run if the worst case (the deadline × about $4.51/h at list prices, DBUs counted) is more than the credit left plus the cash left. The credit counts only if it is still valid after the deadline.
  - **Deadline:** the calculated run time × 1.5, rounded up to the half hour. That gives 4 h for the smoke run and about 12 h for an exit attempt.
  - **Faults** 1, 3, 4 and 5 fire one at a time, at random moments: once each in the smoke profile, 3 times each in the exit profile. Fault 1 deletes the job cluster through the Jobs API (`runs/get`, then `clusters/delete`). Fault 3 is `az containerapp revision restart`. Fault 4 starts the real upkeep job (`az containerapp job start`), a Landing log compaction mid-stream. Fault 5 starts a Listing Store `OPTIMIZE`.
  - **Recovery:** every 10 s, the poll checks whether each query's committed offset covers the version recorded at injection (the Landing log's for ingest, the Listing Store's for export): `(V + 1, −1)` or `(V, last index)`. Each fault gets 20 minutes. Past that it is logged as not recovered, and the run goes on to the drain and the oracles.
  - **After each window:** stop the load, wait for backlog 0 and the last export file, stop the streaming job, run the check job, and gate on its exit code.
  - **Records:** the job summary and a JSON artifact with the method line. The cost job adds the ledger 72 h after teardown: gross cost from Cost Management over the `run` group, the Databricks managed group and `base` for the run's dates; trial DBU-hours × $0.30 as the post-trial price; cash = max(0, gross − $200) while the credit is active.
  - **Teardown** breaks a stale state lease before `terraform destroy`, and greps the earlier jobs' logs for the Merchant key.
- **Exit:** `terraform` is required and green; `stress.yml`, the `base`-apply workflow and the wizard are merged; the feature list and the runbook's cloud sections exist. Nothing has been applied.

**The paid runs — P1–P7** (each stage stops for you)
- **The smoke run's gates (P3)** use the exit's own pass lines. A p99 near 300 s is a warning sign, since the exit's store is 10× bigger.
  - **Stop gates** (tear down, fix, rerun): the apply (the VNet-injected workspace, Unity Catalog objects through workspace-admin grants, the image pulled, the model files built); the canary within 15 minutes; the Merchant key never in the run's logs (a hit means rotating the key).
  - **Exit gates** (the run goes on and returns its numbers; no exit attempt until fixed): the backlog doesn't grow at 50/s over 3 or more intervals, and freshness p99 < 300 s; each fault recovers within 20 minutes, fault 1 ends the job run FAILED and a retry starts, and faults 4 and 5 fail no MERGE; the three oracles pass, the snapshot guard included; each export file's mtime falls inside its batch; NAT data processed < 1 GB; teardown leaves nothing; the next run's opening drops the leftover Unity Catalog objects.
  - **Records:** the drain rate and the rate per core; per-batch time at the cap; bytes per Change; the `append_retry` rate and the 409-or-412 status; the replica memory peak; image pull time; whether a restart replaces both replicas at once; cluster start time; `OPTIMIZE`'s duration; cluster log delivery; the model files skipped on the next run.
- **v2's exit (P6):** a clean start; the 1M initial load (on the student, or on `fake` if P4's rule says so); then 30 minutes at 50 Changes/s with freshness p99 under 300 s; then the chaos window (faults 1, 3, 4 and 5, three times each, each recovered within 20 minutes); the drain; the three oracles passing; and the run's cost recorded ([exit][t127], [chaos][t138], [events and oracles][t160]).

## Where each decision lives

Where a later ticket changed an earlier one, the later answer wins, and an update comment on the earlier ticket says what changed.

| Area | Tickets |
| --- | --- |
| Money | [What would v2 cost on Azure and Databricks?][t124] · [What is the cash cap, and when do the cloud credits start?][t125] · [What does a Delta commit cost on ADLS, for delta-rs and for Databricks?][t130] |
| Shape | [Which v1 processes become Databricks-native, and which stay Python?][t126] · [Classic or serverless Databricks workspace for v2?][t142] · [Which processes leave the laptop, and where does each run?][t165] |
| Exit and chaos | [What exit must v2's cloud stress run pass?][t127] · [Which chaos scenarios does the cloud stress run include?][t138] · [What does the first paid-trial smoke check run, and what must each check show?][t163] · [May v2 publish timings measured on the Azure Databricks trial and on Azure?][t167] |
| Before sign-up | [Where does v2's Spark code run before the cloud sign-up?][t128] · [Can Databricks Free Edition run v2's smoke check?][t129] · [What does CI run for v2?][t162] · [How is the Databricks-side code packaged for Python 3.12?][t141] · [Which pre-v2 refactors are worth finishing before v2?][t134] |
| Data path | [How does a Spark stream consume a Landing log that delta-rs writes?][t131] · [How do Changes reach the Landing log, and Outcomes the Submission, in v2?][t136] · [What is the design of the streaming job that replaces the Ingestion workers?][t137] · [How does the student classifier run inside a Spark job on CPU?][t132] · [Can GPU workers on Databricks run the student's classification step, and at what cost?][t155] · [How do Change Export, Listing Store Snapshots and the Listing Store's maintenance run on Databricks?][t156] · [Where do v2's events and metrics live, and how are the oracles measured in the cloud?][t160] |
| API | [How does the Ingestion API run on Container Apps?][t161] |
| Cloud setup | [Which tool provisions and tears down the v2 cloud setup?][t133] · [How is the v2 cloud setup provisioned, guarded and torn down?][t139] |
| Words, review and order | [What do the glossary's component terms mean under v2?][t135] · [Which closed v2 decisions change after the Oct 28 review?][t166] · [What is plan-v2's phase order, and what does the v2 ADR decide?][t164] |

[map]: https://github.com/Hien-Trinh/Shopping/issues/123
[t124]: https://github.com/Hien-Trinh/Shopping/issues/124
[t125]: https://github.com/Hien-Trinh/Shopping/issues/125
[t126]: https://github.com/Hien-Trinh/Shopping/issues/126
[t127]: https://github.com/Hien-Trinh/Shopping/issues/127
[t128]: https://github.com/Hien-Trinh/Shopping/issues/128
[t129]: https://github.com/Hien-Trinh/Shopping/issues/129
[t130]: https://github.com/Hien-Trinh/Shopping/issues/130
[t131]: https://github.com/Hien-Trinh/Shopping/issues/131
[t132]: https://github.com/Hien-Trinh/Shopping/issues/132
[t133]: https://github.com/Hien-Trinh/Shopping/issues/133
[t134]: https://github.com/Hien-Trinh/Shopping/issues/134
[t135]: https://github.com/Hien-Trinh/Shopping/issues/135
[t136]: https://github.com/Hien-Trinh/Shopping/issues/136
[t137]: https://github.com/Hien-Trinh/Shopping/issues/137
[t138]: https://github.com/Hien-Trinh/Shopping/issues/138
[t139]: https://github.com/Hien-Trinh/Shopping/issues/139
[t141]: https://github.com/Hien-Trinh/Shopping/issues/141
[t142]: https://github.com/Hien-Trinh/Shopping/issues/142
[t155]: https://github.com/Hien-Trinh/Shopping/issues/155
[t156]: https://github.com/Hien-Trinh/Shopping/issues/156
[t160]: https://github.com/Hien-Trinh/Shopping/issues/160
[t161]: https://github.com/Hien-Trinh/Shopping/issues/161
[t162]: https://github.com/Hien-Trinh/Shopping/issues/162
[t163]: https://github.com/Hien-Trinh/Shopping/issues/163
[t164]: https://github.com/Hien-Trinh/Shopping/issues/164
[t165]: https://github.com/Hien-Trinh/Shopping/issues/165
[t166]: https://github.com/Hien-Trinh/Shopping/issues/166
[t167]: https://github.com/Hien-Trinh/Shopping/issues/167
