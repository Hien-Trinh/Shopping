---
status: proposed
---

# Run on Azure and Databricks, split at the Landing log

v2 moves the Listing pipeline to Azure and Databricks, to run v1's stress exit in the cloud inside a $50 cash cap beyond the free credits. The Ingestion API stays a small Python service on Container Apps and writes the Landing log on ADLS with delta-rs. Everything after the Landing log (the Ingestion job, Change Export, Listing Store Snapshots and the Listing Store's maintenance) runs in one Databricks streaming job. The Landing log stays the queue: a Delta table both engines read, with no broker. This supersedes [ADR-0002](0002-local-first-delta-no-queue.md).

## Considered Options

- **Port v1's Python processes as they are.** Rejected: it keeps every delta-rs cost, step 7f's copy-on-write churn among them, and teaches little.
- **Host the API as a Databricks App.** Rejected: it bills $0.475/h while it is up.
- **Event Hubs as the queue.** Rejected: it bills while idle, and the Landing log would stop being a table the store oracle reads.
- **JSON files through Auto Loader.** Rejected: an extra hop and a split Landing log.
- **A serverless workspace.** Rejected: it has no compute policies to guard the cap, and only `AvailableNow` or `Once` triggers.
- **Free Edition for the stress runs.** Rejected: it reaches neither ADLS nor delta-rs, and its acceptable use policy forbids publishing timings. It runs a pass/fail smoke check only.

## Consequences

Each of ADR-0002's consequences, in its order, and what replaces it:

- Every Delta write is still a commit, so writers still batch. The API group-commits the Landing log and the Submission log. The Ingestion job does one MERGE per 60 s micro-batch, capped by `maxBytesPerTrigger`. An hourly `OPTIMIZE` replaces per-worker compaction.
- Change Export is a second change-feed query in the same job. Its checkpoint is the watermark: it commits only after the export file is written.
- Listing Store Snapshots are Unity Catalog shallow clones, not copies. A clone shares the Listing Store's files, so the Listing Store's VACUUM retention stays at least the age of the oldest Snapshot: Delta's 7-day default, the Snapshots' lifetime.
- There is no event log. Outcomes are facts in two single-writer tables, the API's Submission log and the Ingestion job's Outcomes. Diagnostics go to Log Analytics and to cluster logs on a Unity Catalog volume.
- Deletion vectors on the Listing Store replace copy-on-write rewrites, which ends step 7f's churn.
- A commit is durable once ADLS acknowledges it, so v1's "no fsync" ceiling is gone.
- Retention windows are fixed: the Landing log 7 days, export files 3 days, Snapshots 7 days. The stress-test ceiling is the $50 cap and the vCPU quota, not one disk.
- The API ships as a Docker image, since MLX's reason is gone: the student classifies on CPU executors. GitHub Actions drives each paid run by OIDC, so the laptop only issues commands.
- The cross-engine tables keep to features both engines read and write. The Landing log stays at reader 1 / writer 4, and only delta-rs writes it. Outcomes has no deletion vectors and uses classic checkpoints, so delta-rs can read it. The ADLS interop is first proven in the paid smoke run.
- No partition writers race any more. On Databricks, `OPTIMIZE` can't conflict with the MERGE (row-level concurrency). Local open-source Delta can, so the MERGE retries inside the batch.

New in v2:

- The API is public over HTTPS, with per-Merchant rate limits. v1 binds to `127.0.0.1` and lists rate limits as future work.
- Python 3.12 everywhere, the Databricks Runtime's version.
- A preflight enforces the $50 cash cap, since Azure budgets only alert. A deadline watchdog deletes a `run` that outlives its deadline.
