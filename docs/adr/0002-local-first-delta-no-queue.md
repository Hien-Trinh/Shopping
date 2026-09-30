# Run local-first on Delta tables, with the Landing log as the queue

v1 runs as plain Python processes on a Mac. The Landing log, Listing Store and Catalog Snapshots are Delta tables on local disk, read and written with the `deltalake` library (delta-rs). No Spark, cluster or message broker is involved. Workers read the Landing log by offset instead of consuming from a separate queue. The goals are free, unlimited stress testing, and running the Laya classifier natively (MLX needs Apple Silicon). Delta keeps a later move to Databricks a port rather than a rewrite.

## Considered Options

- **Databricks Free Edition from day one.** Rejected: its usage quotas would throttle stress tests, and Laya can't run there.
- **Azure-native (Cosmos DB, its change feed, ADLS).** Rejected: three products to wire together for what Delta gives in one format.
- **A separate queue (Kafka, Redis) between the Ingestion API and workers.** Rejected at about 50 changes/s: the Landing log already has to exist as the only complete record of every version. Using it as the queue removes a system, and replaying is just resetting an offset.
- **DuckDB as the event/metrics store.** Rejected: only one process at a time can hold a DuckDB database open for writing.

## Consequences

- Every Delta write is a commit, so writers batch: the API group-commits for up to 100 ms, and workers do one MERGE per batch of up to 1,000 changes. Workers compact their own partitions.
- Change Export uses the Listing Store's Delta change feed. Its watermark is the last exported table version, advanced only after the export file is written. If cleanup has passed the watermark, Change Export falls back to a full snapshot export.
- Catalog Snapshots are explicit copies, not Delta time travel, because vacuum removes old table versions.
- Events are per-process JSONL files, read by a reader that trusts only complete lines (DuckDB's `ignore_errors` returns partial events). DuckDB is only a read-only query engine for metrics over Delta (via Arrow) and Parquet, with in-memory connections only.
- Retention is capped by one laptop disk: Landing log 7 days, events and exports 3 days. The stress-test ceiling is one machine.
- No Docker: MLX can't use the GPU inside it. Processes run natively from a `Procfile`.
- Verified in a spike with delta-rs 1.6.6: the conditional MERGE ignores stale writes, MERGE writes the change feed, and 8 processes writing disjoint partitions had 0 conflicts. One unexplained MERGE hang was seen, and a heartbeat watchdog restarts stuck workers.
