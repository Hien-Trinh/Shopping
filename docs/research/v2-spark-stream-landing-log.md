# How a Spark stream consumes a Landing log that delta-rs writes

> Research of Oct 12, 2026, for [#131](https://github.com/Hien-Trinh/Shopping/issues/131) on the v2 map ([#123](https://github.com/Hien-Trinh/Shopping/issues/123)). Primary sources only: Databricks docs, the Delta docs and protocol, and the delta-rs and delta-spark source. Each fact carries the date its page showed or the date it was read. "Measured" means a local probe on Oct 12 with the repo's own `landing.ensure` and `landing.append` on `deltalake` 1.6.6, the version `uv.lock` pins. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Answer

1. **Yes, Spark can stream the Landing log as delta-rs writes it.** delta-rs 1.6.6 creates it at reader version 1, writer version 4, with no table features, and writes classic one-file Parquet checkpoints (measured). Every Databricks runtime and delta-spark reads that.
2. **The retention DELETE breaks a plain stream.** A delta-spark stream fails on any commit that removes data, unless `skipChangeCommits` is set. With it, the DELETE commits are skipped whole. They add no new Changes, so nothing is lost. Compaction (`OPTIMIZE`, `dataChange=false`) is ignored without any option.
3. **Exactly-once into the Listing Store is the MERGE's job, as in v1.** `foreachBatch` is at-least-once. delta-spark's MERGE does honor `txnAppId`/`txnVersion`, set through the session config, so a replayed batch is skipped. v1's conditional MERGE (`s.source_version > t.source_version`, or equal version and equal hash) already makes a replay harmless. Events emitted inside the batch can still repeat.
4. **A micro-batch must be cut to one row per key before the MERGE.** Databricks fails a MERGE when two source rows match one target row. The stream will put several Changes for one Listing in one batch, which v1's workers also had to handle.
5. **Latency: no published number for 50 Changes/s.** Databricks says to use a continuous Lakeflow Job with `processingTime` 0 for operational streams, never `AvailableNow`. The default trigger is quoted at 3–5 s. Serverless (so Free Edition) runs only `AvailableNow`, so the latency question can only be measured in the paid trial.
6. **Retention is the stream's risk, not the format.** v1 keeps the Landing log's history 1 hour and cleans by the slowest reader's offset. A stream that falls behind removed log or data files fails with `DELTA_FILE_NOT_FOUND_DETAILED`. v2's maintenance must read the stream's checkpoint offset, or keep a fixed window longer than any outage.
7. **Keep the Landing log an external table, written by path.** Unity Catalog does not list delta-rs as a supported external writer, and writes to managed tables need catalog commits that delta-rs does not do. External tables accept writes by path, but Unity Catalog then governs nothing.

## What delta-rs 1.6.6 writes (measured Oct 12, 2026)

`landing.ensure` creates the table with `delta.enableChangeDataFeed`, `delta.enableExpiredLogCleanup=false` and `delta.logRetentionDuration=interval 1 hours` ([src/catalog/delta.py](../../src/catalog/delta.py)).

| Item | What the log holds | Spark/Databricks reader |
|---|---|---|
| Protocol | `{"minReaderVersion":1,"minWriterVersion":4}`, no `readerFeatures` or `writerFeatures` | Change data feed is writer version 4, reader 1; supported on every Databricks runtime ([Databricks, Sep 11, 2026](https://docs.databricks.com/aws/en/delta/feature-compatibility)) |
| Append (`write_deltalake`, mode append) | one `add` per partition touched, `dataChange: true`, stats as JSON | the rows a stream emits |
| `optimize.compact()` | `remove` and `add`, all `dataChange: false` | skipped by the stream ([DeltaSource.scala](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSource.scala), master at 0e1c865, Oct 10, 2026); the protocol says streams may skip such actions ([PROTOCOL.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md), read Oct 12) |
| `delete(...)` (retention) | `remove` with `dataChange: true`, plus a `cdc` file | the stream fails unless `skipChangeCommits` (see below) |
| Checkpoints | classic `NNN.checkpoint.parquet` and `_last_checkpoint`; automatic every 100 commits (versions 99, 199 after 205 appends) | classic checkpoints are the V1 spec; V2 checkpoints only exist when their table feature is on ([PROTOCOL.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)) |
| `commitInfo` | `engineInfo: delta-rs:py-1.6.6` | informational |

- The default checkpoint interval is 100 commits in delta-rs ([config.rs at python-v1.6.6](https://github.com/delta-io/delta-rs/blob/python-v1.6.6/crates/core/src/table/config.rs), released Sep 24, 2026). delta-spark's default is 10 ([DeltaConfig.scala](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala), master Oct 10, 2026).
- delta-rs 1.6.6 lists writer versions 2–5 and 7 (table features) as supported, and identity columns (writer 6) as not ([feature-table.md at python-v1.6.6](https://github.com/delta-io/delta-rs/blob/python-v1.6.6/docs/feature-table.md)).
- delta-rs writes data with copy-on-write. An open feature request says it can read and keep deletion vectors but not write them ([delta-rs #4512](https://github.com/delta-io/delta-rs/issues/4512), read Oct 12).
- delta-rs commits safely to Azure without a locking provider; only AWS S3 needs one ([writing-to-s3-with-locking-provider.md at python-v1.6.6](https://github.com/delta-io/delta-rs/blob/python-v1.6.6/docs/usage/writing/writing-to-s3-with-locking-provider.md)). Delta gives full transactional guarantees for concurrent writers from several clusters on Azure Blob and ADLS Gen2 ([Delta storage docs](https://docs.delta.io/latest/delta-storage.html), read Oct 12).

## The streaming source and source changes

| Commit on the Landing log | No option | `skipChangeCommits` | Source |
|---|---|---|---|
| Append (adds only) | emitted | emitted | [DeltaSource.scala](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSource.scala) |
| Compaction (`dataChange=false`) | ignored | ignored | same; only `dataChange` adds and removes are checked |
| DELETE removing whole files | fails ("deletes") | commit skipped | same |
| DELETE rewriting a file (remove + add) | fails ("changes") | commit skipped, so survivors are not re-emitted | same |

- Default: "Structured Streaming only accepts append inputs"; an `UPDATE`, `DELETE`, `MERGE` or `OVERWRITE` on the source fails the stream ([Databricks, Sep 11, 2026](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)).
- `skipChangeCommits` needs DBR 12.2 LTS or above; Databricks recommends it over `ignoreChanges` and `ignoreDeletes` ([same page](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)).
- `maxFilesPerTrigger` defaults to 1,000 files; `maxBytesPerTrigger` is unset and soft; with both, a batch ends at whichever comes first ([same page](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)).
- `startingVersion` applies to a new query only; once the checkpoint holds progress it is ignored. A missing start version fails the stream ([same page](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)).
- A commit that changes table metadata (properties, protocol, schema) stops a streaming read; it must restart ([Databricks isolation levels, Sep 11, 2026](https://docs.databricks.com/aws/en/optimizations/isolation-level)). Changing the Landing log's properties from either side restarts the job.
- The source's schema is always the table's latest ([Databricks, Sep 11, 2026](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)).

## Exactly-once into the Listing Store

| Mechanism | What it guarantees | Source |
|---|---|---|
| `foreachBatch` | at-least-once; a batch can be replayed with the same `batchId` | [Databricks foreachBatch, Sep 11, 2026](https://docs.databricks.com/aws/en/structured-streaming/foreach) |
| `txnAppId` + `txnVersion` (writer options) | Delta skips a write whose version is not above the table's last for that app id | [Delta batch docs](https://github.com/delta-io/delta/blob/master/docs/src/content/docs/delta-batch.mdx), master Oct 10, 2026 |
| Same pair via `spark.databricks.delta.write.txnAppId` / `txnVersion` session confs | MERGE, UPDATE and DELETE call `hasBeenExecuted`, so a MERGE with them set is skipped on replay | [DeltaCommand.scala](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/commands/DeltaCommand.scala) and `MergeIntoCommand.scala`, master Oct 10, 2026 |
| v1's conditional MERGE | a replayed row matches `equal version AND equal hash` and rewrites the same row | [src/catalog/store.py](../../src/catalog/store.py) |

- A new checkpoint restarts batch ids at 0, so it needs a new `txnAppId`, or every write is ignored ([Databricks, Sep 11, 2026](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)).
- On restart, the batch planned in the last run is processed first, with the same files ([Databricks production, Oct 8, 2026](https://docs.databricks.com/aws/en/structured-streaming/production)).
- `foreachBatch` can receive an empty DataFrame and must handle it ([Databricks foreachBatch, Sep 11, 2026](https://docs.databricks.com/aws/en/structured-streaming/foreach)).
- MERGE: "Only a single row from the source table can match a given row in the target table." From DBR 16.0 the check includes `WHEN MATCHED` conditions ([Databricks MERGE, Sep 11, 2026](https://docs.databricks.com/aws/en/delta/merge)).
- `WHEN MATCHED AND <cond>` clauses run in order; a matched pair that meets no condition leaves the target row unchanged ([same page](https://docs.databricks.com/aws/en/delta/merge)). That is how stale and conflict Changes stay unapplied, as in v1.
- Stale and conflict outcomes are not reported by MERGE per row. v1 decides them before the MERGE against a read of the store ([src/catalog/decide.py](../../src/catalog/decide.py)); the stream would do the same inside the batch.

## Deletion vectors and change data feed on the Listing Store

In v2 only Databricks writes the Listing Store ([#126](https://github.com/Hien-Trinh/Shopping/issues/126)), so delta-rs's lack of deletion-vector writes does not matter there.

- Deletion vectors need reader 3, writer 7. Non-Photon MERGE writes them from DBR 14.3 LTS; Photon from 12.2 LTS ([Databricks, Sep 11, 2026](https://docs.databricks.com/aws/en/delta/deletion-vectors)).
- Workspaces can turn deletion vectors on for **new** tables on DBR 14.3 LTS and above; defaults vary by region ([same page](https://docs.databricks.com/aws/en/delta/deletion-vectors)).
- The change data feed page lists no limitation with deletion vectors. Its files go with `VACUUM`; the feed of a removed version is gone ([Databricks CDF, Oct 9, 2026](https://docs.databricks.com/aws/en/delta/delta-change-data-feed)).

## Latency and triggers

| Option | What Databricks says | Source |
|---|---|---|
| Default (`processingTime` 0) | a new batch as soon as the last ends; "3–5 second latency" in the trigger table; more storage API calls | [Triggers, Oct 8, 2026](https://docs.databricks.com/aws/en/structured-streaming/triggers) |
| `processingTime` N | fixed interval | same |
| `AvailableNow` | everything available, then stop; "Don't use AvailableNow, Once, or Continuous for operational workloads" | [Production, Oct 8, 2026](https://docs.databricks.com/aws/en/structured-streaming/production) |
| Continuous processing | not supported | [Triggers](https://docs.databricks.com/aws/en/structured-streaming/triggers) |
| Real-time mode | under 1 s at the tail, about 300 ms typical | same |
| Serverless | `AvailableNow` and `Once` only | same |

- Run the stream as a Lakeflow Job with a Continuous schedule: it restarts on failure with backoff and prevents concurrent runs. Use jobs compute without autoscaling ([Production, Oct 8, 2026](https://docs.databricks.com/aws/en/structured-streaming/production)).
- Free Edition is serverless-only ([Free Edition limitations, Sep 29, 2026](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)). So the Free Edition smoke check of [#128](https://github.com/Hien-Trinh/Shopping/issues/128) can run the job only as `AvailableNow`.
- File count per batch: v1's group commit lands at most 10 commits a second ([src/catalog/landing.py](../../src/catalog/landing.py)), each with one file per partition touched, up to 64. The default 1,000-file cap is then 1.5–100 s of data per batch.

## Retention, VACUUM and the stream's checkpoint

- The stream "must run at least one time within the source table's retention window". Defaults: 7 days for data files, 30 days for the log ([Databricks, Sep 11, 2026](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)).
- Past that window it fails with `DELTA_FILE_NOT_FOUND_DETAILED` and needs a full refresh. `failOnDataLoss=false` skips lost data; `ignoreMissingFiles` "silently produces incorrect results" ([same page](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)).
- v1 sets the Landing log's log retention to 1 hour and turns expired-log cleanup off, so only maintenance removes log files, never past the slowest offset ([src/catalog/delta.py](../../src/catalog/delta.py)). delta-spark reads the same `delta.enableExpiredLogCleanup` and `delta.logRetentionDuration` properties ([DeltaConfig.scala](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala)).
- Metadata cleanup must keep a checkpoint for the oldest kept version ([PROTOCOL.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)).
- `VACUUM` skips directories that start with `_`, so a checkpoint under the table path in `_checkpoints` survives ([Databricks, Sep 11, 2026](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)).
- Compaction leaves old files as tombstones. A stream behind a compaction still reads the original files, so the Landing log's vacuum must keep them until the stream passes.

## Unity Catalog and who may write the Landing log

- External tables keep data in storage you manage; Unity Catalog governs metadata but not lifecycle or layout ([Azure Databricks external tables, Oct 6, 2026](https://learn.microsoft.com/en-us/azure/databricks/tables/external)).
- Metadata changed by a non-Databricks client does not sync to Unity Catalog; run `MSCK REPAIR TABLE ... SYNC METADATA` after ([same page](https://learn.microsoft.com/en-us/azure/databricks/tables/external)).
- The Unity REST API supports writes to external Delta tables from Apache Spark; Flink, DuckDB, StreamNative and Starburst are in Beta. delta-rs is not listed ([Azure Databricks, Sep 11, 2026](https://learn.microsoft.com/en-us/azure/databricks/external-access/unity-rest)).
- External writes to managed tables need catalog commits (DBR 16.4+) ([same page](https://learn.microsoft.com/en-us/azure/databricks/external-access/unity-rest)).

## Not confirmed

- That a Databricks or local delta-spark stream reads this exact table end to end. The facts above say it should; no Spark was run. The #128 local Spark setup is the place to check it.
- p99 freshness at 50 Changes/s on one small jobs cluster with `processingTime` 0. No primary source gives a number; it needs the paid trial.
- The cost of many tiny files per batch on ADLS (up to 640 new files a second at v1's commit shape).
- Whether a Databricks `DELETE` or `OPTIMIZE` on the Landing log, if v2's maintenance runs there, writes deletion vectors and so upgrades the protocol past what delta-rs 1.6.6 writes. The workspace default is documented for new tables only.
- Whether delta-rs 1.6.6 can append to a table at reader 3 / writer 7 with deletion vectors present. The feature request says it reads and keeps them; not tested here.
- Whether Free Edition can reach an ADLS account the user owns. The limitations page restricts outbound access and does not say.
