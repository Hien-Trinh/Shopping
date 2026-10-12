# What a Delta commit costs on ADLS, for delta-rs and for Databricks

> Research of Oct 12, 2026, for the v2 cloud plan (ticket #130). It builds on [v2-cloud-costs.md](https://github.com/Hien-Trinh/Shopping/blob/research/v2-cloud-costs/docs/research/v2-cloud-costs.md) on the `research/v2-cloud-costs` branch and does not redo it. Primary sources only: the delta-rs and object_store source at the versions this repo runs, the Delta Lake source, learn.microsoft.com (each page's `ms.date` is given), azure.microsoft.com, and Microsoft's Retail Prices API (`prices.azure.com`). Prices are US dollars, pay as you go, East US, list price, read Oct 12, 2026. Terms follow [GLOSSARY.md](../../GLOSSARY.md).
>
> **Unconfirmed** marks something no primary source settled. **Inferred** marks a step of reasoning, not a quoted fact.

## Answer

1. **delta-rs never renames on Azure.** It commits with one conditional Put Blob (`If-None-Match: *`) on the Blob endpoint, even for an `abfss://` URL. A small data file is one more Put Blob. So a delta-rs commit is about 1 + (files) write operations, plus one list and a few reads. This holds with hierarchical namespace on or off.
2. **Databricks most likely renames once per commit.** Open-source Delta's `AzureLogStore` writes a temp file, then renames it into `_delta_log`. Databricks Runtime is closed source, so this is **inferred** for Databricks.
3. **The rename may not be the $13 risk.** Microsoft's two sources disagree on a file rename's price. Learn maps `RenamePathFile` to "Iterative Write" at $0.065 per 100. The Data Lake pricing page lists `RenamePathFile` as an ordinary write, at $0.065 per 10,000. For about 11,250 Databricks commits an hour that is **$0.07 or $7.31**. Which one the bill uses is **unconfirmed**.
4. **Concurrent writing is safe in both engines, and safe side by side.** delta-rs relies on Azure's atomic create-if-absent. Databricks relies on atomic rename without overwrite, which needs hierarchical namespace. They write different tables, so their commit files never meet.
5. **Unity Catalog decides the namespace: external locations need hierarchical namespace.** Databricks must read the Landing log, so the Landing log must sit on a hierarchical-namespace account too.
6. **Recommended account: one StorageV2 account, hierarchical namespace on, Hot, LRS, blob soft delete off** (or 1 day). Cool is worse: double the write price, and a 30-day early-deletion charge on every file the MERGE churn rewrites.
7. **Storage cost per SLO hour, on v1's measured shape (21 Changes/s): about $2.3 to $2.8 if a rename bills as a write, about $9.6 to $10.0 if it bills as an iterative write.** At 50 Changes/s, scaled linearly as an upper bound: about $5.6 to $6.6, or $23 to $24. Data-file writes ($1.65 at 21/s) are the largest line once renames are cheap.

## 1. delta-rs's commit path on Azure (source, as pinned)

The repo pins `deltalake` 1.6.6 in [uv.lock](../../uv.lock). That wheel is tag [`python-v1.6.6`](https://github.com/delta-io/delta-rs/tree/python-v1.6.6) (commit `104886f`, Sep 24, 2026). The wheel's native library embeds `object_store-0.13.2` (54 matches in `strings` of `deltalake/_internal.abi3.so`, read Oct 12, 2026). object_store [`v0.13.2`](https://github.com/apache/arrow-rs-object-store/tree/v0.13.2) is commit `7a65b75` (Mar 19, 2026).

| Step | What the source does | Source |
|---|---|---|
| Which log store Azure gets | The Azure factory registers `az`, `adl`, `azure`, `abfs` and `abfss`, and returns `default_logstore`. No Azure-specific log store exists | [`crates/azure/src/lib.rs`](https://github.com/delta-io/delta-rs/blob/python-v1.6.6/crates/azure/src/lib.rs) |
| Bytes or temp file | `DefaultLogStore` and `LakeFSLogStore` get the commit as bytes. Every other store first writes `_delta_log/_commit_<uuid>.json.tmp` and later renames it | [`crates/core/src/kernel/transaction/mod.rs`](https://github.com/delta-io/delta-rs/blob/python-v1.6.6/crates/core/src/kernel/transaction/mod.rs) (`into_prepared_commit_future`) |
| The commit itself | `put_opts(<version>.json, bytes, PutMode::Create)`. `AlreadyExists` becomes `VersionAlreadyExists`, and the commit loop re-reads the log, checks conflicts and tries the next version | [`crates/core/src/logstore/default_logstore.rs`](https://github.com/delta-io/delta-rs/blob/python-v1.6.6/crates/core/src/logstore/default_logstore.rs) |
| What `PutMode::Create` sends | One Put Blob (`BlockBlob`) with the header `If-None-Match: *` | [`src/azure/client.rs`](https://github.com/apache/arrow-rs-object-store/blob/v0.13.2/src/azure/client.rs) (`put_blob`) |
| Which endpoint | Every request goes to `https://<account>.blob.core.windows.net`. An `abfss://...dfs.core.windows.net` URL is parsed for the account and container only. The DFS endpoint is used only for Fabric | [`src/azure/builder.rs`](https://github.com/apache/arrow-rs-object-store/blob/v0.13.2/src/azure/builder.rs) (`parse_url`, `build`) |
| Data files | A `BufWriter` with a 5 MiB buffer (`DEFAULT_UPLOAD_PART_SIZE`). A file under 5 MiB is one Put Blob. A bigger file becomes Put Block calls plus one Put Block List | [`crates/core/src/datafile/writer.rs`](https://github.com/delta-io/delta-rs/blob/python-v1.6.6/crates/core/src/datafile/writer.rs); [`src/buffered.rs`](https://github.com/apache/arrow-rs-object-store/blob/v0.13.2/src/buffered.rs) |
| Checkpoints | Every 100 commits by default (`delta.checkpointInterval`) | [`crates/core/src/table/config.rs`](https://github.com/delta-io/delta-rs/blob/python-v1.6.6/crates/core/src/table/config.rs) |

The Listing Store and Landing log files are about 1 MiB or less ([ADR-0002](../adr/0002-local-first-delta-no-queue.md), [stress.md](../stress.md)). So in v1's shape **every delta-rs data file is one Put Blob**, not the 3 operations (create, append, flush) the earlier cost model assumed.

### Operations per delta-rs commit

| Operation | Count | Category on HNS (Hot LRS) | Category on flat (Hot LRS) |
|---|---|---|---|
| Put Blob per data file (< 5 MiB) | 1 per file | Write, $0.065 / 10k | Write, $0.05 / 10k |
| Put Blob for `NNN.json` (conditional) | 1 (more on a lost race) | Write, $0.065 / 10k | Write, $0.05 / 10k |
| List Blobs on `_delta_log` to refresh the snapshot | about 1 (**inferred**: the listing lives in delta-kernel, not read) | **Unconfirmed**; likely Iterative Read, $0.065 / 10k | List, $0.05 / 10k |
| Get Blob for new commit files and read data files | a few | Read, $0.0052 / 10k | Read, $0.004 / 10k |
| Checkpoint Parquet + `_last_checkpoint` | 2 every 100 commits | Write | Write |
| Rename | **0** | — | — |

Category mapping: Put Blob is a write and List Blobs is "List and create container" ([Learn: map REST ops to prices](https://learn.microsoft.com/en-us/azure/storage/blobs/map-rest-apis-transaction-categories), ms.date 2025-05-14). That page prices Blob API calls with the Blob pricing page. On a hierarchical-namespace account the bill uses the Data Lake meters, which have no "List and create container" meter. Which Data Lake meter a Blob-API list hits is **unconfirmed**.

### Is delta-rs's commit safe?

- `If-None-Match: *` makes the operation run "only if the resource doesn't exist, and fail the operation if it does exist" ([Learn: conditional headers](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations), ms.date 2023-01-31). Put Blob supports it ([Learn: Put Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob), ms.date 2023-01-30). So two delta-rs writers can never both create version N. No lock service is needed, unlike S3's old DynamoDB setup.
- Blob APIs are fully supported on hierarchical-namespace accounts ([Learn: feature support](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-feature-support-in-storage-accounts), ms.date 2026-05-18). Blob and Data Lake APIs "can operate on the same data" ([Learn: known issues](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-known-issues), ms.date 2026-07-06).
- **One gap, unconfirmed:** Learn says a failed write condition returns 412. object_store maps 409 to `AlreadyExists` and 412 to `Precondition` ([`src/client/retry.rs`](https://github.com/apache/arrow-rs-object-store/blob/v0.13.2/src/client/retry.rs)). Its Azure integration test expects `AlreadyExists` for a second `PutMode::Create` ([`src/integration.rs`](https://github.com/apache/arrow-rs-object-store/blob/v0.13.2/src/integration.rs), `put_opts`), but CI runs it on Azurite, not Azure ([`.github/workflows/ci.yml`](https://github.com/apache/arrow-rs-object-store/blob/v0.13.2/.github/workflows/ci.yml)). If real Azure answers 412, a lost commit race in delta-rs would fail the write instead of retrying it. The table stays correct either way. **Inferred:** check it with two writers racing on the first cloud smoke test.

## 2. Databricks's commit path on ADLS

| Fact | Source |
|---|---|
| Open-source Delta maps `abfs`, `abfss`, `adl`, `wasb` and `wasbs` to `AzureLogStore` | [`DelegatingLogStore.scala`](https://github.com/delta-io/delta/blob/v4.4.1/spark/src/main/scala/org/apache/spark/sql/delta/storage/DelegatingLogStore.scala) at v4.4.1 (released Oct 5, 2026) |
| `AzureLogStore` assumes "Rename without overwrite is atomic" and "List-after-write is consistent", and calls `writeWithRename` | [`AzureLogStore.java`](https://github.com/delta-io/delta/blob/v4.4.1/storage/src/main/java/io/delta/storage/AzureLogStore.java) |
| `writeWithRename`: check the parent exists, check the target doesn't exist, create a temp file, write, close, `fs.rename(temp, target)`. If the rename returns false and the target exists, it throws `FileAlreadyExistsException` | [`HadoopFileSystemLogStore.java`](https://github.com/delta-io/delta/blob/v4.4.1/storage/src/main/java/io/delta/storage/HadoopFileSystemLogStore.java) |
| Delta on Azure gives "full transactional guarantees for concurrent reads and writes from multiple clusters"; it needs `FileSystem.rename()` to be atomic | [docs.delta.io: storage configuration](https://docs.delta.io/delta-storage/) (no page date; read Oct 12, 2026) |
| Databricks: "Delta Lake prevents data corruption when multiple clusters write to the same table concurrently" | [Learn: ACID guarantees](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/acid) (ms.date 2026-09-11) |
| Databricks: "All Azure Databricks writes to cloud object storage use transactional commits", creating `_started_<id>` and `_committed_<id>` files beside data files | Same page |
| Catalog commits move commit coordination to Unity Catalog. Managed tables only, opt-in with `delta.feature.catalogManaged`, Runtime 16.4+. External engines' writes to such tables are Beta | [Learn: catalog commits](https://learn.microsoft.com/en-us/azure/databricks/tables/features/catalog-commits) (ms.date 2026-09-11) |

**Inferred operations per Databricks commit** (open-source path; Databricks Runtime is closed, so **unconfirmed** for Databricks):

| Operation | Data Lake REST call | Category ([Learn mapping](https://learn.microsoft.com/en-us/azure/storage/blobs/map-rest-apis-transaction-categories)) | Count |
|---|---|---|---|
| Parent and target existence checks | Path Get Properties | Read | 2 |
| Temp commit file | Path Create + Append + Flush | Write × 3 | 3 |
| Rename into `_delta_log` | Path Create with rename source (`RenamePathFile`) | Iterative Write per Learn; Write per the pricing page | 1 |
| Each data file | Path Create + Append + Flush | Write × 3 | 3 per file |
| Snapshot refresh | Path List | Iterative Read | about 1 |
| `_started_` / `_committed_` markers | Path Create + Append + Flush | Write | **unconfirmed** whether Delta writes make them |

### The rename price: two Microsoft sources disagree

| Source | What it says about a file rename | Price, Hot LRS |
|---|---|---|
| [Learn: map REST ops to prices](https://learn.microsoft.com/en-us/azure/storage/blobs/map-rest-apis-transaction-categories) (ms.date 2025-05-14) | `RenamePathFile` is "Iterative Write" | $0.065 per 100 |
| [Data Lake pricing page](https://azure.microsoft.com/en-us/pricing/details/storage/data-lake/) (read Oct 12, 2026) | Write operations include "RenameFile, RenamePathFile". Iterative writes are "RenameDirectory, RenamePath, RenamePathDir" | $0.065 per 10,000 |
| [Retail Prices API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Storage%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20productName%20eq%20%27Azure%20Data%20Lake%20Storage%20Gen2%20Hierarchical%20Namespace%27) | "Hot Iterative Write Operations" $0.065 per 100; "Hot Write Operations" $0.065 per 10K | (meters only, no mapping) |

The cost model below shows both.

## 3. Unity Catalog and hierarchical namespace

"Azure Data Lake Storage storage accounts that you use as external locations must have a hierarchical namespace" ([Learn: ADLS external location](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-adls), ms.date 2026-09-11). The same page rules out containers with immutability (WORM) policies, because Unity Catalog needs delete permission.

Hierarchical namespace can't be turned off once on ([Learn: hierarchical namespace](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace), ms.date 2026-07-02).

**Inferred:** the Landing log is written by delta-rs and read by the Databricks streaming job through Unity Catalog. So one hierarchical-namespace account must hold both. A second, flat account for the Landing log alone would save about 23% of its write cost (a few cents an hour) and Databricks couldn't read it through an external location.

## 4. Concurrent writers on one account, different tables

| Writer | Commit primitive | Safe with many writers on the same table? | Needs HNS? |
|---|---|---|---|
| delta-rs (Ingestion API → Landing log) | Conditional Put Blob, create-if-absent | Yes (section 1), subject to the 409/412 gap | No |
| Databricks (streaming job → Listing Store, feed, Snapshots) | Rename without overwrite (inferred) | Yes, per Databricks and Delta docs (section 2) | Yes for atomic rename and for Unity Catalog |
| Both, different tables | Each writes only its own `_delta_log` | **Inferred yes**: no shared commit file, so no race between the two primitives | — |

Two cautions, both **inferred**:

- **Keep the Landing log out of catalog commits.** A Unity Catalog managed table with `catalogManaged` takes its commits through the catalog. delta-rs 1.6.6 writing straight to storage would bypass it. Register the Landing log as an external table, or leave catalog commits off.
- **Don't let both engines write the same table.** A delta-rs Put-if-absent and a Hadoop rename-if-absent both refuse to overwrite, so they would likely still exclude each other. Nothing in the sources tests that mix.

## 5. Account settings

| Setting | Choice | Why | Source |
|---|---|---|---|
| Hierarchical namespace | **On** | Unity Catalog external locations require it; Delta's Azure log store needs atomic rename | Sections 2 and 3 |
| Access tier | **Hot** (the default for new StorageV2 accounts) | Cool writes cost $0.13 per 10k, twice Hot. Cool has a 30-day minimum, and "Early deletion charges also occur if the entire object is rewritten". The MERGE churn deletes about 74 GB an hour, so Cool adds about $1.12 an hour (74 GB × $0.0152) on top of doubled writes | [Learn: access tiers](https://learn.microsoft.com/en-us/azure/storage/blobs/access-tiers-overview) (ms.date 2026-04-02); [API, HNS meters](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Storage%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20productName%20eq%20%27Azure%20Data%20Lake%20Storage%20Gen2%20Hierarchical%20Namespace%27) |
| Redundancy | **LRS** | ZRS writes cost $0.0813 per 10k (+25%). The tables are rebuilt from the 1M load, so zone loss is acceptable for a learning run | Same API |
| Blob soft delete | **Off**, or 1 day | Soft-deleted data "is billed at the same rate as active data" until retention ends. At 7 days, one SLO hour's 74 GB of churn costs about $0.36 more | [Learn: soft delete](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview) (ms.date 2026-10-02) |
| Versioning, change feed | Not available | Not supported with HNS | [Learn: feature support](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-feature-support-in-storage-accounts) |
| Region | Same as the workspace | "To avoid egress charges" | [Learn: ADLS external location](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-adls) |

HNS also bills a "Hot LRS Index" meter at $0.0297 per GB-month for metadata ([API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Storage%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20productName%20eq%20%27Azure%20Data%20Lake%20Storage%20Gen2%20Hierarchical%20Namespace%27)). At a few GB it is cents a month.

### Prices used (East US, Hot LRS, per 10,000 unless noted)

| Meter | HNS on | HNS off (General Block Blob v2) |
|---|---|---|
| Write | $0.065 | $0.05 |
| Read | $0.0052 | $0.004 |
| Iterative read / List | $0.065 | $0.05 ("List and create container") |
| Iterative write | **$0.065 per 100** | no such meter |
| Other | $0.0052 | $0.004 |
| Delete | free | free |
| Data stored | $0.0208 / GB-month | $0.0208 / GB-month |

From the Retail Prices API on Oct 12, 2026: [HNS](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Storage%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20productName%20eq%20%27Azure%20Data%20Lake%20Storage%20Gen2%20Hierarchical%20Namespace%27), [flat](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Storage%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20productName%20eq%20%27General%20Block%20Blob%20v2%27). Operations are counted "every 4 MB" ([Data Lake pricing page](https://azure.microsoft.com/en-us/pricing/details/storage/data-lake/)), so a 1 MiB file is one operation per call.

## 6. Cost per SLO hour, recommended account

Inputs are v1's measured shape from the earlier cost research, scaled to one hour at 21 Changes/s ([stress.md](../stress.md), 7f.2 run 4): 11,250 MERGE commits, 84,700 files (74 GB) rewritten, about 8,600 Landing group commits. Per the map, the API (delta-rs) writes the Landing log and Databricks runs the MERGEs. **Inferred:** Landing files per commit ≤ Changes per commit, so 8,600 to 75,600 Landing files. Databricks MERGE churn is assumed equal to delta-rs's; deletion vectors could shrink it (still open).

| Line | Arithmetic | Rename = write | Rename = iterative |
|---|---|---|---|
| delta-rs: Landing commits | 8,600 Put Blob × $0.065/10k | $0.06 | $0.06 |
| delta-rs: Landing files | 8,600 to 75,600 Put Blob × $0.065/10k | $0.06 to $0.49 | $0.06 to $0.49 |
| delta-rs: snapshot lists | 8,600 × $0.065/10k | $0.06 | $0.06 |
| Databricks: data files | 84,700 × 3 writes × $0.065/10k | $1.65 | $1.65 |
| Databricks: data reads | 84,700 × $0.0052/10k | $0.04 | $0.04 |
| Databricks: temp commit files + checks | 11,250 × (3 writes + 2 reads) | $0.23 | $0.23 |
| Databricks: lists (workers + stream) | about 15,000 × $0.065/10k | $0.10 | $0.10 |
| **Databricks: renames** | 11,250 × $0.065/10k or /100 | **$0.07** | **$7.31** |
| Checkpoints, storage at rest, index | about 200 checkpoints; a few GB | < $0.05 | < $0.05 |
| **Total, 21 Changes/s** | | **≈ $2.3 to $2.8** | **≈ $9.6 to $10.0** |
| Upper bound at 50 Changes/s (× 2.4) | linear in Changes; larger batches would make it less | ≈ $5.6 to $6.6 | ≈ $23 to $24 |

The 30-minute exit run at 50/s is half the last row. Add about $0.36 per hour if soft delete stays at 7 days. Compute is extra (about $1.2 to $6.6 an hour, from the earlier research).

**HNS off, for comparison** (not usable with Unity Catalog): no rename meter and writes 23% cheaper, so about $1.8 to $2.1 at 21/s. **Inferred:** Hadoop's rename on a flat account is a copy and a delete, not atomic, so Databricks's multi-writer guarantee would also be in doubt there.

## Not confirmed

- Whether a file rename (`RenamePathFile`) bills as a write ($0.065/10k) or an iterative write ($0.065/100). Learn and the pricing page disagree. This is the $0.07 vs $7.31 line. One MERGE in the trial, then the account's Transactions metric split by API name and the Cost Management meter, would settle it.
- Whether Databricks Runtime uses open-source Delta's `AzureLogStore` (temp file + rename) on `abfss://`. The runtime is closed source.
- Whether Databricks's `_started_` / `_committed_` markers apply to Delta writes. If so, each write adds about 6 write operations.
- Whether Azure returns 409 or 412 when a conditional Put Blob loses. object_store treats only 409 as "already exists".
- Which Data Lake meter a Blob-API List Blobs hits on an HNS account.
- How many files delta-rs writes per Landing group commit in the cloud (one per partition touched, inferred from the 64-partition layout).
- Whether Databricks MERGE with deletion vectors rewrites far fewer files than the 84,700 an hour measured with delta-rs.
- The default blob soft-delete setting for a new account created in the portal (the Learn page read doesn't state it).
