# Can Databricks Free Edition run v2's smoke check?

> Research of Oct 12, 2026, for [#129](https://github.com/Hien-Trinh/Shopping/issues/129) on the v2 cloud map ([#123](https://github.com/Hien-Trinh/Shopping/issues/123)). Primary sources only: docs.databricks.com, learn.microsoft.com and databricks.com/legal. Each fact carries the page's "last updated" date or `ms.date`. Costs come from [v2-cloud-costs.md](https://github.com/Hien-Trinh/Shopping/blob/research/v2-cloud-costs/docs/research/v2-cloud-costs.md) on its branch and are not redone here. Terms follow [GLOSSARY.md](../../GLOSSARY.md).
>
> **Unconfirmed** marks something no primary source settled. **Inferred** marks a step of reasoning, not a quoted fact.

## Answer

**Yes, for a Spark-only smoke check on Free Edition's own storage. No for anything that touches ADLS or delta-rs.**

1. **No ADLS.** Free Edition has no account console, no custom storage, and no private networking. Serverless reaches cloud storage only through Unity Catalog external locations, and no Free Edition page says you can make one. Plan on its default storage. The ADLS path is then tested locally against Azurite and on the paid trial's first run.
2. **delta-rs cannot read Free Edition tables.** Default storage gives external Delta clients no file access and no credential vending. So "CDF readable by delta-rs" can't be checked there. Check it locally (local Spark writes, delta-rs reads) and once on the trial.
3. **Our package installs as a wheel, but not as it is today.** Serverless takes a wheel from `/Workspace/` or `/Volumes/`. But its newest environment runs **Python 3.12.3**, and `pyproject.toml` asks for `>=3.14`. Classic Runtime 18.0 is also on 3.12.3. This hits the paid trial too, not only Free Edition.
4. **Streaming fits v2's shape.** Serverless runs only `AvailableNow` (or the deprecated `Once`). Delta is a supported source and sink. Python `foreachBatch` works with caveats. MERGE writes deletion vectors on current runtimes. Shallow `CLONE` works on Unity Catalog managed tables.
5. **Quotas have no published numbers.** Going over shuts compute down for the rest of the day, sometimes the month. Data is kept. A 10–20 minute run is plausible but **unconfirmed**. Only a try settles it.
6. **A public learning repo fits the terms.** Personal, academic and not-for-profit use is allowed. Two catches: Databricks gets a perpetual license to what you run there, and you may not publish benchmark results without its permission. So no Free Edition timings in the repo.
7. **Fallback:** the smoke check's first run on the paid trial. Trial DBUs are free and a D4as_v5 VM costs $0.172/h, so a 20-minute classic job costs about **$0.06** plus storage. Without the trial it is about $0.16.

## 1. Storage: own ADLS or only managed storage?

| Fact | Source |
|---|---|
| Free Edition is "one serverless workspace", with no classic compute | [Learn: trial vs Free Edition](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial-vs-free-edition) (ms.date 2026-09-11) |
| Each new workspace comes with serverless compute and default storage | [Learn: Free Edition](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition) (ms.date 2026-09-11) |
| "Custom workspace storage locations" are unsupported | [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) (Sep 29, 2026) |
| No account console or account-level API. No security customization or private networking. One workspace and one metastore | Same |
| Outbound internet is limited to a set of trusted domains. No list is published | Same |
| On serverless, external data goes through Unity Catalog, and cloud storage access needs external locations | [Serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations) (Oct 7, 2026) |
| External Delta and Iceberg clients cannot read default storage's metadata or data files. Credential vending is not supported for external systems | [Default storage](https://docs.databricks.com/aws/en/storage/default-storage) (Oct 9, 2026) |
| External clients can reach volume files through the Files API | Same |

- **Unconfirmed:** whether a Free Edition user can create a storage credential and an external location. No Free Edition page mentions them. Creating an Azure credential needs an Entra service principal or access connector. **Inferred:** with no account console and restricted networking, plan as if it can't.
- **Unconfirmed:** which cloud and region host Free Edition. The docs sit under `/aws/`, but no page says.
- **Does the smoke check need ADLS at all?** **Inferred: no.** The smoke check's job is "our Spark code runs on the real Databricks runtime". Managed tables in the default catalog cover that. ADLS-specific behaviour (abfss paths, external locations, delta-rs reading the tables) belongs to local tests against Azurite and to the paid trial.

## 2. Installing our package

| Fact | Source |
|---|---|
| Notebooks: add dependencies in the Environment pane. Any requirements.txt line works, including PyPI names | [Serverless dependencies](https://docs.databricks.com/aws/en/compute/serverless/dependencies) (Sep 29, 2026) |
| A wheel is added by absolute path: `/Workspace/...` or `/Volumes/<catalog>/<schema>/<volume>/<file>.whl` | Same |
| Jobs: Python script and wheel tasks require an environment. Dependencies can be workspace paths, volume paths or `-r requirements.txt` | Same |
| Don't install PySpark or anything that pulls it in; it stops the session | Same |
| Serverless may run on `aarch64` or `x86_64`, and it can change between runs. Use `py3-none-any` wheels or both variants | Same |
| No compute-scoped libraries or init scripts. No Maven coordinates. Task libraries aren't supported for notebook tasks | [Serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations) (Oct 7, 2026) |
| Environment 6 (Sep 3, 2026, newest): Python 3.12.3, Databricks Connect 19, pyarrow 21.0.0, numpy 2.3.4, deltalake 1.5.1, pydantic 2.13.3 | [Environment versions](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/) (Sep 11, 2026); [Environment 6](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/six) (Sep 15, 2026) |
| No environment ships Python above 3.12.3 | [Environment versions](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/) |
| Classic Databricks Runtime 18.0 also ships Python 3.12.3 | [Runtime 18.0](https://docs.databricks.com/aws/en/release-notes/runtime/18.0) (Sep 11, 2026) |

- **The clash:** `pyproject.toml` says `requires-python = ">=3.14"` and pins `deltalake>=1.6.6`, `pyarrow>=25.0.1`. Serverless has 3.12 and older versions of both. **Inferred:** the Databricks-side code needs its own package (or a lower floor) that runs on 3.12, and must not depend on PySpark itself.
- **Unconfirmed:** whether PyPI (`pypi.org` and `files.pythonhosted.org`) is among Free Edition's trusted domains. A wheel in a volume with no third-party dependencies avoids the question. **Inferred:** the pre-installed numpy, pyarrow and pydantic may cover the rest.

## 3. Streaming and Delta features on serverless

| Feature | Status | Source |
|---|---|---|
| Triggers | Only `Trigger.AvailableNow()` (recommended) and `Trigger.Once()` (deprecated). `ProcessingTime` and `Continuous` raise `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. Spark's default trigger is unsupported, so set one explicitly | [Serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations) (Oct 7, 2026) |
| Always-on stream | Run a continuous job with `AvailableNow`, or a continuous pipeline | Same |
| Delta read and write | Supported source and sink, including DML | Same |
| Python `foreachBatch` | Works. No `ThreadPoolExecutor` or other threads inside it, `print()` goes to driver logs, no `dbutils.widgets`, referenced objects must serialize | [Standard access mode limitations](https://docs.databricks.com/aws/en/compute/standard-limitations) (Sep 11, 2026) |
| RDDs, `sc` | Not supported. Spark Connect defers analysis to execution time | Same |
| MERGE with deletion vectors | Runtime writes deletion vectors for MERGE from 12.2 LTS with Photon, 14.3 LTS without. Not on by default for every Delta table; a workspace setting controls new tables. Enabling them upgrades the table protocol, and readers without support can't read the table | [Deletion vectors](https://docs.databricks.com/aws/en/delta/deletion-vectors) (Sep 11, 2026) |
| Change data feed | Already researched: automatic CDF (Runtime 19+, row tracking) is readable only by Databricks readers; the per-table `delta.enableChangeDataFeed` still works | [v2-cloud-costs.md §3](https://github.com/Hien-Trinh/Shopping/blob/research/v2-cloud-costs/docs/research/v2-cloud-costs.md#3-databricks-free-edition) |
| CDF readable by delta-rs | Not testable on Free Edition: delta-rs has no file access to default storage | [Default storage](https://docs.databricks.com/aws/en/storage/default-storage) (Oct 9, 2026) |
| `CLONE` | Shallow clones of Unity Catalog managed tables work on Runtime 13.3 LTS and later. `CREATE OR REPLACE` can't overwrite a UC shallow clone. Clones don't copy history. Streaming tables and materialized views can't be cloned | [Clone](https://docs.databricks.com/aws/en/delta/clone) (Sep 11, 2026) |
| Caching | `cache()` and `CACHE TABLE` raise an exception | [Serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations) (Oct 7, 2026) |
| Run length | 7 days at most per serverless run | Same |

- **Unconfirmed:** deep `CLONE` of a managed table; how `CLONE` treats deletion vectors and CDF; whether deletion vectors are on by default in a Free Edition workspace. Free Edition lists none of these as unsupported.

## 4. Quotas

| Fact | Source |
|---|---|
| Compute size and usage are limited; no numbers are published | [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) (Sep 29, 2026) |
| Over quota: compute shuts down for the rest of the day, "in extreme cases" the rest of the month. Data and settings are kept | Same |
| Jobs: at most 5 concurrent job tasks per account. One SQL warehouse, 2X-Small. One active pipeline per pipeline type | Same |
| LinkedIn identity verification can raise some limits | Same |
| You may not work around usage limits, and may hold at most two accounts | [Free Edition AUP](https://www.databricks.com/legal/acceptable-use-policy-fe) (no date on page; read Oct 12, 2026) |
| No login for 60 days can mark an account inactive; it may be deleted with its data | Same |

- **Unconfirmed:** whether a 10–20 minute run fits a day's quota. **Inferred:** a few thousand Changes through one `AvailableNow` batch is small, but only a first run shows it. Keep the smoke run small and one job at a time (the 5-task cap is not the binding limit).

## 5. Terms

| Fact | Source |
|---|---|
| Use must be personal, academic or not-for-profit, including noncommercial research and education | [Free Edition Terms §3.1](https://www.databricks.com/legal/databricks-free-edition) (no date on page; read Oct 12, 2026) |
| Databricks gets a royalty-free, perpetual, irrevocable, worldwide license to your content, data and code, and may train AI models on it | Same, §6; [Learn: trial vs Free Edition](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial-vs-free-edition) (ms.date 2026-09-11) |
| Databricks can suspend, change or end the service at any time | Same, §7–8 |
| Benchmark tests are allowed, but results may not be disclosed without Databricks' permission | [Free Edition AUP](https://www.databricks.com/legal/acceptable-use-policy-fe) |
| No SLA, no support; no Marketplace provider | [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) (Sep 29, 2026) |

- **Inferred:** a public learning repo with synthetic data is personal and educational, so it fits. The code is public anyway, so the content license costs nothing.
- **Inferred:** the smoke check records pass or fail, not timings. Free Edition timings stay out of the repo and out of `stress.md`.

## 6. Fallback if Free Edition can't

| Option | Cost | Source |
|---|---|---|
| Smoke check as the first job on the 14-day trial, classic single node D4as_v5 | DBUs free on the trial; VM $0.172/h, so about $0.06 for 20 minutes | [v2-cloud-costs.md §2](https://github.com/Hien-Trinh/Shopping/blob/research/v2-cloud-costs/docs/research/v2-cloud-costs.md#2-azure-databricks) |
| Same after the trial | $0.472/h (DBU + VM), so about $0.16 for 20 minutes | Same |
| Serverless job instead | $0.45/DBU; DBUs per hour unpublished | Same |

- **Inferred:** the fallback costs cents, but only exists once the trial starts, which is late ([#125](https://github.com/Hien-Trinh/Shopping/issues/125)). So Free Edition is still worth trying first, for the runtime and API check.

## Could not confirm

- Whether Free Edition can create storage credentials or external locations, and so reach any ADLS account.
- Which cloud and region host Free Edition.
- Whether PyPI is among Free Edition's trusted domains.
- Free Edition's quota sizes, and whether a 10–20 minute run fits a day.
- Whether personal access tokens or OAuth work for the CLI and Jobs API on Free Edition (needed to start the smoke check from a script). Only community posts discuss it.
- Deep `CLONE` of managed tables; `CLONE` with deletion vectors and CDF; the deletion-vector default on Free Edition.
- Dates on the Free Edition Terms and AUP pages: neither shows one.
