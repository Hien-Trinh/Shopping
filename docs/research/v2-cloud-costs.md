# What v2 on Azure and Databricks would cost

> Research of Oct 12, 2026, for the v2 cloud plan. Primary sources only: azure.microsoft.com, learn.microsoft.com, docs.databricks.com, and Microsoft's own Retail Prices API (`prices.azure.com`), which returns the same list prices as the pricing pages. The pricing pages render their numbers with JavaScript, so most prices come from that API. Learn pages carry their `ms.date`. All prices are US dollars, pay as you go, East US, list price. The v1 figures come from [stress.md](../stress.md). Terms follow [GLOSSARY.md](../../GLOSSARY.md).
>
> **Unconfirmed** marks something no primary source settled. **Inferred** marks a step of reasoning, not a quoted fact.

## The short version

1. **A free account cannot run Azure Databricks.** The workspace needs a subscription that is not a Free Trial. You have to move to pay as you go and remove the spending limit. Once you upgrade, the $200 credit stays usable until day 30.
2. **The smallest classic node costs about $0.47 to $0.78 an hour**: DBUs plus the VM, for a 4-vCPU node. A one-hour SLO run costs about **$2.3 to $23**, most likely $4 to $6. A 1M bulk load with the student costs about **$3.3 to $11**.
3. **Storage transactions may cost more than compute.** That depends on how many Delta commits rename a file on ADLS Gen2, which is unconfirmed. A rename is billed at $0.065 per 100, a hundred times the normal write rate.
4. **No guardrail stops spending in time.** Azure budgets only send alerts, and they check costs every 24 hours. Databricks budgets also only alert (blocking applies to Unity Gateway only), with up to 24 hours of delay. The only hard cap is the free account's spending limit, and Databricks requires you to remove it.
5. **Databricks Free Edition costs nothing but is a weak fit.** It is serverless only, with daily quotas, 5 concurrent job tasks, 3 apps that stop after 24 hours, and restricted outbound internet. It is also for non-commercial use only. Whether it can reach your own ADLS account is unconfirmed.

## 1. Azure free account

| Item | Value | Source |
|---|---|---|
| Credit | $200, to use within 30 days | [Azure account page](https://azure.microsoft.com/en-us/pricing/purchase-options/azure-account) (read Oct 12, 2026); [Learn: avoid charges](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/avoid-charges-free-account) (ms.date 2026-03-03) |
| When the credit ends or day 30 passes | The subscription and its services are disabled until you upgrade | [Learn: avoid charges](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/avoid-charges-free-account) |
| Upgrading early | You keep the unused credit until day 30, then get 12 months of free services | [Learn: upgrade](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/upgrade-azure-subscription) (ms.date 2026-06-25) |
| Credit exclusions | Support plans, DevOps, Visual Studio, ExpressRoute, third-party branded products, products sold through Azure Marketplace, and separately licensed products | [Offer MS-AZR-0044P](https://azure.microsoft.com/en-us/offers/ms-azr-0044p/) (read Oct 12, 2026) |
| Free services | 20+ services free for 12 months for new customers, and 65+ services always free | [Azure account page](https://azure.microsoft.com/en-us/pricing/purchase-options/azure-account) |
| Free amounts don't roll over | Each month's free amount expires at the end of the month | [Learn: avoid charges](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/avoid-charges-free-account) |

### Free amounts relevant to this pipeline

Read from the [Azure account page](https://azure.microsoft.com/en-us/pricing/purchase-options/azure-account) on Oct 12, 2026, unless another source is named.

| Service | Free amount | How long |
|---|---|---|
| Container Apps (Consumption) | 180,000 vCPU-s, 360,000 GiB-s and 2M requests a month, per subscription. Also confirmed in [Learn: Container Apps billing](https://learn.microsoft.com/en-us/azure/container-apps/billing) (ms.date 2025-12-09) | Always |
| Functions | 1M requests | Always |
| App Service | Up to 10 web or API apps, 1 GB storage, 1 hour a day | Always |
| Container Registry | 1 Standard registry, 100 GB storage, 10 webhooks | 12 months |
| VMs (B1s, B2pts v2, B2ats v2) | 750 hours each | 12 months |
| Azure Monitor / Log Analytics | The page says "see Azure Monitor pricing". The Retail Prices API shows the first 5 GB of Analytics Logs ingestion at $0, then $2.30/GB ([API query](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Log%20Analytics%27%20and%20armRegionName%20eq%20%27eastus%27)). The 5 GB is read from the API's price tier. Whether it is per month and per billing account is **unconfirmed** | Always |
| Blob Storage / ADLS Gen2 | **Unconfirmed.** The free-services list loads with JavaScript and its storage entry was not readable. No Learn page states a blob amount | — |
| Event Hubs, Key Vault | Not on the free list as read. Paid prices are in section 4 | — |

### Azure for Students

| Item | Value | Source |
|---|---|---|
| Credit | $100, to use within 12 months, no credit card | [Offer MS-AZR-0170P](https://azure.microsoft.com/en-us/offers/ms-azr-0170p/); [Students page](https://azure.microsoft.com/en-us/free/students) (read Oct 12, 2026) |
| When the credit runs out | The subscription is disabled. You can then upgrade to pay as you go | Same offer page; [Learn: upgrade](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/upgrade-azure-subscription) |
| Renewal | You can renew each year while you are a student | [Students page](https://azure.microsoft.com/en-us/free/students) |
| Quotas | To raise quotas or limits, you upgrade to pay as you go | [Offer MS-AZR-0170P](https://azure.microsoft.com/en-us/offers/ms-azr-0170p/) |
| Databricks | **Unconfirmed** for a student subscription. The Databricks prerequisite rules out a "Free Trial Subscription" and asks for a vCPU quota increase. The student offer says raising quotas needs pay as you go. **Inferred:** in practice, a student account has to upgrade too | [Learn: Databricks free trial](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial) (ms.date 2026-10-06) |

### Does Databricks need pay as you go?

Yes. The Databricks prerequisites say you need "an Azure subscription that isn't a **Free Trial Subscription**". For a free account, they list three steps: switch to pay as you go, remove the spending limit, and request a vCPU quota increase ([Learn: Databricks free trial](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial), ms.date 2026-10-06).

**Does the $200 credit pay for DBUs?** **Unconfirmed, but likely.** The Retail Prices API lists Azure Databricks DBUs as first-party Azure meters (`serviceName: Azure Databricks`). The credit exclusions name Marketplace and third-party products, not Azure Databricks ([offer terms](https://azure.microsoft.com/en-us/offers/ms-azr-0044p/)). No page says yes in plain words. The VMs under a cluster are ordinary Azure VMs, so the credit pays for those. **Inferred:** after you upgrade before day 30, the remaining credit should cover both DBUs and VMs until day 30. Check the first invoice in Cost Management.

## 2. Azure Databricks

### Tier and trial

| Item | Value | Source |
|---|---|---|
| Standard tier | Retired on Oct 1, 2026. Only Premium remains | [Pricing page](https://azure.microsoft.com/en-us/pricing/details/databricks/) (banner, read Oct 12, 2026); [Learn: Standard tier end of life](https://learn.microsoft.com/en-us/azure/databricks/admin/account-settings/standard-tier) |
| Free trial | "Trial (Premium - 14-Days Free DBUs)" tier, chosen when you create the workspace. It gives free Premium DBUs for 14 days | [Learn: free trial](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial) (ms.date 2026-10-06) |
| Trial cap | "Up to $400 in credits" | [Learn: trial vs Free Edition](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial-vs-free-edition) (ms.date 2026-09-11) |
| What the trial covers | **DBUs only.** The trial meters are $0 DBU meters: Premium Free Trial All-purpose, Jobs, SQL Pro and Serverless SQL ([API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Azure%20Databricks%27%20and%20armRegionName%20eq%20%27eastus%27)). VMs, disks, storage and public IPs bill as normal ([pricing page](https://azure.microsoft.com/en-us/pricing/details/databricks/): "In addition to virtual machines, Azure Databricks will also bill for managed, disk, blob storage, Public IP Address"). **Unconfirmed:** whether serverless jobs and notebooks are free during the trial. No trial meter exists for Automated or Interactive Serverless | |

### DBU rates, Premium, East US

From the [Retail Prices API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Azure%20Databricks%27%20and%20armRegionName%20eq%20%27eastus%27) on Oct 12, 2026. The pricing page's embedded data shows the same rates.

| Workload | $ per DBU-hour | Effective since |
|---|---|---|
| Jobs Compute (classic) | 0.30 | 2019-03-01 |
| Jobs Light Compute | 0.22 | 2019-03-01 |
| All-Purpose Compute (classic) | 0.55 | 2018-05-01 |
| Automated Serverless (serverless jobs and pipelines) | 0.45 | 2025-05-01 |
| Interactive Serverless (notebooks, Databricks Apps) | 0.95 | 2025-05-01 |
| SQL Classic | 0.22 | 2021-06-01 |
| SQL Pro | 0.55 | 2024-02-01 |
| Serverless SQL | 0.70 | 2024-02-01 |
| Lakeflow pipelines, classic Core / Pro / Advanced | 0.30 / 0.38 / 0.54 | 2022-04-01 |

### Smallest viable classic node, per hour

DBUs per node come from the [pricing page](https://azure.microsoft.com/en-us/pricing/details/databricks/), read from its embedded price data on Oct 12, 2026. VM prices are Linux pay-as-you-go prices from the [Retail Prices API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Virtual%20Machines%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20armSkuName%20eq%20%27Standard_D4ds_v5%27).

| Node (4 vCPU) | DBU/h | VM $/h | Jobs $/h (DBU + VM) | All-purpose $/h | Jobs with Photon |
|---|---|---|---|---|---|
| D4as_v5, 16 GiB | 1.00 | 0.172 | **0.472** | 0.722 | 2.5 DBU: 0.922 |
| D4s_v5, 16 GiB | 1.00 | 0.192 | 0.492 | 0.742 | 0.942 |
| D4ds_v5, 16 GiB | 1.00 | 0.226 | 0.526 | 0.776 | 0.976 |
| DS3_v2, 14 GiB | 0.75 | 0.293 | 0.518 | 0.706 | 1.88 DBU: 0.856 |

- **F4s_v2 is not on the Databricks pricing page.** It lists no F-series rows. The VM itself costs $0.169/h. **Unconfirmed** whether Databricks supports it.
- **Photon is on by default** on Databricks Runtime 9.1 LTS and later ([Learn: compute configuration](https://learn.microsoft.com/en-us/azure/databricks/compute/configure), ms.date 2026-10-06). It raises the DBU count to 2.5× on jobs and 2× on all-purpose for these nodes. Turn it off unless it pays for itself.
- **Spot VMs**: D4as_v5 spot costs $0.036/h. The driver is always on demand, so a single-node cluster can't use spot ([Learn: compute configuration](https://learn.microsoft.com/en-us/azure/databricks/compute/configure)).
- **Single node**: the driver also acts as the worker. It can't be converted to multi-node later ([Learn: compute configuration](https://learn.microsoft.com/en-us/azure/databricks/compute/configure)).

### Serverless

| Item | Value | Source |
|---|---|---|
| Serverless jobs | Billed on the Automated Serverless SKU at a 1× multiplier, $0.45/DBU | [Learn: serverless DBU by SKU](https://learn.microsoft.com/en-us/azure/databricks/resources/pricing) (ms.date 2026-09-11) |
| Serverless notebooks | Interactive Serverless SKU, 1×, $0.95/DBU | Same |
| Databricks Apps | Medium is 0.5 DBU/h (**$0.475/h**). Large is 1 DBU/h ($0.95/h). Billed for every hour the app runs | Same; [Learn: Databricks Apps](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/) (ms.date 2026-09-16) |
| Serverless SQL warehouse | 2X-Small is 4 DBU/h, so **$2.80/h** | Same |
| Predictive optimization, data quality monitoring | Billed on the serverless jobs SKU even if you never run a serverless job | [Learn: serverless compute FAQ](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/) (ms.date 2026-09-11) |
| DBUs per hour for a serverless job, and any minimum | **Unconfirmed.** No page publishes them. Databricks says to benchmark the workload and read `system.billing.usage` | [Learn: serverless compute FAQ](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/) |
| Streaming on serverless | Only `Trigger.AvailableNow()` and `Once()` work. `ProcessingTime` and `Continuous` raise an error. For an always-on stream, run a continuous job with AvailableNow, or a continuous Lakeflow pipeline | [Learn: serverless limitations](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/limitations) (ms.date 2026-10-07) |
| Maximum runtime | 7 days per run | Same |

## 3. Databricks Free Edition

From [Learn: Free Edition limitations](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition-limitations) (ms.date 2026-09-29) and [docs.databricks.com: Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) (last updated Sep 29, 2026), unless another source is named.

| Question | Answer |
|---|---|
| Cost | Free forever, with daily usage limits ([Learn: trial vs Free Edition](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial-vs-free-edition)) |
| Compute | Serverless only, no classic compute, "limited compute size and usage". No numbers are published (**unconfirmed**) |
| Over quota | Compute shuts down for the rest of the day, "in extreme cases, the rest of the month". Data is kept |
| Jobs | At most 5 concurrent job tasks per account |
| SQL | One warehouse, 2X-Small |
| Pipelines | One active pipeline per pipeline type |
| Apps (a REST endpoint) | Up to 3 apps. Each stops 24 hours after it starts or is redeployed, and you can restart it. Apps run Python or Node.js frameworks ([Learn: Databricks Apps](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/)). **Unconfirmed:** whether an outside client can call an app with a token. The overview names OAuth but not machine-to-machine calls |
| Continuous streaming or jobs | Serverless allows AvailableNow only (section 2), runs last at most 7 days, and quota cut-offs stop compute. **Inferred:** fine for runs of an hour, unfit for an always-on service |
| Own ADLS storage | **Unconfirmed.** "Custom workspace storage locations" is unsupported. Outbound internet is "restricted to a limited set of trusted domains". No Free Edition page mentions external locations or storage credentials. Serverless reaches cloud storage only through Unity Catalog external locations ([serverless limitations](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/limitations)). **Inferred:** plan on Free Edition's default storage only |
| Delta MERGE | Supported. Delta is a supported write format on serverless ([serverless limitations](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/limitations)), and Free Edition doesn't list it as unsupported |
| Change data feed | Automatic CDF needs Databricks Runtime 19+ and a Unity Catalog table with row tracking. **Only Databricks readers can read automatic CDF on Delta.** Legacy CDF, set with `delta.enableChangeDataFeed`, still works ([Learn: change data feed](https://learn.microsoft.com/en-us/azure/databricks/tables/features/change-data-feed), ms.date 2026-09-16). Not listed as unsupported on Free Edition (**unconfirmed** in practice) |
| Python libraries | Serverless uses notebook-scoped libraries and environments, not compute-scoped libraries or init scripts ([serverless limitations](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/limitations)). **Unconfirmed:** whether PyPI is among Free Edition's trusted domains |
| Terms | Non-commercial use only. Databricks reserves the right to train on your data. No SLA. Accounts left inactive may be deleted |

## 4. Cost model

### Prices used

| Meter | Price | Source |
|---|---|---|
| ADLS Gen2 Hot LRS, write ops | $0.065 per 10,000, counted per 4 MB written | [API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Storage%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20productName%20eq%20%27Azure%20Data%20Lake%20Storage%20Gen2%20Hierarchical%20Namespace%27); "every 4 MB" from the [Data Lake pricing page](https://azure.microsoft.com/en-us/pricing/details/storage/data-lake/) |
| ADLS Gen2 Hot LRS, read ops | $0.0052 per 10,000, per 4 MB | Same |
| ADLS Gen2 **iterative write** (RenamePath, RenamePathFile) | **$0.065 per 100** | Same; mapping in [Learn: map REST ops to prices](https://learn.microsoft.com/en-us/azure/storage/blobs/map-rest-apis-transaction-categories) (ms.date 2025-05-14) |
| ADLS Gen2 iterative read (List Path) | $0.065 per 10,000 | Same |
| ADLS Gen2 delete | Free | Same |
| ADLS Gen2 Hot LRS storage | $0.0208/GB-month (first 50 TB) | Same |
| Which calls count as writes | CreatePathFile, AppendFile and FlushFile are each a write op, so **one small file costs about 3 write ops** | [Learn: map REST ops to prices](https://learn.microsoft.com/en-us/azure/storage/blobs/map-rest-apis-transaction-categories) |
| Plain Blob (no hierarchical namespace), Hot LRS | Write $0.05, read $0.004 per 10,000; no rename meter | [API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Storage%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20productName%20eq%20%27General%20Block%20Blob%20v2%27) |
| Container Apps, Consumption, beyond the free grant | Active vCPU $0.000024/s, memory $0.000003/GiB-s, $0.40 per 1M requests | [API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Azure%20Container%20Apps%27%20and%20armRegionName%20eq%20%27eastus%27) |
| Container Apps "Environment Management Hour" | $0.10/h, effective 2026-09-01 in the API. The Learn billing page (Dec 2025) charges a management fee only for dedicated profiles, private endpoints or planned maintenance. **Unconfirmed** whether a plain Consumption environment now pays it. If it does, that is $73 a month | Same API; [Learn: billing](https://learn.microsoft.com/en-us/azure/container-apps/billing) |
| Event Hubs Basic | $0.015 per throughput-unit hour + $0.028 per 1M events (Standard: $0.03/h) | [API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Event%20Hubs%27%20and%20armRegionName%20eq%20%27eastus%27) |
| Key Vault Standard | $0.03 per 10,000 operations | [API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Key%20Vault%27%20and%20armRegionName%20eq%20%27eastus%27) |
| Container Registry Basic | $0.1666/day (about $5/month), after the 12 free months | [API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Container%20Registry%27%20and%20armRegionName%20eq%20%27eastus%27) |

### What v1 measured (the inputs)

From [stress.md](../stress.md), 7f.2 run 4 (Oct 10, 71 minutes at 21.3 Changes/s): 90,000 Changes, **13,318 worker MERGE commits**, and **100,216 files / 87.6 GB rewritten** and swept (about 1.2 GB a minute, after the 7f.1 fixes). The machine was 10 Mac cores, CPU-bound. Scaled to one hour (× 60/71 = 0.845): **11,250 MERGE commits, 84,700 files, 74 GB.** The Landing log's group commits came about 0.42 s apart in the run 3 rerun, so **about 8,600 commits an hour** (inferred, rate-dependent). The 7d.2a 1M load: 102 Landing commits, 1,006 worker batches, 33,534 store files and 6,633 Landing files ever written, and 1.9 GB on disk. The student classified about 110 Listings/s on the 10 cores, so a 1M bulk is about 2.5 hours, or **25 Mac core-hours**.

### Unknowns in the storage cost

- **How many storage ops one Delta commit takes on ADLS Gen2.** This assumes 3 write ops per new file (create, append, flush) and 1 list per commit. The big unknown is whether each commit renames a temp file into `_delta_log`. A rename bills as an iterative write at $0.065 per 100. **Unconfirmed** for both Databricks and delta-rs. The model shows both cases.
- **Whether Databricks MERGE rewrites as much as delta-rs.** Deletion vectors could shrink the churn a lot. **Unconfirmed:** the default state of deletion vectors and their effect on this workload were not researched.

### (a) A one-hour SLO run at 21 Changes/s

| Line | Arithmetic | Low | High |
|---|---|---|---|
| Compute: 4 Python workers + classifier | 8 to 16 vCPU = 2 to 4 nodes × 1.25 h (start and stop included). Low: 2 × D4as_v5 jobs at $0.472. High: 4 × D4ds_v5 all-purpose with Photon left on, $1.326 | $1.18 | $6.63 |
| Ingestion API | Container Apps, 1 vCPU / 2 GiB for 1 h = 3,600 vCPU-s and 7,200 GiB-s; 90k requests. All inside the free grant. High: Databricks Apps Medium for 1 h ($0.475) or the env-management hour ($0.10) | $0 | $0.48 |
| Data-file writes | 84,700 files × 1 to 3 ops = 85k to 254k ops × $0.065/10k | $0.55 | $1.65 |
| Data-file reads (MERGE reads what it rewrites) | 85k to 254k ops × $0.0052/10k | $0.04 | $0.13 |
| Log writes | (11,250 + 8,600) ≈ 20k commits × 3 ops × $0.065/10k | $0.39 | $0.39 |
| Log listing | 20k to 100k list ops × $0.065/10k | $0.13 | $0.65 |
| **Log renames, if every commit renames** | 20k × $0.065/100 | $0 | **$12.90** |
| Storage at rest | about 13 GB × $0.0208 / 730 h | <$0.01 | <$0.01 |
| **Total** | | **≈ $2.3** | **≈ $22.8** |

A realistic middle is about **$4 to $6**: 2 to 3 jobs nodes, no renames, 3 ops per file.

### (b) A 1M-Change bulk load

| Line | Arithmetic | Low | High |
|---|---|---|---|
| Compute, student classifier | 25 Mac core-hours ≈ 25 to 50 vCPU-hours (assumes 1 M4 core ≈ 1 to 2 vCPU, **inferred**) = 6.25 to 12.5 node-hours × $0.472 (jobs) to $0.776 (all-purpose) | $2.95 | $9.70 |
| Compute, `fake` classifier (storage only) | 7d.2a drained 1M in about 14 min: 1 to 2 nodes × 0.5 h | $0.24 | $0.78 |
| File writes | (33.5k + 6.6k) ≈ 40k files × 3 ops × $0.065/10k | $0.26 | $0.78 |
| Reads, lists, log | about 1.1k commits plus file reads: cents | $0.05 | $0.10 |
| Renames, if every commit renames | 1.1k × $0.065/100 | $0 | $0.73 |
| **Total with the student** | | **≈ $3.3** | **≈ $11.3** |

The bulk load is cheap because it makes few, large commits (102 Landing commits of 10,000 Changes each). The SLO run is the expensive shape: about 20,000 small commits an hour. Per Change, the bulk costs about $0.000003 to $0.00001. The SLO hour costs about $0.00003 to $0.00025.

### Fixed monthly costs while nothing runs (inferred)

The tables hold a few GB, so storage is about $0.10 to $0.30. The workspace has no hourly fee: the pricing page bills only VMs, DBUs, disks, storage and IPs. Container Registry is free for 12 months, then about $5. Log Analytics stays under its 5 GB free tier if logging is kept small. Watch the Container Apps environment hour (above) and any serverless SQL warehouse left without auto-stop ($2.80/h).

## 5. Cost guardrails

| Guardrail | Does it stop spending? | Detail | Source |
|---|---|---|---|
| Azure spending limit | **Yes.** At the limit, resources are disabled, VMs are deallocated and storage becomes read-only | Equal to the credit ($200 on a free account), and you can't change the amount. Not available on pay-as-you-go subscriptions. **Databricks requires removing it.** It also doesn't stop Marketplace or third-party charges | [Learn: spending limit](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/spending-limit) (ms.date 2026-04-29) |
| Azure budgets | **No.** "Resources aren't affected, and your consumption isn't stopped" | Cost data arrives within 8 to 24 h, and budgets are evaluated every 24 h. Alerts fire on actual or forecast cost. A budget can call an **action group**, which can run automation such as a Function or webhook. You would write the stop logic yourself | [Learn: create budgets](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-acm-create-budgets) (ms.date 2025-06-26) |
| Databricks budgets | **No**, except for Unity Gateway (model) usage, which can be blocked | Up to 4 thresholds, email alerts, up to 24 h of delay. Measured at list price and ignores credits | [Learn: Databricks budgets](https://learn.microsoft.com/en-us/azure/databricks/admin/account-settings/budgets) (ms.date 2026-10-07) |
| Serverless usage policies | **No.** They only add tags for attribution | Public Preview | [Learn: serverless usage policies](https://learn.microsoft.com/en-us/azure/databricks/admin/usage/budget-policies) (ms.date 2026-09-28) |
| Cluster auto-termination | Yes, for idle all-purpose compute | Set an idle period in minutes. Job compute ends with its job. The default value was not on the page read (**unconfirmed**) | [Learn: compute configuration](https://learn.microsoft.com/en-us/azure/databricks/compute/configure) |
| Compute policies | Limits what users can create, for example node types and sizes | Not supported on serverless | Same; [serverless limitations](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/limitations) |
| Serverless job timeout | Yes, per run | No query timeout by default. Set `spark.databricks.execution.timeout`. Runs are capped at 7 days | [serverless limitations](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/limitations) |

**Inferred, for a cost-safe setup:** the hard stop has to live in our own code. Give each run a timeout and tear it down when it ends: jobs compute, never all-purpose for runs. Put an Azure budget at $25 and $50 a month, with an action group that runs a Function to stop Container Apps and cancel Databricks runs. Turn Photon off. Use a single storage account so its cost meters can be watched.

## Not confirmed

- The Blob/ADLS free amount on the free account (the list renders with JavaScript).
- Whether the $200 credit pays for Databricks DBUs. Likely, since they are first-party Azure meters and not in the exclusion list, but no page says so.
- Whether the trial's free DBUs cover serverless jobs and notebooks.
- DBUs per hour and any minimum for serverless jobs.
- Whether F4s_v2 works on Databricks.
- Whether Free Edition can reach external ADLS, whether PyPI is among its trusted domains, and the size of its quotas.
- Whether a Delta commit on ADLS Gen2 renames a file, through Databricks or through delta-rs. This decides a $0 to $13 line per SLO hour.
- Whether the Container Apps Environment Management Hour applies to Consumption-only environments.
- The default auto-termination time for all-purpose compute.
- Whether Databricks deletion vectors would shrink the MERGE churn measured on delta-rs.
