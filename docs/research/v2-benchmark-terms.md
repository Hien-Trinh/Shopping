# May v2 publish timings measured on the Azure Databricks trial and on Azure?

> Research of Oct 30, 2026, for [#167](https://github.com/Hien-Trinh/Shopping/issues/167) on the v2 cloud map ([#123](https://github.com/Hien-Trinh/Shopping/issues/123)). It follows [v2-free-edition-fit.md](https://github.com/Hien-Trinh/Shopping/blob/research/v2-free-edition-fit/docs/research/v2-free-edition-fit.md) (#129), which found Free Edition's ban on disclosing benchmark results. Primary sources only: Microsoft's Product Terms site, azure.microsoft.com/support/legal, learn.microsoft.com (with `ms.date`) and databricks.com/legal. All read Oct 30, 2026. Each fact carries the date its page shows. The Product Terms pages show none, so they carry the site's latest publish date from its [Summary of Changes](https://www.microsoft.com/licensing/terms/product/changes/MCA): Oct 28, 2026 (the Microsoft Azure terms last changed Oct 15, 2026). This is a reading of the published terms, not legal advice. Terms follow [GLOSSARY.md](../../GLOSSARY.md).
>
> **Unconfirmed** marks something no primary source settled. **Inferred** marks a step of reasoning, not a quoted fact.

## Answer

**Yes. No term read forbids publishing timings or our own bill from the paid Azure Databricks trial, Container Apps or ADLS Gen2. Leave out numbers that depend on Beta, Private Preview or Experimental features.**

1. **Microsoft's terms govern the workspace, not Databricks'.** Azure Databricks is an Azure service under our Azure agreement (MOSA or MCA) and the Product Terms. The Product Terms' only Azure Databricks clause is about third-party models. Databricks' own Master Cloud Services Agreement (MCSA) does not govern Databricks Powered Services, and Databricks lists Azure Databricks as one, provided by Microsoft. So Databricks' acceptable use policy (AUP), and its benchmark rules, don't bind us.
2. **Microsoft's one benchmark clause for online services is about competitors.** The clause named Competitive Benchmarking applies only to a customer that offers a product competing with the service. Even then it doesn't forbid publishing: Microsoft may ask for the details to replicate the test and for access to the rival product. We offer no competing product (**inferred**).
3. **The prior-approval clause is for software, not services.** The clause named Benchmark Testing requires Microsoft's written approval before disclosing results, but only for Server Products and the Desktop Optimization Pack. Container Apps, ADLS Gen2 and Azure Databricks are online services, so request latency, image pull time and commit conflict rates are free to publish (**inferred**).
4. **The Trial tier adds no terms.** It is a pricing tier: 14 days of free Premium DBUs on a pay-as-you-go subscription, then billed through Azure. No page links extra terms for it.
5. **Previews are the one catch.** Non-public information about Azure Databricks Private Preview and Beta features is confidential. Azure's preview terms make the use of Early Access Previews (private preview, experimental and the like) confidential too. No page read gives Public Preview or GA such a rule. So time GA or Public Preview features only, and print pass or fail for the rest.
6. **Our own bill is ours to publish (inferred).** No term read names cost data. The MOSA's only confidentiality duty is to keep sign-in credentials secret, and pay-as-you-go prices are public. The MCA's own text was not read, and which agreement covers our account is **unconfirmed**.
7. **Publish each number with its method (inferred, good practice):** the commit, workflow run, region, VM sizes, runtime version and data size. That meets the replication duty in Microsoft's competitor clause and in Databricks' own AUP, if either ever applied. Free Edition stays pass or fail (#129).

## 1. Which terms govern an Azure Databricks workspace

| Fact | Source |
|---|---|
| Azure bought online, free trials included, is governed by the Microsoft Customer Agreement (MCA). Where the MCA isn't available, the Microsoft Online Subscription Agreement (MOSA) governs instead. The Product Terms give the use terms | [Microsoft Azure Legal Information](https://azure.microsoft.com/en-us/support/legal/) (Last updated: April 2023) |
| A web sign-up with pay-as-you-go rates gets a Microsoft Online Services Program billing account. Some customers in select regions get an MCA for an individual instead. The type shows in Cost Management + Billing > Properties > Type | [Learn: view your billing accounts](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/view-all-accounts) (ms.date 2026-06-25) |
| The MOSA is its own terms plus the Online Services Terms (now part of the Product Terms), the SLAs and the Offer Details | [MOSA](https://azure.microsoft.com/en-us/support/legal/subscription-agreement/) (Last updated: March 2019) |
| Product Terms, Microsoft Azure > Service Specific Terms > **Azure Databricks**: one sentence, which deems the third-party models offered there Non-Microsoft Products. Nothing else on the page names Databricks, and no Databricks AUP or MCSA is brought in. The MCA and MOSA versions read the same | [Product Terms: Microsoft Azure (MCA)](https://www.microsoft.com/licensing/terms/productoffering/MicrosoftAzure/MCA), [(MOSA)](https://www.microsoft.com/licensing/terms/productoffering/MicrosoftAzure/MOSA) (no date on page; publish of Oct 28, 2026) |
| Databricks lists Azure Databricks as a Databricks Powered Service: "Azure Databricks (provided by Microsoft Corporation)". To use one, you contract with that provider | [Databricks Cloud Provider Directory](https://www.databricks.com/legal/cloud-provider-directory) (no date on page) |
| MCSA, Databricks Powered Services: "This MCSA does not govern Customer’s use of Databricks Powered Services." The MCSA's Use Restrictions are what bring in the AUP | [Databricks MCSA](https://www.databricks.com/legal/mcsa) (Last Updated February 20, 2026) |
| Databricks' paid AUP binds use of the Databricks Services, with terms defined by an agreement between you and Databricks | [Databricks AUP, paid (including trial) version](https://www.databricks.com/legal/acceptable-use-policy) (Last modified: March 20, 2026) |

- **Inferred:** our workspace is covered by our Azure agreement plus the Product Terms: the Universal License Terms For Online Services and the Microsoft Azure page. We have no agreement with Databricks, so its MCSA and AUP don't bind us. The trial in the AUP's title is Databricks' own trial, not the Azure Trial tier.
- **Inferred:** third-party models would carry their own terms. v2 uses none; the Ingestion job runs our own student encoder.

## 2. Benchmarks on Azure Databricks, and the Trial tier

| Fact | Source |
|---|---|
| For Online Services > Other > **Competitive Benchmarking** applies "If Customer offers a product or service competitive to an Online Service". Such a customer waives its own benchmark bans. If it discloses benchmarks of any online service, it must give Microsoft, on request, what is needed to replicate them and access to its competing product. A competitor that won't agree may not use the service | [Product Terms: For Online Services (MCA)](https://www.microsoft.com/licensing/terms/product/ForOnlineServices/MCA), same text in [MOSA](https://www.microsoft.com/licensing/terms/product/ForOnlineServices/MOSA) (no date on page; publish of Oct 28, 2026) |
| That is the only benchmark text on the page. Its Using the Online Services > **Acceptable Use Policy** bans unlawful use, malware, crypto mining, scraping, reverse engineering and the like. It says nothing about benchmarks or publishing results | Same |
| The Microsoft Azure page has no benchmark text at all | [Product Terms: Microsoft Azure (MCA)](https://www.microsoft.com/licensing/terms/productoffering/MicrosoftAzure/MCA) |
| The Trial tier, "Trial (Premium - 14-Days Free DBUs)", is a pricing tier picked when creating the workspace. It gives free Premium DBUs for 14 days. It needs a subscription that isn't an Azure Free Trial (pay as you go, spending limit removed). The page links no extra terms | [Learn: sign up for Azure Databricks for free](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial) (ms.date 2026-10-15) |
| The trial is "designed for commercial use", with up to $400 in credits, and is paid through the Azure subscription afterwards. Free Edition is the option with its own terms | [Learn: free trial vs Free Edition](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial-vs-free-edition) (ms.date 2026-10-27) |
| Private Preview and Beta features are not for production and come as-is. "Non-public information about the preview is confidential." | [Learn: manage Azure Databricks previews](https://learn.microsoft.com/en-us/azure/databricks/admin/workspace-settings/manage-previews) (ms.date 2026-10-27) |
| Platform features ship as Private Preview (invite only), Beta (most customers, off by default), Public Preview or GA. Runtime features ship as Private preview, Experimental or Public preview | [Learn: Azure Databricks preview releases](https://learn.microsoft.com/en-us/azure/databricks/release-notes/release-types) (ms.date 2026-10-29) |
| For comparison, Databricks' own paid AUP allows benchmarks and their disclosure, except for Beta Services. A disclosure must include all information needed to replicate it, and lets Databricks benchmark, and publish on, any of your products it covers | [Databricks AUP, paid](https://www.databricks.com/legal/acceptable-use-policy) (March 20, 2026) |
| Free Edition's AUP allows benchmark tests but bars disclosing the results unless Databricks permits it | [Free Edition AUP](https://www.databricks.com/legal/acceptable-use-policy-fe) (no date on page) |

- **Inferred:** drain rate, freshness p99, cluster start and recovery times and per-core classifier throughput are benchmarks in the plain sense. Nothing in Microsoft's terms stops us publishing them. The competitor clause doesn't reach us, and where it does apply it asks for disclosure, not silence.
- **Inferred:** a number that depends on a Beta, Private Preview or Experimental feature is non-public information about that preview. Leave it out or print pass or fail. Before a timed run, check the release type of the runtime version and of any preview turned on in the workspace.
- **Unconfirmed:** the Learn previews page ties its as-is terms to an agreement with Databricks, which an Azure customer doesn't have. It doesn't say which agreement it means for Azure.

## 3. Azure Container Apps and ADLS Gen2

| Fact | Source |
|---|---|
| The Microsoft Azure page's Service Specific Terms has 43 entries. None is for Container Apps or Storage (ADLS Gen2 included) | [Product Terms: Microsoft Azure (MCA)](https://www.microsoft.com/licensing/terms/productoffering/MicrosoftAzure/MCA) (no date on page; publish of Oct 28, 2026) |
| So they fall under the Universal License Terms For Online Services only, where Competitive Benchmarking is the sole benchmark clause (Section 2) | [Product Terms: For Online Services](https://www.microsoft.com/licensing/terms/product/ForOnlineServices/MCA) |
| For all Software > **Benchmark Testing**: Microsoft's prior written approval is needed to disclose the "results of any benchmark test of any Server Product" or of the Desktop Optimization Pack | [Product Terms: For all Software](https://www.microsoft.com/licensing/terms/product/ForallSoftware/MCA) (no date on page; publish of Oct 28, 2026) |
| The MOSA has no benchmark clause | [MOSA](https://azure.microsoft.com/en-us/support/legal/subscription-agreement/) (March 2019) |
| Azure's preview terms have no benchmark clause either. Early Access Previews (features marked frontier, early access, private preview, research or experimental) get a **Confidentiality** term: "Your use of Early Access Previews is confidential". There are no preview terms specific to Container Apps, Storage or Azure Databricks | [Supplemental Terms of Use for Microsoft Azure Previews](https://azure.microsoft.com/en-us/support/legal/preview-supplemental-terms/) (Last Updated: June 2026) |

- **Inferred:** Container Apps, ADLS Gen2 and Azure Databricks are online services, not Server Products. So Benchmark Testing doesn't reach request latency, image pull time or commit conflict rates. It would if we ever timed Windows Server or SQL Server software.
- **Inferred:** the preview rule from Section 2 holds here too. Keep private-preview and experimental Azure features out of published timings.
- **Unconfirmed:** what the MCA itself says (Section 4).

## 4. What is allowed, and does our bill count

| Fact | Source |
|---|---|
| Where a benchmark clause applies, it asks for disclosure, not silence. Microsoft (competitors only): replication details and access, on request. Databricks' paid AUP (which doesn't bind us): replication details and benchmarks in return, and no Beta results | Sections 2 and 3 |
| Prior consent is needed only for Server Product and Desktop Optimization Pack benchmarks (Microsoft) and for Free Edition results (Databricks). Previews are a confidentiality rule, not a consent one (Sections 2 and 3) | [For all Software](https://www.microsoft.com/licensing/terms/product/ForallSoftware/MCA); [Free Edition AUP](https://www.databricks.com/legal/acceptable-use-policy-fe) |
| No page read names cost data, bills or prices as restricted. The MOSA's only confidentiality duty is to keep sign-in credentials secret | [MOSA](https://azure.microsoft.com/en-us/support/legal/subscription-agreement/) (March 2019) |
| Pay-as-you-go prices are published, as estimates, on Azure's pricing pages | [Azure Databricks pricing](https://azure.microsoft.com/en-us/pricing/details/databricks/) (no date on page) |
| The MCA is offered only as a downloaded document (its licensing page shows "Last Update: March 1, 2023"). It was not downloaded or read | [MCA page](https://www.microsoft.com/licensing/docs/customeragreement) |

- **Inferred:** publishing timings is allowed. Pass or fail only is needed just for Free Edition and for numbers that depend on a preview feature marked Beta, Private Preview or Experimental.
- **Inferred:** a cost report isn't a benchmark under either clause. Our bill is public prices times our own usage, so it is nobody's secret, and nothing in the MOSA restricts publishing it. If the account turns out to be under an MCA, read its Confidentiality section before the first cost report goes out.
- **Inferred, good practice:** publish each number with the commit, workflow run link, region, VM sizes, runtime version and data size. That is the replication information both benchmark clauses ask for, and it makes the numbers checkable.

## Unconfirmed

- Which agreement covers our subscription, MOSA or MCA (Cost Management + Billing > Properties > Type).
- The MCA's own text: whether it has a benchmark clause, and whether its confidentiality clause could reach a bill. It is offered only as a download, which this research didn't do.
- Which v2 features will be Beta, Private Preview or Experimental at run time: the runtime version, workspace previews, Container Apps features. That is a per-feature check before each timed run.
- Which agreement the Learn previews page means by an agreement with Databricks, for an Azure customer.
- Whether the Azure portal or the workspace shows extra click-through terms for the Trial tier or at first sign-in. No page names any.
- Dates on the Product Terms pages, the Cloud Provider Directory and the Free Edition AUP: none shows one.
