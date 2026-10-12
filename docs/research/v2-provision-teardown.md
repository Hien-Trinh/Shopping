# Provisioning and tearing down v2 on Azure and Databricks

> Research of Oct 12, 2026, for ticket #133 of the v2 cloud plan (#123). Primary sources only: learn.microsoft.com, the Terraform providers' own docs in their GitHub repos (the Registry pages render with JavaScript), the Azure Bicep repo, and Microsoft's Retail Prices API (`prices.azure.com`). Learn pages carry their `ms.date`. Prices are US dollars, list price. Budget facts are in [v2-cloud-costs.md](https://github.com/Hien-Trinh/Shopping/blob/research/v2-cloud-costs/docs/research/v2-cloud-costs.md) section 5 and are not repeated here. Terms follow [GLOSSARY.md](../../GLOSSARY.md).
>
> **Unconfirmed** marks something no primary source settled. **Inferred** marks a step of reasoning, not a quoted fact.

## Answer

1. **Only Terraform covers the whole setup.** The `azurerm` provider makes the Azure side: storage account, Container Apps, Key Vault, the Databricks workspace and its access connector, the budget and action group. The `databricks` provider makes the inside of the workspace: Unity Catalog objects, jobs, compute policies, secret scopes. `terraform destroy` removes both.
2. **Bicep stops at the workspace.** It has no Unity Catalog, job or policy resources. The feature request has been open since March 2023. Bicep would need a second tool for everything inside Databricks.
3. **Bundles stop at the workspace edge.** Declarative Automation Bundles (the new name for Databricks Asset Bundles) deploy jobs, policies, secret scopes, catalogs and external locations, and `databricks bundle destroy` removes them. They cannot make the workspace, the storage account, the metastore or a storage credential.
4. **An `az` script can create anything, but its teardown is a resource-group delete, not a diff.** It keeps no state, so the script has to know what it made.
5. **A classic workspace costs about $1.20 a day while it sits idle.** With the default managed network, Databricks puts a NAT gateway and a public IP in the managed resource group. They cost $0.045/h and $0.005/h, so about $36 a month, close to the whole $50 cap. So the workspace has to be created for a run and deleted after it, or be a **serverless workspace**, which has no NAT gateway.
6. **Guardrails that act in time are per run, not per budget.** These are a job `timeout_seconds`, a compute policy (fixed node type, worker cap, `dbus_per_hour` cap, Photon off, job compute only), `max_concurrent_runs = 1`, a Container Apps `maxReplicas`, and teardown when the run ends. A budget can call an action group, which can run automation that stops resources. But budgets are checked once a day, so they are a backstop only.
7. **The pipeline needs almost no secrets in Databricks.** Databricks reaches our ADLS account through the access connector's managed identity. No keys are involved. Merchant keys are needed by the API only. They go in Key Vault, and Container Apps reads them through a managed identity.
8. **Lean recommendation:** use Terraform, in two root modules. A small **base** module stays alive: the storage account with the data, Key Vault, the budget and action group, and the registry. A **run** module is created and destroyed around each paid run: the workspace, access connector, Unity Catalog objects, policy, jobs and Container Apps. Bundles are optional, for deploying the job code. The final pick belongs to the grilling ticket.

## 1. What each tool covers

| Resource | Terraform `azurerm` + `databricks` | Bicep / ARM | Bundles | `az` CLI script |
|---|---|---|---|---|
| Storage account, ADLS container | Yes: `azurerm_storage_account`, `azurerm_storage_data_lake_gen2_filesystem` | Yes | No | Yes |
| Container Apps environment, app, job | Yes: `azurerm_container_app_environment`, `azurerm_container_app`, `azurerm_container_app_job` | Yes | No | Yes (`az containerapp`) |
| Container registry | Yes: `azurerm_container_registry` | Yes | No | Yes |
| Key Vault and secrets | Yes: `azurerm_key_vault`, `azurerm_key_vault_secret` | Vault yes; secrets are data plane (see §4) | No | Yes |
| Databricks workspace | Yes: `azurerm_databricks_workspace` (classic). Serverless mode: not in `azurerm` docs; **inferred** via the `azapi` provider | Yes, `computeMode` `Hybrid` or `Serverless` | No | Yes |
| Access connector (managed identity for UC) | Yes: `azurerm_databricks_access_connector` | Yes | No | Yes |
| Unity Catalog metastore, storage credential | Yes: `databricks_metastore`, `databricks_storage_credential` | No | No | Through the `databricks` CLI only |
| Catalog, schema, external location | Yes: `databricks_catalog`, `databricks_schema`, `databricks_external_location` | Only an initial catalog at workspace creation (`defaultCatalog`) | Yes (needs `engine: direct`) | Through the `databricks` CLI |
| Jobs | Yes: `databricks_job` | No | Yes | Through the `databricks` CLI |
| Compute policies | Yes: `databricks_cluster_policy` | No | Yes | Through the `databricks` CLI |
| Secret scopes | Yes: `databricks_secret_scope` | No | Yes | Through the `databricks` CLI |
| Azure budget and action group | Yes: `azurerm_consumption_budget_resource_group`, `azurerm_monitor_action_group` | Yes | No | Yes |
| Databricks budget | Yes: `databricks_budget`, `databricks_budget_policy` | No | No | Through the `databricks` CLI |
| One-command teardown | `terraform destroy` (both providers, from state) | Deployment stack: `az stack group delete --action-on-unmanage deleteAll`, or delete the resource group | `databricks bundle destroy` (jobs, pipelines, artifacts it deployed) | `az group delete` |
| State | State file, so it knows what it made | Deployment stack tracks managed resources | Bundle state in the workspace | None |
| Latest release seen | `azurerm` v5.9.0 (Oct 8, 2026); `databricks` v1.137.0 (Oct 6, 2026) | API version 2026-01-01 for workspaces | Databricks CLI v1.20.0 (Oct 7, 2026) | — |

Sources:

- The `azurerm` resource list comes from the provider's docs folder ([website/docs/r](https://github.com/hashicorp/terraform-provider-azurerm/tree/main/website/docs/r), read Oct 12, 2026). The `databricks` list comes from its [docs/resources](https://github.com/databricks/terraform-provider-databricks/tree/main/docs/resources) folder (read Oct 12, 2026). Release dates come from each repo's GitHub releases ([azurerm](https://github.com/hashicorp/terraform-provider-azurerm/releases), [databricks](https://github.com/databricks/terraform-provider-databricks/releases), [CLI](https://github.com/databricks/cli/releases)).
- Microsoft's split: "You use the Azure Provider to provision Azure Databricks workspaces". The Databricks provider "deploy[s] and manage[s] clusters and jobs and ... configure[s] data access" ([Learn: Databricks Terraform provider](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/terraform/), ms.date 2026-09-11).
- Bicep: [Learn: Microsoft.Databricks/workspaces template reference](https://learn.microsoft.com/en-us/azure/templates/microsoft.databricks/workspaces) (updated 2026-07-20). It has `computeMode` ("Required on create, cannot be changed. Possible values include: 'Serverless', 'Hybrid'") and `defaultCatalog`. No UC resource types exist. The request for UC support is [Azure/bicep#10115](https://github.com/Azure/bicep/issues/10115), open since 2023-03-15 and labelled "Needs: Upvote" (checked Oct 12, 2026).
- Bundles: the resource list has 38 types, including job, cluster_policy, secret_scope, catalog, schema, external_location and volume. It has no workspace, metastore or storage credential. Catalogs, external locations and cluster policies need `engine: direct` ([Learn: bundle resources](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources), ms.date 2026-10-07). The direct engine is the default. Bundles were first built on the Databricks Terraform provider ([Databricks: migrate to the direct engine](https://docs.databricks.com/aws/en/dev-tools/bundles/direct), read Oct 12, 2026). `bundle destroy` "permanently deletes a bundle's previously-deployed jobs, pipelines, and artifacts" ([Learn: bundle command group](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands), read Oct 12, 2026). Development mode pauses all schedules and triggers ([Learn: deployment modes](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes), ms.date 2026-09-11).
- Deployment stacks: `deleteAll` deletes "both the resources and the resource groups". Unmanaged resources in the stack's resource group block that group's deletion. Deny settings don't cover implicitly created resources ([Learn: deployment stacks](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/deployment-stacks), ms.date 2026-06-18).

## 2. The workspace's hidden idle cost

| Fact | Source |
|---|---|
| Secure cluster connectivity (no public IP) is on by default for new workspaces (portal and ARM) | [Learn: secure cluster connectivity](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/secure-cluster-connectivity) (ms.date 2026-10-08) |
| With the default managed VNet, "Azure Databricks automatically creates a NAT gateway" in the managed resource group. You cannot modify it, and it "incurs additional cost" | Same |
| The `azurerm` workspace resource names it `nat-gateway`, with a public IP `nat-gw-public-ip`, and `no_public_ip` defaults to `true` | [azurerm docs: databricks_workspace](https://github.com/hashicorp/terraform-provider-azurerm/blob/main/website/docs/r/databricks_workspace.html.markdown) (read Oct 12, 2026) |
| NAT gateway: $0.045 per hour plus $0.045 per GB processed | [Retail Prices API](https://prices.azure.com/api/retail/prices?$filter=contains(productName,%27NAT%27)) (meter effective 2020-06-01, read Oct 12, 2026) |
| Standard static public IP: $0.005 per hour | [Retail Prices API](https://prices.azure.com/api/retail/prices?$filter=armRegionName%20eq%20%27eastus%27%20and%20serviceName%20eq%20%27Virtual%20Network%27) (read Oct 12, 2026) |
| VNet injection instead: after March 31, 2026, new workspaces "will require a secure outbound method such as a NAT Gateway". So bringing your own VNet doesn't avoid the cost | [Learn: secure cluster connectivity](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/secure-cluster-connectivity) |
| Serverless workspaces: all compute is serverless, with default storage. NAT gateway and public IP settings are "Not allowed in Serverless ComputeMode workspace" | [Learn: serverless workspaces](https://learn.microsoft.com/en-us/azure/databricks/admin/workspace/serverless-workspaces) (ms.date 2026-09-11); [template reference](https://learn.microsoft.com/en-us/azure/templates/microsoft.databricks/workspaces) |

**Inferred:** a classic workspace left standing costs about $0.05/h, so $1.20 a day or $36 a month, before any compute. A serverless workspace has no NAT gateway, so it should cost about $0 when idle, plus its default storage. Serverless brings the limits listed in the costs research: no compute policies, and streaming only with `AvailableNow`/`Once`.

## 3. Guardrails each tool can set

All of these are settings on resources, so any tool that makes the resource can set them. Terraform and Bundles can set every Databricks setting. Bicep can set none of them.

| Guardrail | What it does | Source |
|---|---|---|
| Compute policy `autotermination_minutes` | Fixes idle shutdown for all-purpose compute. "A value of 0 represents no auto termination" | [Learn: compute policy reference](https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policy-definition) (ms.date 2026-10-01) |
| Compute policy `runtime_engine` | `PHOTON` or `STANDARD`. Fix it to `STANDARD` to turn Photon off | Same |
| Compute policy `node_type_id`, `driver_node_type_id` | `fixed`, `allowlist` or `regex` limits on VM sizes | Same |
| Compute policy `num_workers`, `autoscale.max_workers` | `range` cap on workers | Same |
| Compute policy `dbus_per_hour` | A virtual attribute: "the maximum DBUs a resource can use on an hourly basis including the driver node" | Same |
| Compute policy `cluster_type` | Allow `job` only, so nobody makes all-purpose compute | Same |
| Job `timeout_seconds` | Per run, at job or task level. "The default behavior is to have no timeout" | [databricks provider: job](https://github.com/databricks/terraform-provider-databricks/blob/main/docs/resources/job.md) (read Oct 12, 2026) |
| Job `max_concurrent_runs` | Defaults to 1. Bundles in development mode raise it, so pin it to 1 | Same; [Learn: deployment modes](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes) |
| Job `health` rule `RUN_DURATION_SECONDS` | Notifies only. It doesn't stop the run | [databricks provider: job](https://github.com/databricks/terraform-provider-databricks/blob/main/docs/resources/job.md) |
| Serverless job `performance_target` | `STANDARD` is the cost-efficient mode. `PERFORMANCE_OPTIMIZED` is the default | Same |
| Container Apps scale to zero | "When a revision is scaled to zero replicas, no resource consumption charges are incurred." A Consumption-only environment has no plan management fee. That fee applies with a Dedicated profile, private endpoints or planned maintenance | [Learn: Container Apps billing](https://learn.microsoft.com/en-us/azure/container-apps/billing) (ms.date 2025-12-09) |
| Budget → action group → automation | The tutorial chains a budget, an action group, a Logic App and an Automation runbook that stops VMs at 80% and 100%. "In the most basic of scenarios ... you could directly call the automation script from Monitor" | [Learn: budget scenario](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/cost-management-budget-scenario) (ms.date 2025-06-26) |

**Inferred:** for us, the action group's automation would cancel Databricks runs, scale Container Apps to zero, or run the teardown. No example in the docs does this for Databricks or Container Apps; we would write it. Budgets are evaluated once every 24 h (costs research §5). So the run script's own timer and teardown are the real cap, and the budget only catches a teardown that failed.

## 4. Secrets

| Need | Option | Facts | Source |
|---|---|---|---|
| Merchant keys for the API | Container Apps secret as a Key Vault reference | The app needs a managed identity with the **Key Vault Secrets User** RBAC role. Without a version in the URI, the app picks up a new version within 30 minutes and restarts revisions that use it in env vars. Secrets are app-scoped. Changing one makes no new revision | [Learn: Container Apps secrets](https://learn.microsoft.com/en-us/azure/container-apps/manage-secrets) (ms.date 2026-09-04) |
| Merchant keys, simpler | Container Apps secret with a direct value | Works, but Microsoft says to avoid it in production. Through Terraform, the value also lands in state (**inferred**) | Same |
| Databricks to ADLS | Access connector + UC storage credential + external location | A managed identity: "do not require you to maintain credentials or rotate secrets". Grant it **Storage Blob Data Contributor** on the account or container | [Learn: managed identities in UC](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/azure-managed-identities) (ms.date 2026-09-11) |
| Secrets in Databricks code | Databricks-backed secret scope | Stored by Databricks, made by CLI, API, Terraform or Bundles | [Learn: secret management](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/) (ms.date 2026-09-11) |
| Secrets in Databricks code, from Key Vault | Key Vault-backed secret scope | Read-only. Supports **only the Vault access policy permission model, not Azure RBAC**. The creator needs Contributor or Owner on the vault. Terraform can't create it with a personal access token; it needs Azure-specific auth | Same; [databricks provider: secret_scope](https://github.com/databricks/terraform-provider-databricks/blob/main/docs/resources/secret_scope.md) (read Oct 12, 2026) |
| UC-governed secrets | Secrets in Unity Catalog | Exists (bundle resource `secret`). Not read in depth | [Learn: bundle resources](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources) |

**Inferred:** Container Apps wants an RBAC vault, and a Key Vault-backed scope wants an access-policy vault. One vault can't serve both the documented way. Since the Databricks side needs no merchant key, the simple path is one RBAC vault for the API and no secret scope at all.

## 5. Keep alive or recreate

Prices for storage, Key Vault, the registry and Log Analytics come from the costs research §4. The rest is cited here.

| Resource | Idle cost | Keep or recreate (**inferred**) |
|---|---|---|
| ADLS account with the Delta tables | $0.0208/GB-month, so cents for a few GB | Keep. It holds the data and the Landing log, and v2's runs build on it |
| Key Vault | No idle fee; $0.03 per 10,000 operations | Keep. It holds the merchant keys |
| Container registry (Basic) | Free 12 months, then $0.1666/day | Keep, or rebuild the image per run |
| Budget, action group, the automation behind it | Not priced here (**unconfirmed**) | Keep. It is the backstop and must outlive the runs |
| Container Apps environment and app at 0 replicas | $0 resource charges ([billing](https://learn.microsoft.com/en-us/azure/container-apps/billing)) | Either. Recreating is cheap and removes any chance of traffic |
| Classic Databricks workspace (managed VNet) | About $0.05/h for NAT gateway + IP, $36 a month (§2), plus the managed DBFS storage account | **Recreate per run** |
| Serverless Databricks workspace | No NAT. Default storage billed by use | Could be kept. Not priced (**unconfirmed**) |
| Access connector | Not priced (**unconfirmed**) | Goes with the workspace |
| UC metastore | Account-level, one per region; UC is on for all workspaces created after Nov 9, 2023 ([Learn: get started with UC](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started), ms.date 2026-09-11) | It persists outside the workspace (**unconfirmed** what deleting the workspace does to it) |

## Not confirmed

- Whether the NAT gateway is created with the workspace or only when the first cluster starts. The `azurerm` defaults suggest creation with the workspace.
- Whether `azurerm` can create a serverless workspace. Its docs (provider v5.9.0) have no `compute_mode`. The `azapi` provider should, since it takes the raw ARM type, but this wasn't tested.
- What deleting a workspace does to its Unity Catalog metastore assignment, the workspace catalog, and catalog metadata for tables in our own ADLS.
- The idle price of a serverless workspace's default storage, the access connector, an Azure budget, and an action group.
- Whether the free trial's $0 DBU meters cover serverless jobs (carried over from the costs research). This decides classic vs serverless cost per run.
- Whether a Container Apps Key Vault reference works against a vault in the access-policy model. Only RBAC is documented.
- Whether Unity Catalog secrets fit our case. That page wasn't read.
