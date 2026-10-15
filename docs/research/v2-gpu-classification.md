# The student classifier on GPU workers

> Research of Oct 15, 2026, for the v2 cloud plan ([#155](https://github.com/Hien-Trinh/Shopping/issues/155), map [#123](https://github.com/Hien-Trinh/Shopping/issues/123)). It builds on [v2-classifier-on-spark.md](https://github.com/Hien-Trinh/Shopping/blob/research/v2-classifier-on-spark/docs/research/v2-classifier-on-spark.md) (#132, the CPU shape and its throughput estimate) and [v2-cloud-costs.md](https://github.com/Hien-Trinh/Shopping/blob/research/v2-cloud-costs/docs/research/v2-cloud-costs.md) (#124, the CPU prices). Primary sources only: learn.microsoft.com (with `ms.date`), docs.databricks.com (with its "last updated" date), the Azure Databricks pricing page (its embedded price data), Microsoft's Retail Prices API (`prices.azure.com`), onnxruntime.ai, PyPI's JSON API, qdrant/fastembed and huggingface/text-embeddings-inference on GitHub, NVIDIA's product pages and CUDA release notes, and the model's `config.json` on Hugging Face. All read Oct 15, 2026. All prices are US dollars, pay as you go, East US, Linux, list price. Terms follow [GLOSSARY.md](../../GLOSSARY.md).
>
> **Unconfirmed** marks something no primary source settled. **Inferred** marks a step of reasoning, not a quoted fact.

## The short version

1. **Yes, it can run on GPU, but it isn't worth it for the SLO stream.** Databricks supports T4, A10, A100 and H100 nodes on classic compute. The cheapest is **NC4as_T4_v3: 1 T4, 4 vCPUs, $0.826 an hour on jobs compute** (1.00 DBU × $0.30 + $0.526 VM). That is 1.75× a D4as_v5. Spot brings the VM to $0.149, but only on workers.
2. **One T4 does the work of about 45 to 175 vCPUs (inferred).** It runs about 500 to 1,000 Listings a second at 64 to 128 tokens. At 50 Changes/s it would sit 90 to 95% idle. The SLO hour costs about the same on every shape: **$1.89 to $2.36 on CPU, $2.12 to $2.48 with GPU workers, $1.77 to $2.24 with a separate GPU job**.
3. **The GPU wins only the 1M load: about $0.3 to $1.3 against $3.6 to $7.2 on CPU (inferred).** That saves $3 to $6 once. Every shape fits the $50 cap many times over.
4. **It isn't simpler.** A GPU node needs Databricks Runtime ML for GPU (Photon off, dedicated access mode), `fastembed-gpu` instead of `fastembed` (the two can't share an environment), and an `onnxruntime-gpu` build that matches the runtime's CUDA. Runtime 18 LTS ML ships CUDA 12.9, but `onnxruntime-gpu` 1.27 and later on PyPI are built for CUDA 13.0. So pin `onnxruntime-gpu` 1.26.x there, or use Runtime 19 ML (CUDA 13.0). Python is still 3.12.3, so the py312 fix from #132 still applies.
5. **GPU quota is a gate with no published default.** Free Trial subscriptions can't get quota increases. After the move to pay as you go, the request is self-service and "usually" answered "within a few minutes", or it goes to a support ticket with no stated time. Azure ML says GPU families start at zero cores. Whether plain VM quota does too is **unconfirmed**, but plan on asking.
6. **Recommendation: stay on CPU.** Don't add a GPU path to v2. Revisit if bulk loads grow (10M+), repeat often, or if the first CPU measurement lands at the slow end of #132's range. Even then, use shape (b) for the bulk load only: a one-node NC4as_T4_v3 job, not GPU workers in the stream.

## 1. GPU node types, DBUs and prices

### What Databricks supports

| Fact | Source |
|---|---|
| Supported GPU instances: NCads_H100_v5 (NC40ads 1×H100 NVL 94 GB, NC80adis 2×), NC_A100_v4 (NC24ads/48ads/96ads, A100 PCIe 80 GB), NDasrA100_v4 (ND96asr, 8×A100 40 GB), NVadsA10_v5 (NV36ads and NV36adms 1×A10 24 GB, NV72ads 2×), NCasT4_v3 (NC4as/8as/16as 1×T4 16 GB, NC64as 4×), NC_v3 (V100) | [Learn: GPU-enabled compute](https://learn.microsoft.com/en-us/azure/databricks/compute/gpu) (ms.date 2026-09-11) |
| NC_v3 (V100) is being dropped: Azure deprecated NC6s/12s/24s_v3 on Sep 30, 2025 | Same |
| Some GPU types are in Beta, marked so in the driver and worker drop-downs | Same |
| The partial-GPU A10 sizes (NV6ads, NV12ads, NV18ads: 1/6 to 1/2 of an A10) are not on Databricks' list or pricing page. Only the full-GPU NV36ads and up are | Same; [pricing page](https://azure.microsoft.com/en-us/pricing/details/databricks/); [Learn: NVadsA10v5](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/gpu-accelerated/nvadsa10v5-series) (ms.date 2024-07-31) |
| No NC A10 v4 rows in the pricing page or the GPU doc, and no East US price in the Retail Prices API | Same; [API query](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Virtual%20Machines%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20armSkuName%20eq%20%27Standard_NC8ads_A10_v4%27) |
| NCasT4_v3 vCPUs are "non-multithreaded" AMD EPYC 7V12 (Rome) cores, 2.45 GHz base, 3.1 GHz all-core peak | [Learn: NCasT4_v3](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/gpu-accelerated/ncast4v3-series) (ms.date 2024-07-31) |
| A T4 does 8.1 TFLOPS FP32 and 65 TFLOPS FP16 (tensor cores). An A10 does 31.2 TFLOPS FP32, 62.5 TF32, 125 FP16 | [NVIDIA T4](https://www.nvidia.com/en-us/data-center/tesla-t4/); [NVIDIA A10](https://www.nvidia.com/en-us/data-center/products/a10-gpu/) |

### Rules for building a GPU cluster

| Fact | Source |
|---|---|
| "The Machine learning checkbox must be checked." The GPU ML version is picked from the worker type | [Learn: GPU-enabled compute](https://learn.microsoft.com/en-us/azure/databricks/compute/gpu) |
| "Photon is not supported with GPU instance types" | Same |
| "The Worker type must be a GPU instance type." Single node gives one GPU instance | Same |
| Driver and worker can be different instance types. By default the driver uses the worker's type | [Learn: compute configuration](https://learn.microsoft.com/en-us/azure/databricks/compute/configure) (ms.date 2026-10-12) |
| Neither page says whether the **driver** may be a CPU type while workers are GPU. The GPU page only constrains the worker type, and says the NVIDIA driver is installed "on Spark driver and worker instances". **Unconfirmed**; the costs below show both | Same two pages |
| The driver is always on demand. Spot applies to workers only | [Learn: compute configuration](https://learn.microsoft.com/en-us/azure/databricks/compute/configure) |
| Access mode "Auto" picks **Dedicated** when a GPU instance type or an ML runtime is selected | Same |
| Default GPU scheduling is one GPU per task. Setting `spark.task.resource.gpu.amount` to 0.5, 0.33 or 0.25 runs 2, 3 or 4 tasks per GPU. GPU scheduling is off on single-node compute | [Learn: GPU-enabled compute](https://learn.microsoft.com/en-us/azure/databricks/compute/gpu) |
| A capacity reservation group can guarantee "constrained VM types, including GPUs, that might otherwise fail to launch because of Azure stockouts". It needs your own VNet and on-demand instances | [Learn: compute configuration](https://learn.microsoft.com/en-us/azure/databricks/compute/configure) |

**Inferred, a trap:** with the default one GPU per task, an NC4as_T4_v3 worker runs **one task at a time**, even for MERGE and parsing stages that don't touch the GPU. On a mixed workload set `spark.task.resource.gpu.amount` to 0.25 (four tasks share the T4, one per vCPU).

### Prices per hour

DBU/h is the Jobs Compute table of the [pricing page](https://azure.microsoft.com/en-us/pricing/details/databricks/), read from its embedded price data (the `us-east` value; the same rows show $0.30 per DBU). VM prices are the `Consumption` rows of the [Retail Prices API](https://prices.azure.com/api/retail/prices?$filter=serviceName%20eq%20%27Virtual%20Machines%27%20and%20armRegionName%20eq%20%27eastus%27%20and%20armSkuName%20eq%20%27Standard_NC4as_T4_v3%27%20and%20priceType%20eq%20%27Consumption%27) (filter `armSkuName eq 'Standard_<size>'`), Linux. Jobs $/h = DBU × $0.30 + VM. Spot changes the VM part only.

| Node | GPU | vCPU / RAM | DBU/h | VM $/h | **Jobs $/h** | Spot VM $/h | Jobs $/h on spot (workers only) |
|---|---|---|---|---|---|---|---|
| D4as_v5 (CPU, for comparison) | — | 4 / 16 GiB | 1.00 | 0.172 | **0.472** | 0.036 | 0.336 |
| **NC4as_T4_v3** | 1 × T4 | 4 / 28 GiB | 1.00 | 0.526 | **0.826** | 0.149 | 0.449 |
| NC8as_T4_v3 | 1 × T4 | 8 / 56 GiB | 1.50 | 0.752 | 1.202 | 0.213 | 0.663 |
| NC16as_T4_v3 | 1 × T4 | 16 / 110 GiB | 2.50 | 1.204 | 1.954 | 0.341 | 1.091 |
| NC64as_T4_v3 | 4 × T4 | 64 / 440 GiB | 10.00 | 4.352 | 7.352 | 1.234 | 4.234 |
| NV36ads_A10_v5 | 1 × A10 | 36 / 440 GiB | 4.00 | 3.200 | 4.400 | 0.591 | 1.791 |
| NV36adms_A10_v5 | 1 × A10 | 36 / 880 GiB | 6.00 | 4.520 | 6.320 | 0.835 | 2.635 |
| NV72ads_A10_v5 | 2 × A10 | 72 / 880 GiB | 8.00 | 6.520 | 8.920 | 1.205 | 3.605 |
| NC24ads_A100_v4 | 1 × A100 80 GB | 24 / 220 GiB | 10.00 | 3.673 | 6.673 | 0.679 | 3.679 |
| NC48ads_A100_v4 | 2 × A100 | 48 / 440 GiB | 20.00 | 7.346 | 13.346 | 1.358 | 7.358 |
| NC96ads_A100_v4 | 4 × A100 | 96 / 880 GiB | 44.00 | 14.692 | 27.892 | 2.715 | 15.915 |
| NC40ads_H100_v5 | 1 × H100 NVL | 40 / 320 GiB | 13.06 | 6.980 | 10.898 | 1.290 | 5.208 |
| NC80adis_H100_v5 | 2 × H100 NVL | 80 / 640 GiB | 26.12 | 13.960 | 21.796 | 2.580 | 10.416 |
| ND96asr A100 v4 | 8 × A100 40 GB | 96 / 900 GiB | 44.00 | 27.197 | 40.397 | 5.904 | 19.104 |
| NC6s_v3 (V100, deprecated) | 1 × V100 | 6 / 112 GiB | 5.00 | 3.060 | 4.560 | 0.565 | 2.065 |

- The spot rows' `effectiveStartDate` is recent (Sep 1, 2026 for NCasT4_v3), so spot moves. The on-demand NCasT4_v3 price dates from Nov 1, 2021.
- **The ML runtime costs the same DBUs.** The pricing page has one Jobs Compute row per instance and no ML-specific meter (#132 found the same for CPU).
- **During the 14-day trial the DBUs are free and only the VM bills** (#124). An NC4as_T4_v3 then costs $0.526 an hour.

## 2. GPU quota

| Fact | Source |
|---|---|
| "Free Azure trial subscriptions aren't eligible for limit or quota increases." Upgrade to pay as you go first | [Learn: subscription and service limits](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/azure-subscription-service-limits) (ms.date 2026-08-04) |
| vCPU quotas are enforced per region at two tiers: total regional vCPUs, and per VM family. A family increase raises the regional quota by the same amount | [Learn: VM-family quota requests](https://learn.microsoft.com/en-us/azure/quotas/per-vm-quota-requests) (ms.date 2025-01-22); [Learn: regional quota requests](https://learn.microsoft.com/en-us/azure/quotas/regional-quota-requests) (ms.date 2024-03-13) |
| How: Azure Home → **My quotas** → select the family (for the T4, the NCasT4_v3 family in East US) → New Quota Request → a new limit. Needs the Contributor role | [Learn: VM-family quota requests](https://learn.microsoft.com/en-us/azure/quotas/per-vm-quota-requests) |
| How long: "Your request will be reviewed, and you'll be notified if the request can be fulfilled. This usually happens within a few minutes." If not, the page links to a support request. No time is stated for that path | Same |
| Approval "is subject to Microsoft's review, including consideration of current usage, service availability, and capacity constraints". Quota "does not reserve or guarantee capacity". No cost to ask | [Learn: quotas overview](https://learn.microsoft.com/en-us/azure/quotas/quotas-overview) (ms.date 2025-03-25) |
| Spot vCPU quota is a separate quota | [Learn: VM-family quota requests](https://learn.microsoft.com/en-us/azure/quotas/per-vm-quota-requests) |
| Azure ML: "Specialized VM families like NCasT4_v3, NC_A100_v4, or NDv2 series start with a default of zero cores. GPUs also default to zero cores." This is Azure ML compute's quota, not plain VM quota | [Learn: Azure ML quotas](https://learn.microsoft.com/en-us/azure/machine-learning/how-to-manage-quotas) (ms.date 2026-01-27) |
| The VM quota page's sample output shows `Standard NC Family vCPUs 0 / 48` | [Learn: VM vCPU quotas](https://learn.microsoft.com/en-us/azure/virtual-machines/quotas) (ms.date 2026-01-16) |

- **The default GPU family quota for a pay-as-you-go subscription is not published. Unconfirmed.** The only stated default (Azure ML) is zero. **Inferred:** plan on a request for 4 vCPUs (one NC4as_T4_v3) or 8 (two) in the NCasT4_v3 family, plus enough regional vCPUs, and check My quotas after the upgrade.
- **No source says a subscription upgraded from a free trial gets GPU requests denied.** The rule above is only about subscriptions that are still free trials. Whether Microsoft treats new pay-as-you-go accounts more strictly for GPUs is **unconfirmed**.
- The exact quota display name ("Standard NCASv3_T4 Family vCPUs" or similar) is **unconfirmed**; no page fetched names it.

## 3. Runtime, CUDA and the ONNX stack

### Databricks Runtime ML for GPU

| Runtime | Python | CUDA | cuDNN | torch (GPU) | sentence-transformers | onnxruntime | Source |
|---|---|---|---|---|---|---|---|
| 19 ML (Jun 2026, not LTS) | 3.12.3 | 13.0 | 9.23.0.39 | 2.12.0 | 5.5.1 | not shipped | [19 ML](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/19ml) (ms.date 2026-09-11); [19](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/19) (ms.date 2026-10-08) |
| 18 LTS ML (Jun 2026, LTS to Jun 2029) | 3.12.3 | 12.9 | 9.10.2.21 | 2.9.0+cu129 | 5.1.2 | not shipped | [18 ML](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/18ml) (ms.date 2026-09-11); [18](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/18) (ms.date 2026-09-11) |
| 17.3 LTS ML | 3.12.3 | 12.6 | 9.5.1.17 | 2.7.0 | 4.0.1 | not shipped | [17.3 LTS ML](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/17.3lts-ml); [17.3 LTS](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/17.3lts) (ms.date 2026-09-11) |

| Fact | Source |
|---|---|
| Supported runtimes: 19, 18 LTS, 17.3 LTS, 16.4 LTS, 15.4 LTS, 14.3 LTS, each with an ML variant | [Learn: runtime release notes](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/) (ms.date 2026-09-11) |
| "PyTorch uses the CUDA PyPI dependencies to provide CUDA support instead of the CUDA library versions that are built into" the runtime | 18 ML and 19 ML notes above |
| **GPU needs the ML runtime.** The one exception: a custom container (Public Preview) on a *standard* runtime with GPU. Custom GPU images are based on the official CUDA containers, and the NVIDIA driver can't be changed | [Learn: GPU-enabled compute](https://learn.microsoft.com/en-us/azure/databricks/compute/gpu) |
| NVIDIA driver: "535.54.03, which supports CUDA 11.0"; NV A10 v5 gets 535.154.05 | Same |
| CUDA 13.x needs driver ≥ 580. CUDA 12.x runs on ≥ 525 under minor-version compatibility. CUDA 12.9 GA's own driver is ≥ 575.51.03 | [NVIDIA CUDA release notes](https://docs.nvidia.com/cuda/cuda-toolkit-release-notes/index.html), tables 2 and 3 |

**Unconfirmed:** the driver line looks stale. A 535 driver can't run Runtime 19 ML's CUDA 13.0, which Databricks ships. The real driver is visible with `nvidia-smi` on a running cluster.

### onnxruntime-gpu and fastembed-gpu

| Fact | Source |
|---|---|
| `fastembed-gpu` 0.9.0 (Oct 7, 2026) depends on `onnxruntime-gpu` (≥1.17, not 1.20.0 or 1.24.0/1.24.1, on Python 3.11 and 3.12). `fastembed` 0.9.0 depends on `onnxruntime` with the same pins | [PyPI: fastembed-gpu](https://pypi.org/pypi/fastembed-gpu/json); [PyPI: fastembed](https://pypi.org/pypi/fastembed/json) |
| "`onnxruntime-gpu` and `onnxruntime` can't be installed in the same environment … Same is true for `fastembed` and `fastembed-gpu`." FastEmbed doesn't include CUDA or cuDNN | [fastembed: FastEmbed_GPU example](https://github.com/qdrant/fastembed/blob/main/docs/examples/FastEmbed_GPU.ipynb) |
| Usage: `TextEmbedding(model_name=..., providers=["CUDAExecutionProvider"])`; also `cuda=True, device_ids=[...]` | Same; [fastembed README](https://github.com/qdrant/fastembed/blob/main/README.md) |
| `onnxruntime-gpu` 1.31.0 (Oct 9, 2026) has cp312 manylinux x86_64 wheels. Extras `[cuda]` and `[cudnn]` pull NVIDIA's CUDA 13 / cuDNN 9 wheels | [PyPI: onnxruntime-gpu](https://pypi.org/pypi/onnxruntime-gpu/json) |
| "Starting with version 1.27, GPU packages published to PyPI … are built with CUDA 13.0 by default." Separate CUDA 12.x builds need CUDA ≥ 12.8. 1.21 to 1.26 are CUDA 12.8 / cuDNN 9.x on PyPI | [onnxruntime: CUDA EP requirements](https://onnxruntime.ai/docs/execution-providers/CUDA-ExecutionProvider.html#requirements) |
| "ONNX Runtime built with cuDNN 8.x is not compatible with cuDNN 9.x, and vice versa" | Same |
| To reuse PyTorch's CUDA and cuDNN libraries: `import torch` before creating the session, or call `onnxruntime.preload_dlls()` (since 1.21) | Same, "Compatibility with PyTorch" and "Preload DLLs" |

**What this means (inferred):**

- **Runtime 19 ML:** `pip install fastembed-gpu` resolves to `onnxruntime-gpu` 1.31 (CUDA 13.0, cuDNN 9). That matches the runtime's CUDA 13.0 and cuDNN 9.23. Runtime 19 isn't LTS.
- **Runtime 18 LTS ML:** pin `onnxruntime-gpu==1.26.*` (CUDA 12.8 build). It runs on CUDA 12.9 and cuDNN 9.10, and on the torch cu129 wheels via `import torch`. The default 1.27+ wheel would fail to load its CUDA provider.
- **Install order matters.** Install `fastembed-gpu`, never `fastembed`, and the repo's wheel with `--no-deps`, which #132 already requires. Otherwise pip brings back the CPU `onnxruntime`. Neither ML runtime ships `onnxruntime`, so there is nothing to uninstall.
- **The code change is one keyword.** `classify.fastembed` already passes `**kwargs` to `TextEmbedding`, so the student's loader would add `providers=["CUDAExecutionProvider"]`. The ONNX file, the index and the version string stay the same. **Unconfirmed:** whether CUDA's fp32 kernels give vectors close enough to the CPU's that no kNN label flips. The 6l.3 self-check (torch against ONNX within 1e-3) suggests yes; one run on 1,000 Listings settles it.
- **The PyTorch path is not simpler.** Runtime ML ships torch and sentence-transformers, but the student is a pinned ONNX file on the Hub. Whether that revision also holds torch weights is **unconfirmed**. Switching encoders changes the classifier: a new version string, a rebuilt 41k-vector index, and a Backfill that reclassifies every stored Listing (step 6m's rules). The only gain is skipping a `pip install`.

## 4. Throughput per GPU

### What the sources measured

| Measurement | Value | Source |
|---|---|---|
| fastembed, bge-small-en-v1.5, 500 copies of a 6-word sentence (about 11 tokens with the specials, **inferred**), GPU | 43.4 ms ± 2.1 ms, so **about 11,500 texts/s** | [fastembed: FastEmbed_GPU example](https://github.com/qdrant/fastembed/blob/main/docs/examples/FastEmbed_GPU.ipynb) (a Colab run; the GPU model isn't printed, **unconfirmed** that it is a T4) |
| Same notebook, CPU | 4.33 s, about 115 texts/s (100× slower) | Same |
| TEI, bge-**base**-en-v1.5 (110M), 512 tokens, A10G, batch 32 | about 460 req/s (TEI), about 420 (ONNX O4), about 115 (Transformers), read off the chart | [TEI README](https://github.com/huggingface/text-embeddings-inference/blob/main/README.md), image `assets/bs32-tp.png` |
| bge-small-en-v1.5 shape | 12 layers, hidden 384, FFN 1,536, 12 heads | [HF: config.json](https://huggingface.co/BAAI/bge-small-en-v1.5/raw/main/config.json) |
| The Listing text | title plus 200 description characters, about 64 tokens, max 128 | [step-6l.3.md](../specs/step-6l.3.md) |

No source publishes bge-small throughput on a T4 or an A10 at 64 to 128 tokens. Everything below is **inferred**.

### Estimate (all inferred)

- **FLOPs per Listing.** About 21M non-embedding parameters, so about 42 MFLOP per token plus attention. That is about **2.9 GFLOP at 64 tokens and 5.8 GFLOP at 128**.
- **From the fastembed run.** Scale 11,500/s at about 11 tokens by 11/64 to 11/128: **about 1,000 to 2,000 Listings/s**. That implies about 5 TFLOPS achieved, 67% of a T4's FP32 peak, so plausible for a T4.
- **From the T4's FP32 peak** at 30 to 60% efficiency: **about 420 to 1,680 Listings/s**.
- **From TEI.** bge-small has about 1/4 the matmul FLOPs of bge-base, and 128 tokens about 1/4 of 512. 460 × 16 ≈ 7,000/s on an A10G, but probably in fp16. An A10's FP32 peak is 3.9× a T4's.
- **Planning numbers:** **T4: 500 to 1,000 Listings/s. A10: 2,000 to 4,000.** The student runs fp32 (step 6m). An fp16 export could double the T4's rate, but that is a new encoder file and a new version.
- **Against the CPU.** #132 gives 5.7 to 11.4 Listings/s per vCPU, so one T4 ≈ **45 to 175 vCPUs**. Per dollar at full load: NC4as_T4_v3 at $0.826/h gives about 2.2M to 4.4M Listings a dollar. D4as_v5 vCPUs at $0.118/h give about 0.17M to 0.35M. **The GPU is about 6 to 25× cheaper per Listing, but only when it is busy.**
- **At 50 Changes/s, one T4 is 5 to 10% busy.** No GPU node is small enough to fit that load.
- **Batch size.** fastembed pads each batch to its longest text. The repo embeds in chunks of 256 (`student.CHUNK`). That is enough to fill a T4 at these lengths. A micro-batch at 50/s with a 30 s trigger (about 1,500 rows) is 6 chunks. Sorting texts by length before chunking would cut padding. GPU memory isn't a limit: 133 MB of weights plus small activations, against 16 GB.
- **The 4 vCPUs may limit an NC4as_T4_v3 before the T4 does.** Tokenization, Arrow conversion, the kNN and Delta I/O all run on the CPU. **Unconfirmed.**

## 5. Job shapes and costs

A Databricks cluster has one worker type, so classification on GPU means either (a) every worker is a GPU node, or (b) classification runs as its own stream or job on a GPU cluster.

### Assumptions (inferred)

- **CPU baseline**, from #132: classification needs 5 to 9 vCPUs at 50/s. #132 planned 12 to 16 vCPUs in all: a D4as_v5 driver plus 3 or 4 D4as_v5 workers.
- **Without classification** the stream needs about 4 to 8 vCPUs for parsing, MERGE and the rest: a driver plus 1 or 2 D4as_v5 workers.
- **GPU rate:** 500 to 1,000 Listings/s per T4 (section 4); a pessimistic 200/s if the 4 vCPUs bottleneck.
- **Startup:** 5 to 10 minutes of billed VM time per cluster start. No page publishes it (**unconfirmed**).
- **The SLO hour** is a full hour of cluster. **The 1M load** is one job run.
- Prices are on demand. Spot is possible for workers only.

### Shapes

- **(a) One stream, GPU workers.** The driver is a D4as_v5, if a CPU driver is allowed (**unconfirmed**), or an NC4as_T4_v3. Workers are 2 × NC4as_T4_v3: 8 vCPUs for the non-GPU work, with `spark.task.resource.gpu.amount = 0.25`. Simplest code (one stream), but the whole stream moves to Runtime ML for GPU, dedicated access mode and no Photon.
- **(b) Two streams.** The CPU stream (driver + 1 or 2 D4as_v5) writes unclassified rows to a Delta table. A single-node NC4as_T4_v3 job reads it as a stream, classifies, and writes back. That adds one Delta hop: one more trigger interval and commit. **Inferred:** about 10 to 60 s on p99, against a 300 s budget and v1's measured p99 of 24 s ([stress.md](../stress.md)). It also adds a table, a checkpoint, a second job to watch, and more storage transactions (#124 flagged those as a possible top cost, **unconfirmed** in size).

### One SLO hour

| Shape | Nodes | $/h list | DBUs free (trial), VM only |
|---|---|---|---|
| **CPU baseline** (#132) | driver + 3 to 4 D4as_v5 | 4 to 5 × 0.472 = **$1.89 to $2.36** | $0.69 to $0.86 |
| (a) GPU workers, CPU driver | D4as_v5 + 2 × NC4as_T4_v3 | 0.472 + 1.652 = **$2.12** | $1.22 |
| (a) GPU workers, GPU driver | 3 × NC4as_T4_v3 | **$2.48** | $1.58 |
| (b) Separate GPU job | driver + 1 to 2 D4as_v5, plus 1 NC4as_T4_v3 single node | 0.944 to 1.416 + 0.826 = **$1.77 to $2.24** | $0.87 to $1.04 |

The four rows are within about 30% of each other. GPU doesn't pay for the SLO hour because the T4 idles.

### The 1M-Listing load

| Shape | Time | Cost list | DBUs free, VM only |
|---|---|---|---|
| **CPU baseline** (#132) | 1.5 to 3 h on 16 slots | 24 to 49 vCPU-h × $0.118 = $2.9 to $5.8, plus the driver $0.7 to $1.4 = **$3.6 to $7.2** | $1.3 to $2.6 |
| (b) Single-node NC4as_T4_v3 | 17 to 33 min at 500 to 1,000/s; 83 min at 200/s | $0.23 to $0.46; $1.15 pessimistic; plus about $0.1 to start = **$0.3 to $1.3** | $0.2 to $0.8 |
| (a) D4as_v5 driver + 1 NC4as_T4_v3 worker | Same | 1.298/h × the same = **$0.5 to $1.9** | $0.3 to $1.0 |

The GPU saves about **$3 to $6 per 1M load**, or $1 to $2 during the trial.

### Against the $50 cap

- Even the most expensive mix fits. Three SLO hours on shape (a) plus two 1M loads on CPU is about 3 × $2.48 + 2 × $7.2 ≈ $22. **Inferred.**
- GPU's saving on the 1M load is small next to the cap and next to #124's unknown storage-transaction cost.
- **Quota and capacity risk cost time, not dollars.** The request is usually minutes but can go to a support ticket. Approval and capacity aren't guaranteed. A stockout means the job doesn't start. The CPU path has to exist anyway, as the fallback.

### Recommendation

1. **Stay on CPU for v2.** At 50 Changes/s a GPU idles, costs about the same per SLO hour, and adds a GPU runtime, a CUDA-matched `onnxruntime-gpu` pin, dedicated access mode, a quota request, and (for shape b) a second stream with an extra Delta hop.
2. **Don't request GPU quota now.** It is free to ask, but nothing in v1 parity needs it.
3. **If the 1M load ever matters** (it repeats, grows to 10M+, or the CPU measurement lands at 5.7/s per vCPU), add **shape (b) for the bulk load only**: a single-node NC4as_T4_v3 job on Runtime 19 ML, `fastembed-gpu`, `providers=["CUDAExecutionProvider"]`. Check first that its vectors match the CPU's on 1,000 Listings.
4. **What would change this:** one timed run of 10,000 Listings on a D4as_v5 (the #132 measurement), and, if GPU is ever tried, the same 10,000 on an NC4as_T4_v3 with `nvidia-smi` output (the driver version) recorded.

## Unconfirmed

- Whether a GPU cluster may use a CPU **driver** node.
- The default NCasT4_v3 **quota** on a new pay-as-you-go subscription, the exact quota name, and how strictly GPU requests from new accounts are reviewed.
- The real **NVIDIA driver** on Databricks GPU nodes (the docs say 535, which can't run Runtime 19 ML's CUDA 13.0).
- **Throughput** of bge-small fp32 on a T4 or an A10 at 64 to 128 tokens; whether the fastembed notebook ran on a T4; whether the NC4as_T4_v3's 4 vCPUs bottleneck before the GPU.
- Whether CUDA fp32 vectors flip any **kNN label** against the CPU's.
- **Cluster start time**, and whether the student's Hub revision has torch weights.
