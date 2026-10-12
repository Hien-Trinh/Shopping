# The student classifier inside a Spark job, on CPU

> Research of Oct 12, 2026, for the v2 cloud plan ([#132](https://github.com/Hien-Trinh/Shopping/issues/132), map [#123](https://github.com/Hien-Trinh/Shopping/issues/123)). Primary sources only: docs.databricks.com (with its "last updated" date), spark.apache.org (the 4.2.0 docs), learn.microsoft.com (with `ms.date`), onnxruntime.ai, PyPI's JSON API, fastembed's source on GitHub, and Apple's spec page. The Mac numbers come from [stress.md](../stress.md) and [step-6m.md](../specs/step-6m.md). Prices come from [v2-cloud-costs.md](https://github.com/Hien-Trinh/Shopping/blob/research/v2-cloud-costs/docs/research/v2-cloud-costs.md) (#124). Terms follow [GLOSSARY.md](../../GLOSSARY.md).
>
> **Unconfirmed** marks something no primary source settled. **Inferred** marks a step of reasoning, not a quoted fact.

## Answer

1. **Run it as an iterator pandas UDF** (or `mapInPandas`). The function loads the encoder and the index once, then classifies each Arrow batch. Databricks names this exact case: loading a model file once for every input batch. Spark reuses Python workers by default, so a module-level cache loads once per task slot, not once per batch.
2. **The blocker is the Python version, not Spark.** Every current Databricks runtime and serverless environment runs **Python 3.12.3**. The repo pins Python 3.14. Ruff finds **7 syntax errors** under 3.12 (`except A, B:` without parentheses, new in 3.14), one of them in `student.py`. They must go, and CI must check 3.12, before the code can run there. Custom containers can't help: Databricks Runtime ML doesn't support them, and they don't promise a different Python.
3. **Use the standard runtime, not ML.** The student runs on fastembed and ONNX Runtime. Neither runtime ships them. Runtime ML adds torch, transformers and sentence-transformers, which the student never imports. No Azure price meter is specific to Runtime ML, so the DBU rate is the same (inferred). Install `fastembed` as the job's only extra dependency. Don't install the repo's wheel with its dependencies: it would upgrade pyarrow (25 vs 21), deltalake and, on Runtime 18, numpy under the runtime.
4. **Ship the files on a Unity Catalog Volume.** Copy the pinned fp32 snapshot (ONNX, tokenizer, config) and the index `.npz` there once. Load with `local_files_only`, as today. Serverless UDFs can't reach the internet, so the Hugging Face download can't happen inside the job. One code change: `version()` and `index_path()` read `train/` to hash it. Compute both on the driver, or store the hash beside the index.
5. **Throughput, inferred: about 6 to 11 Listings a second per vCPU.** The Mac classifies about 114 a second on 10 cores. Azure vCPUs are hyperthreads, and no benchmark compares them. Use one ONNX thread per task and one task per vCPU. **50 Changes/s needs about 5 to 9 vCPUs for classification.** The 1M load needs about **24 to 49 vCPU-hours**: 1.5 to 3 hours on 16 vCPUs, about $3 to $6 on D4as_v5 jobs compute.
6. **Repartition before the UDF.** A micro-batch at 50/s holds a few thousand rows. That is less than the default Arrow batch of 10,000, so the batch may run as one task on one vCPU.
7. **The local test story is the same code in local Spark.** PySpark 4.2.0 supports Python 3.10 to 3.14 and needs Java 17, 21 or 25. delta-spark 4.4.1 matches. Unit tests use a fake `embed` in a local SparkSession. A `model`-marked test runs the real encoder on the Mac. Add a ruff `py312` target, or a 3.12 CI job, because local tests on 3.14 won't catch 3.12 syntax.

## 1. How the code runs in Spark

### The UDF shape

| Fact | Source |
|---|---|
| An iterator pandas UDF (`Iterator[pd.Series] -> Iterator[pd.Series]`) "is useful when the UDF execution requires initializing some state", "for example, loading a machine learning model file to apply inference to every input batch" | [Databricks: pandas UDFs](https://docs.databricks.com/aws/en/udf/pandas) (updated Sep 11, 2026); [Spark 4.2.0: Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html) |
| `mapInPandas` maps an iterator of pandas DataFrames to another. It can return any number of rows, and internally it works like a Series-to-Series UDF | [Spark 4.2.0: Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html) |
| `spark.sql.execution.arrow.maxRecordsPerBatch` caps the rows per Arrow batch. The default is 10,000 | Same; [Databricks: pandas UDFs](https://docs.databricks.com/aws/en/udf/pandas) |
| That setting "has no impact on serverless compute or on compute configured with standard access mode" on Runtime 13.3 LTS through 14.2. On serverless, "the platform manages Arrow batch sizing internally" | [Databricks: pandas UDFs](https://docs.databricks.com/aws/en/udf/pandas) |
| `spark.python.worker.reuse` defaults to `true`: "a fixed number of Python workers", no fork per task | [Spark 4.2.0: configuration](https://spark.apache.org/docs/latest/configuration.html) |
| `spark.task.cpus` defaults to 1 | Same |
| Pandas UDFs need Runtime 13.3 LTS or later on standard access mode. `sparkContext` is available in Python on 14.0 and later. RDD APIs are not | [Databricks: access mode limitations](https://docs.databricks.com/aws/en/compute/access-mode-limitations) (updated Sep 11, 2026) |
| PySpark needs pandas 2.2.0 and PyArrow 18.0.0 or later | [Spark 4.2.0: Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html) |

**Inferred shape.** A Series-to-Series iterator UDF fits `StudentClassifier.classify`: one path and one confidence per row, in order. It returns a struct of the two. The UDF builds the classifier once per Python worker, using a module-level cache, then calls `classify` per Arrow batch. `embed_chunks` already splits each batch into runs of 256.

### Threads

| Fact | Source |
|---|---|
| ONNX Runtime's default intra-op thread count is the number of **physical** cores | [onnxruntime.ai: thread management](https://onnxruntime.ai/docs/performance/tune-performance/threading.html) (no date shown) |
| For parallel sessions, it suggests separate core affinities or a shared global pool to avoid contention | Same |
| fastembed's `TextEmbedding(threads=...)` sets both `intra_op_num_threads` and `inter_op_num_threads` | [fastembed source: onnx_model.py](https://github.com/qdrant/fastembed/blob/main/fastembed/common/onnx_model.py), [text_embedding.py](https://github.com/qdrant/fastembed/blob/main/fastembed/text/text_embedding.py) (main, read Oct 12, 2026) |

**Inferred.** With one task per vCPU, every Python worker on a node would open an ONNX session with threads equal to all the node's physical cores. That is the Mac's four-sessions-on-ten-cores contention again ([stress.md](../stress.md), 7d.2b point 4). Pass `threads=1` into `classify.fastembed` for the Spark path. This is the `ponytail:` knob that `student.pipeline` already names.

### Where the files come from

| Fact | Source |
|---|---|
| On serverless, "UDFs cannot access the internet", and user code such as UDFs "cannot exceed 1 GB in memory usage" | [Databricks: serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations) (updated Oct 7, 2026) |
| Volumes give POSIX-style paths that work with FUSE-dependent tools | [Databricks: work with files in volumes](https://docs.databricks.com/aws/en/volumes/volume-files) (updated Oct 7, 2026) |
| Volume limits: no `dbutils.fs` distributed to executors, no volume paths in Unity Catalog UDFs, no RDD access. On Runtime 14.2 and below, standard access mode gives FUSE to the driver only, not executors | [Databricks: what are volumes](https://docs.databricks.com/aws/en/volumes/) (updated Oct 6, 2026) |
| Databricks recommends uploading libraries to workspace files or Unity Catalog volumes. Libraries in the DBFS root are deprecated | [Databricks: libraries](https://docs.databricks.com/aws/en/libraries/) (updated Sep 11, 2026) |

What the job needs, measured from this repo on Oct 12, 2026: the fp32 encoder at `FT_REVISION` (133 MB ONNX, plus tokenizer and config), the index `.npz` (61 MB, 41k × 384 float32), and `train/` (about 21 MB) if the version is computed where the classifier runs.

- **Inferred:** a plain pandas UDF (not a Unity Catalog UDF) on Runtime 14.3 LTS or later can read `/Volumes/...` from executors. The only page that lists executor limits scopes the driver-only rule to 14.2 and below. **Unconfirmed** in words. Check it in the Free Edition smoke check (#128).
- **Unconfirmed:** whether a volume keeps the Hugging Face cache's symlinks (`snapshots/` → `blobs/`). Copy with symlinks resolved (`cp -L`), or point `specific_model_path` at a plain directory.
- **Code change (inferred, from reading `student.py`):** `version()` and `index_path()` call `train_hash()`, which reads `train/` relative to the working directory. On executors there's no checkout. Compute the version on the driver and pass it in, or write the hash beside the index.
- **Memory (inferred):** about 200 MB of files plus ONNX's arena per Python worker. A 4-vCPU, 16 GiB node with 4 workers holds 4 copies, about 1 to 2 GB. On serverless it should fit under the 1 GB cap, but not with much room. The Mac never measured a worker's RSS.

## 2. Runtime: standard vs ML

The runtimes in support, from [Databricks: runtime release notes](https://docs.databricks.com/aws/en/release-notes/runtime/) (updated Sep 11, 2026):

| Runtime | Spark | GA | End of support |
|---|---|---|---|
| 19 | 4.2.0 | Jul 23, 2026 (beta Jun 15) | set when it becomes LTS |
| 18 LTS | 4.1.0 | Jun 10, 2026 | Jun 10, 2029 |
| 17.3 LTS | 4.0.0 | Oct 22, 2025 | Oct 22, 2028 |

Preinstalled versions, from each release-notes page:

| Package | Repo pins (`pyproject.toml`) | 18 LTS | 18 LTS ML | 19 | 19 ML |
|---|---|---|---|---|---|
| Python | 3.14 (`.python-version`, `>=3.14`) | **3.12.3** | not stated | **3.12.3** | not stated |
| fastembed | ≥ 0.8.1 | not listed | not listed | not listed | not listed |
| onnxruntime | via fastembed | not listed | not listed | not listed | not listed |
| huggingface-hub | via fastembed | 1.2.4 | 0.36.0 | **removed** ("about 90 packages were removed, including huggingface_hub") | 0.36.2 |
| tokenizers | via fastembed | 0.22.2 | 0.22.1 | not listed | 0.22.1 |
| numpy | ≥ 2.3 | 2.1.3 | 2.1.3 | 2.3.4 | 2.3.4 |
| pyarrow | ≥ 25.0.1 | 21.0.0 | 21.0.0 | 21.0.0 | 21.0.0 |
| pandas | — | 2.2.3 | 2.2.3 | 2.3.3 | 2.3.3 |
| deltalake | ≥ 1.6.6 | 1.1.4 | 1.1.4 | 1.5.1 | 1.5.1 |
| torch / transformers / sentence-transformers | training only (`finetune` group) | not listed | 2.9.0+cpu / 4.57.1 / 5.1.2 | not listed | 2.12.0+cpu / 4.57.6 / 5.5.1 |

Sources: [18 LTS](https://docs.databricks.com/aws/en/release-notes/runtime/18) (updated Sep 11, 2026), [18 LTS ML](https://docs.databricks.com/aws/en/release-notes/runtime/18ml) (Sep 11, 2026), [19](https://docs.databricks.com/aws/en/release-notes/runtime/19) (Oct 8, 2026), [19 ML](https://docs.databricks.com/aws/en/release-notes/runtime/19ml) (Sep 11, 2026). Serverless environments 3 to 6 are also Python 3.12.3. Version 6 was released Sep 3, 2026 ([serverless environment versions](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/), updated Sep 11, 2026).

| Question | Standard runtime | Runtime ML |
|---|---|---|
| What the student needs | `pip install fastembed`. On Python 3.12, fastembed 0.9.0 asks for onnxruntime ≥ 1.17, numpy ≥ 1.26 and huggingface-hub < 2.0 ([PyPI](https://pypi.org/pypi/fastembed/json), read Oct 12). onnxruntime 1.31.0 (Oct 9, 2026) ships cp312 wheels for manylinux x86_64 and aarch64 ([PyPI](https://pypi.org/pypi/onnxruntime/json)) | The same. Runtime ML ships neither fastembed nor onnxruntime |
| DBU price | Jobs Compute $0.30/DBU, Premium, East US | **Inferred: the same.** The Retail Prices API has no ML-specific meter for Azure Databricks (queried Oct 12, 2026). **Unconfirmed** in words |
| Custom containers | Supported on dedicated compute, beta on standard access mode | "Databricks Runtime for Machine Learning does not support Databricks Container Services" ([custom containers](https://docs.databricks.com/aws/en/compute/custom-containers), updated Sep 11, 2026) |
| What it buys | Less to install, and a smaller image (start time **unconfirmed**) | torch, transformers and sentence-transformers. The student uses none of them at serving time |

**Inferred: install `fastembed` only, and the repo's wheel with `--no-deps`.** Installing the wheel with its dependencies would replace pyarrow 21 with 25 and deltalake with 1.6.6, plus numpy 2.1 with 2.3 on Runtime 18, under a runtime whose PySpark was built against its own copies. Spark's notes guarantee only PyArrow ≥ 18 and pandas ≥ 2.2. Serverless adds two rules: don't install anything that pulls in PySpark, and native wheels must cover both aarch64 and x86_64, since serverless may run either ([serverless dependencies](https://docs.databricks.com/aws/en/compute/serverless/dependencies), updated Sep 29, 2026). onnxruntime covers both.

**Python 3.14 vs 3.12.** `ruff check --target-version py312 src` finds 7 syntax errors, all "Multiple exception types must be parenthesized" (run Oct 12, 2026, on `main` at 840bad5). They are in `api.py`, `jev.py`, `load.py`, `state.py` and `student.py`. Ruff checks syntax only, so library calls new in 3.13 or 3.14 are **unconfirmed**. A custom container doesn't fix this. Its page says the image "must also include packages that are built into Databricks compute, such as Python", and it says nothing about another Python version.

## 3. Throughput and cluster size

### What the Mac measured

| Measurement | Value | Source |
|---|---|---|
| Hardware | MacBook Air M4: 10-core CPU, 4 performance and 6 efficiency cores | [Apple: MacBook Air M4 specs](https://support.apple.com/en-us/122209) |
| One worker alone, 1,000 Listings | 9.0 s (9.0 ms a Listing), one ONNX session on all cores | [stress.md](../stress.md), 7d.2b point 4 (Oct 7) |
| Four workers at once | 4,000 Listings in 35 s, **about 114 a second**. Same machine total | Same |
| Per Listing, one at a time | p50 5.8 ms, p99 12.5 ms (fp32) | [step-6m.md](../specs/step-6m.md), Outcome |

The machine is CPU-bound at about **114 Listings/s on 10 cores, so 11.4 per core-second** (114 / 10).

### Azure node facts

| Node | vCPU / RAM | CPU | SMT | Source |
|---|---|---|---|---|
| D4as_v5 | 4 / 16 GiB | AMD EPYC 7763v (Milan) or 9004 (Genoa) | not stated on the page | [Learn: Dasv5](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dasv5-series) (ms.date 2026-03-10) |
| D4ds_v5 | 4 / 16 GiB | Intel Xeon 8370C (Ice Lake) or 8573C (Emerald Rapids), 3.5 GHz all-core turbo, AVX-512 | "hyper threaded" | [Learn: Ddsv5](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/ddsv5-series) (ms.date 2026-03-10) |

Prices from #124: D4as_v5 jobs compute $0.472 an hour (1.0 DBU × $0.30 + VM $0.172), so **$0.118 per vCPU-hour**.

### Estimate (all inferred)

No source compares an M4 core to an Azure vCPU on this model. #124 assumed 1 M4 core ≈ 1 to 2 vCPUs. This estimate keeps that range, so the two documents agree.

- **Per vCPU:** 11.4 / 2 to 11.4 / 1 = **5.7 to 11.4 Listings/s**.
- **50 Changes/s** (every Change classified, the worst case): 50 / 11.4 to 50 / 5.7 = **4.4 to 8.8 vCPUs** of task slots for classification alone.
  - Multi-node: the driver runs no tasks. That is a driver plus 2 or 3 D4as_v5 workers (8 to 12 slots).
  - Single node: the driver does the work. One D8as_v5 (8 vCPU) or one D16as_v5 (16 vCPU, headroom for MERGE).
  - Classification is then 50 to 100% of an 8-vCPU node, which is too tight beside MERGE. Plan on 12 to 16 vCPUs until measured.
- **The 1M load:** 1,000,000 / 11.4 = 87,700 vCPU-s, up to 1,000,000 / 5.7 = 175,400 vCPU-s. That is **24 to 49 vCPU-hours**.
  - 16 task slots (4 D4as_v5 workers): 1.5 to 3.0 hours.
  - 32 task slots (8 workers): 0.8 to 1.5 hours.
  - Cost: 24 to 49 × $0.118 = **$2.9 to $5.8**, plus the driver ($0.47 an hour). #124's $3 to $10 range covers this.
- **Partitions:** a micro-batch at 50/s with a 30 s trigger holds about 1,500 rows. That is under the 10,000-row Arrow batch, and a small stream read may land in one partition. Without `repartition(n_slots)` before the UDF, the batch could run as one task on one vCPU. This follows from Spark's one-task-per-partition model. **Unconfirmed** for the Landing log's actual partition count.
- **Serverless:** no page publishes cores per serverless task, and `maxRecordsPerBatch` doesn't apply there. A per-vCPU estimate can't be made. Measure it with `system.billing.usage`, as #124 says.

**What would change these numbers:** the vCPU ratio, not the model. One timed run of 10,000 Listings on a single D4as_v5 (threads=1, 4 slots) replaces the 1-to-2 range with a measurement.

## 4. Local test story

| Fact | Source |
|---|---|
| Spark 4.2.0 "runs on Java 17/21/25, Scala 2.13, Python 3.10+". Local mode is `local[N]`, and Java must be on `PATH` or in `JAVA_HOME` | [Spark 4.2.0 overview](https://spark.apache.org/docs/latest/) |
| PySpark 4.2.0 (Jul 14, 2026) needs Python ≥ 3.10 and lists 3.10 to 3.14 | [PyPI: pyspark](https://pypi.org/pypi/pyspark/json) (read Oct 12, 2026) |
| delta-spark 4.4.1 (Oct 5, 2026) needs `pyspark>=4.0.1,<=4.2.0` | [PyPI: delta-spark](https://pypi.org/pypi/delta-spark/json) |
| PySpark's testing guide: a pytest fixture shares one SparkSession, and `pyspark.testing.assertDataFrameEqual` compares results | [Spark 4.2.0: testing PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html) |

**Inferred plan, consistent with #128 (local Spark + Delta for TDD and CI):**

- **CI unit tests:** the UDF builds its classifier from an injected `embed`, as the 6m.1 tests do today, with a fake `embed` and a toy index, in a `local[2]` session. Same function, same Arrow path, no model download.
- **`model`-marked test on the Mac:** the real fp32 encoder and index through the UDF on a few Listings. Check that the answers equal `StudentClassifier.classify` on the same rows.
- **Python 3.12:** add `target-version = "py312"` to ruff, or a 3.12 CI job. The tests run on 3.14 and would never see the syntax error Databricks hits.
- **Free Edition smoke check (#128):** serverless, Python 3.12, 1 GB UDF memory, no internet. It is the cheapest place to prove the Volume read from a UDF and the memory fit.

## Couldn't confirm

- Whether a plain pandas UDF reads `/Volumes` paths on executors on Runtime 14.3 LTS or later. Inferred yes.
- Whether volumes keep symlinks from the Hugging Face cache.
- That Runtime ML bills the same DBU rate. Inferred from the price meters.
- Whether the Dasv5 vCPUs are SMT threads (the page doesn't say). Ddsv5's are.
- Any M4-vs-Azure vCPU benchmark for ONNX encoders. The per-vCPU range is inferred.
- Cores and memory per serverless task. Whether 4 worker copies, or one copy plus the ONNX arena, stay under serverless's 1 GB UDF cap.
- Whether library calls in `src/` need Python 3.13 or 3.14. Ruff checks syntax only.
- Start-up time of Runtime ML vs the standard runtime.
