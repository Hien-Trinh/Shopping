# A cheap, fast classifier: local on the Mac, then on Azure and Databricks

> Research of Oct 6, 2026, by three web-research agents (local models; Azure Databricks; Azure). Kept as written for the 6m spec and the later port. Prices are list prices read on Oct 6, 2026; accuracy ranges for models we have not trained are extrapolations from the cited papers, not measurements. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## The short version

1. **The model and the hosting are separate decisions.** The student is a 33M-parameter ONNX encoder (bge-small, 34 MB int8) plus a 40k × 384 float32 matrix (61 MB). That artifact runs in-process in the Ingestion workers today, and it runs in-process in a Databricks job or an Azure container tomorrow. Nothing about it changes with the move.
2. **Hosting is nearly free if the classifier stays in the worker process.** A separate endpoint costs $200+ a month on Databricks Model Serving and buys nothing at 50 Listings/s. Managed vector stores ($74–245 a month) are not worth it for a 61 MB matrix.
3. **The accuracy work is what's left, and it is portable.** The measured path is fine-tuning the encoder end-to-end (inferred +5 to +10 points over frozen kNN) and giving the hosted LLM a top-10 shortlist to rerank (+3 to +4 points measured in the closest published setup).
4. **The LLM fallback is where the money goes.** Every option is $33–$2,700 per 1M Listings; the local model is under $5 per 1M. Each point of the unsure rate removed saves more than any hosting choice.

## Where we are (measured)

| | Exact on the 1,020 | Kept local | Cost per 1M | Latency |
|---|---|---|---|---|
| Jev alone (production settings) | 66.9% | 0% | about $62 | 250 ms |
| kNN (k=10) alone, 40k training rows | 52–55% | 100% | about $0 | 10 ms p50 |
| kNN + Jev below τ (6l.2) | 70.0% | 83.7% | about $10 | 10 ms / 250 ms |
| Opus 5.5 (the teacher) | 86.7% | 0% | about $2,700 | seconds |

## Local model options (the MacBook Air M4, 16 GB)

Training on the M4; inference in the 8 worker processes.

| Option | Train time | Inference | RAM per worker | Expected exact (basis) | New deps | Effort | Verdict |
|---|---|---|---|---|---|---|---|
| **Current: bge-small + numpy kNN** | none | 10 ms (measured) | 34–133 MB + 61 MB | 52–55% (measured) | none | 0 | Baseline |
| **sklearn heads on what we have:** logistic regression on the frozen embeddings, and TF-IDF (word + char n-grams) + LinearSVC, ensembled with kNN | minutes | +<1 ms | +3 MB (logistic); cap TF-IDF at ~50k features × float32, ~340 MB, or it is 2.7 GB | 50–58% (Rakuten: flat SVM beat BM25-kNN by 5.8 points, with 35× our rows per class) | scikit-learn | 3–6 h | **Cheap first test.** Token features catch brands and model numbers that embeddings blur; the logistic head gives a calibrated probability for τ |
| **Fine-tune bge-small end-to-end + 1,700-way head, export to ONNX** | 0.5–1 h on MPS (estimate); 3× on CPU | 10 ms, unchanged | unchanged | 57–64% (inferred: fine-tuned BERT beat fastText by 5.5 points on 2,214 Amazon categories; linear beat kNN by 6 on Rakuten) | torch + transformers at training time only; serving stays fastembed/ONNX | 8–16 h | **Best-supported real gain.** MPS training can be flaky; CPU or a rented GPU is the fallback |
| ModernBERT-base / DeBERTa-v3-small fine-tune | 2–4 h | 3–5× slower | 150–600 MB | +1–3 over bge-small (unknown on product data) | same | 10–16 h | ModernBERT's speed needs CUDA unpadding; not on a Mac. Later, if bge-small plateaus |
| fastText supervised | under 1 min | 0.1 ms | 800 MB default; quantize to tens of MB | 45–55% (below BERT by 5.5 and below SVM on Rakuten) | fasttext (Meta's repo is archived) | 2–4 h | Skip |
| SetFit | hours (contrastive pairs over 1,700 classes) | as the encoder | as the encoder | ≤ plain fine-tune (built for few-shot; we have ~24 rows per class) | setfit, torch | 6–10 h | Skip |
| Small LLM + LoRA on MLX (Qwen3-0.6B/1.7B, Gemma 3 1B) | ~1 h per epoch for 0.6B | hundreds of ms | 0.4–3.5 GB per process; 8 copies of 1.7B don't fit | ≈ a fine-tuned encoder at best; no published 1,700-class product result; a 400M encoder beat a 1B decoder on classification | mlx, mlx-lm | 16–30 h | Skip (6e's Laya result stands) |

Techniques with measured gains, for either path: a **flat leaf classifier beats top-down level-wise models** (Rakuten: 0.837 flat vs 0.817 top-down); **category names or definitions as label text** (+0.7 to +1.5 points); **top-10 from the local model, then the LLM picks** (+3 to +4 points; a small cross-encoder reranker did not help); **temperature scaling** barely changes the confidence ranking, so pick τ on held-out rows as 6l.2 does. Serving: ONNX Runtime on CPU; the CoreML provider measured 1.6–2.3× *slower*; set `intra_op_num_threads` to 1–2 per worker; no shared classifier process is needed at 34 MB int8.

## Cloud hosting options (Azure + Databricks)

Idle cost assumes long quiet periods; "2 h/day" is the scale-to-zero case. Rates: Databricks serverless real-time inference $0.07 per DBU in East US, $0.084 West Europe; Container Apps West Europe is ~40% dearer than East US.

| Option | What runs where | Latency per item | Cost per 1M Listings | Fixed monthly | Scale-to-zero | Ops | Verdict |
|---|---|---|---|---|---|---|---|
| **In-process in the Ingestion workers** (wherever they run) | ONNX + numpy in the worker | 10 ms | ~$0 | $0 | n/a | lowest | **Default.** The 61 MB matrix and 34 MB model load in each worker |
| **Databricks Job, ONNX in a pandas UDF over Delta** | job compute (serverless or classic) | batch only; 4–6 min startup on standard serverless | $1–5 (estimate; pilot it) | $0 | yes | low | **For the 1M bulk loads.** Reads and writes Delta directly |
| **Azure Container Apps, consumption** | your FastAPI + onnxruntime container | 10 ms + HTTP; cold start unknown for a 130 MB image | $1–3 | ~$1 at 2 h/day (free grant); ~$22 (East US) / ~$30 (West Europe) with min replica 1 | yes | low–medium | **If a separate service is ever wanted.** Cheapest endpoint |
| Databricks Model Serving, CPU Small | pyfunc (ONNX + kNN matrix) in the serving container, 4 GB per concurrency | <20 ms overhead with route optimization (set at creation, OAuth only) | ~$1 marginal | ~$204 (East US) / ~$245 (West Europe) always on; ~$21–26 at 2.5 h/day | optional; 10–20 s cold start, sometimes minutes, no SLA | medium | Pay only if the team standardizes on it. Needs a service principal; a new version takes ~10 min to build |
| Databricks FM API embeddings (Qwen3-0.6B at 0.286 DBU/1M tokens; GTE-large 1.857) + own kNN | Databricks-hosted embedder, kNN in the worker | <50 ms overhead | $1.2 (Qwen3) / $7.8 (GTE) | $0 | yes | low | Only if we drop bge-small; re-embeds the 40k index and changes `taxonomy_version` |
| Azure Functions Flex Consumption | zip deploy, 2 GB instance | 10 ms warm; cold start unknown | ~$0.5 batched; up to ~$52 if 1M single calls each bill the 1 s minimum | $0; ~$21 always-ready | yes | low | Package-size limit for a 130 MB model unverified |
| Azure ML managed online endpoint | managed VM | 10 ms + network | ~$0.3 | $71 (F2s_v2) – $99 (DS2_v2) | **no** | medium | Worst fit for an idle-heavy load |
| `ai_classify` (Databricks SQL) + embedding shortlist | serverless SQL + managed GPU | batch | not published | $0 | yes | medium | **2–500 labels max**, so it needs the shortlist; not on views; per-row price undisclosed |
| Azure AI Language custom text classification | managed trainable classifier | unknown | **$5,000** ($5 per 1k records) | $0.50 + $3/h training | n/a | low | **200-class cap.** No |
| Managed kNN: Databricks AI Search (Standard unit, 4 DBU/h) / Azure AI Search Basic–S1 | HNSW service | 20–50 ms | query cost not published | ~$204 / $74–245 | **no** | medium | Not for 61 MB. Revisit at tens of millions of vectors or when several services share it |

## The LLM fallback (what Jev costs against the alternatives)

600 input + 15 output tokens per Listing (a 50-option shortlist). The output side is small, so input price dominates.

| Model | Where | $/1M tokens in / out | Per 1M Listings | Notes |
|---|---|---|---|---|
| Jev (jev-1.13.0) | TypeSafe API | $0.042 / free | **$62** (measured) | 80 requests/s; 66.9% exact; the current fallback |
| GPT-4.1-nano | Azure OpenAI, global | $0.10 / $0.40 | $66; **$33 batch** | Cheapest first-party path for bulk; accuracy on our eval unknown |
| GPT-OSS-20B | Databricks FM API | 1.0 / 4.286 DBU | $45 (East US) – $54 (West Europe) | FM API limit of 1M input tokens/min gives ~28 Listings/s, under the 50/s peak |
| Phi-4-mini | Foundry serverless | $0.075 / $0.30 | ~$50 | Accuracy on 1,700 leaves unknown |
| Claude Haiku 4.5 | Anthropic API / Foundry | $1 / $5 | $675; $338 with the Batch API (Anthropic API only; **no batch on Foundry**); +10% US Data Zone | 10× Jev |
| Claude Sonnet 5.5 | Anthropic API / Foundry | $2 / $10 | $1,350 | |
| Claude Opus 5.5 (the teacher) | Anthropic API / Foundry | $4 / $20 | $2,700; $1,350 batch (API only) | 86.7% exact; its tokenizer counts ~30% more tokens than assumed here. Labeling, not serving |

Not found: first-party latency figures for any hosted LLM; an EU data zone for Claude on Foundry (docs list Global Standard and US Data Zone only).

## What this means for the plan

- **6m (the student in the pipeline) does not depend on the cloud decision.** Keep weights and the index as files the workers load; on Databricks they become a Unity Catalog volume path or an MLflow `pyfunc` artifact (the kNN matrix goes in `artifacts`, loaded in `load_context`; pin `mlflow`, not `mlflow-skinny`).
- **The next accuracy step, if we take one before the port:** the sklearn ensemble test (half a day) tells us whether token features help; the end-to-end fine-tune (one to two days) is the one with evidence for a real jump. Both ship as the same ONNX file.
- **The cheapest change to the cascade needs no new model:** send Jev the student's top-10 instead of the embedding's top-50 (fewer input tokens, and +3 to +4 points in the Amazon study), once the student's top-10 recall is measured.
- **Jev stays until something beats it on the 1,020.** On price, GPT-4.1-nano batch ($33) and GPT-OSS-20B ($45–54) are in the same band; Claude models are 10–40× dearer and belong to labeling.

## Sources

Local models: Amazon RetailProducts2023 dual-expert, aclanthology.org/2024.customnlp4u-1.22.pdf · Rakuten 2018 overview, ceur-ws.org/Vol-2319/ecom18DC_paper_13.pdf · MWPD 2020 ProBERT, ceur-ws.org/Vol-2720/paper7.pdf · Shopify engineering (2018, 2020, 2024 posts on product classification) · superlinked.com/docs/examples/taxonomy-classification · fastText, arxiv.org/abs/1607.01759 · FastFit, arxiv.org/html/2404.12365 · Revisiting k-NN, arxiv.org/pdf/2304.09058 · calibration, arxiv.org/abs/2003.07892 and arxiv.org/pdf/2208.12084 · Ettin, arxiv.org/abs/2507.11412 · causal LLMs for classification, arxiv.org/html/2512.12677v1 · ModernBERT, arxiv.org/pdf/2412.13663 and arxiv.org/html/2504.08716v1 · Apple ANE transformers, machinelearning.apple.com/research/neural-engine-transformers · Xenova/bge-small-en-v1.5 ONNX sizes on Hugging Face.

Databricks (Azure docs, updated Sep–Oct 2026): resources/pricing · model-serving/custom-models · model-serving/create-manage-serving-endpoints · model-serving/model-serving-limits · model-serving/route-optimization · foundation-model-apis/limits · sql/language-manual/functions/ai_classify · large-language-models/classify-documents-labels-tutorial · large-language-models/ai-functions · ai-search/ai-search, best-practices, cost-management · jobs/run-serverless-jobs · model-serving/deploy-custom-python-code · databricks.com/product/pricing/foundation-model-serving and /vector-search · prices.azure.com/api/retail/prices.

Azure: prices.azure.com/api/retail/prices (Container Apps, Functions, VMs, Azure OpenAI eastus2, Phi, Foundry Tools, Cognitive Search) · azure.microsoft.com/pricing/details/container-apps · learn.microsoft.com/azure/azure-functions/flex-consumption-plan · learn.microsoft.com/azure/machine-learning/how-to-autoscale-endpoints · learn.microsoft.com/azure/ai-services/language-service/custom-text-classification/service-limits · platform.claude.com/docs/en/about-claude/pricing · platform.claude.com/docs/en/build-with-claude/claude-in-microsoft-foundry · Foundry launch post, Nov 18, 2025.
