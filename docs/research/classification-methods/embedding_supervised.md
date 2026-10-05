# Non-LLM methods for large hierarchical product taxonomy classification (embeddings, rerankers, supervised)

Scope note: researched 2026-10-04. Baseline to beat (our own eval, 198 Listings, Shopify taxonomy 2026-08, 1,862 leaves): bge-small-en-v1.5 cosine vs Category path = 23.7% exact leaf / 51.5% top level; embedding top-50 + hosted LLM = 54.0% exact. Numbers below are on product data unless marked "general benchmark". Tool budget limited this to ~17 searches/fetches; several items are gaps.

## Stronger embedding models (CPU / MLX) and product-categorization results

### Takeaway
The only public, directly comparable result (Shopify taxonomy, Shopify product data, zero-shot text-to-Category-path retrieval) shows a 1.5B embedding model reaching only ~0.43 strict hierarchical F1, and small models ~0.24–0.30: a bigger zero-shot embedding alone will not get near 54% exact leaf. General-benchmark leaders that fit a 16 GB Mac are Qwen3-Embedding-0.6B and EmbeddingGemma-300M; Marqo's e-commerce models are image+text (CLIP-style) and benchmarked on retrieval, not taxonomy classification.

### Cited Findings
- Superlinked benchmark (published 2026-10-02) on the Shopify Product Taxonomy (3 levels used: 26 L1, 213 L2, 1,790 nodes total), 2,309 products from the Hugging Face `Shopify/product-catalogue` dataset, metric = hierarchical F1 (strict = only the ground-truth category counts; lenient = any of the listed "potential" categories counts) — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
  - `NovaSearch/stella_en_1.5B_v5` (1.5B): full-path 0.425 strict / 0.553 lenient; leaf-name only 0.334 / 0.450 — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
  - `intfloat/multilingual-e5-large` (0.6B): full-path 0.301 / 0.356; leaf 0.295 / 0.385 — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
  - `all-MiniLM-L6-v2` (23M): full-path 0.239 / 0.312; leaf 0.253 / 0.344 — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
  - Image-only: CLIP-ViT-H-14 (1B) full-path 0.353 / 0.451; clip-vit-base-patch32 0.208 / 0.258 — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
  - Zero-shot NLI (L1 only): gliclass-large-v3.0 0.302 / 0.384; nli-deberta-v3-base 0.204 / 0.285 — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
  - No latency or memory figures were reported — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
- Qwen3-Embedding-0.6B: 0.6B params, 28 layers, 32K context, output dims user-selectable 32–1024 (MRL), instruction-aware; model card says instructions typically give a 1–5% boost; MTEB (general benchmark) multilingual mean 64.33, classification 66.83 — [Hugging Face model card](https://huggingface.co/Qwen/Qwen3-Embedding-0.6B); [Accenture model catalog mirror](https://sdk.airefinery.accenture.com/distiller/model_catalog/Embedding/Qwen/Qwen3-Embedding-0.6B/)
- EmbeddingGemma-300M: 300M params, 768-d with MRL truncation to 512/256/128, 2,048-token context, Gemma licence (not Apache/MIT), MTEB English v2 (general benchmark) mean-task 69.67 / mean-tasktype 65.11; prompt formats `task: classification | query: {text}` and for retrieval `task: search result | query: ...` vs document `title: {title|none} | text: ...`; QAT Q4_0/Q8_0 checkpoints; does not support float16 activations (use fp32/bf16) — [Hugging Face model card](https://huggingface.co/google/embeddinggemma-300m)
- Marqo-Ecommerce-B (768-d) and -L (652M params, 1024-d) are multimodal image+text product embedding models; Marqo claims up to 88% better than Amazon Titan Multimodal and up to 31% better than ViT-SO400M-14-SigLIP on its own e-commerce retrieval benchmarks (marqo-ecommerce-easy ~200k products, -hard ~4M listings); e.g. text-to-image MRR +43.7% vs Titan. These are retrieval benchmarks, not taxonomy classification — [Marqo blog via search snippet](https://marqo.ai/blog/introducing-marqos-ecommerce-embedding-models); [HF collection](https://huggingface.co/collections/Marqo/marqo-ecommerce-embeddings). (A direct fetch of the blog redirected to an unrelated marketing page, so params/licence could not be re-verified.)

### Inferences
- The Superlinked numbers are hierarchical F1 (partial credit for correct ancestors), so exact-leaf accuracy is lower than 0.425; our 23.7% exact / 51.5% top level with bge-small is in the same band as their small/mid models. Moving bge-small → a 0.3–0.6B model plausibly adds single-digit to ~10 points exact, not 30.
- Speed: a 0.3–0.6B encoder is roughly 3–20x the compute of bge-small (33M); at ~9 ms/Listing today, expect tens of ms per Listing on CPU unbatched, which threatens the 100 Listings/s/process bulk target unless batched or run via MLX/GPU. Not measured in any source found.
- Qwen3-Embedding-0.6B and EmbeddingGemma are the candidates worth a local A/B; stella 1.5B is likely too heavy for 4–8 workers on 16 GB.

### Gaps
- No public product-taxonomy results found for bge-base/large, gte, nomic-embed, mxbai-embed, jina v3, snowflake arctic-embed on Shopify or Google taxonomy.
- No measured Apple Silicon CPU/MLX throughput numbers for these models found.
- jina-embeddings-v3 licence is CC-BY-NC (from prior knowledge, not re-verified here) — confirm before use.

## Instructions, enriched Category text, full path vs leaf name

### Takeaway
Full path beats leaf name for strong models (+9 points strict hF1 for stella 1.5B) but not for tiny models; instruction prompts are claimed to give 1–5% (general). No measured, product-taxonomy-specific gain from LLM-written Category descriptions was found.

### Cited Findings
- Full path vs leaf: stella 1.5B 0.425 vs 0.334 strict hF1; multilingual-e5-large 0.301 vs 0.295; MiniLM 0.239 vs 0.253 (leaf slightly better for the tiny model) — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
- Qwen3-Embedding instructions: "typically 1–5% improvement" (general claim, not product data) — [Hugging Face model card](https://huggingface.co/Qwen/Qwen3-Embedding-0.6B)
- EmbeddingGemma defines separate query/document prompt formats and a classification prompt — [Hugging Face model card](https://huggingface.co/google/embeddinggemma-300m)
- Research on LLM-generated class definitions as embedding "semantic prototypes" exists (iterative refinement of definitions improves zero-shot classification), but the main evaluated dataset has 10 classes, not a product taxonomy — [arXiv 2604.27335 (May 2026)](https://arxiv.org/pdf/2604.27335)

### Inferences
- Since we already use path text, the cheap experiments are: add query/document instructions (EmbeddingGemma/Qwen3), and generate a one-line description + synonyms per Category with an LLM once (1,862 calls, one-off cost) and embed path+description. Expect modest gains; must measure on our 198-item eval.

### Gaps
- No product-domain measurement of synonym/description enrichment for 1,000+ class taxonomies found.

## Cross-encoder rerankers over a top-50 shortlist

### Takeaway
On the Shopify taxonomy benchmark, a reranker (mxbai-rerank-base-v2) gave no gain over the best embedding retrieval (0.425 vs 0.425 strict), and a small MS MARCO cross-encoder was much worse. Off-the-shelf rerankers are trained for query→passage relevance, not product→Category-label matching; little evidence they close the gap without fine-tuning.

### Cited Findings
- `mixedbread-ai/mxbai-rerank-base-v2` (0.5B): 0.425 strict / 0.548 lenient hF1, "marginally below" the stella full-path baseline (0.425 / 0.553) — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
- `cross-encoder/ms-marco-MiniLM-L-6-v2` (23M): 0.315 / 0.424 — [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)
- Qwen3-Reranker-0.6B: 595M params, 32K context, positioned for latency-sensitive use; no CPU latency benchmark found — [dev.co summary](https://dev.co/ai/llms/qwen3-reranker-0-6b); [OpenRouter](https://openrouter.ai/qwen/qwen3-reranker-0.6b)
- bge-reranker-v2-m3: 568M params, ~1.1 GB at FP16; one report of ~80–120 ms to rerank 100 candidates on an RTX 4060 Ti GPU; no CPU per-pair latency found — [search summary; PromptLayer ONNX CPU listing](https://www.promptlayer.com/models/bge-reranker-v2-m3-onnx-o3-cpu)

### Inferences
- 50 pairs per Listing through a ~0.5B cross-encoder on CPU likely costs hundreds of ms per Listing (GPU is ~1 ms/pair by the figure above; CPU is typically an order of magnitude slower) — incompatible with 100 Listings/s/process for bulk loads, acceptable perhaps only for the ~50 changes/s live path if spread across workers. Unverified estimate.
- A reranker becomes interesting only if fine-tuned on (Listing, correct Category) pairs, which brings it into the supervised bucket below.

### Gaps
- No public product-taxonomy results for bge-reranker, jina-reranker, or Qwen3-Reranker; no Apple Silicon latency numbers.

## kNN over labelled Listings and supervised classifiers: data needs and accuracy

### Takeaway
Supervised models on large product datasets reach far higher accuracy than zero-shot similarity: the Rakuten 2018 challenge (800k labelled titles, ~3,000 leaf categories) topped at 0.85 weighted F1, and Shopify's own production classifier was plain logistic regression on hashed text features over 5,000+ categories. The catch is data volume: these used hundreds of thousands to millions of labelled items; with ~1k labels for 335 classes a fine-tuned BERT got only 32%.

### Cited Findings
- SIGIR 2018 eCom Rakuten Data Challenge: 1M product titles (0.8M train / 0.2M test), 3,008 categories, full-path prediction, 26 teams / 28 systems, top weighted F1 0.8513 — [Overview paper](https://ir.webis.de/anthology/2018.sigirconf_workshop-2018ecom.21); [NUS paper](https://www.comp.nus.edu.sg/~skok/papers/ecomdc18.pdf)
- A hierarchical BERT/RoBERTa model on product classification reported 96% accuracy at layer 2 (RoBERTa best) — [search summary of challenge-related work](https://run.unl.pt/bitstreams/c02c1805-e5f7-4c00-a6b6-bb6b5351f04d/download) (not fully verified)
- Shopify (2020): logistic regression with Kesler's construction (one-vs-all turned into a single binary classifier), HashingTF text features (title, description, collection, tags, vendor, product type), Google Product Taxonomy with 5,000+ categories, greedy top-down hierarchical inference; trained on Shopify's catalogue ("over a billion products"); no accuracy figure published — [Shopify Engineering](https://shopify.engineering/categorizing-products-at-scale)
- Shopify later moved to fine-tuned vision LLMs with a 95% accuracy target and a fuzzy lookup to the nearest valid category needed <2% of the time — [Shopify Engineering / ICLR 2025 talk recap](https://shopify.engineering/leveraging-multimodal-llms); Toloka-assisted labelling with an ensemble of vector-based and tree-search methods over 10,000+ categories reported >95% accuracy — [Toloka case study](https://www.casestudies.com/company/toloka/case-study/shopify-achieves-95-taxonomy-accuracy-with-toloka)
- Weighted kNN with IR similarity has been proposed for large-scale product taxonomy classification — [YorkSpace thesis](https://yorkspace.library.yorku.ca/items/c75d6380-8bce-48df-ab0c-b5878af0d567/full)
- Low-data regime: BERT-base fine-tuned on 800 labelled examples of an Amazon product dataset with 335 classes got 0.320 accuracy — [PGKD, arXiv 2411.05045](https://arxiv.org/html/2411.05045v1)
- SetFit (contrastive fine-tune of a sentence-transformer + classification head) typically uses 8–16 examples per class; on an Amazon benchmark accuracy went 58.25% (20 labels) → 69.95% (130 labels) — few classes, general/few-shot setting — [HyperAI SOTA listing](https://beta.hyper.ai/en/sota/tasks/few-shot-text-classification/benchmark/few-shot-text-classification-on-amazon)
- A public labelled source exists: `Shopify/product-catalogue` on Hugging Face (updated 2025-12-12), labelled to the Shopify taxonomy (Superlinked used 2,309 cleaned items) — [Hugging Face Shopify datasets](https://huggingface.co/Shopify/datasets); [Superlinked](https://superlinked.com/docs/examples/taxonomy-classification.md)

### Inferences
- For 1,862 leaves, SetFit-style 8–16 per class means ~15k–30k labelled Listings; logistic regression / fastText to reach Rakuten-like accuracy wants ~100+ per class (~200k+), and long-tail leaves will remain sparse. kNN over labelled Listings degrades gracefully: it can mix with Category-text similarity (use Category text when no neighbour is close).
- Logistic regression on frozen embeddings (or fastText) is essentially free at inference (<1 ms on top of the embedding) and small in RAM, so it fits the 100 Listings/s target; a fine-tuned DistilBERT/small encoder is ~bge-base cost.
- Hierarchical (top-down) inference like Shopify's helps where per-leaf data is thin, and our top-level is already 51.5% zero-shot, so a supervised top-level/L2 gate plus embedding similarity within the branch is a plausible hybrid.

### Gaps
- Could not extract the fastText/CNN per-team scores from the Rakuten challenge PDFs (binary PDF fetch failed).
- No public accuracy curve vs labels-per-class for 1,000+ class product taxonomies found.

## LLM-labelled training sets (distillation / weak supervision)

### Takeaway
Distillation works: in PGKD, a BERT-base student trained with LLM (Claude 3 Sonnet) feedback beat its own teacher on a 335-class Amazon product task (44.3% vs 41.6% zero-shot teacher) and ran ~130x faster / 25x cheaper. Given our 54% LLM pipeline costs $62 per 1M Listings, labelling e.g. 100k–300k Listings with it costs ~$6–19, so the training set itself is cheap; the student's ceiling is roughly the teacher's accuracy.

### Cited Findings
- PGKD (Performance-Guided Knowledge Distillation, arXiv 2411.05045, Nov 2024): teacher Claude-3 Sonnet via Bedrock, student BERT-base, 1,000 initial labelled samples (800 train / 200 val). On AMZN Reviews product categorisation (level 3, 335 classes, 40k train pool): BERT-base 0.320 → BERT+PGKD 0.443; teacher zero-shot 0.416. On 41-class HuffPost: 0.474 → 0.519 (teacher 0.442) — [arXiv 2411.05045](https://arxiv.org/html/2411.05045v1)
- PGKD cost/speed: student processes a 64-item batch in 21.45 s on CPU ($0.0046) or 0.46 s on GPU ($0.0107) vs 60.64 s and $0.38 for Claude Sonnet; "up to 130x faster and 25x less expensive" — [arXiv 2411.05045](https://arxiv.org/html/2411.05045v1)
- Distilling step-by-step: small models trained with LLM rationales as extra supervision can outperform the larger LLM with less data (general NLP tasks, not product taxonomy) — [arXiv 2305.02301](https://arxiv.org/html/2305.02301v2)
- Active knowledge distillation reduces the number of LLM labelling calls needed for classifying large corpora — [arXiv 2511.11574](https://arxiv.org/html/2511.11574)
- LLM distillation for web content filtering into 30 categories reported a 9% accuracy improvement for the student — [Papers with Code](https://cs.paperswithcode.com/paper/web-content-filtering-through-knowledge)
- Shopify's own pipeline relies on LLM-generated labels at scale (tens of millions of predictions daily) with a golden dataset — [Shopify Engineering](https://shopify.engineering/leveraging-multimodal-llms)

### Inferences
- Most promising path to "local, free at inference, near 54%": run the current embedding+LLM pipeline once over a large unlabelled pool of Listings (cost ≈ $62 per 1M), then train (a) logistic regression / MLP on frozen bge-small or EmbeddingGemma vectors, or (b) a kNN index over the LLM-labelled Listings, optionally (c) contrastively fine-tune the embedding model on (Listing, Category path) pairs so the existing cosine pipeline improves. Then keep the LLM only for low-confidence cases.
- Expected ceiling ≈ teacher accuracy (~54% exact on our eval), possibly slightly above per PGKD; label noise from the teacher carries over. Coverage of rare leaves depends on the unlabelled pool's distribution.
- The 198-item eval is small (95% CI roughly ±7 points at 50%), so comparing approaches needs a larger held-out set, which the same LLM labelling could provide (with human spot checks).

### Gaps
- No published distillation result on the Shopify taxonomy or any 1,000+ leaf product taxonomy found.
- No measured numbers for fine-tuning bge-small/EmbeddingGemma contrastively on product→category pairs.
