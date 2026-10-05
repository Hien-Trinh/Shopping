# LLM methods for large hierarchical product taxonomy classification

Research date: 2026-10-04. Our baseline for comparison: embedding shortlist of 50 Category paths, then a hosted choice API picks one, gives 54.0% exact leaf / 74.7% top level on 198 Listings. Shortlist of 10 gives 48.5%. Local small model, hierarchical walk: 13.1%. bge-small alone: 23.7%.

Caveat for the whole note: almost no published paper uses the Shopify Standard Product Taxonomy with ~1.8k leaves and title + 200 characters of text. The numbers below come from different taxonomies, label sets, metrics (accuracy, weighted F1, hierarchical F1, SME acceptance) and input richness. Treat cross-paper numbers as direction, not as targets.

## 1. Retrieve-then-rerank / shortlist-then-LLM-choose: shortlist sizes, prompts, gains vs embeddings alone

### Takeaway
This is the dominant published and production pattern (Amazon, Elsevier/SSRN, Instacart, IReRa), and it beats the retriever alone by large margins (+20 to +33 points in the papers that report both). Shortlist sizes in use are K=10 (Amazon, with a *fine-tuned* retriever) to K=40 (Elsevier, with an off-the-shelf bi-encoder). The two levers with measured gains are (a) better candidate recall (fine-tuned retriever) and (b) richer Category descriptions in the prompt (LLM-generated definitions), not chain-of-thought.

### Cited Findings
- Amazon "Dual-Expert" (CustomNLP4U @ EMNLP 2024): a fine-tuned XLM-R "domain expert" returns top K=10 leaf Categories, then an off-the-shelf LLM (Mixtral, temperature 0) picks one. On RetailProducts2023 (95,526 products, 2,214 Categories), weighted F1: fastText 0.836, BERT 0.891, XLM-R 0.899, domain expert alone 0.921, Dual-Expert 0.968; macro F1 0.825 -> 0.925 (the long tail gains most). — [Amazon, ACL Anthology](https://aclanthology.org/2024.customnlp4u-1.22.pdf)
- Same paper, 3 locales with 10K human-reviewed eval items each: Dual-Expert is +3.81%, +4.01%, +3.14% over the DHPC baseline, vs +1.01/+1.33/+1.57% for the domain expert alone. Replacing the LLM with a fine-tuned XLM-R binary (product, Category) selector gave no gain over the domain expert alone ("likely learned the same noise"). — [Amazon, ACL Anthology](https://aclanthology.org/2024.customnlp4u-1.22.pdf)
- Same paper, prompt ablation (accuracy delta vs DHPC, 3 locales): short/ambiguous Category names +0.85/+0.53/−0.68%; descriptive full path names +1.23/+0.80/+2.06%; descriptive names + LLM self-generated Category definitions (summaries of labelled items per Category) +3.81/+4.01/+3.14%; "rank all candidates" +3.85/+3.55/+2.26%; CoT + rank +3.32/+3.59/+2.86%. i.e. Category definitions were the biggest single gain; CoT did not help beyond that. — [Amazon, ACL Anthology](https://aclanthology.org/2024.customnlp4u-1.22.pdf)
- Same paper: thresholding the domain expert's confidence routes only ~20% of traffic to the LLM "while maintaining overall accuracy improvements". LLM gains are largest when categories have few training examples; with plentiful, well-separated categories the discriminative model is on par. — [Amazon, ACL Anthology](https://aclanthology.org/2024.customnlp4u-1.22.pdf)
- Elsevier/SSRN (COLING 2025 industry): thousands of dynamic labels; all-mpnet-base-v2 bi-encoder retrieves; they explored top-k 10 to 100 and chose top 40 leaves with full root paths as the pruned taxonomy. SME-judged accuracy: previous SOTA (SPECTER2) 61.5%, LLM-only hierarchical traversal ("Trav-Select") 50.0%, bi-encoder + LLM listwise rerank 70.0%, bi-encoder + LLM pointwise select with label descriptions and parent context ("LLM-SelectP") 94.3%. Removing label descriptions: 85.7%; removing parent-path context: 85.7%. Cost fell from $3.50/document (human) to ~$0.20. Note: multi-label, scientific documents, SME judgment not a gold set. — [Tabatabaei et al., COLING 2025](https://aclanthology.org/2025.coling-industry.14.pdf)
- Infer-Retrieve-Rank (IReRa, 2024): LLM infers query terms, a frozen retriever maps them to labels, a second LLM (GPT-4) reranks. RP@10 vs naive retrieval: HOUSE 65.76 vs 36.76, TECH 70.23 vs 49.79, TECHWOLF 65.17 vs 42.13, BioDEX 27.67 vs 11.71. Needed only ~50 labelled examples to optimize prompts with DSPy. Labels here are ~thousands to ~tens of thousands (ESCO skills, adverse reactions). — [D'Oosterlinck et al., arXiv 2401.12178](https://ar5iv.labs.arxiv.org/html/2401.12178)
- Instacart (Nov 2025): query-to-taxonomy classification = top-K candidates from conversion history -> LLM rerank with injected context -> embedding-similarity guardrail. Production model is a LoRA fine-tuned Llama-3-8B distilled from an offline teacher LLM pipeline; latency ~700 ms -> 300 ms target; ~98% of traffic served from cache. No accuracy numbers given for the category step specifically. — [Instacart](https://company.instacart.com/tech-innovation/building-the-intent-engine-how-instacart-is-revamping-query-understanding-with-llms)

### Inferences
- Our 50 vs 10 result (54.0% vs 48.5%) is consistent with the literature: with an off-the-shelf small embedder, recall@10 is the binding constraint, so a larger K helps; Amazon can use K=10 only because its retriever is fine-tuned. The first thing to measure is recall@K of the gold leaf in our shortlist (at 10/50/100). If recall@50 is, say, ~70%, then the chooser is already near ceiling and gains must come from retrieval.
- The two highest-evidence cheap wins for us: (1) put the full Category path plus a short LLM-generated definition (from Shopify taxonomy attributes or from summarizing labelled Listings) in the shortlist; (2) improve the retriever (better embedder, or embed "path + definition" rather than path alone). Both showed +3 to +9 points in the papers above.
- A confidence gate (only send uncertain items to the expensive chooser) is a proven cost lever (Amazon: 80% traffic cut).

### Gaps
- No paper found that sweeps K finely (10/20/50/100) for a single-label product task and reports chooser accuracy per K. Elsevier explored 10–100 but only reports the chosen 40.
- None of the papers report recall@K of the retriever alongside final accuracy for product data, so the ceiling cannot be separated from chooser error.

## 2. Hierarchical (level-by-level) LLM classification vs flat choice over a shortlist

### Takeaway
Flat choice over a retrieved shortlist consistently wins over LLM level-by-level walks in the published comparisons; the stated reason is error propagation (a wrong top-level choice cannot be recovered). Hierarchy helps when used as *context* (showing full paths / parent nodes to a flat chooser) or as a *consistency constraint* (masking), not as a sequence of hard decisions.

### Cited Findings
- Elsevier/SSRN: LLM-only BFS traversal of the taxonomy (Trav-Select) scored 50.0% SME accuracy, below the old SOTA (61.5%) and far below retrieve-then-select (94.3%); "the importance of effective initial label selection, particularly for large taxonomies". — [Tabatabaei et al., COLING 2025](https://aclanthology.org/2025.coling-industry.14.pdf)
- Gholamian et al. (EMNLP 2024 CustomNLP4U, "LLM-Based Robust Product Classification in Commerce and Compliance"), as tabulated in a Nova SBE thesis (Jan 2025): on clean data, Llama-2-70b-chat flat 0.50 vs hierarchical 0.65 vs few-shot 0.97; GPT-3.5 flat 0.90 / hierarchical 0.88 / few-shot 0.98; GPT-4 flat 0.94 / hierarchical 0.89 / few-shot 0.99 (first metric column); DeBERTaV3 supervised flat 0.98, hierarchical 0.97. The thesis summarizes: hierarchical configurations "showed limitations due to error propagation", flat + few-shot "consistently delivered the best results". Dataset appears small in label count (near-ceiling scores), so not directly comparable to 1.8k leaves. — [Luedecke, Nova SBE thesis 2025](https://run.unl.pt/bitstream/10362/181471/1/Master_Thesis_FALL25_58411.pdf); original [arXiv 2408.05874](https://arxiv.org/abs/2408.05874)
- Classic large-scale result (NeurIPS 2013): flat vs hierarchical trade-off in large taxonomies; top-down errors at upper levels propagate. — [Babbar et al., NeurIPS 2013](http://papers.neurips.cc/paper/5082-on-flat-versus-hierarchical-classification-in-large-scale-taxonomies.pdf)
- Cross-platform multimodal categorization (2025, 271,700 products, Google taxonomy, non-LLM): uses hierarchical prediction with *dynamic masking* (constrain finer levels to children of the predicted parent) to guarantee valid paths; best hF1 98.59% (CLIP late fusion). — [arXiv 2508.20013](https://arxiv.org/html/2508.20013v1)
- Shopify production (May 2025): Vision LLM (LLaVA 1.5 7B -> Llama 3.2 11B -> Qwen2VL 7B) over a taxonomy of 10,000+ Categories; evaluated on precision/recall at multiple hierarchy levels; 85% merchant acceptance of predicted categories; "hierarchical precision and recall have doubled" vs the earlier neural model. The posts do not say whether decoding is level-by-level or full path in one go. — [Shopify Engineering, May 2025](https://shopify.engineering/evolution-product-classification)

### Inferences
- Our 13.1% for a small local model doing a hierarchical walk fits the literature: small models + level-by-level decisions compound errors (if top-level is ~75% and each of ~4 further levels is ~80%, exact leaf is ~30% even for a decent model; a weak model collapses much faster).
- A good use of hierarchy for us: generate the shortlist flat, but present full paths (already done) and possibly a two-stage "pick top-level from shortlist, then pick leaf among that branch's shortlist members" only as an ablation. Don't expect walking the tree to beat flat choice.

### Gaps
- No LLM paper found that compares flat-over-shortlist vs level-by-level on a 1k+ leaf *product* taxonomy with identical models; the evidence is from scientific documents (Elsevier) and small-label product data (Gholamian).

## 3. Few-shot with retrieved labelled examples (in-context kNN) vs zero-shot with Category names

### Takeaway
Few-shot examples help a lot for weaker/open models and modestly for strong ones; for large label spaces, Amazon found per-Category *definitions* (LLM summaries of labelled examples) beat raw example products. Retrieved nearest-neighbour labelled Listings also double as a strong candidate generator (their labels can be merged into the shortlist).

### Cited Findings
- Gholamian et al. via Nova thesis: few-shot vs zero-shot flat: Llama-2-70b 0.97 vs 0.50; GPT-3.5 0.98 vs 0.90; GPT-4 0.99 vs 0.94. — [Luedecke, Nova SBE thesis 2025](https://run.unl.pt/bitstream/10362/181471/1/Master_Thesis_FALL25_58411.pdf)
- Amazon Dual-Expert: chose LLM-summarized Category definitions *instead of* few-shot example products, arguing that example products contain irrelevant information and poorly represent the Category; definitions gave +2.6 to +3.5 points over descriptive names alone (Table 4). — [Amazon, ACL Anthology](https://aclanthology.org/2024.customnlp4u-1.22.pdf)
- IReRa: ~50 labelled examples plus automated prompt optimization (DSPy) was enough for +16 to +29 RP@10 over retrieval. — [arXiv 2401.12178](https://ar5iv.labs.arxiv.org/html/2401.12178)
- Nova thesis own results (Icecat, WDC-222): best LLM approach (prompting/RAG) hF 0.921 vs traditional DL benchmark 0.85 hF; fine-tuned model second (wF 0.804, hF 0.874); knowledge-graph prompting hF 0.827; LLM entity matching (embedding top-k blocking + LLM match against labelled products, i.e. kNN-style) hF 0.839. — [Luedecke, Nova SBE thesis 2025](https://run.unl.pt/bitstream/10362/181471/1/Master_Thesis_FALL25_58411.pdf)

### Inferences
- For us, with only 198 labelled Listings, in-context kNN examples are not yet feasible at scale; but once a labelled/pseudo-labelled pool exists (e.g. from the 1M bulk load classified by the hosted chooser and spot-checked), kNN examples are a cheap add. More immediately feasible: generate a 1–2 sentence definition per Category (1,862 one-off LLM calls) and include it for shortlisted Categories.
- Cost note: each few-shot example adds ~50–100 tokens; at $0.042/M input this is negligible for the current hosted chooser.

### Gaps
- No product-categorization paper found that measures in-context kNN examples vs zero-shot on a 1k+ leaf taxonomy with a modern small model.

## 4. Frontier hosted models vs small local models: accuracy, cost per 1M items, throughput, batch APIs

### Takeaway
Hosted frontier "mini/flash/haiku" models are cheap enough that cost is rarely the blocker for 1M items, but none is close to our current $0.042/M chooser on price. Small local models work in production only when *fine-tuned/distilled* (Shopify Qwen2VL 7B, Instacart Llama-3-8B LoRA); zero-shot small models on large taxonomies perform poorly (consistent with our 13.1%).

### Cited Findings
- Zero-shot product classification study (2025): GPT-4o, GPT-4o-mini, Claude 3.5 Sonnet, Claude 3.5 Haiku; accuracy range 63.71%–85.81%; GPT-4o 76.77% acc (F1 0.7608), GPT-4o-mini 74.19% (F1 0.7368) in one configuration. Reported as 248 product categories but also "random chance 3.21%" (≈1/31), so the effective label set per decision may be ~31; full text was not accessible (403). — [ScienceDirect 2025](https://www.sciencedirect.com/science/article/pii/S2949719125000184) (figures via search snippet; unverified)
- Gholamian et al.: open Llama-2-70b zero-shot flat 0.50 vs GPT-4 0.94, gap closes with few-shot (0.97 vs 0.99). — [Nova thesis table](https://run.unl.pt/bitstream/10362/181471/1/Master_Thesis_FALL25_58411.pdf)
- Shopify: production classifier is a fine-tuned open 7B VLM (Qwen2VL 7B, FP8, in-flight batching), 30M+ predictions/day (May 2025); "40 million LLM calls daily", 16B tokens/day; selective-field training cut latency from 2 s to 500 ms and GPU use by 40% (July 2025). Training labels come from multiple LLM annotator agents + an LLM arbitrator, with human annotators for test sets. — [Shopify, May 2025](https://shopify.engineering/evolution-product-classification); [Shopify, July 2025](https://shopify.engineering/leveraging-multimodal-llms)
- Instacart: LoRA Llama-3-8B, 300 ms target latency on H100, 98% cache hit. — [Instacart, Nov 2025](https://company.instacart.com/tech-innovation/building-the-intent-engine-how-instacart-is-revamping-query-understanding-with-llms)
- Pricing (official pages, fetched 2026-10-04), per 1M tokens input / output:
  - Anthropic: Claude Haiku 4.5 $1 / $5 (batch $0.50 / $2.50; cache hit $0.10); Sonnet 5 and 5.5 $2 / $10 (batch $1 / $5); Sonnet 4.6 $3 / $15. Batch API = 50% off; Claude 4.7+ tokenizer yields ~30% more tokens for same text. — [Anthropic pricing](https://platform.claude.com/docs/en/about-claude/pricing)
  - OpenAI: gpt-5-nano $0.05 / $0.40 (batch $0.025 / $0.20); gpt-5-mini $0.25 / $2.00 (batch $0.125 / $1.00); gpt-5.4-nano $0.20 / $1.25; gpt-5.4-mini $0.75 / $4.50; gpt-4.1-nano $0.10 / $0.40; gpt-4.1-mini $0.40 / $1.60; gpt-4o-mini $0.15 / $0.60 (batch $0.075 / $0.30). — [OpenAI pricing](https://developers.openai.com/api/docs/pricing)
  - Google (page dated 2026-10-01): Gemini 2.5 Flash-Lite $0.10 / $0.40 (batch $0.05 / $0.20); 3.1 Flash-Lite $0.25 / $1.50 (batch $0.125 / $0.75); 3.5 Flash-Lite $0.30 / $2.50; 2.5 Flash $0.30 / $2.50; 3.6–3.8 Flash $0.75 / $3.75 promotional through 2026-12-31, doubling 2027-01-01. — [Gemini pricing](https://ai.google.dev/gemini-api/docs/pricing)
  - Aggregator pages give different figures for some of these (e.g. one lists GPT-5 mini output at $4.50, which is actually gpt-5.4-mini's price); prefer the official pages above. — [aimagicx aggregator](https://www.aimagicx.com/blog/llm-api-pricing-comparison-2026)

### Inferences
- Rough cost per 1M Listings (my calculation; assumes ~1,000 input tokens per call for title + 200 chars + 50 Category paths + instructions, and ~20 output tokens; shortlist differs per item so little is cacheable; reasoning models may add hidden output tokens):
  - Current chooser: 1B input tokens × $0.042/M ≈ $42.
  - gpt-5-nano ≈ $58 standard / $29 batch; gpt-4.1-nano ≈ $108; Gemini 2.5 Flash-Lite ≈ $108 / $54 batch; gpt-4o-mini ≈ $162 / $81 batch; gpt-5-mini ≈ $290 / $145 batch (more if reasoning tokens); Gemini 3.1 Flash-Lite ≈ $280; Claude Haiku 4.5 ≈ $1,100 / $550 batch; Sonnet 5 ≈ $2,200 / $1,100 batch.
  - Adding Category definitions (~30 tokens × 50) roughly doubles input tokens and costs.
- Throughput: 1M items at 80 req/s is ~3.5 hours; batch APIs (24 h SLA) fit bulk loads; the 50 changes/s live peak fits within 80 req/s for a hosted API.
- Local on a 16 GB Mac: a 1–8B model with a 1,000-token prompt per item; plausible throughput on M-series with MLX is well below 50 items/s per process for prefill-heavy prompts (anecdotal, not measured here), and zero-shot accuracy is likely poor. Local only becomes attractive after fine-tuning (see Q6) and with a short prompt.
- Plausible candidates to beat 54% exact: (a) same pipeline with a stronger chooser (Haiku 4.5, gpt-5-mini, Gemini Flash) — at least worth a 198-item A/B (~$0.20 per run); (b) better retrieval recall + Category definitions with the current cheap chooser; (c) an LLM-as-retriever step (IReRa-style: generate a guessed path/keywords, embed that, union with direct-embedding shortlist).

### Gaps
- No published head-to-head of Haiku 4.5 / GPT-5-mini / Gemini 3.x Flash / Qwen3 / Llama 3.x 8B on the Shopify taxonomy was found.
- No measured MLX throughput figures for 1–8B classification prompts on 16 GB Apple Silicon were found in this pass.
- I have no public information on "TypeSafe Jev" accuracy beyond our own eval.

## 5. Generative approaches: constrained decoding over a taxonomy trie, path generation, LLM-generated Category descriptions

### Takeaway
Trie-constrained decoding guarantees a valid Category path and is well established for generative retrieval, but I found no product-taxonomy paper reporting that it beats retrieve-then-choose. LLM-generated Category descriptions have the strongest measured evidence in this group (Amazon +2–3.5 pts; Elsevier: removing descriptions cost 8.6 pts).

### Cited Findings
- Trie-constrained decoding: tokenize full "A > B > C" paths, build a prefix tree, mask logits to valid continuations; guarantees output is a valid label. — [Kalsi blog (practitioner, anecdotal)](https://sachinkalsi.github.io/blog/constrained-decoding-forcing-llms-to-respect-your-taxonomy/)
- Naive trie constrained decoding is slow on accelerators; vectorized trie methods address latency for generative retrieval (Feb 2026). — [arXiv 2602.22647](https://arxiv.org/html/2602.22647v1)
- Nova thesis: notes LLM outputs need exact label names; "even minor deviations, such as subtle wording differences or typos" break mapping — motivating constraints or post-hoc mapping. — [Luedecke 2025](https://run.unl.pt/bitstream/10362/181471/1/Master_Thesis_FALL25_58411.pdf)
- LLM-generated descriptions: Amazon (definitions from summarizing labelled items, +3.8/+4.0/+3.1% over baseline, best prompt); Elsevier (label descriptions + parent context: 94.3% vs 85.7% without either). — [Amazon](https://aclanthology.org/2024.customnlp4u-1.22.pdf); [Elsevier](https://aclanthology.org/2025.coling-industry.14.pdf)
- IReRa's "Infer" step (LLM generates free-text label guesses, then retrieval maps to real labels) is a generative-then-retrieve variant with large gains over plain retrieval. — [arXiv 2401.12178](https://ar5iv.labs.arxiv.org/html/2401.12178)

### Inferences
- For a small local model, constraining the output to the shortlist (enum / grammar via llama.cpp GBNF or MLX logit masks) fixes invalid outputs but not the knowledge gap; it is a hygiene measure, not an accuracy lever.
- "Generate a path, then embed it and retrieve" (HyDE-style) is a cheap way to raise shortlist recall for items whose titles are brand/model-heavy.

### Gaps
- No product categorization paper found reporting exact-leaf accuracy for full-taxonomy trie-constrained generation vs shortlist choice.
- No paper found measuring LLM-generated Category descriptions specifically for improving *embedding retrieval* (vs the chooser prompt) on product taxonomies.

## 6. Fine-tuning small LLMs (LoRA) for product categorization: data and results

### Takeaway
Fine-tuned small models are what large companies actually run (Shopify 7B VLM, Instacart 8B LoRA), typically distilled from bigger LLM teachers. Public evidence suggests fine-tuned 7B-class models match or beat zero-shot GPT-4-class models on classification, but needs thousands to tens of thousands of labelled items; with 198 labels we would have to pseudo-label with the hosted chooser first.

### Cited Findings
- Instacart: LoRA Llama-3-8B trained on data from an offline teacher LLM pipeline; serves the taxonomy rerank in production. — [Instacart, Nov 2025](https://company.instacart.com/tech-innovation/building-the-intent-engine-how-instacart-is-revamping-query-understanding-with-llms)
- Shopify: labels for training produced by multiple LLM annotator agents and an LLM arbitrator; model swaps (LLaVA 7B -> Llama 3.2 11B -> Qwen2VL 7B) each improved accuracy while cutting GPU use. Dataset size not disclosed. — [Shopify, July 2025](https://shopify.engineering/leveraging-multimodal-llms)
- Nova thesis: QLoRA fine-tuning of Pythia-410m, Llama 3.2 1B/3B, Mistral-7B-v0.3 on ~30,000 resampled observations (Icecat/WDC); fine-tuned model was second best (hF 0.874) behind prompting/RAG with a stronger model (0.921), and did not beat proprietary GPT. — [Luedecke 2025](https://run.unl.pt/bitstream/10362/181471/1/Master_Thesis_FALL25_58411.pdf)
- eCeLLM (ICML 2024): instruction-tuned Llama-2 7B/13B, Mistral-7B, Phi-2, Flan-T5 on 116,528 samples across 10 e-commerce tasks (incl. multi-class product classification); +10.7% average in-domain and +9.3% out-of-domain over best baselines. — [eCeLLM, PMLR](https://proceedings.mlr.press/v235/peng24c.html)
- General text classification: fine-tuned smaller models significantly outperform zero-shot generative models (2024). — [Bucher & Martini, arXiv 2406.08660](https://arxiv.org/html/2406.08660v1); LoRA Land: 224 of 310 LoRA fine-tuned models beat GPT-4 across 31 tasks (avg 0.756 vs 0.661). — [arXiv 2405.00732](https://arxiv.org/pdf/2405.00732)
- Amazon: the retriever (domain expert) is fine-tuned on "millions" of catalog items per locale; this is what makes K=10 viable. — [Amazon](https://aclanthology.org/2024.customnlp4u-1.22.pdf)

### Inferences
- A realistic path for us: classify the ~1M bulk load with the best hosted pipeline, sample-audit, then (a) fine-tune the *embedder* on (Listing, Category path) pairs to raise shortlist recall (cheapest, biggest proven lever per Amazon), and/or (b) LoRA a 1–3B MLX model as a chooser over a shortlist. (a) is far cheaper to run on a 16 GB Mac than (b).
- With ~1,862 classes, a few thousand pseudo-labelled examples won't cover the long tail; expect ~10–50 per Category minimum (Amazon required ≥10 per Category for its dataset).

### Gaps
- No public paper found that LoRA fine-tunes a 1–8B model on the Shopify Standard Product Taxonomy and reports exact-leaf accuracy, nor one giving a data-size vs accuracy curve for 1k+ leaf product taxonomies.
