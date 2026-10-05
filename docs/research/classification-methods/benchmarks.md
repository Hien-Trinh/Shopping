# Published accuracy benchmarks for product categorization into large hierarchical taxonomies

Our reference point (from the assignment, not a source): 198 hand-labelled Amazon Reviews '23 Listings, Shopify taxonomy 2026-08, 1,862 Categories. bge-small cosine = 23.7% exact / 51.5% top-level; embedding top-50 shortlist + LLM choice = 54.0% exact / 65.7% level-2 / 74.7% top-level; small local MLX choice model = 13.1% exact.

## 1. How does Shopify itself classify products into its taxonomy, and what accuracy does it report?

### Takeaway
Shopify uses fine-tuned Vision Language Models (image + title + description) and reports only an 85% merchant acceptance rate and "doubled" hierarchical precision/recall. It publishes no exact-leaf accuracy. A vendor case study says single automated methods reached only about 60% on the 10,000+ category taxonomy before human review.

### Cited Findings
- Timeline: 2018 TF-IDF + logistic regression; 2020 multimodal model; from early 2023, Vision Language Models tied to the structured taxonomy. Models used in turn: LLaVA 1.5 7B, LLaMA 3.2 11B, then Qwen2VL 7B (current). Served with FP8 quantization, in-flight batching (Nvidia Dynamo) and KV-cache optimization — [Shopify Engineering, "Evolution of product classification"](https://shopify.engineering/evolution-product-classification) (2025).
- Reported metrics: "85% acceptance rate of predicted categories" by merchants; "hierarchical precision and recall have doubled compared to our earlier neural network approach"; more than 30M predictions per day; taxonomy of "over 10,000 product categories", 1,000+ attributes, 26+ verticals. **Not comparable** to exact leaf accuracy: acceptance means merchants did not override, and the hierarchical P/R figure is relative only — [Shopify Engineering](https://shopify.engineering/evolution-product-classification) (2025).
- Two-step VLM design: step 1 takes image + title + description and predicts category plus metadata (a simplified product description, an image description); a later step extracts attributes — [ZenML LLMOps database summary of the Shopify talk](https://www.zenml.io/llmops-database/automated-product-classification-and-attribute-extraction-using-vision-llms) (secondary source).
- 2021 model: multi-task, multi-class classifier with more than 250M parameters (Multilingual BERT for text, MobileNet-V2 for images) on the Google Product Taxonomy: "over 5,500 categories", 7 levels, 21 Level-1 classes, 500+ Level-3 classes. Reported only as relative gains: "increased our leaf precision by 8% while doubling our coverage". No absolute numbers — [Shopify Engineering, "Using rich image and text data to categorize products at scale"](https://shopify.engineering/using-rich-image-text-data-categorize-products) (Sep 8, 2021).
- Taxonomy maintenance is now AI-agent assisted (multi-stage analysis, "specialized AI judges"). It is described as "10,000+ categories and 2,000+ attributes". No classification accuracy is reported — [Shopify Engineering, "Product taxonomy at scale"](https://shopify.engineering/product-taxonomy-at-scale) (Oct 9, 2025).
- Toloka case study on the Shopify catalogue: over 10,000 categories, "up to seven levels of depth", 22,000+ active collections. Each automated method alone "hovered around 60% accuracy". The 95% target needed an ensemble plus human review of high-disagreement items. Hard cases needed images, e.g. infant vs toddler apparel. The methods behind the "60%" are not specified — [Toloka blog](https://toloka.ai/blog/building-shopify-s-product-catalogue-at-ai-speed/) (2026). The same case is at [casestudies.com](https://www.casestudies.com/company/toloka/case-study/shopify-achieves-95-taxonomy-accuracy-with-toloka).
- Search summaries of that work say VLM performance "at certain taxonomy depths wasn't consistent enough for high-accuracy targets", so fine-grained items were routed to annotators — [Toloka blog](https://toloka.ai/blog/building-shopify-s-product-catalogue-at-ai-speed/).

### Inferences
- The closest public figure to our setting is about 60% for a single automated method on the full Shopify taxonomy (Toloka, metric unspecified). Our 54% exact (text only, zero-shot, 1,862 Categories) is in the same range.
- Shopify's production system has advantages we lack: images, fine-tuning on millions of merchant-labelled products, and a lenient metric (acceptance). The 85% should not be read as an exact-leaf target.
- Shopify's own experience (images are needed for age/size distinctions) suggests some of our deep-level errors are not fixable from text alone.

### Gaps
- No Shopify publication gives exact-leaf, per-level or top-level accuracy for its VLM classifier. I found no peer-reviewed Shopify paper with these numbers.
- Shopify says "10,000+ categories" while our 2026-08 version has 1,862 Categories. The larger figure probably counts all nodes or includes attribute-value expansions; I did not verify this in the taxonomy repo.
- I found no public details on how Shopify Magic's category auto-suggest differs from the backend classifier.

## 2. Public datasets/benchmarks (Shopify / Google Product Taxonomy / Rakuten / Amazon / WDC / Icecat) and SOTA

### Takeaway
There is now an official Shopify-labelled benchmark on Hugging Face (Shopify/product-catalogue, about 48k products), but it has no published leaderboard. Classic benchmarks (Rakuten 2018, WDC-222, Icecat) report weighted F1, which is dominated by frequent classes. They have fewer classes or use supervised training, so their 85–98% figures overstate what zero-shot exact-leaf accuracy on 1,800+ classes can reach.

### Cited Findings
- **Shopify/product-catalogue** (Hugging Face, Apache-2.0): 48.3k rows (train 38.6k, test 9.66k). Fields: product_title, product_description, product_image, potential_product_categories (a candidate list), ground_truth_category (full Shopify path, e.g. "Home & Garden > Decor > Piggy Banks & Money Jars > Piggy Banks"), ground_truth_brand, ground_truth_is_secondhand. The card positions it as a benchmark for VLM product classification. No baseline scores are on the card; the number of distinct categories and how labels were made are not stated — [HF dataset card](https://huggingface.co/datasets/Shopify/product-catalogue) (date not given).
- **Shopify/product-taxonomy** GitHub repo: the open-source taxonomy (categories, attributes, values) with distribution files. It contains no labelled product benchmark — [GitHub](https://github.com/Shopify/product-taxonomy); [2025-09 release](https://shopify.github.io/product-taxonomy/releases/2025-09/).
- **Rakuten Data Challenge (SIGIR eCom 2018)**: 1M product titles with full category paths (0.8M train, 0.2M test); metric is weighted precision/recall/F1 on the full path. 26 teams, 28 systems; best **0.8513 weighted F1** (Duetto, "Balanced Pooling Views"); a KNN/BM25 entry scored 0.7809. Supervised and title-only. Weighted F1 ≠ exact accuracy, but the two are close for single-label tasks — [Rakuten Institute of Technology](https://rit.rakuten.com/?p=333); [Duetto press release](https://www.duettocloud.com/press-releases/duettos-michael-skinner-wins-2018-rakuten-data-challenge-acm-sigir-2018); [CEUR proceedings Vol-2319](https://ceur-ws.org/Vol-2319/ecom18DC_paper_13.pdf) (2018).
- **Icecat and WDC-222** (arXiv 2408.05874, "LLM-Based Robust Product Classification in Commerce and Compliance", v. Oct 15, 2024):
  - Icecat: 370 leaf classes, 3 levels, 489,902 training items, 5,000-item test sample.
  - WDC-222: 222 leaves, test-only gold set of 2,984 items; the top level is always "Computers & Electronics" with 17 second-level classes.
  - Clean-data results (flat), macro F1 / weighted F1:

    | Model | Icecat | WDC-222 |
    |---|---|---|
    | GPT-4 few-shot | 92.8 / 98.6 | 76.9 / 94.4 |
    | GPT-3.5 few-shot | 87.0 / 97.0 | 75.1 / 92.5 |
    | Llama-2-70B few-shot | 88.3 / 95.9 | 69.4 / 85.6 |
    | DeBERTaV3 supervised | 88.3 / 97.8 | 35.1 / 72.9 |

  - Supervised models collapse when moved to another distribution (trained on Icecat, tested on WDC); in-context LLMs do not.
  - Source: [arXiv HTML](https://arxiv.org/html/2408.05874). Caveat: one narrow vertical (electronics), small label space, few-shot examples supplied.
- An LLM/RAG approach on WDC-222 reached **hierarchical F1 0.921** vs about 0.85 for earlier deep-learning baselines. It did not beat proprietary GPT models. Hierarchical F1 gives partial credit for correct ancestors and is **not comparable** to exact leaf accuracy — [NOVA master's thesis "Enhancing Product Categorization with LLMs"](https://run.unl.pt/bitstream/10362/181471/1/Master_Thesis_FALL25_58411.pdf) (2025; thesis, not peer-reviewed).
- **Amazon RetailProducts2023**: 95,526 products, 2,214 categories, at least 10 items per class. Weighted F1: fastText 0.836, BERT 0.891, XLM-R 0.899, fine-tuned Domain Expert 0.921, **Dual-Expert (fine-tuned top-10 shortlist + Mixtral chooses) 0.968**. Macro F1: 0.716 / 0.779 / 0.782 / 0.825 / 0.925. On three internal locales (10k human-reviewed eval items each), the Dual-Expert beat the DHPC SOTA baseline by +3.1 to +4.0 points of accuracy (relative figures only). A fine-tuned binary selector in place of the LLM did not help — [Cheng et al., Amazon, CustomNLP4U @ EMNLP 2024](https://aclanthology.org/2024.customnlp4u-1.22/).
- **Walmart** (KDD 2019 workshop paper): about 25M crowd labels, about 6,000 leaf product types, supervised. Top-1 accuracy: hierarchical model **70%**; flat Multi-CNN + structured attributes **92.15%**; best flat Multi-LSTM variant **92.28%**. Top-3 accuracy is about 97.5% — [Krishnan & Amarthaluri, arXiv 1903.04254](https://arxiv.org/pdf/1903.04254) (2019).
- **TaxoGlimpse** (VLDB 2024) benchmarks LLMs on taxonomy *structure* questions (e.g. "is X a kind of Y"), not product classification. Shopping taxonomies (including Google Product Category) get about 80% QA accuracy from most LLMs; accuracy drops by up to 30% from root to leaf levels and from common to specialized domains. **Not a product-classification metric** — [Sun et al., VLDB 2024](https://vldb.org/pvldb/vol17/p2919-sun.pdf); [arXiv 2406.11131](https://arxiv.org/pdf/2406.11131).
- MAVE is an attribute-value extraction dataset, not a categorization benchmark. I found no categorization SOTA for it.

### Inferences
- Shopify/product-catalogue is the most directly comparable public test set: same taxonomy, title + description available. Running our pipeline on its test split would give a far more reliable number than 198 items (the ±7 pt 95% CI on n=198 is wide).
- The "potential_product_categories" field suggests Shopify itself frames the task as choosing from a shortlist, like our embedding top-50 → choose design.
- Published 90%+ numbers come from (a) supervised training on hundreds of thousands to millions of in-domain labels, (b) ≤2,214 classes or one vertical, and (c) weighted F1 dominated by head classes. None applies to our zero-shot, 1,862-class, cross-vertical, uniform-sample setting.

### Gaps
- No public leaderboard exists for Shopify/product-catalogue. I found no paper reporting exact accuracy on it.
- No reliable published exact-leaf accuracy for zero-shot LLMs on the full Google Product Taxonomy (about 5,500 classes).
- I could not access the 2025 ScienceDirect zero-shot GPT/Claude product-classification study (HTTP 403). A search snippet says it covered 248 Amazon categories with GPT-4o, GPT-4o mini, Claude 3.5 Sonnet and Haiku, reporting mean absolute error. Its accuracies are unverified — [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S2949719125000184).

## 3. Typical leaf-level vs top-level accuracies: zero-shot vs supervised, 1,000+ classes

### Takeaway
With 1,000+ classes, supervised in-domain models reach about 85–92% top-1 or weighted F1. Retrieve-then-LLM-choose pipelines add a few points on top of a fine-tuned retriever. Pure zero-shot leaf accuracy is much lower, roughly 55–65% in the few comparable reports. The shortlist step is critical: leaving it out or using an LLM-only traversal cost about 32 points in one study.

### Cited Findings
- Supervised, about 6,000 leaves: 92% top-1 (flat) vs 70% (hierarchical, top-down) — [Walmart, arXiv 1903.04254](https://arxiv.org/pdf/1903.04254) (2019).
- Supervised + LLM selector, 2,214 classes: weighted F1 0.968 with a fine-tuned top-10 candidate generator; the LLM corrected training-label noise, e.g. snowball clippers mislabelled as beach toys — [Amazon Dual-Expert, 2024](https://aclanthology.org/2024.customnlp4u-1.22/).
- In the same paper, describing categories with longer generated descriptions instead of short names improved accuracy by +3.8 / +4.0 / +3.1 points across three locales. Feeding only the short category name gave "relatively low" accuracy — [Amazon Dual-Expert, 2024](https://aclanthology.org/2024.customnlp4u-1.22/).
- Zero-shot, thousands of dynamic labels (scientific documents, SSRN; not e-commerce):
  - Bi-encoder retrieval of candidates followed by an LLM choosing among them ("LLM-SelectP") scored **0.943** accuracy vs **0.615** for the supervised SPECTER2 SOTA.
  - Skipping the label-reduction step cost about 32 points.
  - Having the LLM traverse the tree alone ("Trav-Select") was worse than all proposed methods *and* the old SOTA.
  - Removing label descriptions cost about 9 points.
  - The metric is SME-judged acceptability (multi-label), **not exact match** to a single gold label.
  - Source: [Tabatabaei et al., COLING 2025 Industry](https://aclanthology.org/2025.coling-industry.14.pdf).
- Shopify (2021, Google Product Taxonomy): 21 classes at Level 1 vs 500+ at Level 3, which illustrates how class counts grow with depth. No per-level accuracy was published — [Shopify Engineering 2021](https://shopify.engineering/using-rich-image-text-data-categorize-products).
- In LLM taxonomy QA, accuracy falls by up to 30% from root to leaf levels — [TaxoGlimpse, VLDB 2024](https://vldb.org/pvldb/vol17/p2919-sun.pdf).
- Single automated methods on the 10k-category Shopify taxonomy reached about 60% — [Toloka 2026](https://toloka.ai/blog/building-shopify-s-product-catalogue-at-ai-speed/).

### Inferences
- The step from our raw embedding (23.7% exact) to embedding shortlist + LLM choice (54.0%) matches the literature: the retrieve-then-choose pattern beats either part alone.
- The main remaining levers the literature points to:
  - a better or fine-tuned retriever, since shortlist recall caps accuracy;
  - richer category descriptions instead of bare paths (+3–9 pts reported);
  - a stronger chooser LLM;
  - images, if available.
- Our top-level accuracy (74.7%) falling to 65.7% at level 2 and 54.0% exact follows the usual depth decay. A 75% top level is low compared with supervised systems (top level is usually 90%+), which suggests the shortlist sometimes misses the right branch entirely. Measuring recall@50 of the shortlist would confirm or rule this out.
- A realistic zero-shot, text-only target on a cross-vertical 1,800-class taxonomy is about 60–70% exact. Reaching 85–90%+ appears to need supervised fine-tuning on in-domain labels, as at Amazon, Walmart and Shopify.

### Gaps
- No paper found reports exact-leaf *and* top-level accuracy for a zero-shot LLM on a 1,000+ class product taxonomy in the same table. The 60–70% estimate is an inference, not a published figure.
- No published recall@K for embedding shortlists over product taxonomies.

## 4. How much do label noise and inter-annotator agreement cap achievable accuracy?

### Takeaway
Product-category labels are noisy: seller-chosen labels, crowd "close-enough" choices and genuinely multi-functional products. Hard numbers for inter-annotator agreement on product taxonomies are scarce. In an analogous large-taxonomy task, human accuracy was 65–90%, and Shopify-scale projects still need humans to reach 95%.

### Cited Findings
- Walmart: crowd workers "do not have an intimate knowledge" of the taxonomy and are "likely to pick a close-enough label" when the right one is not suggested, which creates systematic label bias. Hierarchical models are especially sensitive to it — [arXiv 1903.04254](https://arxiv.org/pdf/1903.04254) (2019).
- Amazon: seller-chosen categories "can be noisy due to the vast number of labels and different interpretation of the categories". Sources include outdated categorization, biased internal corrections and wrong seller suggestions. Multi-functional products have several valid categories but carry a single label. Their eval sets needed "multiple iterations of human review" — [Amazon Dual-Expert, 2024](https://aclanthology.org/2024.customnlp4u-1.22/).
- SSRN large-taxonomy document classification: human (time-constrained) classification accuracy measured against a senior expert "varies between 65% and 90%". The authors judged single-gold exact match unusable and moved to SME rating — [COLING 2025 Industry](https://aclanthology.org/2025.coling-industry.14.pdf) (not e-commerce).
- Shopify/Toloka: reaching 95% needed an ensemble plus human review of disagreements. Up to 10% variance was tolerated as acceptable outliers — [Toloka 2026](https://toloka.ai/blog/building-shopify-s-product-catalogue-at-ai-speed/).
- Real taxonomies "often" have multiple valid labels per product (10,000+ categories, up to 8 levels) — [Superlinked taxonomy classification example](https://superlinked.com/docs/examples/taxonomy-classification) (vendor docs).
- Inter-rater agreement can be used to estimate the noise distribution and to learn under it (generic method, CVPR 2023) — [CVPR 2023 poster](https://cvpr.thecvf.com/virtual/2023/poster/21123).

### Inferences
- With one labeller and no adjudication, our 198 hand labels probably have a several-percent disagreement rate against a second labeller, mostly at the leaf level (sibling categories, "Other" buckets, multi-purpose products). Exact-match accuracy above about 85–90% may not be measurable on this set.
- A cheap check: double-label 50 items, and score leaf-or-sibling or "acceptable" alongside exact match, as SSRN and Shopify (acceptance rate) do.

### Gaps
- I found no published inter-annotator agreement (kappa or percent agreement) for Shopify, Google or Amazon product taxonomies specifically.
- I found no published estimate of label-error rates in Amazon Reviews '23 metadata categories (which use Amazon's own taxonomy, not Shopify's).
