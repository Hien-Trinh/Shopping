# Labelled data and taxonomy mappings for Shopify Standard Product Taxonomy classification

Research date: 2026-10-04. Context: Listings from Amazon Reviews '23 are classified into one of 1,862 Categories of Shopify taxonomy 2026-08. Per this repo's own specs (docs/specs/step-6a.md), that file is the real v2026-08 taxonomy **trimmed to depth 3**. Today's best result is 54.0% exact leaf accuracy on 198 labelled Listings (embedding shortlist of 50 plus an LLM choice).

## Q1. Does Shopify publish mappings to Google / Amazon / others? Completeness, accuracy, versioning

### Takeaway
Shopify's MIT-licensed repo ships exactly one external mapping: Shopify to Google Product Taxonomy (Google version 2021-09-21). It goes in one direction only (`from_shopify.yml`) and covers every Shopify category, but it is many-to-one: about 2.3 Shopify leaves per Google category. There is no Amazon mapping. The other mappings are Shopify-version-to-Shopify-version and contain very few rules. Shopify publishes no accuracy figure for any mapping.

### Cited Findings
- The repo is released under the MIT License. It holds `data/integrations/` for mappings ("conversion rules between Shopify's taxonomy and other taxonomies"). Versions use CalVer tied to the Shopify API schedule, at most quarterly, and the current version is 2026-08. The `dist/` files are marked deprecated as of 31 Oct 2026, and release assets ship as gzipped txt/json. — [Shopify/product-taxonomy README](https://github.com/Shopify/product-taxonomy)
- `integrations.yml` lists only `google/2021-09-21` and `shopify/2022-02, 2024-07, 2024-10, 2025-03, 2025-09, 2025-12, 2026-02, 2026-05, 2026-08`. No Amazon, Meta or other marketplace integration exists. — [integrations.yml](https://raw.githubusercontent.com/Shopify/product-taxonomy/main/data/integrations/integrations.yml)
- Mapping rule format: input category id → output list of category ids. An optional `unmapped_product_category_ids` lists categories with no equivalent. — [data/integrations](https://github.com/Shopify/product-taxonomy/tree/main/data/integrations)
- I downloaded and parsed `google/2021-09-21/mappings/from_shopify.yml` (1.4 MB). Header: `input_taxonomy: shopify/2026-11-unstable`, `output_taxonomy: google/2021-09-21`. The Google directory has only `from_shopify.yml` and no `to_shopify.yml`. — [from_shopify.yml](https://raw.githubusercontent.com/Shopify/product-taxonomy/main/data/integrations/google/2021-09-21/mappings/from_shopify.yml)
  - 14,528 rules, one per Shopify category (all depths). Every rule has exactly one Google output.
  - 11,881 Shopify leaf categories map to 5,065 distinct Google ids, a mean of 2.35 Shopify leaves per Google id. The largest fan-in is 50 Shopify leaves onto Google id 1795, then 32 onto 187 and 30 onto 2562. 3,178 Google ids receive exactly one Shopify leaf.
  - Deep Shopify nodes often fall back to an ancestor Google id. For example, `aa-1-1`, `aa-1-1-1` and `aa-1-1-1-1` all map to Google `5322`.
  - (My own analysis of the file; no Shopify documentation describes how it was built.)
- The Shopify version-to-version mappings are tiny. `shopify/2024-07/mappings/to_shopify.yml` is 487 bytes and has `rules: []` plus a commented example (the Beeswax category moved under Candle Making Materials). `shopify/2025-09` is 167 bytes. — [GitHub contents API, shopify/2024-07 mapping](https://raw.githubusercontent.com/Shopify/product-taxonomy/main/data/integrations/shopify/2024-07/mappings/to_shopify.yml)
- Shopify help: "If you already have a Google Product Category for your products … Shopify maps the accurate product category to your products". A CSV import accepts either the category ID or the breadcrumb. — [Shopify Help: product taxonomy](https://help.shopify.com/en/manual/products/details/product-category)
- Community reports say the two taxonomies have diverged. Example: Google has "Shirts & Tops" where Shopify has "Shirts". — [Shopify Community](https://community.shopify.com/t/google-vs-shopify-product-codes-not-matching/350525)

### Inferences
- Google → Shopify means inverting `from_shopify.yml`. Where one Shopify leaf maps to a Google id (3,178 of 5,065 ids at full depth), the inversion is exact. Otherwise it yields a candidate set. With the taxonomy trimmed to depth 3 (1,862 Categories), fan-in should shrink a lot, because many sibling leaves collapse to the same depth-3 ancestor. Compute this on the trimmed file before relying on it.
- The mapping was clearly produced by hand or rules, not learned, so per-rule accuracy should be high. But it targets Google taxonomy 2021-09-21; Google has since added and renamed some ids. Unknown Google ids need a fallback.
- Version drift between Shopify releases is negligible for a depth-3 trim (rule sets are empty or near-empty).

### Gaps
- Shopify publishes no accuracy or coverage statistics, and no methodology for the Google mapping.
- I did not compute fan-in at depth 3; it needs the trimmed file from this repo.
- Whether the Google mapping will be updated to a newer Google taxonomy release is unknown.

## Q2. Amazon browse nodes / Amazon Reviews '23 categories → Google/Shopify: published mapping? Viability and legitimacy

### Takeaway
I found no public, licensed mapping from Amazon browse nodes or Amazon Reviews '23 `categories` breadcrumbs to Google or Shopify taxonomies. Only commercial feed tools (Feedonomics and similar) do this, and their tables are proprietary. A one-off mapping of unique breadcrumbs to Shopify Categories (by LLM plus human check) is cheap and likely very accurate. But it is a source-specific shortcut: real merchant feeds carry `product_type` / `google_product_category`, not Amazon breadcrumbs.

### Cited Findings
- Amazon Reviews '23: 571.54M reviews and 48.19M items across 33 `main_category` domains plus "Unknown". Item metadata includes `main_category` and a hierarchical `categories` field. The site notes "some items lack metadata". — [Amazon Reviews '23 site](https://amazon-reviews-2023.github.io/); [HF dataset card](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023)
- The site and HF card mention no mapping to Google or other standard taxonomies. — [Amazon Reviews '23 site](https://amazon-reviews-2023.github.io/)
- Licence: the code repo is MIT, and the authors ask for citation of the paper. Neither the site nor the HF card states an explicit data licence. — [hyp1231/AmazonReviews2023](https://github.com/hyp1231/AmazonReviews2023); paper: Hou et al., "Bridging Language and Items for Retrieval and Recommendation" (arXiv 2403.03952, revised Apr 2026, ACL 2026) — [arXiv](https://arxiv.org/abs/2403.03952)
- Search turned up only commercial Amazon↔Google category mapping services (for example, Feedonomics) and the ProMap product-matching datasets (Amazon-Google offer pairs, not category mappings). — [Feedonomics](https://feedonomics.com/amazon-category-taxonomy/); [ProMap arXiv 2309.06882](https://arxiv.org/pdf/2309.06882)
- Icecat's taxonomy reportedly includes mappings to Amazon and Google taxonomies, but these are for Icecat's own taxonomy, not browse nodes in general. — [search result citing Icecat](https://icecat.co.uk/fa/menu/manufstandardization) (not verified in detail)

### Inferences
- **Viability:** Amazon breadcrumbs are deep (often 4 to 6 levels) and semantically close to Shopify depth-3 Categories. The number of unique breadcrumbs among sampled Listings is far smaller than the number of Listings. Labelling each unique breadcrumb once (LLM, plus a human check on ambiguous ones) turns most Listings into a lookup. Failure modes: generic breadcrumbs (for example, only `main_category`), empty `categories`, and Amazon nodes that mix product types.
- **Legitimacy:** A merchant-feed pipeline can legitimately use whatever category signal the merchant sends. Shopify maps a supplied Google category to its own (Q4), and Meta and Google accept merchant categories as overrides. But the Amazon breadcrumb is Amazon's own assignment, not a merchant's `product_type`. Treat it as a stand-in for a merchant's category field, and report accuracy with and without it, because real merchant `product_type` values are free text and noisier.
- **Eval leakage risk:** If the 198 labels were researched while looking at the Amazon breadcrumb, a breadcrumb-based classifier will look better than it would on real merchant data. Keep a breadcrumb-blind baseline.

### Gaps
- No public dataset quantifies how accurate an Amazon→Google or Amazon→Shopify category mapping is.
- I did not measure the share of Amazon Reviews '23 items with empty or generic `categories`.

## Q3. Public datasets labelled in Shopify or Google Product Taxonomy

### Takeaway
The standout is **Shopify's "The Catalogue"** on Hugging Face (`Shopify/product-catalogue`): 48,289 real Shopify-merchant products labelled with full Shopify taxonomy paths across 10,476 categories, under Apache 2.0. Truncated to depth 3, it could directly seed a kNN or supervised classifier for the 1,862-Category trim. Other public sets (WDC-222, Icecat, Atlas) use different taxonomies and cover narrow verticals.

### Cited Findings
- `Shopify/product-catalogue` ("The Catalogue: Product Taxonomy Classification Benchmark", Shopify, 2025):
  - 48,289 samples: 38.6k train and 9.66k test.
  - 10,476 unique categories and 28,913 brands; category depth 1 to 8, average 4.5.
  - Fields: title, description (present for 92.9%), image, brand (98.2%), `ground_truth_category`, `potential_product_categories` (plausible alternatives), and `ground_truth_is_secondhand`.
  - Top-level mix: Home & Garden 16.4%, Sporting Goods 14.4%, Arts & Entertainment 11.5%.
  - Licence Apache 2.0; last updated around Dec 2025. The citation URL refers to `Shopify/the-catalogue-public-beta`.
  - Neither the taxonomy version nor the labelling method is stated.
  - Sources: [HF dataset card](https://huggingface.co/datasets/Shopify/product-catalogue); [README](https://huggingface.co/datasets/Shopify/product-catalogue/raw/main/README.md)
- Shopify's internal labelling process (likely related): a multi-LLM annotation system with arbitration plus a human validation layer for edge cases. Shopify's VLM classifier makes 30M+ predictions a day, with an 85% merchant acceptance rate. — [Shopify Engineering, 8 May 2025](https://shopify.engineering/evolution-product-classification)
- A Toloka case study for Shopify: two automated methods (vector RAG and tree search) were each about 60% accurate, humans were routed to cases where they disagreed, and the target was ≥95% accuracy. This was for mapping merchant *collections*, not products. — [Toloka blog](https://toloka.ai/blog/building-shopify-s-product-catalogue-at-ai-speed/)
- WDC-222 Gold Standard: 2,984 offers from many e-shops, 222 Icecat leaf categories, Computers & Electronics only, "rather 'dirty' and heterogeneous". Icecat set: 765,473 examples across 370 categories. Licence not stated on the page. — [WDC categorization page](https://data.dws.informatik.uni-mannheim.de/largescaleproductcorpus/categorization)
- WDC-25: about 24k offers, flat 25 labels built with reference to Amazon, Google and UNSPSC. — [arXiv 2109.01411](https://arxiv.org/pdf/2109.01411)
- Atlas: clothing-only categorization benchmark. — [arXiv 1908.08984](https://arxiv.org/pdf/1908.08984)

### Inferences
- The Catalogue is the best fit by far: same taxonomy, real merchant products, permissive licence, 48k examples.
- Truncating `ground_truth_category` to 3 levels maps it onto the 1,862 Categories. It probably covers a large share but not all of them (10,476 full-depth categories, with a skewed top-level distribution). Coverage should be measured.
- Domain shift: Shopify merchant listings, not Amazon. That is fine for kNN over embeddings.
- `potential_product_categories` also gives a ready-made way to measure "acceptable-alternative" accuracy.
- Use it as (a) a kNN index (embed title plus 200 description characters, majority vote over neighbours' depth-3 labels), (b) few-shot examples for the LLM step, or (c) a much larger eval set than 198.
- Version: names from an unstated (probably 2024/2025) taxonomy version must be matched to 2026-08 paths. The version mappings are near-empty, so drift should be small; match by path string and flag misses.

### Gaps
- I found no publicly documented label-quality audit of The Catalogue.
- I found no large public dataset labelled with Google Product Taxonomy IDs that has a clear licence. Searches returned only small paid Gumroad lists and vendor pages.
- Open Food Facts (food-only, its own taxonomy) was not examined in depth.

## Q4. Merchant feed conventions: do merchants supply google_product_category / product_type, and how do platforms use it?

### Takeaway
Every major platform treats the merchant's category as optional. All of them auto-categorize from title, images and text, then let a merchant-supplied category override or seed the result. Shopify explicitly converts a supplied Google category into its own taxonomy. I found no reliable public statistic on how often merchants actually fill these fields.

### Cited Findings
- Google Merchant Center: `google_product_category` is "Optional for each product". "All products are automatically assigned a product category from Google's continuously evolving product taxonomy". The merchant attribute "can be used to override Google's automatic categorization in specific cases". `product_type` is for "your store-specific product categorization system". — [Google Merchant Center Help](https://support.google.com/merchants/answer/6324436)
- Meta catalogs: Google product category (GPC) and Facebook product category (FPC) are both optional, and adding a GPC is "recommended". Without a valid category for checkout items, Meta auto-assigns one from images, title and other information. Meta "never overrides your selected category after you've provided one". — [Meta developer docs, Product Categories](https://developers.facebook.com/docs/commerce-platform/catalog/categories)
- Shopify: AI category suggestion comes from name, description and images. A merchant-supplied Google Product Category is mapped to the Shopify category. For checkout on Instagram/Facebook, merchants "must provide Google Product Categories for tax reasons". — [Shopify Help](https://help.shopify.com/en/manual/products/details/product-category)
- Shopify reports an 85% merchant acceptance rate for its predicted categories. — [Shopify Engineering](https://shopify.engineering/evolution-product-classification)

### Inferences
- An industry-standard design: (1) if a valid Google category id is present, map it via the inverted Shopify→Google table; (2) otherwise classify from text, optionally using free-text `product_type` as an extra input. For this project, the Amazon breadcrumb plays the role of `product_type`.

### Gaps
- No primary-source figure on the share of merchant feeds that include `google_product_category` or `product_type`. Vendor blogs make claims without data.

## Q5. LLM-generated synthetic training data per Category: reported results

### Takeaway
Synthetic data from LLMs trains reasonable classifiers for objective, topic-like tasks: about 6 points below real data when generated from nothing, and 2 to 4 points below when seeded with a few real examples. Product categorization is a low-subjectivity task. However, no paper I found reports synthetic-data results for a 1,000+-class product taxonomy.

### Cited Findings
- Li et al., EMNLP 2023, "Synthetic Data Generation with LLMs for Text Classification: Potential and Limitations". On AG News (topic, lowest subjectivity), BERT reached 95.3% accuracy on real data, 89.3% with zero-shot synthetic data, and 91.6% with few-shot synthetic data. RoBERTa: 94.6% / 88.6% / 92.9%. The gap widens sharply for subjective tasks (sarcasm: 90.3% real vs 51.2% zero-shot synthetic). The authors find subjectivity is negatively associated with synthetic-data performance. — [arXiv 2310.07849](https://arxiv.org/pdf/2310.07849); [ACL Anthology](https://aclanthology.org/2023.emnlp-main.647)
- Attribute-aware controlled product generation (arXiv 2601.04200, 2026): 99.6% of 2,000 synthetic products were rated natural. On MAVE (attribute extraction, not categorization), synthetic training gave 60.5% vs 60.8% for real data, and hybrid synthetic plus real reached 68.8%. — [arXiv 2601.04200](https://arxiv.org/html/2601.04200)
- Shopify's production labelling uses multiple LLM annotators with arbitration plus targeted human review, rather than purely synthetic products. — [Shopify Engineering](https://shopify.engineering/evolution-product-classification)

### Inferences
- With 1,862 classes, generating about 10 to 20 synthetic titles per Category means 20k to 40k LLM outputs. The key risk is that generated titles are cleaner and more prototypical than real Amazon titles. Few-shot seeding with real Listings narrows the gap in Li et al.
- Since The Catalogue already provides 48k real labelled Shopify products, synthetic data is best used only to fill Categories that The Catalogue does not cover after the depth-3 truncation.

### Gaps
- No published results for synthetic-data training on Shopify or Google Product Taxonomy scale (1,000+ classes).
