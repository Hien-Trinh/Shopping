# Step 6e: Labeled set and classifier experiment (mini PRD)

Status: done Oct 4 (see Outcome). Approved Oct 4, with the split, decisions 1 to 6 and the test points as written; open questions answered below. Plan row: [plan-v1.md, PR steps, 6e](../plan-v1.md) ("Labeled set and classifier experiment: you verify the labels, laya-mlx and Jev need your OK", plus two notes from the 6b review), Phase 6 (label set: "about 200 items from Amazon Reviews '23", an LLM pre-labels, "you verify every label by hand"; exit: "the eval report is committed and the threshold is chosen from it"), B4 (6e measures throughput on this Mac), B5 (memory, one shared process if Laya wins) and Open questions (laya-mlx is reviewed before install). Design: [Categorization](../design-commerce-ingestion-pipeline.md) ("The threshold is chosen from the eval") and Future work (options (a) to (d)). Builds on [step-6d.md](step-6d.md) (the harness, `eval/labels.jsonl`, `eval/results/`, `eval/report.md`) and [step-6b.md](step-6b.md) (`EmbeddingClassifier`, `THRESHOLD`, `DESCRIPTION`). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

The harness exists, but nothing feeds it: there is no labeled set, only one of the four options runs, and `THRESHOLD = 0.5` and the 500-character description are still guesses. The step also has three separate approval gates (your label check, the laya-mlx install, Jev's API key and spend), so one PR would sit blocked on whichever comes last. And the 6b review found a trap for the end of the step: a new threshold or text recipe that keeps the same `taxonomy_version` leaves rows stored under the old one unclassified forever.

## Solution: four PRs

| PR | What | Gate |
|---|---|---|
| **6e.1** | `python -m catalog.evaluate sample` draws about 200 Listings from Amazon Reviews '23; I pre-label them; you check every label; commit `eval/labels.jsonl`. Then run option (c), embeddings only, in three text recipes, and commit the first `eval/report.md` | Downloading the sample (about 33 × 1,000 lines, about 100 MB streamed, nothing kept but the 200); your label check |
| **6e.2** | Options (a) Laya hierarchical choice and (b) embedding shortlist of 10, then a Laya choice: `src/catalog/laya.py`, laya-mlx in an optional dependency group | Your OK on laya-mlx after my review, and on its checkpoint download (about 850 MB into `models/`) |
| **6e.3** | Option (d): embedding shortlist of 10, then a Jev `Choice` call: `src/catalog/jev.py` | Your API key in `TYPESAFE_API_KEY` (you set it; I never see it) and your OK on the spend (about 200 calls; cents at the design's price) |
| **6e.4** | The decision: you pick the classifier, recipe and threshold from the report; the winner's settings go into `taxonomy_version`; `THRESHOLD` and `DESCRIPTION` change; the final report is committed | Your pick |

6e.2 and 6e.3 don't depend on each other. If you decline laya-mlx or Jev, its row in the report says "not run" and the decision is made among the rest.

### 6e.1: the labeled set

1. **`sample`** (new subcommand in `evaluate.py`): `python -m catalog.evaluate sample --per-category 6 --seed 0 [--source URL] > eval/sample.jsonl`. For each Amazon category file, it streams the first 1,000 lines over HTTP (stdlib `urllib`, no new dependency) and draws 6 at random, about 200 in all. Each item becomes `{"id": parent_asin, "title", "description", "amazon_category"}`:
   - `description` is Amazon's `description` list, then its `features` list, joined by newlines and cut to 5,000 characters (`Content`'s limit).
   - A title over 150 characters is cut at the last word boundary within 150 (6d refuses longer ones). An empty title, or a repeated `parent_asin`, is skipped.
2. **Pre-label:** I pick a path for each item from the committed taxonomy, at the deepest level that fits (up to 3), **without** looking at any classifier's answer, so the labels don't lean toward one candidate. An item that fits no Category is dropped and counted in the PR.
3. **Your check:** in chat, in 10 batches of 20: each item's title, the start of its description, the Amazon category and my label. You answer "ok" or corrections by number. A batch is written to `eval/labels.jsonl` only after you answer it. About 5 minutes a batch, under an hour in all.
4. **Runs** (by hand, each its own process): `run --classifier embedding --description 0 --name embedding-d0`, then `d200` and `d500`, each at `--batch 1` (latency per Listing) and once at `--batch 200` (throughput, B4). `--description N` is new on `run`, backed by a new `EmbeddingClassifier.description` field defaulting to `DESCRIPTION`. Then `report`, and commit `eval/results/*.json` and `eval/report.md`.

### 6e.2: Laya

`LayaClassifier(taxonomy, choose, mode)` in `src/catalog/laya.py`, where `choose(text, options) -> list[float]` is one Laya `choice` question (probabilities per option), injected so tests need no MLX:
- **`hierarchical`** (a): choose among the 25 level-1 Categories, then among the chosen one's children, then again; stop at a Category with no children. Confidence is the product of the chosen probabilities, so a confident first step followed by a coin flip doesn't look certain. Each level has at most 80 options.
- **`shortlist`** (b): the embedding's 10 nearest paths (a new `EmbeddingClassifier.top(listings, k)`; `classify` becomes its `k = 1`), then one Laya choice among their full paths. Confidence is Laya's probability.
- laya-mlx goes in a `laya` dependency group with a `sys_platform == 'darwin'` marker, not in the main dependencies, until it wins; `evaluate.candidate` imports it only for `--classifier laya-*`. The `check` job on Linux never imports it.
- Text recipe: the winning embedding recipe from 6e.1. One fixed question text ("Which product category fits this listing?"); prompt tuning is out of scope.

### 6e.3: Jev

`JevClassifier(taxonomy, embed, call)` in `src/catalog/jev.py`: the same shortlist of 10, then one `Choice` question per Listing, with options keyed `"1"` to `"10"` and the full paths as their descriptions (so path text never has to fit an option-name rule). `call` is injected: the real one reads `TYPESAFE_API_KEY` at construction and refuses to start, naming the variable, if it is missing. It adds each response's cost to `usd` (6d decision 4), computed from the response's usage and the published price at run time. Whether `call` uses TypeSafe's SDK or plain HTTP is decided after reading their official API docs, and a new dependency gets your OK. Calls run one at a time (about 300 ms each), well under the 40 requests/s limit.

### 6e.4: the decision

- You choose from the report's summary and threshold tables. The report still highlights no "best" (6d decision 2).
- If the embedding classifier wins: `THRESHOLD` and `DESCRIPTION` take the chosen values, and `_version` becomes `{taxonomy}+{model}+d{description}+t{threshold}` (for example `2026-08+bge-small-en-v1.5+d200+t0.45`), so any change to either one reclassifies the old rows through the Backfill (6c).
- If Laya or Jev wins: the report and the choice are committed, and running it in the pipeline becomes a new plan step (B5's shared classifier process, or Jev's rate limit and timeout path), because neither fits in this PR.
- Switching the `Procfile` from `fake` to `embedding` is a later step (question 3).

## User stories

1. As you, I check 20 labels at a time in chat and never edit JSON by hand.
2. As you, each approval gate blocks only its own PR, so a declined laya-mlx doesn't hold up the labeled set.
3. As you choosing the threshold, every option sits in one report over the same label set, with accuracy, latency, cost and memory side by side.
4. As you, I see what the 500-character description buys against title only and 200 characters, at its real cost per Listing on this Mac.
5. As the worker after 6e.4, rows stored under the old threshold or recipe carry an older `taxonomy_version`, so the Backfill reclassifies them.

## Failure scenarios

| Scenario | Expected |
|---|---|
| The download drops mid-`sample` | The command fails and prints nothing usable: it writes only after every category is read, so no half sample |
| A category file has fewer than 1,000 lines, or fewer than 6 usable items | Takes what it has; the PR states the per-category counts |
| An Amazon title over 150 characters, or with no space before 150 | Cut at the last space within 150, else hard-cut at 150 |
| `description` or `features` missing, empty or not a list | Treated as empty; the title alone is still a Listing |
| The same `parent_asin` in two categories | Kept once |
| An item that fits no Category (gift cards from a store, a bundle of unrelated things) | Dropped from the set and counted, since 6d refuses an Uncategorized label |
| You correct a label to a path not in the taxonomy | `load_labels` refuses the file naming the line (6d), so the run never scores it |
| laya-mlx, or its checkpoint, missing when `--classifier laya-*` runs | Refused at start with the install or download command, before the warm-up |
| Laya runs out of memory on the 16 GB Mac | The process dies and writes no result (6d); the PR records it, and peak RSS from a smaller `--batch` decides B5 |
| `TYPESAFE_API_KEY` unset | Refused at construction, naming the variable; no call made |
| Jev answers 429, times out, or the network drops mid-run | The run fails and writes no result (6d); rerunning costs another few cents |
| Jev answers an option outside `"1"` to `"10"` | Scored as `invalid` (6d) |
| The key leaking into a result or the report | Never: the key is read once and kept only in the client; results hold answers, times and `usd` |
| A run on a machine other than this Mac | Its `machine` column says so; the decision uses this Mac's runs only |
| The threshold or recipe changes later | A new `taxonomy_version`, so stored rows reclassify through the Backfill |

## Implementation decisions

1. **Split into 6e.1 to 6e.4**, one gate each. Alternative: one PR, blocked on the slowest gate and too big to review.
2. **The first 1,000 lines of each category file, sampled with a fixed seed.** Streaming only the start keeps the download small and needs no new dependency. Ceiling: the start of a file may not be a random slice of the category (`ponytail:` comment). 6 per category over Amazon's categories gives breadth, since 200 Listings over 1,862 Categories can't cover them all anyway.
3. **I pre-label blind to every classifier.** Labels taken from the embedding's top answers would favour option (c).
4. **`sample` lives in `evaluate.py`** (6d decision 6: code in the package, `eval/` holds data), and its test streams a local `file://` URL, which `urllib` reads natively.
5. **Laya and Jev answer only with probabilities over given options**, so a candidate can't invent a path. Hierarchical confidence multiplies the levels.
6. **`taxonomy_version` encodes the recipe and threshold**, not only the taxonomy and model: the fix the 6b review asked for, applied in 6e.4 to whatever wins.

## Answered questions

1. **Amazon text in this public repo:** yes, `eval/labels.jsonl` commits the titles and descriptions (research data, fine for learning).
2. **`features`:** folded into the description, as above; no separate recipe.
3. **The `Procfile`:** switched in a later step, so 6e.4 stays a decision PR.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`sample`** (new, red first, `tests/integration/test_evaluate.py`): a local `file://` source with crafted lines gives the same draw for the same seed, cuts a 160-character title at a word boundary, joins `description` and `features`, skips an empty title and a repeated id, and keeps a short category.
  2. **`EmbeddingClassifier.description` and `top`** (new, red first, `tests/integration/test_classify.py`, injected `embed`): `description=0` embeds the title alone; `top(k)` returns `k` paths in order of similarity, and `classify` matches `top(1)` above the threshold.
  3. **`LayaClassifier`** (new, red first, `tests/integration/test_laya.py`, injected `choose`): hierarchical walks three levels and stops at a childless Category; confidence is the product; shortlist offers the embedding's 10; a probability list of the wrong length fails the call.
  4. **`JevClassifier`** (new, red first, `tests/integration/test_jev.py`, injected `call`): options keyed `"1"` to `"10"`, the answer mapped back to its path, an unknown key returned as itself (so 6d counts it `invalid`), `usd` summed; a missing `TYPESAFE_API_KEY` refuses at construction.
  5. **`_version`** (6e.4, changed, `tests/integration/test_classify.py`): a different threshold or description gives a different `taxonomy_version`.
- **No real model, MLX or network in tests.** The real runs are by hand on this Mac; their output is the committed results.
- **Coverage:** `laya.py` and `jev.py` under the 90% gate through their seams; the real `choose` and `call` wrappers are small and excluded with a `pragma: no cover` naming why.

## Out of scope

- Running Laya or Jev in the pipeline (a shared classifier process, Jev's timeout path): a new plan step if one wins.
- Prompt tuning for Laya or Jev, other embedding models, bootstrap intervals.
- Labels beyond about 200, or labels from merchants other than Amazon.
- The `Procfile` switch (a later step).

## Outcome

- **Labels (6e.1):** 198 Listings, 6 from each of 33 Amazon categories. At your request, Claude checked each label against Shopify's full taxonomy (the depth-3 ancestor) instead of you checking all 198, and you decided the 7 ambiguous ones.
- **Results** ([eval/report.md](../../eval/report.md), exact match at threshold 0): embedding title only 19.7%, + 200 characters 23.7%, + 500 22.7%; Laya hierarchical 14.6%, Laya shortlist 17.2%; Jev after a shortlist of 10 48.5%, of 50 54.5% (`--shortlist`, added in 6e.3 at your request). 200 description characters beat 500.
- **Decision (6e.4):** Jev after an embedding shortlist of 50, at threshold 0.40 (54.0% exact, 57.2% precision, 5.6% Uncategorized), with confidence = the chosen option's probability (not Jev's `confidence` field). Moving the pipeline onto it is the new step 6f in the plan, with the `taxonomy_version` encoding from decision 6. The design doc records the choice.

## Size

- 6e.1: about 50 lines of code, 60 of tests, plus `eval/labels.jsonl`, results and the report.
- 6e.2: about 70 lines, 80 of tests. 6e.3: about 60 lines, 70 of tests.
- 6e.4: about 15 lines, 20 of tests, plus the final report.
