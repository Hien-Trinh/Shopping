# Step 6k: Shopify's benchmark as eval and training data, plus 2k Amazon Listings labeled by Opus (mini PRD)

Status: done Oct 6 (see the two Outcomes); approved Oct 6. Question 1: the fetch OK. Question 2: commit `train/shopify.jsonl.gz`, credited in the README. Question 3: 22 Opus agents OK. Question 4: the test points OK. Plan row: [plan-v1.md, PR steps, 6k](../plan-v1.md). Builds on [step-6j.md](step-6j.md) (Opus is the teacher; [eval/teacher.md](../../eval/teacher.md)), [step-6e.md](step-6e.md) (`evaluate sample`, the label format) and [docs/labeling.md](../labeling.md) (the labeling method). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

6l trains a student and needs two things we don't have: training data (tens of thousands of labeled Listings) and an eval that isn't all Amazon and isn't all Claude-labeled. Shopify publishes [Shopify/product-catalogue](https://huggingface.co/datasets/Shopify/product-catalogue) (Apache-2.0): 48,289 products (38,631 train, 9,658 test) with title, description and a full-depth Category from Shopify's merchant catalog. A check of 1,300 rows on Oct 6 found:

- 98.1% of labels match the 2026-08 taxonomy exactly, 98.5% after cutting to level 3. The misses are nodes Shopify renamed since (mostly "Baby & Toddler" became "Baby & Children's"): about 10–20 hand-made table entries.
- About 80–85% of rows are English. The rest (German, Spanish, Japanese, ...) can't go through `bge-small-en`.
- Each row has 8–9 candidate Categories and the label is always among them. Production has no such list, so it is never read.

Shopify's products look different from our Amazon-style Listings, so 6l also gets about 2k Amazon Listings labeled by the teacher.

## Solution

Two PRs: **6k.1** code and data for Shopify, **6k.2** Opus's 2k labels (no code, per [docs/labeling.md](../labeling.md)).

### 6k.1: `python -m catalog.benchmark`

A new module, `src/catalog/benchmark.py` (`evaluate.py` is already about 570 lines):

1. **Fetch** text only through Hugging Face's datasets-server rows API (`/rows?dataset=Shopify/product-catalogue&config=default&split=...&offset=...&length=100`) with stdlib `urllib`, as `sample` does. About 490 requests, a few tens of MB, about 5–10 minutes. Only `product_title`, `product_description` and `ground_truth_category` are kept; the image and candidate columns are never stored. A 429 or 5xx is retried with backoff (3 tries, then the run fails naming the offset).
2. **English only:** a row is kept if under 3% of its letters are non-ASCII and English stopwords outnumber German, French, Spanish, Italian, Portuguese, Dutch and Swedish ones; a row with no stopwords either way (part numbers, short titles) is kept. No new dependency.
3. **Map to our taxonomy:** a label in the 2026-08 full release is cut to level 3 (`taxonomy.ancestor`). One that isn't goes through `src/catalog/data/shopify-renames.json` (old prefix → new prefix), then the same cut. Still unknown: dropped and counted. The table is built by running step 1 and listing every miss.
4. **Shape** each row as the label files are: `id` (the first 16 hex characters of the SHA-256 of title + description, so a repeated product gets one id and a later dataset revision keeps it), `title` (cut to 150 characters at a word boundary, as `sample` does; empty titles dropped), `description` (cut to 5,000), `category`, `source: "shopify-<split>"`. A repeated id keeps its first row.
5. **Write:**
   - `eval/labels-shopify.jsonl`: 2,000 rows drawn with seed 0 from the English test split. `evaluate run --labels` reads it as is.
   - `train/shopify.jsonl.gz`: every usable English train row, about 31k. Any id also in the test split is dropped, so the eval never leaks into training.
   - A summary printed and put in the PR: rows read, non-English, unmapped, duplicates and leaks dropped, and the dataset revision fetched.
6. **Spot-check 50** eval labels against the full taxonomy (as with the Sonnet labels): wrong ones are listed, and if more than 15% are wrong we stop and decide whether this eval can be used.

### 6k.2: 2k Amazon Listings labeled by Opus

Per [docs/labeling.md](../labeling.md):

1. `evaluate sample --per-category 95 --seed 2`, then drop any id in `eval/labels.jsonl` or `eval/labels-sonnet.jsonl` and keep the first 61 per category: about 2,000.
2. 22 Opus agents with 6j's brief, plus 6j's two lessons: before answering above level 3, check the level-3 children once more; and the subscription-box rule spelled out with 6j's four wrong examples.
3. The in-session checks, then my pass: every unsure answer (about 23%, so about 450), every answer above level 3, and a random 100 of the rest. The error rate among the sure ones is reported with its interval.
4. Commit `train/amazon-opus.jsonl`: the sample fields plus `category`, `labeler: "claude-opus-5-5"`.

## User stories

1. As you, 6l can train on about 33k labeled Listings without paying for a single label.
2. As you, the student is scored on 2,000 Shopify products labeled by Shopify, besides our 1,020, so a Claude-flavored score can't hide.
3. As you, nothing in the eval is also in the training data.

## Failure scenarios

| Scenario | Expected |
|---|---|
| The rows API answers 429 or 5xx | Retried with backoff, 3 tries, then the run fails naming split and offset; nothing is written |
| The run is killed partway | Nothing is written (files are written at the end, via `_write`'s temp file and rename); rerun from scratch, about 10 minutes |
| A row lacks a field, or a field isn't a string | Dropped and counted, not fatal: the summary shows it |
| The dataset changes between runs | The summary records the revision; ids are content hashes, so unchanged products keep their ids |
| A label isn't in the taxonomy or the rename table | Dropped and counted; the PR lists the 20 most common |
| Two Shopify products share title and description | One id; the first row is kept |
| A test product also appears in train | Dropped from train and counted |
| Shopify's labels prove poor (over 15% wrong in the spot-check) | Stop; we decide whether to keep the eval |
| An Opus agent fails or invents a path (6k.2) | As in 6j: one retry with a new agent, then I label what's left |

## Implementation decisions

1. **The rows API, not the parquet files:** the files are about 9.5 GB because they carry images; the rows API serves the text alone. The question below asks for your OK.
2. **Content-hash ids,** so dedup and the leak check are one comparison.
3. **The rename table is data, not code**, beside the taxonomy files.
4. **`train/` is a new top-level directory** for training data; `eval/` keeps eval data only.
5. **No new dependency.**

## Testing decisions (test points for your OK)

All in `tests/integration/test_benchmark.py`, with the network replaced by an injected `get`:

1. **English filter:** an English title passes; German, Spanish and Japanese titles fail; a part-number-only title passes.
2. **Mapping:** a path in the release → its level-3 ancestor; a level-2 path → itself; a renamed path → the new path's ancestor; an unknown path → `None`.
3. **Fetch:** pages through offsets until a short page; a 429 then a 200 succeeds; three 503s raise naming the offset; image and candidate columns never reach the output.
4. **Build:** duplicates collapse to one id; a test id is never in train; the eval draw is the same for the same seed and is 2,000 rows (or all, if fewer); the eval file loads with `evaluate.load_labels`.
5. **Shape:** a 200-character title is cut at a word boundary to 150 or fewer; an empty title is dropped; the id is stable for the same text.

## Questions

1. **The fetch:** about 490 requests to Hugging Face's datasets-server over 5–10 minutes, text only, a few tens of MB. OK?
2. **Committing the training data:** `train/shopify.jsonl.gz` is about 8–12 MB, with descriptions cut to 5,000 characters. Commit it so 6l is reproducible even if the dataset changes, or keep it out of git under `/data` and rebuild by command? I'd commit it, with a line in the README crediting Shopify's Apache-2.0 dataset.
3. **Opus usage (6k.2):** 22 agents, about 3M tokens of Max plan usage, plus my pass over roughly 600 answers. OK?
4. **The test points above.** OK?

## Outcome, 6k.1 (Oct 6)

- **Fetched** revision `002ca155` of the dataset: 48,289 rows (9,658 test, 38,631 train). The rows API rate-limits hard (429 with no Retry-After after about 50 pages), so the fetch pauses 1 s between pages and retries up to 6 times with backoff from 15 s, not the 3 tries the spec said; timed-out, reset and truncated reads and non-JSON pages are retried too (the first train run died on a timed-out read). The two splits took about 40 min and 30 min. The rows API serves only the latest revision, so `main` reads the revision before and after the fetch and refuses to write if it changed.
- **Dropped:** 8,907 non-English (18.4%), 29 duplicates, 52 train rows whose id or title (case-folded) is also in test, 4 labeled `Uncategorized`. Nothing malformed.
- **Mapping:** 13 rename-table entries (Shopify's "Baby & Toddler" became "Baby & Children's", Uniforms became Uniforms & Workwear, bottles, bibs and sippy cups moved under Feeding Essentials, scrubs moved to Uniforms & Workwear, closet rods and shelves to Closet Parts & Accessories). A matching rename is tried before the path as is. A label is accepted when its level-3 ancestor is a Category, even if the release has since dropped the deeper node (Raw Candle Wax, Dry Beans): a change from the spec, which also required the full path to be in the release.
- **Ids** hash the stored (cut) title and description, so they can be recomputed from the files.
- **Written:** `eval/labels-shopify.jsonl`, 2,000 rows over 641 Categories; `train/shopify.jsonl.gz`, 31,431 rows over 1,676 Categories, 12 MB. The two files are written one after the other: a run killed between the two leaves a new eval beside an old train, so rerun after any failed run (the build is deterministic).
- **Spot-check of 50 eval labels** (seed 11, from the final file): 2 wrong and 1 arguable (4–6%): an exhaust spring puller under Nail Pullers, a Danish hand sanitizer under Cosmetics (the English filter let it through), a balayage board under Cosmetics rather than Hair Care. An earlier check of 50 from a draft draw found 4 wrong (8%). Under the 15% line, so the eval stays. The English filter lets a little Danish and Italian through and drops a few short English titles.
## Outcome, 6k.2 (Oct 6)

- **The draw:** `sample --per-category 95 --seed 2`, less any id in the 1,020, first 61 per Amazon category: 2,013 Listings (33 × 61).
- **Opus:** 22 agents with 6j's brief plus the level-3 recheck and the subscription-box examples. Every answer a taxonomy path, none `none`, no batch redone; 476 unsure, 118 above level 3 (77 of them leaves with nothing deeper: `Gift Cards`, `Subscription Services`).
- **The review:** the 476 unsure, the other 41 above level 3 and a random 100 sure ones (594 in all) went to 7 Sonnet agents, blind, with the same brief. They agreed on 384; I adjudicated the 210 where they didn't and changed 62 labels (46 unsure, 27 above level 3, 1 random; some in two sets). Most of the rest were format questions the text can't settle (CD or download, DVD or download), kept as Opus had them.
- **Error rate:** 1 of the 100 random sure labels was wrong (a game app under Handheld & PDA Software), so about 1% among the sure labels (95% interval about 0–5%).
- **Committed:** `train/amazon-opus.jsonl`, the sample fields plus `category` and `labeler: "claude-opus-5-5"`.

## Out of scope

- Images, the candidate lists, brand and second-hand fields.
- Non-English rows.
- Training anything (6l).
