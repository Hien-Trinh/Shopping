# Step 6l.2: 8k more Amazon Listings labeled by Opus, then the student again (mini PRD)

Status: done Oct 6 (see the two Outcomes); approved Oct 6. Question 1: the usage OK. Question 2: τ from the held-out Amazon rows only. Question 3: the test points OK. Plan row: [plan-v1.md, PR steps, 6l.2](../plan-v1.md). Builds on [step-6l.md](step-6l.md) (the student; its bar is not met), [step-6k.md](step-6k.md) (6k.2, the first 2k Opus labels) and [docs/labeling.md](../labeling.md) (the method). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

6l's best cascade beats Jev alone only at 45% kept local (68.3% against 66.9% on the 1,020); at 79% kept it falls to 60.3%. On Shopify's 2,000 it ties. 94% of the training rows are Shopify's, so the student is weak on our Amazon-style Listings. More Amazon rows labeled by the teacher should close that gap. The held-out rows that pick τ are 94% Shopify too, so they overstate how right the student is on our Listings (τ = 0.40 kept 68% but lost to Jev).

## Solution

Two PRs: **6l.2a** the labels (no code, per [docs/labeling.md](../labeling.md)), **6l.2b** the student on them and the rerun.

### 6l.2a: about 8k Amazon Listings labeled by Opus

1. **Draw:** `evaluate sample --per-category 1000 --seed 3`, then drop any id in `eval/labels.jsonl`, `eval/labels-sonnet.jsonl` or `train/amazon-opus.jsonl`, and any title (case-folded) of the 1,020 or the Shopify 2,000; keep the first 245 per category. A check on Oct 6: the first 1,000 lines of each category file leave 535–1,000 unused Listings per category (Subscription_Boxes is the smallest, 640 lines in all), so `lines` stays 1,000 and no code changes. **8,085 Listings** (33 × 245).
2. **Opus:** about 85 agents of about 95 Listings, 6k.2's brief (6j's plus the level-3 recheck and the subscription-box examples), in waves of 20.
3. **The in-session checks**, then **the second opinion** (labeling.md §7b): every unsure answer, every answer above level 3 that isn't a leaf, and a random 100 of the rest go blind to Sonnet agents with the same brief; I adjudicate where they differ. At 6k.2's rates that is about 2,400 reviewed and 850 adjudicated. The error among the sure labels is reported with its interval from the random 100.
4. **Commit** `train/amazon-opus-2.jsonl`: the sample fields plus `category` and `labeler: "claude-opus-5-5"`. A new file, not an append: 6k.2's file stays as reviewed, and each file records one draw (seed 2 and seed 3).
5. `docs/labeling.md` gets a row in its table.

### 6l.2b: the student on 10k Amazon rows

1. `student.TRAIN` gains `train/amazon-opus-2.jsonl`. `held_out` already splits 10% of each source, so about 1,010 Amazon rows are held out.
2. **The threshold rule (question 2):** τ picked on the held-out Amazon rows only (about 1,010), not on all held-out rows (about 4,150, 76% Shopify). The report shows both τs and their numbers on the evals.
3. Rerun `python -m catalog.student --models <main checkout>/models`, rewrite `eval/student.md`, and record in this spec whether the bar is met on the 1,020 (unchanged: the cascade beats Jev alone on exact with 70% or more kept local).

## User stories

1. As you, I know whether 10k Amazon rows close the gap enough for 6m, at no API cost.
2. As you, τ is picked on rows that look like our Listings, so the chosen point on the curve holds on the 1,020.

## Failure scenarios

| Scenario | Expected |
|---|---|
| An Opus agent fails, skips ids or invents a path | As in 6k.2: one retry with a new agent for the missing ids, then I label what's left and say so in the PR |
| The session hits a usage limit mid-run | Answers are appended as agents go and kept in scratch; the next wave picks up the unanswered ids |
| A drawn Listing is also an eval Listing | Dropped by id and by title before batching |
| The bar is still not met | Recorded; the next options (fine-tune bge-small, or more rows) go to you before anything else is done |
| Too few Amazon held-out rows give a τ | `pick_tau` already says when no τ qualifies; the report says so |

## Implementation decisions

1. **No new draw code:** 1,000 lines per category is enough (checked above).
2. **A second file** rather than appending to `train/amazon-opus.jsonl`.
3. **The same brief, models and review as 6k.2:** no method change, so the two files are comparable.
4. **Labels and code in separate PRs,** as 6k.

## Testing decisions (test points for your OK, 6l.2b)

In `tests/integration/test_student.py`:

1. `TRAIN` lists the new file and `load_training` reads it.
2. With question 2's change: τ is picked from the held-out rows of the Amazon sources only; a toy set where Shopify rows are right and Amazon rows are wrong at the same confidence gives a higher τ than all rows would.

## Questions

1. **Usage:** 6k.2 took about 1.4k Opus tokens a Listing and about 1.35k Sonnet tokens a reviewed item. For 8,085: about **11M Opus tokens** (about 85 agents) plus about **3.2M Sonnet tokens** (about 25 agents), about 14–15M in all, plus my adjudication of about 850 items. Wall time about 3–4 hours, most of it the review. OK?
2. **The threshold rule:** pick τ on the held-out Amazon rows only (my default), or keep the rule from 6l (all held-out rows)? The bar's verdict reads the whole curve, so it doesn't depend on this; τ is what 6m would ship.
3. **The test points above.** OK?

## Outcome, 6l.2a (Oct 6)

- **The draw:** `sample --per-category 1000 --seed 3`, less the ids and titles above, first 245 per category: 8,085 Listings (33 × 245).
- **Opus:** 85 agents (95–96 Listings each) with 6k.2's brief, in waves of 20. Every batch passed the in-session checks first time; 1,937 unsure (24%), 8 `none`, 55 non-leaf answers above level 3. About 10.8M tokens.
- **The review:** 2,139 items to 24 Sonnet agents, blind: the 1,937 unsure, the 55 above level 3, a random 100, and (added) the 47 confident `Subscription Services` answers, a known failure mode the leaf rule would skip. One reviewer said it labeled from titles with quick searches only, so its batch went to a fresh agent. About 2.9M tokens.
- **Agreement:** 1,199 of 1,937 unsure, 5 of 55 above level 3, 95 of 100 random, 47 of 47 subscription. Of the 793 disagreements, three rules settled 257 (a format pair the text can't settle keeps Opus: 152; a Sonnet `none` keeps Opus: 31; Opus above level 3 with Sonnet on a level-3 child takes Sonnet: 74) and I adjudicated 536 (218 changed).
- **Changed:** 292 labels (240 unsure, 50 of the 55 above level 3, 2 random, 0 subscription). 7 final `none` rows dropped.
- **Error rate:** 2 of the 100 random sure labels changed, so about 2% among the sure labels (95% interval about 0.5–7%).
- **Committed:** `train/amazon-opus-2.jsonl`, 8,078 rows over 790 Categories; [docs/labeling.md](../labeling.md) records the run.

## Outcome, 6l.2b (Oct 6)

**The bar is met on the 1,020** by kNN (k=10, picked on the held-out rows; 6l had k=5). Full curves: [eval/student.md](../../eval/student.md).

| On the 1,020 (exact) | Kept local | System |
|---|---|---|
| Jev alone | 0% | 66.9% |
| kNN alone (6l: 52.3%) | 100% | 65.7% |
| kNN, Jev below τ = 0.20 (lowest τ that meets the bar) | 94.4% | 67.8% |
| kNN, Jev below τ = 0.25 (the τ picked on the Amazon held-out rows) | 83.7% | **70.0%** |
| kNN, Jev below τ = 0.35 | 69.8% | 72.1% |
| kNN, Opus below τ = 0.25 | 83.7% | 74.9% |
| Softmax alone (6l: 47.5%) | 100% | 57.2% |
| Opus alone (the teacher) | 0% | 86.7% |

- **The 8k Amazon rows close most of the gap:** kNN alone went from 52.3% to 65.7% exact (top level 72.4% to 83.6%). The cascade now beats Jev at every τ from 0.20 up, peaking at 72.9% with 52% kept. It is still 16.7 points under the teacher at the chosen τ.
- **The softmax head** improved (47.5% to 57.2%) but misses the bar; kNN stays the student for 6m.
- **The τ rule:** on the Amazon held-out rows (1,007 of 4,150) kNN's τ is 0.25, the same as on all held-out rows; softmax's is 0.15 against 0.25. For kNN the chosen τ lands on the curve where the bar is met.
- **On Shopify's 2,000 it is not met:** 60.9% at 73.5% kept against Jev's 61.8%; the student alone is 55.0% (6l: 55.2%). The new rows help our Listings, not Shopify's.
- **Leakage check:** besides the exact-title drop, 15 of the 1,020 titles (1.5%) share their first 40 normalized characters with a training title, mostly Amazon gift cards and subscription-box variants. Even all 15 counted as wins is under half the 3.1-point margin (about 32 Listings).
- **A caveat for 6m:** the 1,020's labels are Claude-made (the 198 drafted by Claude and checked, the 822 by Sonnet), and the student now learns from 10k Opus labels, so a shared labeling style may favor the student over Jev on this eval. The Shopify 2,000 (Shopify's own labels) shows no gain.
- **Speed:** kNN p50 13–16 ms, p99 18–47 ms per Listing over two runs; the spread is load on the Mac (the first run was also paused partway, which put softmax's p99 at 267 ms).

## Out of scope

- Weighting Amazon rows over Shopify's in training, fine-tuning bge-small, the pipeline (6m).
