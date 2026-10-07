# Step 6l.2: 8k more Amazon Listings labeled by Opus, then the student again (mini PRD)

Status: approved Oct 6. Question 1: the usage OK. Question 2: τ from the held-out Amazon rows only. Question 3: the test points OK. Plan row: [plan-v1.md, PR steps, 6l.2](../plan-v1.md). Builds on [step-6l.md](step-6l.md) (the student; its bar is not met), [step-6k.md](step-6k.md) (6k.2, the first 2k Opus labels) and [docs/labeling.md](../labeling.md) (the method). Terms follow [CONTEXT.md](../../CONTEXT.md).

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

## Out of scope

- Weighting Amazon rows over Shopify's in training, fine-tuning bge-small, the pipeline (6m).
