# Step 6l: the student prototype, a softmax head and kNN over bge-small vectors (mini PRD)

Status: done Oct 6 (see Outcome); approved Oct 6. Question 1: the bar switched (below). Question 2: Jev on the 2,000 Shopify Listings OK. Question 3: the Opus line on the curve, yes. Question 4: the test points OK. Plan row: [plan-v1.md, PR steps, 6l](../plan-v1.md). Builds on [step-6j.md](step-6j.md) (Opus is the teacher), [step-6k.md](step-6k.md) (the training and eval data) and [step-6e.md](step-6e.md) (the eval harness). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

Jev costs about $62 per 1M Listings and 250 ms a call, and gets 67.5% exact on the 1,020. The plan since 6j: a local "student" trained on labeled Listings answers at embedding speed (about 10 ms), and only the Listings it is unsure of go to Jev. 6l answers whether that works before anything enters the pipeline (6m).

## Solution: an eval-only prototype

### The student

`src/catalog/student.py`, numpy only (no new dependency):

1. **Vectors:** each Listing's text (`classify.text`, title plus the first 200 description characters, Jev's setting) through bge-small, the model the workers already load. Unit-normalized, 384 numbers. Training vectors are computed once and cached under `models/` (gitignored), keyed by `classify.cache_key`; about 34k texts, about 5 minutes.
2. **Training data:** `train/shopify.jsonl.gz` (31,431) and `train/amazon-opus.jsonl` (2,013). A random 10% of each is held out to pick thresholds; the evals are never used to tune anything.
3. **Two students:**
   - **Softmax head:** one linear layer, 384 × the Categories seen in training, plus bias, trained by full-batch gradient descent with L2 regularization (Adam, a few hundred steps, seconds on the Mac). Confidence is the top class's probability.
   - **kNN:** cosine similarity to every training vector, the k nearest vote, weighted by similarity. Confidence is the winning label's share of the vote. k picked on the held-out 10% from 5, 10, 20, 50.
4. **The cascade:** a Listing whose student confidence is at or above τ keeps the student's answer; below τ it goes to Jev (the 6h configuration). τ swept from 0 to 1.
5. **A `--classifier student-softmax` and `student-knn` candidate** in `evaluate run`, so results sit in `eval/report.md` beside Jev's. The cascade is scored offline from the student's and Jev's stored answers, with no new Jev calls on the 1,020 (Jev already answered all of them in 6h and 6j).

### The bar (switched Oct 6)

**6m goes ahead if the cascade beats Jev alone on exact accuracy on the 1,020 with 70% or more of Listings kept local.** The report also says how far that is from the teacher. (The earlier bar, within 2 points of the teacher, can't be met with Jev as the fallback: Opus scores 86.7% raw, Jev 67.5%.)

### What 6l reports (`eval/student.md`)

For each student, on the 1,020 (headline: our Listings) and on the 2,000 Shopify Listings:

- the student alone: exact, top level, two levels, p50 and p99 ms per Listing;
- the cascade curve: for each τ, the share kept local and the system's exact accuracy, against Jev alone and Opus (the teacher), plus a second curve with Opus as the fallback, scored from its stored answers on the 1,020;
- the chosen τ from the held-out 10% and its numbers on the evals.

## User stories

1. As you, I know whether a local student plus Jev beats Jev alone, and how much Jev traffic it saves, before 6m changes the pipeline.
2. As you, I see where on the curve between "always Jev" and "never Jev" the bar is met, if at all.

## Failure scenarios

| Scenario | Expected |
|---|---|
| The model isn't in `models/` | `ModelMissing` naming `--download`, as today; in a worktree, pass `--models` |
| The vector cache is stale (data or model changed) | The cache key covers model and texts, so it is recomputed |
| An eval Category never occurs in training | The softmax head can't answer it; the miss counts, and the report lists how many eval labels are unseen |
| Training diverges (NaN loss) | Training stops with an error naming the step; nothing is written |
| Jev's stored answers don't cover an eval Listing (Shopify 2k) | The cascade on that set needs a Jev run (question 2); without it the Shopify cascade isn't reported |

## Implementation decisions

1. **Eval only:** no worker, Backfill or `taxonomy_version` change; that is 6m.
2. **numpy only:** a softmax layer and a dot product don't need torch or sklearn.
3. **Same embedding and text as the pipeline,** so 6m adds no model.
4. **Thresholds from held-out training data,** never from the evals.

## Testing decisions (test points for your OK)

In `tests/integration/test_student.py`, with a fake `embed` (fixed vectors), as `test_evaluate.py` does:

1. **Softmax head:** learns a separable toy set (3 classes, 2-D vectors) to 100% train accuracy; confidence is a probability in [0, 1]; the same seed gives the same weights.
2. **kNN:** the nearest labeled vector's class wins; a tie in votes goes to the higher similarity; confidence is the winner's vote share.
3. **The cascade:** at τ = 0 every answer is the student's, at τ above every confidence every answer is Jev's; the share kept local matches.
4. **Vector cache:** a second run with the same texts doesn't call `embed`; a changed text does.
5. **The candidate:** `evaluate run --classifier student-knn` returns valid taxonomy paths for every Listing.

## Questions

1. **The bar.** Agreed on Oct 6: with the cascade, exact within 2 points of the teacher, and 70% or more of Listings never reaching Jev. The teacher scores 86.7% raw on the 1,020 (96.6% after adjudication), while the fallback, Jev, scores 67.5%. A student plus Jev can't get within 2 points of Opus unless the student alone is near Opus on the Listings it keeps. I'd judge 6l on the curve instead and keep the bar as the target: **ship (6m) if the cascade beats Jev alone on exact at 70% or more kept local; record how far it is from the teacher.** Keep the original bar, or move to this one?
2. **Jev on the 2,000 Shopify Listings,** so the cascade can be scored there too: 2,000 calls, about 8 minutes, about $0.12. OK?
3. **The fallback.** A second option the curve may show: falling back to Opus through the API instead of Jev (roughly $0.01 a Listing, about 160× Jev's $0.00006), or to nothing (Uncategorized, Backfill retries). Out of scope for 6l unless you want the Opus line on the curve, estimated from its stored answers on the 1,020 at no cost. Add it?
4. **The test points above.** OK?

## Outcome (Oct 6)

**The bar is not met** on the 1,020, nor on Shopify's 2,000 (a tie there). Full curves: [eval/student.md](../../eval/student.md).

| On the 1,020 (exact) | Kept local | System |
|---|---|---|
| Jev alone (production settings) | 0% | 66.9% |
| kNN (k=5) alone | 100% | 52.3% |
| kNN, Jev below τ = 0.25 | 78.7% | 60.3% |
| kNN, Jev below τ = 0.45 | 45.1% | **68.3%** |
| kNN, Opus below τ = 0.45 | 45.1% | 81.0% |
| Softmax alone | 100% | 47.5% |
| Opus alone (the teacher) | 0% | 86.7% |

- **kNN beats softmax** everywhere (53.3% against 43.9% on the held-out rows; top level 72.4% against 66.6% on the 1,020). The softmax head first trained to 25%: 300 steps at lr 0.05 is far too little for 1,676 classes. Tuned on the held-out rows (lr 0.5, l2 1e-5) it reached about 45%; more steps didn't help.
- **The cascade beats Jev alone only when less than half the traffic stays local:** 68.3% at 45% kept, against 66.9%, 15.6 points under the teacher. At 70% or more kept it falls to about 60%.
- **On Shopify's 2,000:** 61.8% at 77.5% kept, a tie with Jev's 61.8%; 63.6% at 67.6% kept. The students do better on Shopify's products (31k of the 33k training rows) than on our Amazon-style Listings, a gap the 2k Opus-labeled Amazon rows only partly close.
- **The threshold rule from the held-out rows** (τ = 0.40 for kNN) keeps 68% local but loses to Jev on the 1,020 (63.9%): the held-out rows are 94% Shopify, whichever way they are split, so they overstate how right the student is on our Listings.
- **Speed:** p50 about 10 ms and p99 15–22 ms per Listing, one at a time, on par with the embedding classifier.
- **Leakage:** training rows whose title is an eval title are dropped (5 of the 1,020's titles appeared in `train/`). The `evaluate run --classifier student-*` candidates still train on all of `train/`.
- **Correction to 6j:** the first Jev run on the 822 read 500 description characters, not production's 200. Rerun at 200: 66.9% (was 67.6%). 6j's conclusion stands; [eval/teacher.md](../../eval/teacher.md) notes it.
- **Jev on Shopify's 2,000:** 61.8% exact, $0.12.
- **Review round 1** fixes: per-source held-out split, eval titles dropped from training, the report's top-level, two-level, p50/p99 and unseen-label figures and its bar verdict, an atomic vector cache that recovers from a truncated file, `pick_tau` saying when no τ qualifies, and `classify([])`.

## Out of scope

- The pipeline (6m), fine-tuning bge-small (only if both students miss), images, non-English.
