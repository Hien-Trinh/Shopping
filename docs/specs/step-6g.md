# Step 6g: Shortlist recall (mini PRD)

Status: approved Oct 4, with the test points. Plan row: [plan-v1.md, PR steps, 6g](../plan-v1.md). It comes from the classification-methods research (Oct 4), whose first recommendation is to measure how often the labelled Category is in the embedding shortlist before spending on anything else. Builds on [step-6d.md](step-6d.md) (`evaluate`, the labelled set) and [step-6e.md](step-6e.md) (`EmbeddingClassifier.top`, the Jev shortlist of 50). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

Jev after a shortlist of 50 gets 54.0% of the 198 labelled Listings exactly right, and only 74.7% right at the top level. We can't tell which step loses the rest:

- **Retrieval misses:** the labelled Category never reaches the shortlist, so no chooser could pick it. The fixes are retrieval fixes: Category definitions, a better embedding model, or labelled neighbours.
- **Chooser errors:** the Category is in the shortlist and Jev picks another. The fixes are on the choosing side: definitions in the prompt, or a stronger chooser.

The research ranks these fixes differently depending on that split. Measuring it needs no model download, no paid call and no new dependency: the embedding model is already in `models/` and Jev's answers are already in `eval/results/`.

## Solution

1. `evaluate.recall(shortlists, labels, ks) -> dict`, a pure function. For each k it reports the share of Listings whose labelled Category is in the first k paths, at three depths: the exact path, the same first two levels, and the same top level.
2. `evaluate.misses(result, shortlists, sha, description) -> dict`, a pure function. `shortlists` maps each labelled id to its shortlist. `sha` is the labels' hash, and `description` the characters the shortlist read. k comes from the result's own `shortlist`. It refuses a result it can't match (the failure rows below). It splits a stored result's wrong answers, at threshold 0, into "the label was in the first k" (chooser error) and "it wasn't" (retrieval miss).
3. A `recall` subcommand: `python -m catalog.evaluate recall --description 200 --ks 1 5 10 20 50 100 200 [--split eval/results/jev-shortlist50-d200.json]`.
   - It loads the labels.
   - It builds the shortlist once with `EmbeddingClassifier.top` at the largest k, the same call Jev's shortlist uses, so the measurement matches the pipeline.
   - It writes `eval/recall.md`: one table of recall by k and depth, plus the miss split for each `--split` file, at the k that file's `shortlist` names.
   - It writes atomically, like `report`.
4. I run it once and commit `eval/recall.md`. The PR states the split and which fix it points to.

## User stories

1. As you, I see what share of Listings have their Category in the top 10, 50, 100 and 200 Categories, so I know whether a bigger shortlist alone helps.
2. As you, I see of Jev's 90 wrong answers (at threshold 0) how many were retrieval misses and how many were chooser errors, so the next step is chosen by evidence.
3. As you, I can rerun the measurement after changing the retrieval (definitions, another model) and compare tables.

## Failure scenarios

| Scenario | Expected |
|---|---|
| The model isn't in `models/` | `ModelMissing`, the existing message naming `--download`; no file written |
| A `--split` file comes from another label set (its `labels_sha256` differs) | ValueError naming the file; no file written: its ids would be scored against the wrong labels |
| A `--split` file's `description` differs from `--description` | ValueError naming both values: the shortlist it saw differs from the one rebuilt |
| A `--split` file has no `shortlist` (an embedding or hierarchical run) | ValueError naming the file: there is no shortlist to split on |
| A `--split` file's ids differ from the labels' | ValueError naming the first missing id, in label order |
| `--ks` stops below a `--split` file's shortlist size | The shortlist is built to the deeper of the two, so chooser errors are never counted as misses (review fix) |
| A k is below 1 or above the number of Categories (1,862) | Usage error |
| The same k given twice, or out of order | Sorted and deduplicated |
| Two Categories tie in similarity | The stable order `top` already uses, so reruns agree |
| The run is killed while writing | The old `eval/recall.md` stays, from the temp file then `os.replace` |
| A threshold | None: recall is about the shortlist, and the split counts every wrong answer at threshold 0, so Uncategorized at a later threshold doesn't hide a miss |

## Implementation decisions

1. **Rebuild the shortlist, don't store it.** Results files don't record shortlists. Embeddings are deterministic on one machine, so the rebuilt top 50 is the one Jev saw. The `description` check above guards the one setting that changes it. Storing shortlists in every result would grow every file for one analysis.
2. **One `top` call at the largest k.** The first k paths of the top 200 are the top k, so all ks come from one embedding pass, about 2 s for 198 Listings.
3. **Three depths.** Exact recall answers "could any chooser get it right". Top-level recall answers "does the shortlist even reach the right branch", which the research flagged from the 74.7% top-level score.
4. **A separate `eval/recall.md`, not a section of `report.md`.** `report` reads only result files and needs no model; keeping it that way keeps it instant and offline.
5. **Not a plan row's code step.** It's a measurement, so it lands as one PR with the code, its tests and the committed `eval/recall.md`, auto-merge off like any code PR.

## Testing decisions

- **Test points (seams), to confirm:**
  1. **`recall` and `misses`, pure** (new, red first, `tests/integration/test_evaluate.py` beside the other evaluate tests). Hand-made shortlists and labels. Covers:
     - a hit at rank k counts for k and above, but not below;
     - a two-level match counts at depth 2 but not exact;
     - a top-level match counts at depth 1 only;
     - a wrong answer whose label is in the first k is a chooser error;
     - one whose label isn't is a retrieval miss;
     - a correct answer is neither;
     - mismatched ids, label set or description raise, naming the file.
  2. **The `recall` command** (new, red first, same file). `classify.fastembed` is monkeypatched with tiny hand-made vectors, as `test_the_embedding_candidate_runs_at_threshold_0_without_a_budget` already does. It writes `recall.md` with the expected numbers. A bad k is a usage error. A failing `--split` leaves an existing `recall.md` untouched.
- No `model`-marked test: the real run is the one I commit, and the code path is the same as test point 2's with the real `fastembed`.
- **Coverage:** the new lines are covered by test points 1 and 2, keeping the 90% package gate.

## Out of scope

- Any retrieval change: Category definitions, another embedding model, kNN over labelled Listings (the research's next steps 3, 4 and 6, each its own step if this split points to them).
- A larger eval set from Shopify's product catalogue (research step 2).
- Changing 6f: if retrieval misses dominate, 6f's shortlist size may change, and that is asked in 6f's review, not decided here.

## Size

About 50 lines in `evaluate.py`, 70 of tests, plus the generated `eval/recall.md`.
