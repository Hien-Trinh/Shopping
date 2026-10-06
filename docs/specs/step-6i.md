# Step 6i: 800 more labeled Listings, labeled by Claude Sonnet 5.5 in session (mini PRD)

Status: done Oct 5 (see Outcome); approved Oct 5. Question 1: download OK. Question 2: the adjudicated gate. Question 3: a separate file. Question 4: about 11 agents OK. No code, so no test points. Plan row: new, [plan-v1.md, PR steps, 6i](../plan-v1.md). Builds on [step-6e.md](step-6e.md) (the labeled set and `evaluate sample`), [step-6g.md](step-6g.md) (shortlist recall) and [step-6h.md](step-6h.md) (the deeper Category text). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

The eval set is 198 Listings (`eval/labels.jsonl`), 6 per Amazon category. At that size one Listing moves exact accuracy by 0.5 points, so the gaps between recipes in 6g and 6h (a few points) are within noise.

## Solution: one data PR, no code

The labeling runs the way the 198 were done: in a Claude Code session on your Max plan, not through the API. Nothing is added to the project: no subcommand, no dependency, no key.

1. **The draw**: `evaluate sample --per-category 28 --seed 1` into the session's scratch dir. A one-off filter drops any `id` already in `eval/labels.jsonl` and keeps the first 25 per Amazon category: about 825 Listings. (`sample` has no exclude option, and this step doesn't need one.)
2. **The labelers**: Sonnet 5.5 subagents (the Agent tool with `model: sonnet`), about 100 Listings each, so about 9 agents. Each one gets:
   - the path to the committed taxonomy (`src/catalog/data/shopify-taxonomy.txt`, 1,862 paths) and is told to pick from it, plus Shopify's full taxonomy (from `shopify-taxonomy-full.txt.gz`) to search for the deepest node that fits, answering with that node's level-3 ancestor (added after the 198 check: 13 of Sonnet's 17 errors were items whose node sits below level 3);
   - 6e.1's rule for my labels: the deepest path that fits (up to 3 levels), copied word for word, or `none` if no Category fits; the Amazon category as a hint;
   - its batch, and a scratch file to write `{"id", "category", "confident"}` lines to;
   - never any classifier's answer (6e.1's rule), so the labels don't lean toward the embedding shortlist or Jev.
3. **Checked in session after each agent**: every `category` must be a taxonomy path or `none`, and every `id` in the batch must be answered. A batch that fails goes back to a new agent once.
4. **The check on the 198 first**: two agents label the 198 blind, as above. I compare with `eval/labels.jsonl` (exact, first two levels, top level), then adjudicate every item where they differ against Shopify's full taxonomy, sorting it into **Sonnet wrong**, **hand label wrong** or **both fit**. You see only the items I can't sort, with my default. The gate, both needed:
   - Sonnet clearly wrong on 15% or fewer of the 198 (30 items or fewer), after adjudication;
   - top-level agreement of 90% or more.

   I stop to show you the numbers before the 800 run. Hand labels found wrong are listed in the PR, not changed (out of scope).
5. **My pass** (as for the 198): I research every `confident: false`, every `none`, and a random 50 of the rest against Shopify's full taxonomy, fix the ones I'm sure of, and ask you only about the ones that stay ambiguous, with my default.
6. **Commit** `eval/labels-sonnet.jsonl`: the sample fields plus `category` and `labeler: "claude-sonnet-5-5"`, a file of its own so the 198 stay the hand-checked reference and 6e–6h's results stay comparable. `evaluate run --labels` already takes either file. The 198 check's numbers and my corrections go in the PR.

## User stories

1. As you, I get about 1,000 labeled Listings in place of 198, so recipe and threshold choices rest on differences bigger than the noise.
2. As you, I know how often Sonnet agrees with the hand-checked labels before the 800 are labeled.
3. As you, I only look at the handful of items that stay ambiguous after Claude's checks.

## Failure scenarios

| Scenario | Expected |
|---|---|
| An agent invents a path, or leaves an id out | The in-session check finds it; the batch goes to a new agent once, then I label what's left myself |
| An agent stops partway (usage limit, error) | Its scratch file keeps what it wrote; a new agent gets only the missing ids |
| The Max plan's usage limit is hit mid-run | The run waits for the limit to reset; finished batches are kept |
| Sonnet clearly wrong on over 15% of the 198, or top-level agreement under 90% | The 800 aren't labeled; I show the adjudication and we decide (prompt, Opus agents, or hand labels) |
| The new draw hits an id of the 198 | The filter drops it |
| An Amazon category has under 25 usable items in its first 1,000 lines | Its count is lower; the PR says which |

## Implementation decisions

1. **In session, not in the project**: the labeler runs once, so code for it is code nobody runs again. The Max plan covers it; no API key or spend.
2. **Agents read the whole taxonomy**, not a shortlist: a shortlist would bias labels toward the embedding (what 6g measures).
3. **About 100 per agent**: enough that the taxonomy read (about 45k tokens) is a small share, few enough that one agent's context holds the batch and its answers.
4. **A separate file**: hand-checked and model-labeled sets mean different things; the report can show both.

## Testing decisions

- No test points: no code changes. The checks are the in-session validation (step 3), the 198 comparison (step 4) and my pass (step 5).

## Questions (answered Oct 5)

1. **The download**: like 6e's draw, about 33 × 1,000 lines streamed from the Amazon Reviews '23 files (about 100 MB, nothing kept but the 825). OK? **Yes.**
2. **The gate**: label the 800 only if Sonnet matches the 198 on at least 60% exact. 60% because the hand labels themselves have ambiguous cases. OK, or a different line? **Adjudicated instead: Sonnet clearly wrong on 15% or fewer of the 198, and top-level agreement of 90% or more (step 4). Raw agreement would count ambiguous items and hand-label mistakes against Sonnet. About 30–60 minutes of my review; you see only the unclear ones.**
3. **A separate `eval/labels-sonnet.jsonl`**, rather than appending to `eval/labels.jsonl`. OK? **Yes.**
4. **Usage**: about 11 Sonnet agents, very roughly 1.5M tokens of your Max plan usage in all. OK? **Yes.**

## Outcome (Oct 5)

- **The 198 check** (3-level file only): exact 165, two levels 178, top level 183 (92.4%). Of the 33 differences: Sonnet wrong 17 (8.6%), hand label wrong 4 (a screen protector, two Prime Video titles labeled DVDs, a cat repellent spray, which Sonnet also missed), both fit 11, unclear 2 (kept as hand-labeled). The gate passed. 13 of Sonnet's 17 errors had their node below level 3, so the 800 run searched the full taxonomy too.
- **The 825**: every answer a taxonomy path, none missing. 217 unsure, 2 `none`. My pass over the unsure ones and a random 50 of the rest fixed 5 (2 of the 50, so about 4% errors among the sure ones), and dropped 3 with no usable text ("de Years", "Brand New", "Acepstar Lightning"): 822 committed.
- **Subscription boxes** (your rule): the Category of what's in the box when it holds one kind of goods, `Product Add-Ons > Subscription Services` only for mixed or mystery boxes. 4 changed.
- The hand labels found wrong in the 198 stay as they are (out of scope).

## Out of scope

- Rerunning 6e–6h's experiments on the bigger set (a later step, once the labels are in).
- A labeler in the project, or any API spend.
- Changing any label in the 198.

## Size

Data only: `eval/labels-sonnet.jsonl`, about 825 lines.
