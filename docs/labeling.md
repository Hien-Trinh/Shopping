# Labeling Listings with Claude agents

How to label Listings with Shopify Categories using Claude Code agents on the Max plan: no API key, no spend, no project code. Used three times so far:

| Run | Labeler | Listings | Usage | Wall time | Result |
|---|---|---|---|---|---|
| [6i](specs/step-6i.md) | Sonnet 5.5, 9 agents | 825 | about 1.5M tokens | about 1 h | `eval/labels-sonnet.jsonl`; 7% errors among sure labels on the first audit, 1–2% after the level-3 fix |
| [6j](specs/step-6j.md) | Opus 5.5, 11 agents | 1,020 | about 1.45M tokens | about 3 min (parallel) | `eval/results/opus-agents.jsonl`; 96.6% after adjudication ([eval/teacher.md](../eval/teacher.md)) |
| [6k.2](specs/step-6k.md) | Opus 5.5, 22 agents, then 7 Sonnet 5.5 agents for a second opinion | 2,013 | about 2.8M + 0.8M tokens | about 35 min | `train/amazon-opus.jsonl`; 62 labels changed in review; 1 of 100 random sure labels wrong |

## 1. Draw the Listings

`python -m catalog.evaluate sample --per-category N --seed S > sample.jsonl` (6e.1): up to 1,000 lines streamed per Amazon category, N drawn at random, fields `id`, `title`, `description`, `amazon_category`. Drop any `id` already in a label file. Keep everything in the session's scratch directory until the commit.

## 2. Make blind batches

- Only `id`, `title`, `description`, `amazon_category`. Never a label, a classifier's answer, or which set an item comes from.
- Shuffle, then about 90–100 per agent: few enough that one context holds the batch and its answers, enough that the taxonomy reads (about 45k tokens) are a small share.
- Put plain path lists beside the batches, so agents never need the repository: `taxonomy-3.txt` (the 1,862 allowed answers, from `src/catalog/data/shopify-taxonomy.txt`) and `taxonomy-full.txt` (14,606 paths, from `shopify-taxonomy-full.txt.gz`), each line split on `" : "` and the path kept.

## 3. The brief (6j's, word for word)

Write it to `brief.md` beside the batches; each agent's prompt is only "read and follow brief.md" with `{DIR}`, `{BATCH}` and `{OUT}` filled in.

```markdown
# Labeling brief (step 6j)

You label product Listings with a category from Shopify's product taxonomy (release 2026-08).

## Files you may read (and nothing else)
- Your batch: {BATCH} (JSON lines: id, title, description, amazon_category)
- {DIR}/taxonomy-3.txt: the 1,862 allowed answers, one path per line, cut to 3 levels
- {DIR}/taxonomy-full.txt: the full release (14,606 paths, up to 8 levels), for searching only

Do not open any other file: nothing in any repository, no eval or label files, no results, no other batch or answer file. Labels must be blind to every other labeler and classifier.

## For each Listing
1. Work out what the product is from the title and description. `amazon_category` is a hint only (it is the Amazon store section, often broad or wrong).
2. Search taxonomy-full.txt (grep is fine) for the deepest node that fits the product.
3. Answer with that node cut to its first 3 levels, which must be a line of taxonomy-3.txt copied word for word. If the deepest fitting node is at level 1 or 2, answer it as is, but check first: a level-3 category that fits must be preferred over its parent.
4. If no category fits at all, answer `none`.
5. Subscription boxes: the category of what's in the box when it holds one kind of goods; `Product Add-Ons > Subscription Services` only for mixed or mystery boxes.

`confident`: true if you'd bet the answer is right; false if two categories fit about equally, the text is too thin, or you're unsure of the branch.

## Output
Append one JSON line per Listing to {OUT}, as you go (don't hold them all to the end):
{"id": "<id>", "category": "<path or none>", "confident": true}

Answer every id in your batch exactly once. When done, check: every line parses, every category is a line of taxonomy-3.txt or `none`, every id is answered. Fix any problem, then reply with only: the count answered, the count not confident, and the count `none`.
```

## 4. Run the agents

The Agent tool, `subagent_type: general-purpose`, `model: sonnet` or `opus`, `run_in_background: true`, all in one message so they run in parallel. Each replies with three counts; the files are the result.

## 5. Check every batch in session

Before scoring anything: every line parses; every `category` is a line of `taxonomy-3.txt` or `none`; every `id` of the batch answered exactly once, none extra. A failed batch goes to a new agent once (only the missing ids); then label what's left yourself and say so in the PR.

## 6. Score

Wrap the answers in `run()`'s result shape with `confidence: 1.0` and call `catalog.evaluate.score(result, [0.0])` (exact, top level, two levels). Agents give no latencies, so their numbers go in their own report, not `eval/report.md` (`report` needs per-call seconds). Compare with a classifier at threshold 0, since agents always answer.

## 7. Adjudicate

Every item where the agent differs from the reference label, checked against `taxonomy-full.txt`, goes into one of:

- **Agent wrong**: the label fits better, including the agent stopping above level 3 when a level-3 Category fits.
- **Label wrong**: the agent's answer fits and the label doesn't. List these; don't change the label file in the same step.
- **Both fit**: the text can't settle it (DVD or digital download, CD or download, print or e-book), sibling Categories that overlap (Vinyl, Records & LPs), or a product that sits in two branches.

Only items you can't sort go to the user, each with a default.

**Bias guard:** the reference labels are Claude-made too (6e drafted by Claude and checked by the user, 6i by Sonnet), so when comparing Claude with another classifier, also recheck the items where the label agrees with Claude and the other classifier differs, and give it the same "both fit" credit.

## 7b. Review with a second opinion (6k.2)

For a training set, where there is no reference label: give the items to review (every `confident: false`, every answer above level 3 that isn't a leaf such as `Gift Cards`, and a random 100 of the rest) to a second labeler, blind, with the same brief: Sonnet if Opus labeled. Where both agree, keep the label; adjudicate only where they differ (6k.2: 210 of 594). The random 100's disagreements measure the error among the sure labels.

## 8. Audit a sample (when the labels become a label file)

From 6i: research every `confident: false`, every `none`, and a random 50–100 of the rest against the full taxonomy. Report the error rate among the sure labels with its 95% interval (6i: 7 of 100, about 3–14%).

## Known failure modes

1. **Stopping above level 3** when a level-3 Category fits: both Sonnet (6i) and Opus (6j, 21 of 34 errors). After any run, recheck every answer above level 3.
2. **Subscription boxes:** the rule is the Category of what's in the box when it holds one kind of goods, `Product Add-Ons > Subscription Services` only for mixed or mystery boxes. Opus still put 4 single-kind boxes under Subscription Services.
3. **Format:** Amazon's `Movies_and_TV` and `Digital_Music` hold both physical and digital items; the text rarely says which.
4. **`confident` is informative:** in 6j, confident answers were 94–95% exact, unsure ones 52–63%. Spend review time on the unsure ones.

## Gotchas

- At most 20 agents run at once in one session; start the rest as the first ones finish.

- In a worktree, `models/` is empty: pass `--models /path/to/main/checkout/models` to `evaluate run` rather than downloading again.
- The eval name of the Jev candidate is `jev-shortlist`; plain `jev` (the worker's name) falls through to Laya and fails with "Laya won't load".
