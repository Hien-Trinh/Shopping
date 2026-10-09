# Step 6j: Opus 5.5 against Jev on the 1,020 labeled Listings (mini PRD)

Status: done Oct 6 (see Outcome); approved Oct 6. Question 1: Jev on the 822 OK. Question 2: about 11 Opus agents OK. Question 3: the third bias guard, yes. Question 4: `docs/labeling.md`, yes. No project code, so no test points. Plan row: new, [plan-v1.md, PR steps, 6j](../plan-v1.md). The first of four steps toward a local classifier distilled from a teacher (6j to 6m, below). Builds on [step-6e.md](step-6e.md) (the 198 and the report), [step-6h.md](step-6h.md) (Jev with the deeper texts) and [step-6i.md](step-6i.md) (the 822 and the in-session labeling method). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

The plan is to train a fast local classifier (a "student") on a stronger model's answers (a "teacher") and send only its unsure Listings to Jev. A student can't beat its teacher, so the teacher has to be the best labeler we can get. Jev today: 66.7% exact on the 198 ([eval/report.md](../../eval/report.md)), and 60 of its 90 misses are shortlist misses ([eval/recall.md](../../eval/recall.md)). Claude Opus 5.5 reading the whole taxonomy has no shortlist to miss. Nobody has measured it.

## The road (6j to 6m), agreed Oct 6

| Step | What | Gate |
|---|---|---|
| **6j** (this) | Opus 5.5 labels the 1,020 blind; scored against the labels beside Jev | Picks the teacher |
| 6k | Shopify's benchmark: a 2k English eval slice from its test split and about 31k English training rows from its train split, cut to level 3 with a small rename table (98.5% match as is); plus about 2k Amazon Listings labeled by the teacher. Its 8–9 candidate categories per row are never used | Training and eval sets |
| 6l | Student prototype: a softmax head and kNN over the bge-small vectors, eval only | The student clears the bar: with the cascade, exact within 2 points of the teacher and 70% or more of Listings never reach Jev |
| 6m | The student in the pipeline: a new `--classifier` kind, unsure Listings to Jev | Your review |

## Solution: one data PR, no project code

Like 6i, Opus runs as Claude Code agents on your Max plan, not through the API: no key, no spend, no subcommand.

1. **Jev on the 822** (it has only run on the 198): `python -m catalog.evaluate run --classifier jev --labels eval/labels-sonnet.jsonl --shortlist 50 --texts deeper --name jev-shortlist50-d200-deeper-sonnet` with 6h's settings. 822 calls, about 4 minutes, about $0.05 on your `TYPESAFE_API_KEY`.
2. **The labelers:** Opus 5.5 subagents (the Agent tool, `model: opus`), about 100 Listings each, so about 11 agents for the 1,020. Each gets exactly what 6i's Sonnet agents got: the committed taxonomy (1,862 paths) and the full release to search for the deepest node, answering with its level-3 ancestor; the rule of the deepest path that fits, copied word for word, or `none`; the Amazon category as a hint; its batch; a scratch file for `{"id", "category", "confident"}` lines. **Never** any label, Jev answer or other classifier's answer.
3. **Checked in session after each agent**, as in 6i: every `category` a taxonomy path or `none`, every `id` answered; a failed batch goes to a new agent once.
4. **Scored in session** with `catalog.evaluate.score` (confidence 1.0 for every answer, since agents give no probability), exact, two levels and top level, on the 198 and on the 822, beside Jev's results. Opus doesn't go into `eval/report.md`: `report` needs per-call latencies, which agents don't have, and faking them would mislead.
5. **Adjudication on the 822:** every Listing where Opus and the Sonnet label differ, I sort against Shopify's full taxonomy into **Opus wrong**, **label wrong** or **both fit**, as in 6i. You see only the ones I can't sort, with my default. Wrong labels found are listed, not changed (out of scope, as in 6i).
6. **The gate** (agreed): Opus is the teacher if it beats Jev by **5 points or more exact on the 1,020** (after adjudication on the 822) **and** is no worse than Jev on the 198. Otherwise Jev is the teacher, as roadmap item 8 of [classification-methods.md](../research/classification-methods.md) had it.
7. **Commit:**
   - `eval/teacher.md`: the table (Opus vs Jev on the 198, the 822 raw and adjudicated, the 1,020), the adjudication counts and the decision;
   - `eval/results/opus-agents.jsonl`: Opus's answers (`id`, `category`, `confident`, `labeler: "claude-opus-5-5"`), so 6k can reuse them;
   - Jev's run on the 822 in `eval/results/`;
   - **`docs/labeling.md`**, the method for later runs (your ask): the draw, the agent brief word for word, the in-session checks, the 198 check, the adjudication sort, the audit sample and its error interval, and what each run cost in usage. 6i and 6j are its two worked examples.

## User stories

1. As you, I know whether Opus labels better than Jev before anything is trained on its answers.
2. As you, the number I decide on isn't inflated by Claude agreeing with Claude-made labels.
3. As you, the next labeling run follows a written method, not this chat.

## Failure scenarios

| Scenario | Expected |
|---|---|
| An agent invents a path or leaves an id out | The in-session check finds it; one retry with a new agent, then I label what's left and mark it in the PR |
| An agent stops partway (usage limit, error) | Its scratch file keeps what it wrote; a new agent gets only the missing ids |
| The Max plan's usage limit is hit mid-run | Wait for the reset; finished batches are kept |
| Jev's key is missing or revoked (401) | `run` stops before writing a result; you fix the key and rerun step 1 alone |
| Jev times out or errors on some calls | Same as 6h: the run fails loudly and is rerun, never scored with holes |
| An agent sees a label or Jev answer by mistake | Its batch is thrown away and redone blind |
| Opus and Jev land within noise | The gate fails; Jev stays teacher; the table is committed anyway |

## Implementation decisions

1. **In session, not the API** (your call, Oct 6): free under the Max plan, and nothing in the project is run twice. The API path waits until a step needs Opus at runtime.
2. **Same brief as 6i's Sonnet agents**, so the only thing that changes is the model.
3. **Bias:** the 822 were labeled by Sonnet with the same method, and the 198 were drafted by Claude in session (then checked by you, batch by batch, in 6e). Claude may agree with Claude where Jev, equally right, doesn't. Two guards: the 198, which you checked, is the headline; on the 822 every Opus disagreement is adjudicated. A third (question 3, approved): on the 198, where Opus and Jev disagree and the label sides with Opus, I recheck against Shopify's full taxonomy; where Jev's answer fits equally, it counts as "both fit".
4. **A separate `eval/teacher.md`**, not a row in `eval/report.md` (step 4).

## Testing decisions

- No test points: no project code. The checks are the in-session validation (step 3), the adjudication (step 5) and the gate (step 6).

## Questions

1. **Jev on the 822** (step 1): about $0.05 on your key. OK?
2. **Usage:** about 11 Opus agents, very roughly 2M tokens of Max plan usage (6i's Sonnet run was about 1.5M). OK?
3. **A third bias guard:** on the 198, where Opus and Jev disagree and the label sides with Opus, I recheck each against Shopify's full taxonomy and list any where Jev's answer fits equally. Those count as "both fit", not as Jev wrong. OK, or skip it?
4. **`docs/labeling.md`** at the top of `docs/`, beside the runbook. OK, or somewhere else?

## Outcome (Oct 6)

- **Opus 5.5 is the teacher.** Exact on the 1,020: 96.6% adjudicated (86.7% raw) against Jev's 67.5%. On the 198: 95.5% adjudicated against Jev's 76.8% after the bias guard (66.7% raw). Full table and every adjudicated item: [eval/teacher.md](../../eval/teacher.md).
- **The run:** 11 Opus agents, 1,020 answered, every answer a taxonomy path, no `none`, no batch redone; 230 unsure. About 1.45M tokens, about 3 minutes in parallel. Jev on the 822: 67.6% exact, $0.053.
- **Adjudication:** 198: 28 differences (8 Opus wrong, 3 label wrong, 17 both fit). 822: 108 (26, 5, 77). Bias guard on the 198: Jev counted right on 16 of 49.
- **Opus's errors:** 21 of 34 are stopping above level 3 when a level-3 Category fits; 4 single-kind subscription boxes under Subscription Services. Its `confident` flag separates 94–95% from 52–63% exact. For 6k: recheck unsure answers and every answer above level 3.
- **Label errors found** (listed in `eval/teacher.md`, not changed): 3 in the 198 (two phone apps that belong under Handheld & PDA Software, a mystery box) and 5 in the 822.
- [docs/labeling.md](../labeling.md) written, with 6i and 6j as its examples.

## Out of scope

- Opus through the API, and Opus at runtime.
- Changing any label (wrong ones are listed only).
- 6k to 6m.
