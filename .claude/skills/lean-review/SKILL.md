---
name: lean-review
description: Strict PR review for this repo. Sonnet finder agents scaled to the diff size (1 to 11), including a spec lane and a standards lane after Matt Pocock's two-axis /code-review; in-context verification plus at most 5 Sonnet verifiers; one round per PR. Use instead of /code-review max when reviewing phase PRs. Never escalates to /code-review ultra.
---

# Lean review

Review target: a PR number (default: the current branch's open PR). Every step is mandatory, and the order matters.

1. **Gather the diff and size it.** Save `gh pr diff <n>` to the session scratchpad. Count changed lines (additions + deletions, tests included) from `gh pr view <n> --json additions,deletions,files`. If it is over about 300, say the PR should have been split, and suggest the split for next time. Review it anyway.
2. **Deterministic gates first.** `make check` must pass. If the diff touches `src/catalog/` pure modules, run `make mutate` and note any surviving mutants in changed lines that aren't in plan-v1.md's equivalent list. Fix or explain them before using agent tokens.
3. **Finders, scaled to the diff, in parallel.** Run the `lean-review` workflow (`.claude/workflows/lean-review.js`), which picks the finders from the size:

   | Changed lines | Finders |
   | --- | --- |
   | docs or tooling only (no `src/` or `tests/`) | `reviewer-spec` |
   | 1–50 | `reviewer-correctness`, `reviewer-tests`, `reviewer-failure`, `reviewer-spec` |
   | 51–150 | those 4 + `reviewer-standards` + `reviewer-concurrency` if storage, state, events or worker code changed, else `reviewer-edge` |
   | over 150 | all 11 (adds perf, security, data, observability) |

   `reviewer-spec` asks Matt Pocock's three spec questions (missing or partial, not asked for, implemented but wrong), quoting the spec line. `reviewer-standards` checks the plan's documented coding rules. Both return two kinds of output: **candidates** (defects, like every finder) and **asides**: scope decisions for the user (spec) and code-smell judgement calls (standards).

   Give it the diff path, the scratch directory, the PR's intent in 3–5 lines, and optional per-angle hints. Each finder gets its own scratch subdirectory.
4. **Dedupe, then verify.** The workflow merges duplicate candidates. It sends one Sonnet verifier (reproduce, else refute) only for medium- or high-severity candidates that no finder reproduced, at most 5. Check every candidate yourself against the code, including the ones verifiers refuted, before dropping anything. Mark each CONFIRMED (you can name the trigger), PLAUSIBLE (the mechanism is real but the trigger is uncertain), or REFUTED (quote the line that proves it). Drop the refuted ones.
5. **Report** the verified candidates with ReportFindings (at most 15, most severe first). Then list the asides in text, apart from the findings and never ranked with them, because a decision or a smell isn't a defect: the spec decisions as questions for the user, and the standards judgement calls as a short list.
6. **Fix** every CONFIRMED or PLAUSIBLE finding, or defer it explicitly to a named later step in docs/plan-v1.md. Each fix gets a regression test, and you check that the test fails when the fix is reverted. Then re-report with outcomes.
7. **At most one more round, and only if step 5 had a correctness finding.** It's an in-context review of the fix commits only, with no agents. Then stop and record "Review: round N done" in a PR comment.

Cost: about 25–30k Sonnet tokens per finder, plus up to 5 verifiers. A 2.4M-token review of a 21-line docs-and-pruning PR (10 finders, 24 verifiers, no code findings) is why this scales. Never run `/code-review ultra` or `/code-review max` as part of this. Cleanup angles (reuse, simplification, altitude) are not part of a PR review; they run once at the end of the project.
