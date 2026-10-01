---
name: lean-review
description: Strict PR review for this repo with 10 Sonnet finder agents, in-context verification and one round per PR. Use instead of /code-review max when reviewing phase PRs. Never escalates to /code-review ultra.
---

# Lean review

Review target: a PR number (default: the current branch's open PR). Every step is mandatory, and the order matters.

1. **Gather the diff.** Save `gh pr diff <n>` to the session scratchpad. If the diff is over about 300 changed lines (tests included), say the PR should have been split, and suggest the split for next time. Review it anyway.
2. **Deterministic gates first.** `make check` must pass. If the diff touches `src/catalog/` pure modules, run `make mutate` and note any surviving mutants in changed lines that aren't in plan-v1.md's equivalent list. Fix or explain them before using agent tokens.
3. **Finders: all 10, in parallel, in one message.** Give each one the diff path, its own scratch subdirectory (`<scratch>/<angle>/`, so scripts don't collide) and the PR's intent in 3–5 lines. A finder with nothing in its lane says "none" quickly.
   - `reviewer-correctness`: main-path logic and cross-file contracts
   - `reviewer-concurrency`: processes, crashes, restarts, locks
   - `reviewer-perf`: hot paths at the design's scale
   - `reviewer-tests`: tests that would pass with the code broken
   - `reviewer-design`: drift from the design doc, ADRs, CONTEXT.md and plan rules
   - `reviewer-failure`: error handling and partial failure
   - `reviewer-security`: trust boundaries, injection, isolation, resource exhaustion
   - `reviewer-data`: schema, serialization, time and hash stability
   - `reviewer-observability`: events and metrics needed for status, SLOs and debugging
   - `reviewer-edge`: boundary and adversarial inputs
4. **Dedupe, then verify in-context, with no extra agents.** Merge candidates that name the same mechanism (two finders agreeing raises confidence). Check each one yourself against the code. Mark it CONFIRMED (you can name the trigger), PLAUSIBLE (the mechanism is real but the trigger is uncertain), or REFUTED (quote the line that proves it). Drop the refuted ones.
5. **Report** with ReportFindings (at most 15, most severe first).
6. **Fix** every CONFIRMED or PLAUSIBLE finding, or defer it explicitly to a named later step in docs/plan-v1.md. Each fix gets a regression test, and you check that the test fails when the fix is reverted. Then re-report with outcomes.
7. **At most one more round, and only if step 5 had a correctness finding.** It's an in-context review of the fix commits only, with no agents. Then stop and record "Review: round N done" in a PR comment.

Cost: about 25–30k Sonnet tokens per finder, so roughly 300k per review. Never run `/code-review ultra` or `/code-review max` as part of this. Cleanup angles (reuse, simplification, altitude) are not part of a PR review; they run once at the end of the project.
