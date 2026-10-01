---
name: lean-review
description: Strict but usage-conscious PR review for this repo. Runs 2-3 Sonnet reviewer agents, verifies in-context, and does one round per PR. Use instead of /code-review max when reviewing phase PRs. Never escalates to /code-review ultra.
---

# Lean review

Review target: a PR number (default: the current branch's open PR). Every step is mandatory, and the order matters.

1. **Gather the diff.** Save `gh pr diff <n>` to the session scratchpad. If the diff is over about 600 lines, say the PR should have been split, and suggest the split for next time. Review it anyway.
2. **Deterministic gates first.** `make check` must pass. If the diff touches `src/catalog/` pure modules, run `make mutate` and note any surviving mutants in changed lines. Fix or explain them before using agent tokens.
3. **Finders (parallel, in one message).** Give each one the diff path, the scratch directory and the PR's intent in 3–5 lines:
   - `reviewer-correctness`: always.
   - `reviewer-concurrency`: when the diff touches storage, state, locks, processes or the worker.
   - `reviewer-perf`: only when the diff touches a hot path (worker loop, `landing.read`, `store.read`/`merge`, `events.read`, the API ingest path).
4. **Verify in-context, with no extra agents.** Check each candidate yourself against the code. Mark it CONFIRMED (you can name the trigger), PLAUSIBLE (the mechanism is real but the trigger is uncertain), or REFUTED (quote the line that proves it). Drop the refuted ones.
5. **Report** with ReportFindings (at most 15, most severe first).
6. **Fix** every CONFIRMED or PLAUSIBLE finding, or defer it explicitly to a named later phase in docs/plan-v1.md. Each fix gets a regression test, and you check that the test fails when the fix is reverted. Then re-report with outcomes.
7. **At most one more round, and only if step 5 had a correctness finding.** It's an in-context review of the fix commits only, with no agents. Then stop and record "Review: round N done" in a PR comment.

Never run `/code-review ultra` or `/code-review max` as part of this. Cleanup angles (reuse, simplification, altitude) are not part of a PR review; they run once at the end of the project.
