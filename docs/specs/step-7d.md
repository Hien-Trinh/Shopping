# Step 7d: `stress-smoke` in CI, and the full stress runs (mini PRD)

Status: approved Oct 5, with the test points as written. Question 1: the matrix. Question 2: I add the required check. Question 3: Jev for the SLO run (no Backfill) and the 10k bulk (after a reset). Question 4: 60 s. 7d.2's runs wait until you ask for them. **Amended Oct 7:** step 6m replaces Jev with the student alone (τ = 0, [step-6m.md](step-6m.md)), so 7d.2's runs on the shipped classifier (points 3 and 4) move behind 6m.2 and run on the student; points 1 and 2 (the 1M load on `fake`, the six costs) don't depend on the classifier and run first, as 7d.2a. The Phase 7 exit is measured on the student. The amended points are marked below; the Oct 5 text stays for the record. Plan row: [plan-v1.md, PR steps, 7d](../plan-v1.md) (the `stress-smoke` CI job, then full stress runs on your Mac; from the 7c.2 review, time `catalog.metrics` over a 24 h window). Phase: [plan-v1.md, Phase 7](../plan-v1.md) (the 1M initial load; the five costs to measure at full size; exit: every scenario ends with the three oracles passing, and p99 freshness under 5 minutes at 50/s). CI: [plan-v1.md, CI](../plan-v1.md) (`stress-smoke`: 60 s of every chaos scenario at low rate, every PR, required). Builds on [step-7a.md](step-7a.md) (the load generator), [step-7b.md](step-7b.md) (the chaos runner) and [step-7c.md](step-7c.md) (metrics and the runbook). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Phase 7's exit isn't shown yet. Today:

- The ten chaos scenarios run only by hand, on this Mac (`tests/stress`, marked `slow`, is skipped by `make check`). A PR can break one and still merge. They have never run on Linux.
- Nothing has run at full size: no 1M initial load, no 50/s run long enough for a p99 that means something, and never on the shipped classifier (Jev).
- Phase 7 lists five costs to measure at 1M Listings (`store.read` decoding every column, MERGE file churn, offset fsyncs, `CommitFailedError` between workers, `landing.read` re-reading a bulk commit), and the 7c.2 review added a sixth (`catalog.metrics` parsing each event three times). None is measured at that size.

## Solution: two PRs

### 7d.1: the `stress-smoke` job (tooling)

1. **A matrix job in `ci.yml`**, one runner per scenario, all ten at once on `ubuntu-latest`:

   ```yaml
   stress-smoke:
     strategy: { fail-fast: false, matrix: { scenario: [steady, bulk, …] } }
     steps: checkout, setup-uv, then
       uv run python -m catalog.chaos ${{ matrix.scenario }} --seconds 60 --dir run
   ```

   At the runner's defaults: 20 Changes/s, `fake`, a 120 s settle budget. Same pinned action SHAs as `check`. `fail-fast: false`, so one failure doesn't hide the others.
2. **One required name.** A matrix gives ten check names (`stress-smoke (kill-worker)`, …) that change when a scenario is added. A last job, `stress-smoke`, `needs` the matrix, runs with `if: always()`, and fails unless every leg succeeded. That one name goes into `main`'s required checks.
3. **On failure, the events go up** with `actions/upload-artifact` (pinned, `retention-days: 3`): `run/data/events` only, a few MB. The JSON line in the log already names the differing keys; the events say why.
4. **The matrix can't drift from `chaos.SCENARIOS`**: one test in `tests/unit/test_chaos.py` (or the nearest existing file) reads `ci.yml` as text and asserts each scenario name is in the matrix line, and nothing else is.
5. **`tests/stress/test_scenarios.py` stays** for local runs (15 s each).

### 7d.2: the full runs on your Mac (docs, with numbers)

Run on the real `data/` dir under `caffeinate -dims`, following [the runbook](../runbook.md), reset between runs. No new code; anything that breaks the SLO becomes its own step.

1. **The 1M initial load** on `fake`, so it measures storage and not Jev: the 7a shape (`--changes 1000000 --keys 1000000 --order sequential --batch 1000 --rate 0`). Recorded: wall time, Changes/s, freshness p50/p99, lag over time, the store's size and file count.
2. **The six costs, on that 1M store**, with a timing script kept in `spikes/stress_costs.py` (as `spikes/delta_spikes.py` is) that times each step of one worker batch of 1,000 random keys on a copy of the store:
   - `store.read`, all columns against key columns first (the planned fix, measured, not shipped);
   - the share of files one MERGE rewrites;
   - one tick's offset saves (files written, ms);
   - `landing.read` over one 1,000-row bulk commit at `limit` slices;
   - `CommitFailedError` between workers: `tick_failed` events naming it, over `batch` events, from the 1M run's events;
   - `catalog.metrics --since` over the whole run's events, timed and scaled linearly to 24 h at 50/s.
3. **The SLO run** on the shipped `Procfile` (Jev) with its `backfill` line removed for this run only, on top of the 1M store: 30 minutes of `catalog.load --rate 50 --keys 1000000`, then `catalog.metrics --since 35m`. Pass: p99 freshness under 300 s. Recorded: every `metrics` field, and the Jev spend (`batch.usd`). Without the Backfill: the 1M Listings carry `fake`'s `taxonomy_version`, so a Backfill on Jev would reclassify all of them (about $62).
   **Amended Oct 7 (7d.2b, after 6m.2):** the same run on the shipped `Procfile` with the student (`--classifier student`), the `backfill` line kept: the student is free, so the Backfill reclassifying the 1M `fake`-versioned Listings onto the student's version costs nothing and is itself a measurement (how long the Backfill takes over 1M rows). No key, no spend. Recorded: every `metrics` field, `classify_ms`, and the Backfill's time to leave no row on the old version.
4. **A 10k bulk batch on Jev**, after a reset, on the shipped `Procfile` (Backfill back), so the Backfill sees only those 10k: freshness of its Changes, and how long until the Backfill leaves none flagged (Jev's 80 requests/s can't classify 10k inside one batch's budget, so most go Uncategorized first, by design).
   **Amended Oct 7 (7d.2b):** on the student there is no per-call limit, so the run measures bulk throughput instead: a 10k batch after a reset, freshness of its Changes, and a worker's full 1,000-Change batch time (the `batch` and `classify` events) against the 60 s heartbeat. The Backfill catch-up path is covered by the chaos runner's `classifier-outage` scenario.
5. **The report**, `docs/stress.md`, committed: the machine, each run's numbers, each cost with its verdict (fine at this size, or a new plan row), and whether the Phase 7 exit holds. The plan's 7d row links it. **Amended Oct 7:** written in two parts, 7d.2a (points 1 and 2, the exit still open) and 7d.2b (points 3 and 4 on the student, the exit decided). **Oct 7, 7d.2b's result:** the exit stayed open; the SLO run can't complete on this Mac (the MERGE churn trips the disk guard, plan row 7f), so point 3 is rerun after 7f and decides it then.

## User stories

1. As you, a PR that breaks any chaos scenario can't merge, and the failed leg's events are one click away.
2. As you, I read one page that says whether the system meets its SLO at the target size, on the classifier it ships with.
3. As you, each of the six deferred costs has a number and a verdict, so the next steps fix only what matters.

## Failure scenarios

| Scenario | Expected |
|---|---|
| A scenario fails on Linux only (a `pgrep`/`ps` difference, timing on 4 cores) | Found on 7d.1's own PR, before the check is required; fixed there or in a split-off step |
| A leg flakes (a slow runner, a port race) | It fails, and the summary says which wait ran out. Rerun the failed jobs once; a second failure is a bug, not a flake. No automatic retry, which would hide a real race |
| A leg hangs | The runner has its own budgets (`--settle`, the 60 s stop); `timeout-minutes: 10` on the job as the last stop |
| A docs-only PR | Runs `stress-smoke` too: a required check skipped by a workflow `paths:` filter stays "Expected" forever (plan, CI). About 3 min of free public-repo minutes |
| A new scenario added without the matrix | The drift test fails in `check` |
| The Mac sleeps during a long run | `caffeinate -dims` (B7); a run whose wall time is far above its monotonic time is redone |
| The disk fills during the 1M load | The API's guard refuses writes (503) and the load counts them; the run is redone with space freed. The report notes the store's size |
| The Backfill is left in the Procfile for the SLO run | It reclassifies the 1M `fake` Listings on Jev (about $62). Check `grep backfill Procfile` is empty before starting; stop at once if a `backfill_start` event shows. **Oct 7:** moot on the student (point 3, amended): the Backfill stays in and its run is free |
| Jev's key is refused mid-run | Workers exit 7 and the supervisor stops (7c.1): the run ends, and is redone with a working key. **Oct 7:** moot on the student; a missing student is exit 6 before any batch |
| The disk nears the API's 5 GiB guard during the 1M load (seen Oct 7: 9.1 GiB free at the start, 5.1 GiB a few minutes in) | The load is refused (`low_disk`) and the run is void. Check `df` before starting and free space first; the report records free space before and after |
| The SLO run misses 300 s | The report says by how much and which cost the events point to; the fix is its own step, and the Phase 7 exit stays open |

## Implementation decisions

1. **A matrix, not one job running the ten in turn**: about 3 minutes of wall time instead of about 12, free on a public repo. The gate job costs about 10 lines of YAML.
2. **The chaos runner as is**, not `pytest tests/stress`: one scenario per leg, its JSON line straight in the log, and no pytest marker juggling.
3. **Full runs follow the runbook on the real system**, not the chaos runner: the runner's 1,000-key space and fast pass intervals suit faults, not a 1M store. Running them is also the runbook's real test.
4. **Measure, then fix only what breaks the SLO** (Phase 7's rule). The timing script measures the planned fixes beside today's code, so a later step starts from numbers.
5. **Linear scaling for the 24 h metrics window**: the parse is per event, so time over N events scales with N. Too slow means over 60 s for a 24 h window; then load events straight into DuckDB (its own step).

## Testing decisions

- **Test points (seams), to confirm:**
  1. **The matrix matches `chaos.SCENARIOS`** (new, red first: written before the job, it fails until the matrix lists all ten).
  2. **The job itself**: 7d.1's PR shows all ten legs and the gate green, and one deliberately broken push (an oracle forced to fail, never merged) shows the gate red and the events uploaded.
- **7d.2** has no tests: it is measurement. The timing script is a spike, outside the coverage gate like `spikes/`.
- **Coverage:** unchanged; the 90% gate holds.

## Questions (answered Oct 5)

1. **Matrix (about 3 min, 11 jobs) or one sequential job (about 12 min, 1 job)?** I'd use the matrix: every PR waits for it, and the minutes are free. OK? **Yes.**
2. **Making `stress-smoke` required** changes `main`'s branch protection. Once 7d.1's run is green I'd add it with `gh api` (keeping `check`). OK to do that, or would you rather set it yourself? **You do it.**
3. **Classifiers for the Mac runs:** the 1M load on `fake` (free; Jev would be about $62 and 3.5 h, and measure Jev, not storage), then the 30-minute SLO run and the 10k bulk on Jev (about $6 and $0.60). The Jev runs need `TYPESAFE_API_KEY`; I'd ask you to export it in the shell that starts the supervisor, so the key never passes through me. OK? **Yes, with the SLO run's Procfile minus its `backfill` line, and a reset before the 10k bulk: about $6.20 in all.** **Oct 7:** superseded; the runs on the shipped classifier happen after 6m.2 on the student, for nothing (points 3 and 4, amended).
4. **The 24 h metrics threshold**: fix only if a 24 h window would take over 60 s. OK, or a different line? **60 s.**

## Out of scope

- Fixing any of the six costs: each gets its own step if the SLO run needs it.
- A Linux full stress run: CI runners are too small to mean anything (plan, CI).
- Several faults at once, and a watch on the chaos runner (7b's out of scope).
- Making `model` a required check.

## Size

- 7d.1: about 40 lines of YAML, a 10-line test.
- 7d.2: `docs/stress.md` (about 80 lines) and `spikes/stress_costs.py` (about 80 lines).
