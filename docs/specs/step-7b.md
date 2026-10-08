# Step 7b: chaos scenario runner and the three oracles (mini PRD)

Status: approved Oct 5, with the test points as written. Question 1: keep the flagged-Listing check. Question 2: name the kind `down`. Plan row: [plan-v1.md, PR steps, 7b](../plan-v1.md) (the runner and the three oracles; from 3e, a runner that kills the supervisor must reap it). Phase: [plan-v1.md, Phase 7](../plan-v1.md) (scenarios: out-of-order changes, duplicate retries, a delete followed by a late update, a poison change, a classifier outage, killing a worker, rescaling, and a disk-guard trip; exit: every scenario ends with the three oracles passing). Design: [Observability and testing](../design-commerce-ingestion-pipeline.md) (the three oracles), [plan-v1.md A4](../plan-v1.md). Builds on [step-7a.md](step-7a.md) (the load generator), [step-4e.md](step-4e.md) (the e2e test's scratch system) and [step-6c.md](step-6c.md) (the Backfill). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

Phase 7's exit is "every scenario ends with the three oracles passing". Today:

- The oracles exist only as pure functions (`catalog.replay`) and as inline asserts in single tests (`test_e2e.py`, `test_chaos.py`). Nothing checks all three on a whole running system after a load.
- Each fault has been tested alone, on a few hand-written Changes. None has been run under load, and none together with Change Export and Catalog Snapshots running.
- 7d's `stress-smoke` CI job needs one command per scenario that exits non-zero when an oracle fails.

## Solution

1. **`python -m catalog.chaos SCENARIO`**, a new module. It builds a scratch system, runs a steady load with the scenario's fault in the middle, waits for the system to settle, checks the three oracles, stops everything, and prints one JSON line. Exit 0 only if every oracle holds.
   - Flags: `--seconds` (load time, default 60), `--rate` (default 20 Changes/s, so CI's small runners keep up), `--seed`, `--dir` (default a new temp dir, kept and printed on failure), `--settle` (seconds to wait for every Change's Outcome, default 120).
   - The scratch system is the repo's `Procfile` with the e2e test's rewrite: a free port, `--min-free 0`, the `fake` classifier, short export, maintenance and snapshot intervals. The rewrite moves from `test_e2e.py`'s `System` into `catalog.chaos.procfile(text, port)`, and `System` calls it, so the two can't drift.
   - One Merchant is created with `catalog.merchants`. Its key goes to the load generator through `CATALOG_API_KEY`, never a flag or the output.
2. **Scenarios.** Each is a steady load from `catalog.load`, plus one fault at mid-run. Deletes are on in every scenario (`--deletes 0.1`, see 3).

   | Scenario | Fault |
   |---|---|
   | `steady` | none (the baseline) |
   | `bulk` | one 10k-Change batch posted mid-run |
   | `out-of-order` | a second load at once, same seed, its `start_ms` half the run earlier: each key gets older and newer versions in random arrival order, deletes included, so a late update after a delete is covered |
   | `duplicates` | a second load at once, same seed and same `start_ms`: every Change sent twice, as a retrying client does |
   | `poison` | 10 Changes whose content fails storage appended straight to the Landing log (the API can't let one through), as `test_worker.poison` builds them |
   | `classifier-outage` | the supervisor is stopped and restarted with every `--classifier` set to `down` for a third of the run, then back to `fake` |
   | `kill-worker` | `kill -9` a random worker, three times |
   | `kill-supervisor` | `kill -9` the supervisor, `wait` for it (the 3e rule: an unreaped pid looks alive), wait for its workers to stop, start a new one |
   | `rescale` | stop the supervisor, rewrite the Procfile from 4 workers to 3, start again |
   | `disk-full` | restart with the API's `--min-free` above the disk's free space for a third of the run, then back to 0 |

   The 7a load shapes `steady` and `bulk` are here at scenario size; 7a's 1M initial load stays a hand run (7d).
3. **Load generator additions** (in `catalog.load`):
   - `--deletes FRACTION` (default 0): Change `i` is a delete with that chance, from the same per-Change `rng`, so it stays a pure function of `(seed, i, start_ms)`.
   - `--start-ms` (default now): fixes the run's versions, for `out-of-order` and `duplicates`.
4. **A `down` classifier kind** (`classify.KINDS`): `FakeClassifier(fail=True)`, every call raising, for the worker and the Backfill. It already exists for tests; this only names it on the command line.
5. **Settling.** After the load and the fault end, the runner waits until:
   - every landed Change has an Outcome event, keyed by `(submission_id, change_index)` as `status.fold` keys them;
   - Change Export's watermark has reached the Listing Store's head;
   - for `classifier-outage`, no Listing is still flagged `needs_reclassify` (the Backfill's job; it ticks every 10 s).

   It never waits longer than `--settle`; past that it reports what is still pending and exits 1.
6. **The oracles**, `catalog.chaos.oracles(data, state) -> dict[str, list[str]]`, one list of differing keys per oracle (empty = holds), built on `catalog.replay`:
   - **store**: the whole Landing log from `START`, minus Changes with a `failed` event (matched by `(submission_id, change_index)`), through `expected_store`, against `store.fingerprints`. Rejected Changes never land; conflicts are handled by `expected_store`'s tie rule (A3), so neither needs excluding.
   - **export**: `replay_exports` of every export file against `live(store.fingerprints(listings, watermark))`.
   - **snapshot**: each snapshot against `store.fingerprints(listings, pinned)`.
7. **The summary** (stdout, one JSON line): scenario, seed, the load's summary, Outcomes by kind, each oracle's verdict with at most 20 differing keys, seconds to settle, and the scratch dir. `test_e2e.py` keeps its inline oracle asserts; switching them to `oracles()` is a cleanup for later.

## User stories

1. As you, I run `python -m catalog.chaos kill-worker` and get one line that says whether the system stayed correct.
2. As 7d's CI job, I run every scenario in turn and fail the PR on any non-zero exit.
3. As you, a failed run keeps its scratch dir and names the keys that differ, so I can look at the events.

## Failure scenarios

| Scenario | Expected |
|---|---|
| An oracle fails | Exit 1; the summary names it and up to 20 keys; the scratch dir is kept |
| The system never settles (a Change stuck pending, export stalled) | Exit 1 after `--settle` seconds, with the pending count; never a hang |
| The supervisor exits on its own (a fatal worker exit) | Exit 1 naming its exit code; no oracle is run on a half-stopped system |
| Ctrl-C | SIGTERM to the supervisor, `wait` for it, kill anything left from its events' pids, keep the scratch dir, exit 130 |
| The runner itself is `kill -9`ed | The supervisor is its child in its own session, so it keeps running; its workers stop only with it. Documented: `pkill -f catalog.supervisor` in the runbook (7c). Not fixed: needs a watch on the runner, as workers watch the supervisor |
| The supervisor is killed (`kill-supervisor`) | Reaped with `wait`, then the runner waits up to 10 s for each old worker to stop before starting a new one (3e) |
| The port is taken between choosing it and the API binding | Readiness times out; exit 1 (the e2e test's `ponytail:` ceiling) |
| The machine has under 5 GiB free | `--min-free 0` everywhere except `disk-full`, so runs still pass |
| The load generator exits 1 (`disk-full`'s 503s, a restart's refused connections) | Expected in scenarios that restart the API; the runner records it and goes on. Correctness is the oracles' job: an unaccepted Change never landed |
| Two runners at once | Separate scratch dirs and ports; nothing shared |
| A slow CI runner falls behind the rate | The load's lateness shows it; the oracles still hold once settled. 20/s is the default for that reason |
| A Landing log Change from before the run (a reused `--dir`) | The store oracle must run from an empty store (A4), so `--dir` must be empty or new: exit 2 otherwise |

## Implementation decisions

1. **A module in the package, not only a test.** 7d's CI job, the hand runs and the runbook (7c) all need one command. A slow-marked test (`tests/stress`) calls it per scenario.
2. **Every fault is something the shipped system already allows**: a load, `kill -9`, a supervisor restart with an edited Procfile, or an append to the Landing log beside the API's (the Backfill already appends there). No test-only hook in the pipeline, apart from naming `down` (decision 4 of Solution).
3. **Restarting the supervisor** is how a fault that needs a flag change (outage, rescale, disk) is switched on and off, as an operator would. Its cost: the API is down for a few seconds, so the load counts refused connections.
4. **Deterministic**: the seed fixes the Changes (7a), which worker is killed and when, and which keys get poison. A failed seed reruns the same faults, though not the same timing.
5. **The oracles read everything from disk after settling**, never from the runner's memory of what it sent: they check the system, not the runner.

## Testing decisions

- **Test points (seams), to confirm:**
  1. **`--deletes` and `--start-ms`** (new, red first, `tests/unit/test_load.py`): the same seed gives the same deletes; about the asked fraction over 10k Changes; a delete passes `envelope.check_batch`; `--start-ms` fixes every `source_version`.
  2. **`oracles()` catches each kind of wrong** (new, red first, `tests/integration/test_chaos_runner.py`, a scratch system after a short load):
     - all three hold on a clean run;
     - a Change appended to the Landing log after the system is stopped (so no worker applies it) fails **store**, naming its key;
     - a deleted export file fails **export**;
     - a snapshot with a row removed fails **snapshot**;
     - a poison Change with its `failed` event doesn't fail **store**.
  3. **One short scenario in-process** (new, red first, same file, not slow): `chaos.main(["kill-worker", "--seconds", "5"])` exits 0 and its summary shows a restart and three passing oracles. Keeps `chaos.py` inside the 90% coverage gate.
  4. **Every scenario** (new, `tests/stress/test_scenarios.py`, marked `slow`): parametrized over the ten, 15 s each at 20/s, each exits 0. Local only until 7d's CI job.
  5. **The `Procfile` rewrite moved** (characterization): the existing e2e tests pass unchanged on `chaos.procfile`.
- **The hand run** (after tests pass, numbers in the PR, nothing committed): every scenario at the defaults, 60 s at 20/s, with seconds to settle.
- **Coverage:** new lines covered by test points 1–3; the 90% gate holds.

## Questions (answered Oct 5)

1. **The `classifier-outage` check that no Listing stays flagged** (Solution 5): the oracles ignore the Category on purpose (A4), so without it the outage scenario only shows the store survives. It adds about 10 s of settling (one Backfill tick) and a few lines. Keep it? **Yes.**
2. **The `down` classifier kind** (Solution 4) puts a failure mode on the shipped command line. The other way is a test-only driver like `tests/integration/chaos_driver.py`, but then `catalog.chaos` would depend on the tests folder. I'd name it `down`. OK? **Yes.**

## Outcome (Oct 5)

All ten scenarios passed at the defaults (60 s at 20/s, `fake`, this Mac), each settling in under 1.5 s, with every fault shown by its effect (for example 1,007 stale for out-of-order, 392 × 503 for disk-full). Review fixes (PR #77): a run fails if nothing landed or an oracle had nothing to check, each scenario must show its fault's effect (`EFFECTS`), and settling has one budget in all and names the pending count. Kept, at review: the summary's `process_exits` and `supervisor_restarts`; 1,000 keys and 8 load processes; out-of-order's shift of half the run's Changes; `alive()` in both `chaos.py` and the test helpers until the end-of-project cleanup.

## Out of scope

- Metrics, freshness and the runbook: 7c.
- CI's `stress-smoke` job, the 1M initial load, and the SLO run at 50/s: 7d.
- Several faults at once in one run.
- A watch so the supervisor stops when a `kill -9`ed runner dies.
- Switching `test_e2e.py`'s inline oracle asserts to `oracles()`.

## Size

About 200 lines in `chaos.py`, 10 in `load.py`, 3 in `classify.py` and the worker and Backfill CLIs, `System` shrinking by about 20 in `test_e2e.py`, and about 150 of tests.
