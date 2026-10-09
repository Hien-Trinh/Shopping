# Step 3e: crash, rescale and supervisor-death tests (mini PRD)

Status: approved Oct 3: the test points, the watch design, the restart race and the time budget are confirmed. Plan row: [plan-v1.md, PR steps, 3e](../plan-v1.md). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

Phase 3 claims the Ingestion workers are crash-safe, but only fakes and single crash points have proven it. Three claims have no real-process test:

- An Ingestion worker killed with `kill -9` at any point in a batch loses no Change, duplicates nothing, and leaves every Change of a Submission with a final Outcome.
- Rescaling from 4 workers to 3 resumes every partition from its saved offset.
- A supervisor killed with `kill -9` leaves its workers running unsupervised, holding their partition locks, until someone kills them by hand. The next supervisor's workers then exit 3 and it stops everything.

## Solution

1. Chaos tests that kill real worker processes at every point of a batch and after a restart compare the Listing Store with the replay oracle.
2. A rescale test that runs the documented procedure (stop the supervisor, edit the `Procfile`, start it) from 4 workers to 3.
3. One behaviour change: a worker started by the supervisor watches it, and stops when it dies.

## User stories

1. As an operator, when a worker is killed mid-batch and restarted, I want the Listing Store to end exactly as the replay oracle says, so that a crash never loses or duplicates a Change.
2. As an operator, I want that to hold whichever step the kill lands on: reading, after the MERGE commits, after the events are written, between offset files, or during compaction.
3. As a Merchant, I want every Change in my Submission to reach a final Outcome after a worker crash, so that none stays pending. A written Change may replay as `already_applied`, which is the documented trade-off.
4. As an operator, I want to rescale from 4 workers to 3 by stopping the supervisor, editing the `Procfile` and starting it again, so that each partition resumes from its saved offset, with nothing skipped.
5. As an operator, when the supervisor is killed with `kill -9`, I want each of its workers to notice within a few seconds and stop cleanly, so that none runs unsupervised or keeps its partition locks.
6. As an operator, I want a worker stuck in a native call when its supervisor dies to exit anyway within a bounded time, so that its locks are always released.
7. As an operator, once those workers have exited, I want a new supervisor to start every worker without `PartitionTaken`.
8. As a developer debugging one worker started by hand, I want it unaffected by the supervisor watch.
9. As a developer, I want these tests to be deterministic in what they assert and to run in `make check` within a time budget, so that a crash-safety regression blocks merges without flaky CI.

## Failure scenarios

Each one becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| `kill -9` during `store.read` | Restart replays the batch; the oracle holds |
| `kill -9` after the MERGE commits, before events | Replay reports `already_applied`; the oracle holds; no Change pending |
| `kill -9` after events, before offsets | Duplicate events; status unchanged (best Outcome wins) |
| `kill -9` between two offset files | Partitions resume from their own saved offsets; at-least-once |
| `kill -9` during compaction | The table stays readable and its fingerprints unchanged; the next compaction succeeds |
| `kill -9` at a random moment, repeated | Whatever the timing, the oracle holds |
| Rescale 4 → 3 mid-stream | Every partition's offset carries over; the oracle holds over all Changes |
| Supervisor `kill -9`, worker idle | The worker stops within the check interval plus one tick; its locks are free |
| Supervisor `kill -9`, worker mid-batch | The worker finishes the tick, then stops |
| Supervisor `kill -9`, worker's main thread stuck in a native call | The watch thread exits the process at the hard deadline; its locks are free. (A SIGSTOPped worker freezes every thread, so it isn't this case: it stops normally once resumed.) |
| New supervisor started before the old workers have exited | A worker exits 3 and the supervisor stops, loudly. See decision 4 |

## Implementation decisions

1. **Supervisor watch (the only production change).** The supervisor sets `CATALOG_SUPERVISOR=<its pid>` in its children's environment. When that variable is set, the worker starts a daemon thread that checks the pid every second. When the supervisor is gone, the thread sets the worker's stop event, so the current tick finishes, offsets are saved and claims are released. If the worker is still running a hard deadline later (30 s by default, a parameter so tests can use about 1 s), the thread exits the process with code 1, because the main thread is stuck in a native call. Without the variable, nothing changes (story 8).
   - Rejected alternatives: polling `os.getppid()` breaks behind a wrapper command; `PR_SET_PDEATHSIG` is Linux-only; kqueue is heavy for this.
2. **No crash hooks in production code.** Deterministic kills come from a test-only driver script that runs the worker and wraps one function in that child process to SIGKILL itself before or after the real call.
3. **Rescale:** no code change is expected, since offsets are per partition (A9). If a test exposes a bug, it's fixed test-first in this step.
4. **Restart race:** a supervisor started while the old workers are still exiting keeps 3d's loud and safe behaviour (exit 3). The 7c runbook will say to wait a few seconds. Confirmed.

## Testing decisions

- **Test points (seams), confirmed:**
  - **The watch logic** (new), in-process: when to stop and when to exit hard, with an injected liveness check, wait and exit. TDD, red first: it doesn't exist yet.
  - **The worker CLI** as a black box, in real subprocesses, observed through the Landing log, the Listing Store, offsets and events, against `replay.expected_store` and `status.fold`. Characterization: crash safety is already designed in, so these tests may pass on first run. A failure is a real bug, fixed test-first.
  - **The supervisor CLI** as a black box, in real subprocesses, observed through process liveness, partition claims and exit codes. Red first for story 5.
- **Never fixed sleeps:** poll observable state with deadlines. Randomized kills vary the timing, never the assertion.
- **Prior art:** the end-to-end tests in `test_supervisor.py` (`wait_for`, `read_pid`, `alive`), and the `Env` helper, CLI tests and replay oracle test in `test_worker.py`.
- **Location and budget:** `tests/integration/test_chaos.py`, at most about 30 s in `make check`. Repeated randomized kills beyond that are marked `slow`, for the Phase 7 stress job.

## Out of scope

- Load, long chaos runs and metrics (7a, 7b).
- Rescaling without a restart.
- A real-process stale-heartbeat test: `STALE` is 60 s, and 3d's simulated tests cover it.
- Power loss: a documented ceiling.
- A watchdog for the supervisor itself.

## Size

About 25 lines of production code and roughly 200 of tests, so the PR stays under 300.
