# Step 4c: group-commit appender (mini PRD)

Status: approved Oct 3. Confirmed: the window counted from the oldest waiting request (decision 1), the commit in a thread (decision 2), no row cap (decision 3), `received_at` per row (decision 4), and the test points. Plan row: [plan-v1.md, PR steps, 4c](../plan-v1.md). Design: the [Ingestion API row and lifecycle step 2](../design-commerce-ingestion-pipeline.md), A10 in [plan-v1.md](../plan-v1.md), and [ADR-0002](../adr/0002-local-first-delta-no-queue.md) ("the API group-commits for up to 100 ms"). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Since 4b, every accepted request makes its own Landing log commit, inside the request handler. That breaks two things:

- **Commit count (A10).** At 50 requests/s, the Landing log gets 50 commits a second: 4.3M log files and as many small data files a day. Every commit also moves the offsets of all a worker's partitions (plan-v1, Phase 7), so workers pay for each one too.
- **Throughput.** The commit blocks the event loop, so no other request is read or parsed while it runs (4b, decision 1).

The design says the API group-commits: one appender collects requests for up to 100 ms, commits them once, then answers each with 202. Phase 4's tests include "group commit makes one Delta commit for concurrent requests".

## Solution

1. `landing.Appender`: one asyncio task per API process. A request hands it its rows and awaits the commit. The appender waits until the oldest waiting request is 100 ms old, takes everything queued, and commits it in one `landing.append` run in a thread. Then it answers each waiting request with the commit's version, or with the commit's error.
2. `api.py` starts the appender in FastAPI's lifespan, and the handler awaits `appender.submit(rows)` where it called `landing.append` before. Everything else in the handler, including events after the commit and the 202 after the events, stays as in 4b.
3. `landing.append` takes `received_at` per row rather than per call, since one commit now holds several requests.

## User stories

1. As the operator, concurrent requests share one Landing log commit, so the Landing log grows by at most 10 commits a second however many requests arrive (A10).
2. As a Merchant, my 202 still means my Changes are committed, and my request waits at most about 100 ms plus one commit for the group.
3. As a Merchant, my `submission_id`, `accepted` count and `rejected` list are mine alone, even when my Changes share a commit with other Merchants'.
4. As the operator, each landed row keeps its own request's `received_at`, so freshness (received to applied) isn't skewed by the window.
5. As an Ingestion worker, a request's rows are contiguous in their commit and in index order, and requests are in arrival order, so landing order stays well defined.
6. As the operator, the API keeps reading and parsing requests while a commit runs.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| Several requests arrive within 100 ms | One commit holding all their rows; each gets 202 with its own `submission_id`; rows in arrival order, each request's rows contiguous and in index order |
| One request alone | Committed when it is 100 ms old; 202 as in 4b |
| A request arrives after the window has closed | It goes into the next commit |
| Requests arrive while a commit runs | They go into the next commit, which waits only until the oldest of them is 100 ms old (at once, if the commit took longer) |
| Every Change in a request is invalid | Never queued: 202 at once, no commit, as in 4b |
| The commit fails (disk full, I/O error) | Every request in the group gets 500 and no events; nothing landed (Delta commits atomically); each Merchant retries |
| One request's events fail after the shared commit | That request gets 500 and retries (the duplicate case, A3); the others get their 202s |
| The appender's own code raises (a bug) | The waiting requests get 500; the appender keeps running, so the next request still commits |
| A client disconnects while waiting | Its rows still land (they're already queued); the others in the group are unaffected. Its Merchant got no 202 and retries: the duplicate case, A3 |
| Crash or `kill -9` during the window | Nothing queued has landed and no one got a 202; every Merchant retries, as for 4b's crash |
| Crash after the commit, before events or the 202s | As 4b: retries replay as `already_applied` |
| SIGTERM from the supervisor | uvicorn finishes the requests in flight, which still get their commit; then the lifespan stops the appender. The SIGKILL 10 s later is the crash row |
| A naive `received_at` reaches `landing.append` | `ValueError`, as in 4b, now checked per row |

## Implementation decisions

1. **The window counts from the oldest waiting request, not from the last commit.** The appender sleeps until `oldest.arrived + window`, then takes everything queued. A lone request waits the full 100 ms. The alternative, committing at once when idle and batching only what queues during a commit, adds no latency at low load but lets commits run back to back, so at 50 small requests/s it makes up to 50 commits a second: A10 again. So commits never exceed 10 a second.
2. **The commit runs in a thread (`asyncio.to_thread`).** The event loop keeps reading and parsing requests meanwhile, and they queue for the next commit. Only the appender touches the `DeltaTable`, one commit at a time, so no lock is needed.
3. **A request is never split across commits**, and there's no row cap per commit. Ten concurrent 10k-Change requests make one 100k-row commit. Ceiling: a large commit makes `landing.read`'s per-slice re-read (plan-v1, Phase 7) costlier. Measure it in Phase 7 and cap rows per commit only if it matters.
4. **`landing.append(dt, entries)` takes `(submission_id, change_index, change, received_at)` rows.** One `received_at` per call would stamp every request in a group with one time. `seq` stays the row's position in the commit. 4b's test helpers and `test_worker.py`'s `Env` pass the extra field.
5. **Events stay per request**, written by the handler after its commit resolves, as in 4b. So one request's failed event write fails only that request, and the handler's ordering (commit, events, 202) is unchanged. `EventLog.emit` doesn't fsync, so the extra writes cost little.
6. **`Appender(commit, window)`** takes the commit function, so it is tested with a fake commit and no Delta. `submit(rows)` returns the commit version, or raises the commit's exception. A waiter that was cancelled (a disconnect) is skipped when results go out, so it can't break the others. The loop catches every `Exception` per group and keeps running. Only cancellation, at shutdown, stops it.
7. **Lifespan.** `create_app` builds the appender, and FastAPI's lifespan starts its task and cancels it on shutdown. uvicorn waits for the requests in flight before it runs the lifespan's shutdown, so they still get their commit. `create_app` takes `window` (default 0.1 s) so tests can widen or narrow it.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`Appender`** (new, red first, `tests/unit/test_appender.py`): driven with `asyncio.run` and a fake commit that records its groups (a real `DeltaTable` isn't needed). Covers grouping within the window, arrival order, a request after the window going to the next commit, the deadline counted from the oldest waiter, a failed commit raising in every waiter, a cancelled waiter not breaking the rest, and the loop surviving an exception.
  2. **The HTTP app** (changed, `tests/integration/test_api.py`): the `Api` helper enters `TestClient` as a context manager so the lifespan runs, with a short window so the 4b tests stay fast. One new test, red first: several requests sent from threads within a wide window make exactly one Landing log commit, each with its own `submission_id`, events and `received_at`. One more: a failed shared commit gives every request in the group 500. The 4b tests keep passing unchanged.
  3. **`landing.append`** (changed, red first): rows carry their own `received_at`; a naive one still raises.
- **No timing asserts on latency.** Tests check which requests share a commit, not how long they waited, so a slow CI machine can't fail them. Window-boundary cases use a fake clock or ordered awaits, not sleeps tuned to the machine.
- **Coverage:** `landing.py` and `api.py` stay under the 90% package gate.

## Out of scope

- A row cap per commit (decision 3).
- Adaptive windows, or flushing early once a size is reached.
- `GET /submissions/{id}` (4d) and the end-to-end test (4e).
- Power loss: delta-rs doesn't fsync commits (ADR-0002), so a 202 still covers process crashes only.
- Measuring commits per second and API latency under load (Phase 7).

## Size

About 45 lines of production code (about 35 in `landing.py`, about 10 in `api.py`) and about 130 of tests, so well under 300.
