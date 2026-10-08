# Step 4e: end-to-end test, HTTP → Landing log → worker → Listing Store (mini PRD)

Status: approved Oct 3. Confirmed: the API's supervisor watch is fixed here (decision 5), the end-to-end test runs unmarked in `make check` (decision 4), and the test points. Plan row: [plan-v1.md, PR steps, 4e](../plan-v1.md), and Phase 4's exit criterion: "an end-to-end request travels HTTP → Landing log → worker → Listing Store". Design: the [Change lifecycle](../design-commerce-ingestion-pipeline.md), steps 1–7. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

**Depends on 4c and 4d.** The test reads Submission status through 4d's `GET /submissions/{id}` and lands Changes through 4c's group commit, so it is written after both merge, against their final shapes.

## Problem

Every part of the path is tested on its own, in process: the API with `TestClient` (4b), the worker with `process_batch` and `run` (3a–3c), and the supervisor with workers only (3d, 3e). Nothing yet runs the shipped system the way an operator does, with one supervisor starting the `Procfile`'s API and workers, a Merchant posting over a real socket, and the status coming back over HTTP. Seams that only that run crosses are untested:

- the API under uvicorn as a real process: startup, binding, and stopping on the supervisor's SIGTERM
- the `Procfile`'s default directories: the API, the workers, the supervisor and the admin CLI must all agree on `data/`, `state/` and `data/merchants.sqlite`
- events from two kinds of process (`api-*` and `worker-*` files) folding into one Submission's status
- the supervisor's watch on its children. Workers stop when the supervisor dies (3e), but **the API doesn't**: it ignores `CATALOG_SUPERVISOR`. A `kill -9`ed supervisor leaves the API running on its port, and the next supervisor's API can't bind and exits 1. It is then restarted every 5 s until someone kills the orphan by hand.

## Solution

1. `tests/integration/test_e2e.py`: one test runs the whole system as subprocesses in `tmp_path`. A second test covers the supervisor's death.
2. The API watches its supervisor, as the workers do: when `CATALOG_SUPERVISOR` is set, `catalog.api.main` starts `worker.watch` in a daemon thread. Once the supervisor is gone, the watch tells uvicorn to stop (`server.should_exit = True`), so the requests in flight finish, and the API exits 1 if it is still running 30 s later. This needs `uvicorn.Server` instead of `uvicorn.run`, about 10 lines (decision 5).

## User stories

1. As the operator, I run `python -m catalog.supervisor` from a directory holding the shipped `Procfile` and a registry made with `python -m catalog.merchants create`, and the API and all 4 workers come up with no other flags or setup.
2. As a Merchant, I POST a batch over HTTP, get 202 and a `submission_id`, and polling `GET /submissions/{id}` shows `pending` until the workers finish, then one Outcome per Change: `written`, `rejected`, `stale`, `already_applied`.
3. As the operator, the Listing Store then holds exactly what the replay oracle says the Landing log implies, with every live Listing classified.
4. As the operator, SIGTERM to the supervisor stops the API and every worker, and the supervisor exits 0.
5. As the operator, after a `kill -9`ed supervisor, its API and workers stop on their own, so a new supervisor starts cleanly on the same port and the same partitions.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| A batch with valid upserts covering every worker's partitions, plus one invalid Change | 202. Status reaches done: every valid index `written`, the invalid one `rejected`. The store matches the replay oracle |
| A second batch sent right after the first's 202: a delete, a resent Change and a lower source version | Lands after the first, whatever the workers have processed, so its Outcomes are fixed: `written` (the Tombstone), `already_applied`, `stale` |
| A status request before any worker has run | 200 with the valid Changes `pending` and the invalid one `rejected`, never 404: the API's own events make the Submission exist |
| The first request arrives before uvicorn is listening | The test polls until the port answers (30 s cap). An operator's client retries, as for any 5xx |
| SIGTERM to the supervisor | It SIGTERMs every child's group; uvicorn finishes the requests in flight and exits, the workers finish their tick; the supervisor exits 0 within its 10 s grace; no child pid is left and the port is free |
| `kill -9` of the supervisor | Workers stop (3e). The API stops too, within about a second, and frees its port (new behaviour, decision 5). A new supervisor then serves on the same port |
| The API stuck in a request when its supervisor dies | uvicorn waits for the request; the watch's 30 s deadline then exits 1, as for a stuck worker |
| `kill -9` of the API mid-request | Out of scope: 4b's crash row (no 202, the retry replays as `already_applied`), plus the supervisor's restart-on-exit (3d). 7b's chaos runner covers it under load |
| `kill -9` of a worker mid-batch | Out of scope: 3e |
| The chosen port is taken by another program | The API exits 1 and restarts every 5 s, and the test fails on its 30 s readiness timeout. Ceiling: the test picks a free port by binding port 0, which a race could lose |

## Implementation decisions

1. **Real processes, real socket, nothing injected.** The test copies the repo's `Procfile` into `tmp_path`, changing only the API's line to `--port <free port>`, and runs `python -m catalog.supervisor` there with `cwd=tmp_path`. So the shipped `Procfile` is what's tested, 4 workers and the default directories included. The Merchant comes from `python -m catalog.merchants create --currency USD` in the same directory, with the key read from its stdout. Requests go through `httpx` (already a dev dependency) to `127.0.0.1`. The classifier is the worker's `FakeClassifier`.
2. **The batches.** The first holds upserts of `sku-0`…`sku-199` at source version 1, which cover all 64 partitions (asserted, so a change to the hash can't quietly shrink the test), and one invalid Change at the end. The second holds a delete of `sku-0` at version 2, `sku-1` resent unchanged at version 1, and `sku-2` at version 0. They are sent one after the other, without waiting for the first to finish, because Landing log order alone fixes the second batch's Outcomes.
3. **Asserts, after both Submissions are done:**
   - each Submission's status, read over HTTP, Change by Change
   - `diff(expected_store(landed), actual)` is empty, with `landed` read back from the Landing log and `actual` from the Listing Store
   - every live Listing has the FakeClassifier's category, and `sku-0` is a Tombstone
   - after SIGTERM: the supervisor exits 0, no pid from a `worker_start` event or the API is still alive, and the port can be bound again
4. **Waiting.** Every wait polls with `wait_for` from `test_supervisor.py`, with a 30 s cap: no fixed sleeps. The test is not marked `slow`, so it runs in `make check`. Expected run time is 5–10 s, most of it process startup (deltalake and pyarrow imports, in parallel).
5. **The API watches its supervisor** (`catalog.api.main`). This reuses `worker.watch` and `worker._alive` unchanged, `watch` only calls `stop.set()`, so the API passes an object whose `set` sets `server.should_exit = True`. Its `exit` stays `os._exit(1)` after the 30 s deadline. A malformed `CATALOG_SUPERVISOR` exits 2, as in the worker. The alternative is to leave it for 7b and document the restart loop. I recommend fixing it here: it is a few lines, and this step's supervisor test would otherwise fail on it.
6. **Cleanup.** Like `test_chaos.py`'s `spawn` fixture: whatever a test leaves running is killed at the end, including every pid from a `worker_start` event and the API's pid, so a failed assert never leaks a process holding the port or partition locks.

## Testing decisions

- **Location:** `tests/integration/test_e2e.py`. It reuses `wait_for` and `alive` from `test_supervisor.py`, the `spawn` fixture pattern and `starts()` from `test_chaos.py`, and `expected_store`, `diff` and `fingerprint` from `catalog.replay`.
- **Test points (seams), confirmed:**
  1. **The shipped system** (new): one test, "a Merchant's batches travel HTTP → Landing log → worker → Listing Store", covering failure-scenario rows 1–3 and 5, observed only through HTTP, the Landing log, the Listing Store, events and pids. It is a characterization test of behaviour 4b–4d built: red only if a seam is broken, and any red it shows is a real bug, fixed test-first.
  2. **The API's supervisor watch** (new, red first): `kill -9` the supervisor, then the API's pid is gone and its port is free within 5 s, and a second supervisor serves on the same port. Red today: the API keeps the port.
  3. **`api.main`'s flag check** (new, red first): a malformed `CATALOG_SUPERVISOR` exits 2 naming the variable, the same parametrized values as the worker's test. In process, with `uvicorn.Server.run` monkeypatched.
- **Coverage:** `api.py` stays under the 90% package gate. The watch thread's body is covered by test 2, which runs in a subprocess, so its lines count only if subprocess coverage is on. If they don't count, the gate still holds at the package level.

## Out of scope

- Load, throughput and the freshness SLO (7a, 7d), and many Merchants.
- `kill -9` of the API or a worker mid-request (4b, 3e, 7b).
- Change Export and Snapshots (Phase 5), and the real classifier (6b).
- TLS and any bind address other than `127.0.0.1`.
- Making a missing registry fatal to the supervisor (4b, out of scope there too).

## Size

About 10 lines of production code (`api.py`), about 150 lines of tests. Under 300.
