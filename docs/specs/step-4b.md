# Step 4b: `POST /listings:batch` (mini PRD)

Status: approved Oct 3. Confirmed: `verify` raises `Denied` (decision 3), an all-invalid request gets 202 (decision 7), a `refused` event for every refusal (decision 9), and the test points. Plan row: [plan-v1.md, PR steps, 4b](../plan-v1.md). Design: the [Ingestion API row and lifecycle steps 1–2](../design-commerce-ingestion-pipeline.md), and A12, A13 and B8 in [plan-v1.md](../plan-v1.md). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

Nothing accepts Changes from Merchants yet. The envelope check, the Landing log and the merchant registry all exist, but no Ingestion API joins them, so only a test calling `landing.append` can land a Change. This step builds the API's write path:

- Authenticate the Merchant by API key (`merchants.verify`, 4a).
- Check the envelope (`envelope.check_batch`), rejecting invalid Changes one by one.
- Refuse bodies over 32 MiB (B8), and refuse every write when free disk is under 5 GiB (A13).
- Append the accepted Changes to the Landing log in one commit per request (direct append; 4c replaces it with group commit), then answer 202 with a `submission_id`.

Two items from the 4a review land here. `verify` raises on a missing registry, so the API checks the registry at startup. And auth failures are counted by reason without ever logging the key.

## Solution

1. `src/catalog/api.py`: `create_app(data, db, ...)` returns a FastAPI app with one route, `POST /listings:batch`. A CLI (`python -m catalog.api`) serves it under uvicorn on `127.0.0.1`.
2. `merchants.verify` raises `Denied` with a reason instead of returning None, so the API can count failures by reason, and a forgotten check fails closed.
3. `fastapi` and `uvicorn` become dependencies (design doc, Runtime), and `httpx` a dev dependency (FastAPI's `TestClient` needs it).
4. The `Procfile` gets `api: python -m catalog.api`, so the supervisor runs the API next to the workers.

## User stories

1. As a Merchant, I POST up to 10k Changes with my API key and get 202 with a `submission_id` once they are committed to the Landing log, so I can stop retrying and track them later (4d).
2. As a Merchant, I get the index and reasons of every invalid Change in the same response, and the valid ones are still accepted, so one bad row doesn't block the batch.
3. As a Merchant, a malformed request (not JSON, not `{"changes": [...]}`, or 0 or more than 10k items) gets 400 and nothing in it is accepted, so I know to fix the request rather than single rows.
4. As a Merchant, a missing, wrong or revoked key gets 401, and the response doesn't say which, so it tells a caller nothing extra.
5. As the operator, a body over 32 MiB gets 413 without the API buffering it, so one client can't exhaust memory (B8).
6. As the operator, when free disk is under 5 GiB, every write gets 503 before its body is read or anything lands, so the disk never fills under the Landing log (A13).
7. As the operator, every refused request leaves an event with its status, reason, and Merchant when known, so I can count auth failures by reason, 413s and 503s with SQL. No event ever holds the key or its hash.
8. As the operator, the API refuses to start when the merchant registry is missing, with a message naming the path, instead of answering 401 to every Merchant.
9. As an Ingestion worker, every landed Change carries the authenticated `merchant_id` (never one from the payload), its `partition`, the `submission_id` (UUIDv7) and its index in the request, so its partition's owner processes it and its Outcome folds into the right Submission.
10. As a Merchant checking status (4d), each accepted Change shows `pending` until a worker reports on it, and each invalid one shows `rejected`.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| Valid key, every Change valid | 202 `{submission_id, accepted: n, rejected: []}`; one Landing log commit of n rows; n `accepted` events |
| Some Changes invalid | 202; `rejected` lists `{index, errors}`; only the valid rows land, with their original indexes; a `rejected` event for each other one |
| Every Change invalid | 202 with every index in `rejected`; no commit; only `rejected` events (decision 7) |
| A Change carrying its own `merchant_id` | Rejected as an unknown field; a landed row's `merchant_id` is always the key's |
| No `Authorization` header, or not `Bearer` | 401 with `WWW-Authenticate: Bearer`; a `refused` event, reason `no_key`; the body is never read |
| Unknown or empty key | 401, same body; reason `unknown_key` |
| Revoked Merchant's key | 401, same body; reason `revoked`, with its `merchant_id` |
| `Content-Length` over 32 MiB | 413 before the body is read; reason `too_large` |
| Chunked body (no `Content-Length`) growing past 32 MiB | 413 as soon as the count passes the cap; the rest isn't buffered |
| Body exactly at the cap | Not refused for size |
| Body not JSON, or not decodable | 400, reason `bad_json` |
| JSON nested about 200k deep (a 400 KB body) | 400, not 500: `json.loads` raises `RecursionError`, which isn't a `ValueError` |
| Valid JSON, wrong shape (`BadBatch`) | 400, reason `bad_batch`; nothing lands |
| Free disk under 5 GiB | 503 before auth or the body; reason `low_disk`; nothing lands |
| The Landing log append fails (the disk filled after the guard, an I/O error) | 500; Delta commits atomically, so nothing landed; the Merchant retries |
| Crash or `kill -9` after the commit, before the events or the 202 | The Merchant gets no 202 and retries under a new Submission; the duplicates replay as `already_applied` (A3). The first `submission_id` was never returned, so its missing `accepted` events matter to no one |
| The events write fails after the commit | 500, so the Merchant retries: the same as the row above |
| Writing a `refused` event fails (a full disk) | The refusal still goes out: the event is best effort and never turns a 4xx into a 500 |
| Registry missing at startup | `python -m catalog.api` exits 2 naming the path and serves nothing. Under the supervisor it restarts every 5 s, each exit logged, until a Merchant is created |
| Registry removed while running | `verify` raises: 500 for that request |
| SIGTERM from the supervisor mid-request | uvicorn finishes the requests in flight; the SIGKILL 10 s later is the crash row above |
| Two API processes | The second can't bind the port and exits 1. Appends from both would be safe anyway: Delta appends don't conflict |

## Implementation decisions

1. **One route, with an `async` handler that does the blocking work inline.** The handler must be `async` to read the body as a stream and count it. It then calls `json.loads`, `check_batch`, `landing.append` and `events.emit` directly, which blocks the event loop, so requests run one at a time once their bodies have arrived. Ceiling: no overlap between requests, and parsing a 10k-Change body takes up to about a second (plan-v1, "Is it Python only?"). Threads would buy nothing for a parse the GIL serializes, and would need a lock around the shared `DeltaTable`. 4c's group commit restructures this anyway.
2. **Checks run cheapest first: disk, auth, size, parse, envelope.** The disk guard is one `statvfs`, auth reads only a header, the size check reads `Content-Length` and then counts, and nothing is parsed until the body is complete. Disk comes before auth so that a low disk refuses everything. An unauthenticated caller can then tell the disk is low, which binding to `127.0.0.1` makes moot.
3. **Auth is `Authorization: Bearer <key>`, with the scheme case-insensitive.** `merchants.verify` returns the `Merchant`, or raises `Denied` with reason `unknown_key` or `revoked`. The API adds `no_key` for a missing or non-Bearer header. All three get the same 401 body, with `WWW-Authenticate: Bearer`. Raising rather than returning None means a forgotten check fails closed: an uncaught `Denied` is a 500, never a pass. This changes 4a's "returns nothing" to "raises", and 4a's `verify` tests with it. A missing registry still raises `sqlite3.OperationalError`, as in 4a.
4. **Body cap: 32 MiB (33,554,432 bytes), counted rather than trusted.** A `Content-Length` over the cap is refused before reading. Otherwise the handler counts streamed bytes and stops at cap + 1. B8 said middleware, but this is the only route with a body (4d's is a GET), so the check lives in the handler.
5. **Disk guard: 503 when `shutil.disk_usage(data).free` is under 5 GiB**, checked on every request. The cap and the threshold are `create_app` parameters, so tests can force both without a 32 MiB body or a full disk.
6. **Response shapes.** A 202 body is `{"submission_id", "accepted": <count>, "rejected": [{"index", "errors": [...]}]}`. Every refusal uses FastAPI's `{"detail": "<message>"}`, with status 400 (`bad_json`, `bad_batch`), 401, 413 or 503. A malformed body gets 400 rather than FastAPI's 422, since nothing here is a FastAPI model.
7. **A request whose Changes are all invalid still gets 202 and a `submission_id`**, with no commit. A Merchant then handles one response shape, and `GET /submissions/{id}` reports every Change as `rejected`. The alternative is 422 with the same body and no Submission.
8. **Landing, then events.** Each request takes one `received_at = datetime.now(UTC)` and the matching `now_ms`. Its `submission_id = str(uuid.uuid7())` (standard library) is minted before the append, so every event for it falls in its hour or later (A12). The accepted Changes go to `landing.append` as `(submission_id, index, change)` in one commit, and `append` computes the partition. Only after that commit does one `events.emit` write:
   - an `accepted` event per landed Change: `submission_id`, `change_index`, `merchant_id`, `merchant_product_id`, `partition`
   - a `rejected` event per invalid Change: `submission_id`, `change_index`, `merchant_id`, `errors`

   Writing events after the commit means there is never an `accepted` event for a Change that didn't land. The 202 goes out only after both. The process name is `api`, so its files are `events/<hour>/api-<pid>-<nonce>.jsonl`.
9. **A `refused` event for every 400, 401, 413 and 503**: `{type: "refused", status, reason}`, plus `merchant_id` when known. The reason is one of `low_disk`, `no_key`, `unknown_key`, `revoked`, `too_large`, `bad_json` or `bad_batch`. One helper builds both the response and the event, so counting auth failures by reason (the 4a review's ask) counts 413s and 503s too, at no extra cost. It is best effort: an `OSError` while writing it is swallowed. It never holds the key, its hash or the body.
10. **Startup.** `create_app` probes the registry with the read-only query `verify` runs, and raises on a missing or unreadable one. It then runs `landing.ensure` on the Landing log and opens the `EventLog`. The CLI turns the registry error into argparse's exit 2, with the path in the message.
11. **CLI.** `python -m catalog.api [--data data] [--db data/merchants.sqlite] [--port 8000]` calls `uvicorn.run(app, host="127.0.0.1", port=...)` under `entry.exit_with`. There is no `--host` flag: binding anywhere else needs TLS and rate limits first (plan-v1, section C). uvicorn handles SIGTERM and SIGINT itself and finishes the requests in flight.
12. **Procfile.** Add `api: python -m catalog.api`. The supervisor restarts it like any process. Only a worker's exit code is fatal, so a missing registry becomes a logged restart every 5 s rather than a stop.

## Testing decisions

- **Location:** `tests/integration/test_api.py`, using FastAPI's `TestClient` (in process, no socket) against `create_app` over `tmp_path`. The registry is a real SQLite file filled with `merchants.create`, the Landing log is real and read back with `landing.read`, and events are real, read with `events.read` and folded with `status.fold`. Only the size cap and the disk threshold are injected.
- **Test points (seams), confirmed:**
  1. **The HTTP app** (new, red first): every failure-scenario row except the CLI and crash rows, sent as `TestClient` requests. They are observed through status codes, response bodies, the Landing log (its rows, and its version: exactly one commit per request with an accepted Change, none otherwise), events and `status.fold`.
  2. **`merchants.verify`** (changed, red first): 4a's "is None" asserts become `Denied` with the right reason. The missing-registry test stays.
  3. **The CLI** (new): `main(argv)` with a missing registry exits 2 naming the path. With a good registry and `uvicorn.run` monkeypatched to record its arguments, it binds `127.0.0.1` on the given port. Real serving over a socket is 4e's end-to-end test.
- **Security asserts:** neither the key nor its SHA-256 appears in any event file's bytes after the auth tests, and the 401 body is identical for all three reasons.
- **Crash rows:** the events-failure row is one test: a failing `emit` gives 500 with the rows already landed, which makes a retry the duplicate case. The `kill -9` row needs no new test, because the retry it causes is that same duplicate case, which A3's rules and the replay property already cover.
- **Coverage:** `api.py` is a shell module under the 90% package gate.
- **Prior art:** `test_merchants.py` (the registry fixture, CLI exit codes) and `test_worker.py` (the `Env` helper, CLI tests).

## Out of scope

- Group commit (4c), `GET /submissions/{id}` and its IDOR check (4d), and the end-to-end test through a worker (4e).
- TLS, any bind address other than `127.0.0.1`, per-merchant rate limits, and a cap on concurrent connections (design non-goals, plan-v1 section C).
- Idempotency keys: duplicates are already safe (A3).
- `Retry-After` on 503, content-type checks (every body is parsed as JSON) and OpenAPI docs.
- Making a missing registry fatal to the supervisor, which would be a supervisor change. For now it's a logged restart loop.
- Metrics SQL for freshness and refusals (7c).

## Size

About 90 lines of production code (about 85 in `api.py`, about 5 in `merchants.py`), about 200 of tests and 1 `Procfile` line, so under 300. The `uv.lock` change from the new dependencies is generated, and isn't counted.
