# Step 4d: `GET /submissions/{id}` (mini PRD)

Status: approved Oct 3. Confirmed: 404 for every "not yours" (decision 2), UUIDv7-only ids (decision 3), the response shape (decision 5), minting the id from the API clock (decision 6), and the test points. Plan row: [plan-v1.md, PR steps, 4d](../plan-v1.md). Design: the [Ingestion API row and "Submission status"](../design-commerce-ingestion-pipeline.md), A12 in [plan-v1.md](../plan-v1.md), and the Phase 4 tests ("a merchant can't read another merchant's Submission"; "status moves through pending, then done, then a mix of outcomes"). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

A Merchant gets a `submission_id` back from `POST /listings:batch` (4b) but has no way to learn what happened to its Changes. The pieces exist: the API writes an `accepted` or `rejected` event per Change, workers write one Outcome event per Change, `events.read(since, submission_id)` reads them from an hour onward, and `status.fold` turns them into one Outcome per Change. No route joins them, and nothing stops one Merchant from reading another's Submission.

One gap in 4b blocks A12: the `submission_id` comes from `uuid.uuid7()`, which reads the system clock, while the API's events use its injected clock. In tests (clock fixed at Sep 30) the lookup would start days after the events it needs. The id must carry the API's own clock.

## Solution

1. `src/catalog/api.py`: a second route, `GET /submissions/{submission_id}`, on the same app and the same Bearer auth. It reads the Submission's events from the hour in its UUIDv7 onward, folds them, and answers 200 only when the Submission belongs to the caller; 404 otherwise.
2. The POST mints the `submission_id` from the API's clock, so the timestamp in the id and the hour of its events agree.
3. No change to `status.fold` or `events.read`.

## User stories

1. As a Merchant, I GET my Submission and see each Change's Outcome by index, a count per Outcome, and whether it's done, so I know when to stop polling and which Changes to fix.
2. As a Merchant, right after a 202 my accepted Changes show `pending` and my invalid ones `rejected`; as workers report, `pending` turns into `written`, `stale` and so on.
3. As a Merchant, a Change replayed after a worker crash keeps its best Outcome (a `written` Change never turns into `already_applied`).
4. As a Merchant, another Merchant's `submission_id` gets the same 404 as one that doesn't exist, so I can't tell whether it exists.
5. As a Merchant, a missing, wrong or revoked key gets the same 401 as on the POST.
6. As a Merchant, I can still read status while the disk is low, since a read writes nothing to the Landing log.
7. As the operator, every refused lookup leaves a `refused` event (as in 4b), and a lookup of another Merchant's Submission is counted apart from an unknown one, so I can spot someone probing ids.
8. As the operator, a lookup reads only the event hours from its Submission's timestamp onward (A12), and a slow lookup doesn't stall the POSTs behind it.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| Own Submission, no worker events yet | 200; accepted Changes `pending`, invalid ones `rejected`; `done` false |
| Own Submission, every Change has an Outcome | 200; `done` true, counts per Outcome |
| A Change reported twice (crash replay: `written`, then `already_applied`) | Its Outcome is `written`, the best one |
| Every Change was invalid (4b's 202 with no commit) | 200; every Change `rejected`; `done` true |
| Another Merchant's Submission | 404, the same body as an unknown id; a `refused` event, reason `not_owner`, naming the caller |
| Unknown but well-formed UUIDv7 | 404; reason `unknown_submission` |
| Not a UUID, or a UUID of another version | 404 before any event is read; reason `unknown_submission` |
| Upper-case or brace-wrapped form of a real id | Found: the id is normalized before the lookup |
| A UUIDv7 dated in the future | 404: no event hour is that late |
| No key, unknown key, revoked Merchant | 401 with `WWW-Authenticate: Bearer`, the same body as the POST; reasons as in 4b |
| A rotated key | Still reads the Merchant's older Submissions: ownership is the `merchant_id`, not the key |
| Free disk under 5 GiB | 200: the disk guard is for writes only |
| A worker's event line torn by a crash, or still being written | Skipped by `events.read`; that Change shows its earlier state until the replay writes it again |
| An event file removed mid-scan (future retention) | Skipped by `events.read` |
| An event of the Submission in an hour before its id's timestamp | Not read. Only happens if a clock steps back across an hour boundary between minting the id and writing the event (ceiling, below) |
| `events.read` raises (an unreadable directory) | 500; nothing changes, the Merchant retries |
| Writing the `refused` event fails | The 401 or 404 still goes out (best effort, as in 4b) |
| A lookup of an old Submission scans a lot of hours | It runs in a worker thread, so POSTs keep being served (decision 4) |

## Implementation decisions

1. **One shared auth step.** The POST's header parsing and `verify` call move into a helper both routes use, so the two can't drift. It returns the `Merchant` or the 401 response (with its `refused` event).
2. **Every "not yours" is 404, never 403.** An unknown id, a malformed id, and another Merchant's id get the same `{"detail": "no such submission"}`. A 403 would confirm the id exists. The `refused` event still tells them apart for the operator: reason `not_owner` (with the caller's `merchant_id`) or `unknown_submission`.
3. **The id must parse as a UUIDv7.** `uuid.UUID(raw)` with `version == 7`, else 404 without reading anything. The canonical `str()` of the parsed id is what's looked up, so case and braces don't matter. `since` is the hour of its 48-bit millisecond timestamp.
4. **An `async` handler that runs only the read in a thread.** `events.read` plus `status.fold` go through `run_in_threadpool`, so a long scan doesn't block the event loop, where the POST does its work inline (4b decision 1). Auth and every `events.emit` stay on the loop thread, so the shared `EventLog` is never written from two threads (a large POST write could otherwise interleave with a refusal line and tear both).
5. **Response:** `200 {"submission_id", "done", "counts": {outcome: n}, "changes": [{"index", "outcome"}]}`, Changes in index order. A 10k-Change Submission is about 300 KB. The `errors` of a rejected Change aren't repeated: the POST's 202 already returned them, and `fold` doesn't keep them.
6. **The POST mints the id from the API's clock.** `uuid.uuid7()` supplies the random bits and the version and variant; its 48-bit timestamp is replaced with the request's `received` milliseconds. One line, no new dependency, and production behaviour is unchanged since the default clock is `time.time`. This is a change to 4b's code.
7. **No disk guard on the GET.** It writes only a best-effort `refused` event.
8. **Ownership comes from the events.** `fold` returns the `merchant_id` its events carry (the API stamped it from the key, never from the payload). A Submission is the caller's when that equals the authenticated `merchant_id`.

## Testing decisions

- **Location:** `tests/integration/test_api.py`, extending its `Env` helper with a `get(submission_id, key=...)`. A real registry, Landing log and event files under `tmp_path`, as in 4b.
- **Worker Outcomes are written by an `EventLog("worker")`** with the fields `worker.process_batch` emits (`submission_id`, `change_index`, `merchant_id`, `type`), at a later injected clock. Running a real worker is 4e's end-to-end test.
- **Test points (seams), confirmed:**
  1. **The GET route** (new, red first): every failure-scenario row except the torn-line, removed-file and slow-scan rows, which `events.read` already covers (`test_events.py`) or which can't be observed in process. Observed through status codes, bodies and `refused` events, including that the 404 body for `not_owner` is byte-identical to `unknown_submission`.
  2. **The id's timestamp** (changed POST behaviour, red first): the 202's `submission_id` is a UUIDv7 whose milliseconds equal the injected clock. This test fails today, since the test clock is days behind the system clock.
  3. **The hour floor:** an event of the Submission planted in the hour before its id's timestamp is not read, which proves the lookup starts at the id's hour (A12).
- **The Phase 4 status test** ("pending, then done, then a mix of outcomes") is one test across point 1: POST a mixed batch, GET (pending and rejected), write worker Outcomes for some (still not done), then the rest including a replay (done, best Outcome kept).
- **Coverage:** `api.py` stays a shell module under the 90% package gate; `status.py` keeps 100% with no change.

## Out of scope

- Group commit (4c) and the end-to-end test through a real worker (4e).
- Bounding how far back a lookup scans. A Merchant can craft a UUIDv7 dated 1970 and make one lookup read every event hour; the events retention (3 days, A13) is what bounds it, and the API is on `127.0.0.1` behind auth. Ceiling noted in a `ponytail:` comment.
- Reading from the hour before the id's (to tolerate a clock stepped back across an hour). Noted as a ceiling; NTP slews rather than steps in normal running.
- Listing a Merchant's Submissions, paging the `changes` list, per-Change errors in the GET, and caching folds.
- Metrics SQL for lookups and refusals (7c).

## Size

About 35 lines of production code in `api.py` (the shared auth, the route, the id minting) and about 110 of tests, so well under 300.
