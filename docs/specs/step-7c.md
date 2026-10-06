# Step 7c: metrics SQL, the events behind them, and the runbook (mini PRD)

Status: approved Oct 5, with the test points as written. Question 1: an `audit` table. Question 2: the store's share. Question 3: include maintenance. Plan row: [plan-v1.md, PR steps, 7c](../plan-v1.md) (metrics SQL and runbook, plus nine carried items from earlier reviews). Phase: [plan-v1.md, Phase 7](../plan-v1.md) (metrics as saved DuckDB SQL: freshness p50/p99, lag per partition, classify latency, stale/conflict/failed rates, Uncategorized rate, worker utilization, each Change counted once; a runbook for start, stop, rescale, reset and reading results). Design: [Observability and testing](../design-commerce-ingestion-pipeline.md), [plan-v1.md, DuckDB's role](../plan-v1.md). Builds on [step-7b.md](step-7b.md) (the chaos runner), [step-3e.md](step-3e.md) (the watch; decision 4 left the restart race to this runbook) and [step-4e.md](step-4e.md) (the API's watch). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Phase 7's exit needs the p99 freshness SLO measured at 50/s, and 7d's runs need numbers you can read. Today:

- No metric query exists. `duckdb` is a dependency that nothing in `src/` imports.
- Two of the design's metrics have no events to read: classify latency (nothing emits it) and append conflicts (`landing.append` retries a lost commit race silently).
- Several exits leave no trace in the events:
  - The supervisor logs `process_exit` only for a process it restarts. A process stopped at shutdown, and any other exit in the pass that hit a fatal one (the loop `break`s), go unlogged.
  - A process the watch stops logs a plain `worker_stop`, the same as a SIGTERM; the API logs nothing. The watch's hard exit (`os._exit(1)` after 30 s) leaves no line anywhere.
  - Change Export, Catalog Snapshots and the Backfill exit 1 on any error with the traceback only in stderr; the supervisor's `process_exit` has just the code.
- `classify_failed.error` is free text ("budget spent" or a `repr`), so counting outages means matching strings.
- A wrong or revoked `TYPESAFE_API_KEY` (Jev answers 401) isn't fatal: every Listing goes Uncategorized and flagged, the Backfill re-flags them every 10 s, and nothing stops.
- Merchant create, rotate and revoke leave no record of who changed what, when.
- There's no runbook. 3e left "wait a few seconds after a `kill -9`ed supervisor before starting another" to it, and 7b left "`pkill -f catalog.supervisor` after a `kill -9`ed runner".

## Solution: three PRs

One step would be about 600 lines, so it splits.

### 7c.1: exits you can see (code)

1. **Supervisor exits** (`supervisor.py`): on a fatal exit the pass doesn't `break`; it polls the rest, logs every exit it sees (`reason: exit`, no restart), then stops. `_stop` logs a `process_exit` for each child it stops: `reason: shutdown` with the code, after `wait`. A child that had already exited by then keeps `reason: exit`.
2. **Why the watch stopped a process** (`worker.watch`): before setting `stop` it records `stop.reason = "supervisor_gone"`. The worker's last event becomes `worker_stop` with that `reason`. The API, which has no `worker_stop`, logs `api_stop` with the same reason from its `stop` hook.
3. **A trace of the watch's hard exit**: before `exit(1)`, one line written with `os.open`/`os.write`/`os.close` (`O_APPEND|O_CREAT|O_NONBLOCK`) to a fresh events file, `{"type": "watch_exit", "process", "pid", "ts"}`, the line formatted before the deadline sleep. Every `OSError` is swallowed. No `EventLog`, no `print`, no flush: nothing the stuck thread could hold. The process name is passed to `watch_supervisor`.
4. **The error behind an exit** (`export.py`, `snapshots.py`, `backfill.py`, and `maintenance.py` for the same gap): each `run` loop catches `Exception`, logs `<process>_stop` with `error` (`repr(e)[:500]`, as `worker_stop` does) best effort, and re-raises. A clean stop logs `<process>_stop` with no `error`.
5. **A refused Jev key is fatal** (`jev.py`, `worker.py`): `jev.http` raises `KeyMissing("TYPESAFE_API_KEY was refused (401)")` on 401 and 403, without retrying. `worker._classify` re-raises `KeyMissing` instead of turning it into Uncategorized, and the worker's loop re-raises any `FATAL` type at once instead of after `ATTEMPTS` ticks. The worker exits 7 and the supervisor stops everything. Nothing is merged and no offset moves, so the batch reruns once the key is fixed.

### 7c.2: metric events and the saved SQL (code)

1. **Classify latency** (`worker._classify`): one `classify` event per classifier call, `{listings, ms}`, emitted with the batch's other notes after the MERGE commits (a retried batch doesn't repeat it).
2. **`classify_failed` gets structure**: `reason` (`budget` when the classifier answered None, `error` when it raised or answered with an error), `batch` (how many it was asked), beside the existing `listings`, `partitions` and `error`.
3. **Append conflicts** (`landing.append`): an optional `events` argument; on the `CommitFailedError` retry it logs `append_retry` `{rows, ms}` (the time the lost race added) best effort. The API's `Appender` and the Backfill pass theirs.
4. **Merchant audit** (`merchants.py`): an `audit` table in the same SQLite file (`merchant_id, action, at`), written in the same transaction as the change, so a create, rotate or revoke can't happen without its row. Never the key or its hash.
5. **`python -m catalog.metrics [--since 1h] [--data DIR]`**, a new module: loads `events.read(since)` into DuckDB as an Arrow table of `(type, ts, e)` with `e` the raw JSON (events vary in shape, so no inferred schema), and the Listing Store through `to_pyarrow_dataset()`. Prints one JSON object. Saved queries, each a SQL string in the module:

   | Metric | From |
   |---|---|
   | freshness p50/p99 (s) | `accepted` ts to the Change's first Outcome event ts, per `(submission_id, change_index)` |
   | lag per partition (commits) | the latest `batch` event naming each partition: `head + 1 - next[p][0]` |
   | classify latency p50/p99 (ms) | `classify` events |
   | stale, conflict, failed rates | each Change once, by `(submission_id, change_index)`, keeping its best Outcome by `status.fold`'s rank (Phase 7's rule); over all Changes with an Outcome |
   | Uncategorized rate | share of live Listings in the Listing Store whose Category is Uncategorized (see Question 2) |
   | worker utilization | per worker, the sum of `batch.ms` over the window |
   | append retries | `append_retry` count and mean `ms` |
   | refused writes | `refused` events by `reason` |

   The rank comes from `status._RANK` (made public as `status.RANK`), so the SQL and the fold can't drift.
6. **The chaos runner prints them**: `catalog.chaos`'s summary gains a `metrics` field from the same function, so 7d's runs report numbers without a second command.

### 7c.3: the runbook (docs, auto-merge)

`docs/runbook.md`: first run (download, a Merchant, the key), start, stop (Ctrl-C or `kill <supervisor pid>`), rescale (stop, edit the Procfile's `--workers`, start), a `kill -9`ed supervisor (wait until `pgrep -f catalog.worker` is empty, about 30 s at worst, then start: an early start exits 3 by design), a `kill -9`ed chaos runner (`pkill -f catalog.supervisor`), reset (stop, then remove `data/` and `state/`, which keeps the merchants), reading results (`catalog.metrics`, `GET /submissions/{id}`, which event answers which question), every exit code and what to do, and a chaos run. Linked from the README.

## User stories

1. As you, during a 7d run, I run `python -m catalog.metrics --since 10m` and read p99 freshness against the 5-minute SLO.
2. As you, after a stop I didn't expect, the events say which process exited, with what code, and why.
3. As you, with a bad TypeSafe key, the system stops at once with exit 7 instead of filling the store with Uncategorized Listings.
4. As you, I follow the runbook to rescale or recover from a `kill -9` without reading the code.

## Failure scenarios

| Scenario | Expected |
|---|---|
| Full disk while the supervisor logs exits at shutdown | Best effort, as today: `note` swallows it; every child is still stopped |
| Full disk, or a closed stderr, at the watch's hard exit | The `os.write` fails, is swallowed, and `os._exit(1)` still runs |
| The stuck thread holds a Python lock the trace would need | None needed: the line is built before the sleep, written with raw `os` calls |
| A worker stuck in a native call that doesn't release the GIL | The watch can't run at all (6b's rule); unchanged, and no trace. Documented in the runbook |
| Two exits in the pass that hit a fatal one, the fatal one second | Both logged; the supervisor's exit code is the fatal one's |
| Two fatal exits in one pass | Both logged `fatal`; the supervisor exits with the first one's code |
| Jev 401 mid-batch after some calls answered, or after another call failed | `KeyMissing` is raised anyway, so the worker exits 7 with nothing merged and the batch reruns once the key is fixed (changed at PR #79's review: letting the answers stand stored the rest Uncategorized for one batch) |
| Jev 401 during the Backfill | The Backfill doesn't call Jev, so the workers find it |
| Jev 429 or 5xx | Unchanged: retried, then Uncategorized and flagged |
| `append_retry`'s second attempt fails too | The error propagates as today; the event is still logged first |
| The audit insert fails (disk full) | The transaction rolls back; the key change didn't happen and the CLI says so |
| `catalog.metrics` while the system writes events | `events.read` trusts only complete lines; numbers cover what's complete |
| A window with no events | Counts are 0 and every other metric `null` (or `{}` for the per-key ones), exit 0 |
| An Outcome from a crash replay or a bootstrap | Counted once per Change, best Outcome (Phase 7's rule) |
| A Change accepted before `--since` with its Outcome inside it | No `accepted` event in the window, so it's left out of freshness; `--since` is read an hour wider for `accepted` events |
| A `kill -9`ed supervisor's workers still running when a new one starts | The new workers exit 3 and the new supervisor stops (3e, by design); the runbook says to wait |

## Implementation decisions

1. **Events, not new files or tables**, for every new signal except the audit: the reader, retention and per-process files already exist.
2. **The hard-exit trace skips `EventLog`**: it may raise after a torn write and start a new file name, and it runs Python code the stuck thread might be inside. A raw `os.write` of a prebuilt line is the least that can block.
3. **`KeyMissing` for a refused key**, not a new type: `entry.exit_with` maps exact types to exit codes, and the operator's fix is the same (set a working key).
4. **Events load into DuckDB as raw JSON**, read with `->>`: `pa.Table.from_pylist` on events of many shapes infers a sparse struct, and `batch.next` has partition numbers as keys.
5. **Freshness from events alone**: `accepted` to the first Outcome. It's what the merchant waits for, and needs no Landing log scan.
6. **No new process, no dashboard**: metrics are a command you run, as the plan says.

## Testing decisions

- **Test points (seams), to confirm:**
  1. **Supervisor exits** (`tests/integration/test_supervisor.py`, red first): with three fake children, one exiting fatally and one already exited in the same pass, each gets a `process_exit` and the third a `shutdown` one with its code; a plain stop logs `shutdown` for every child.
  2. **The watch** (`tests/integration/test_worker.py`, red first, with injected `alive`, `sleep` and `exit`): `stop.reason` is `supervisor_gone`; the worker's last event carries it; the hard exit writes one `watch_exit` line, and still calls `exit(1)` when the events dir is read-only.
  3. **The API's stop** (`tests/integration/test_api.py` or `test_e2e.py`, red first): a `kill -9`ed supervisor leaves an `api_stop` event with the reason.
  4. **Exit errors** (red first, one test each in `test_export.py`, `test_snapshots.py`, `test_backfill.py`, `test_maintenance.py`): a `run` whose tick raises logs `<process>_stop` with the error, then raises.
  5. **Refused key** (red first, `test_jev.py` and `test_worker.py`): a 401 from the fake `urlopen` raises `KeyMissing` after one attempt; a worker whose classifier raises it exits with 7 within one tick and moves no offset.
  6. **Classify events** (red first, `test_worker.py`): a batch with Writes to classify logs one `classify` event; a budget miss gives `classify_failed` with `reason: budget` and `batch`; a raise gives `reason: error`.
  7. **`append_retry`** (red first, `test_landing.py`): a commit race forced as 5d.1's test does logs one event with `rows`.
  8. **Audit** (red first, `test_merchants.py`): create, rotate and revoke each add one row; a rotate of an unknown Merchant adds none; no row holds the key.
  9. **Metrics** (red first, `tests/unit/test_metrics.py`, events written by hand to a temp dir, no system): freshness counts a replayed Change once; lag reads the latest `batch`; best-Outcome rates match `status.fold` on the same events; an empty window gives nulls.
  10. **The runner's summary has `metrics`** (existing `test_chaos_runner.py` in-process scenario, one assert added).
- **The hand run** (after 7c.2, numbers in the PR): `catalog.chaos steady --seconds 60` and `kill-worker`, with their metrics.
- **Coverage:** new lines covered by the points above; the 90% gate holds.

## Questions (answered Oct 5)

1. **The merchant audit: SQLite table or events?** The plan says "audit event". Events are kept 3 days (A13), so an audit there is gone in 3 days, and the CLI would write an event file under `data/`. A table in the merchants SQLite file is kept as long as the Merchants, and commits with the change. I'd use the table. OK? **Yes, the table.**
2. **The Uncategorized rate: the store's share, or per Change?** Outcome events don't carry the Category. The Listing Store's share of live Listings that are Uncategorized is what merchants see, and needs no new field. A per-Change rate would add `category` to every `written` event (about 30 more bytes per event, A13's budget). I'd use the store's share, plus `classify_failed.listings` over `classify.listings` as the outage rate. OK? **Yes, the store's share.**
3. **`maintenance.py` in 7c.1's exit errors**: the plan names Change Export, Catalog Snapshots and the Backfill; maintenance has the same gap and costs about 5 lines. Include it? **Yes.**

## Outcome

- **7c.1** (PR #79, Oct 5): every exit is in the events. Review fixes: a refused Jev key is raised even after another call answered or failed first (the failure row above, approved), a stale kill still dying at shutdown is logged `stale`, and the API runs under `events.stopping`. Kept, at review: `watch_exit`'s `deadline` field and its `ts` as the planned exit time; the watch tests beside the existing one in `test_chaos.py`.
- **7c.2**: the metric tests went to `tests/integration/test_metrics.py`, not `tests/unit`, since they write event files and a Listing Store. Review fixes (PR #80): a Change counts only if its `accepted` event is in the read range, so a replay of an old Change isn't a new one, and Backfill and rejected Changes drop out with no filter; lag is measured against the newest head any worker read, so a stalled partition shows; the API logs commit retries to a file of its own (the commit runs in a thread); `--since` takes `10m`-style durations; a corrupt Listing Store raises instead of reading as missing. Deferred to 7d: parsing each event three times.

## Out of scope

- A dashboard or a long-running metrics process.
- The Phase 7 cost measurements (`store.read`, MERGE file churn, offset fsyncs, landing re-reads): 7d, on the stress run.
- A watch on the chaos runner (7b's out of scope).
- Making the supervisor wait for a previous supervisor's workers: 3e chose the loud exit 3; the runbook covers it.
- Switching `test_e2e.py`'s inline oracle asserts to `chaos.oracles()`.

## Size

- 7c.1: about 90 lines of code, 150 of tests.
- 7c.2: about 150 lines of code (100 of them `metrics.py`), 140 of tests.
- 7c.3: the runbook, about 120 lines of docs.
