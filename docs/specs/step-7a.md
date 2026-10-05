# Step 7a: load generator (mini PRD)

Status: approved Oct 5, with the test points as written. Question 1: keep `np.maximum.at`. Question 2: the hand run uses `fake` only. Plan row: [plan-v1.md, PR steps, 7a](../plan-v1.md) (the load generator; from 6f.3's review, the `deeper` shortlist's scoring under load; from the backfill-test fix (#58), a `--min-free` flag on the API). Phase: [plan-v1.md, Phase 7](../plan-v1.md) (scenarios: steady 50/s, a 10k-item bulk batch, a 1M initial load). Design: [Goals](../design-commerce-ingestion-pipeline.md) ("about 1M Listings, about 50 changes/s at peak, and batches of up to 10k"; "stress-testable for free"). Builds on [step-4b.md](step-4b.md) (`POST /listings:batch`, the disk guard), [step-4e.md](step-4e.md) (the e2e test's Procfile rewrite) and [step-6f.md](step-6f.md) (6f.3's shortlist). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Phase 7 has to show the p99 freshness SLO (under 5 minutes) at 50 changes/s, plus a 10k bulk batch and a 1M initial load. Nothing sends that traffic today: the e2e test posts a handful of Changes by hand.

Two smaller items ride along:

- **The shortlist's scoring.** 6f.3 scores 14,605 Category texts and keeps each Category's best with `np.maximum.at`, which the review guessed at about 1.2 s per 1,000 Listings.
- **The disk guard in tests.** The API refuses every write below 5 GiB free (A13), with no flag to change it. `test_e2e.py` runs the real API process, so on a nearly full machine every post gets 503 and the test fails, as after the 6e eval runs.

## Measured (Oct 5, this Mac)

The `deeper` scoring on random vectors of the real shape (14,605 texts, 1,862 Categories), for 1,000 Listings:

| | chunks of 16 (as `top` runs) | one chunk of 1,000 |
|---|---|---|
| `np.maximum.at` (today) | 0.40 s | 0.44 s |
| `np.maximum.reduceat` over texts sorted by Category | 0.02 s | 0.04 s |

Both give the same answers. 0.40 s is a third of the review's guess, and it isn't the cost that matters: embedding 1,000 Listings takes about 10 s (16 in about 160 ms), and Jev's 10 s batch budget only shortlists the Listings it reaches, about 200 per batch at 20 calls/s, so the scoring spends about 0.08 s per batch.

## Solution

1. **`python -m catalog.load`**, a new module. It posts generated Changes to a running API and prints one JSON summary line.
   - Flags: `--url` (default `http://127.0.0.1:8000`), `--rate` (Changes/s, 0 = as fast as the API answers), `--changes` (total), `--batch` (Changes per request, up to 10,000), `--keys` (the key space), `--order {random,sequential}`, `--processes` (default 16; 4 at approval, see Outcome), `--seed`.
   - The Merchant's key comes from `CATALOG_API_KEY`, never a flag (a flag shows in `ps`), and is never printed.
   - The three Phase 7 load shapes are flag sets, not code:
     - steady: `--rate 50 --changes 30000 --batch 1`
     - bulk: `--changes 10000 --batch 10000`
     - initial load: `--changes 1000000 --keys 1000000 --order sequential --batch 1000 --rate 0`
2. **Changes.** Change `i` of a run is a pure function of `(seed, i, start_ms)`:
   - key `p{n}`, with `n` random in `0..keys-1`, or `i` itself when sequential;
   - `source_version = start_ms + i`, so a later Change of a key is newer, and a second run is newer than the first;
   - an upsert whose title and description (first 500 characters) cycle through `eval/labels.jsonl`'s 198 real Amazon Listings, so an `embedding` or `jev` run sees real text. Price and availability come from the seed.
3. **Processes.** Process `j` of `P` sends batches `j, j+P, j+2P, …` at `rate / P`, with `multiprocessing` and stdlib `urllib`; the HTTP client (`httpx2`) is a dev dependency only. Each send is due at `t0 + n / rate`; a late one goes at once, and the summary reports how late the latest was.
4. **The summary** (stdout, one JSON line): Changes sent and accepted, requests by HTTP status, connection errors, achieved Changes/s, POST latency p50/p99/max, the latest send's lateness, and `start_ms` (so 7b and 7c can find the run's Changes). Freshness is 7c's metrics SQL over the events, not the generator's job.
5. **`--min-free` on the API** (bytes, default `MIN_FREE`, 5 GiB), passed to `create_app`. The e2e test's Procfile rewrite adds `--min-free 0` to the `api` line.
6. **The shortlist stays on `np.maximum.at`.** A `ponytail:` comment on `_similarity` records the measurement and the switch to `reduceat` if a run ever shortlists whole batches (question 1).

## User stories

1. As you, I run one command and the system takes 50 changes/s, a 10k batch or a 1M initial load, and I see what the API did with it.
2. As you, two runs with the same seed send the same Listings, so a slow run can be repeated.
3. As you, the e2e test passes on a machine with under 5 GiB free.

## Failure scenarios

| Scenario | Expected |
|---|---|
| `CATALOG_API_KEY` unset | Exit 2 naming the variable, before any process starts |
| A wrong or revoked key (401) | That process stops after its first 401; the run exits 1 and the summary counts the 401s. The key is in no output |
| The API isn't up (connection refused), or goes down mid-run | Counted as connection errors; sending goes on (the supervisor may restart it). Exit 1 if no request got a 202 |
| The API refuses for low disk (503) | Counted by status; sending goes on |
| A request times out (30 s) | Counted as an error; not retried, since a retry is a duplicate (7b's scenario, on purpose) |
| The API answers slower than the rate | Sends fall behind; achieved rate and lateness show it. No error |
| Ctrl-C | Every process stops after its request in flight; the summary covers what was sent; exit 130 |
| A child process crashes | The parent reports which, prints the summary of the others, exits 1 |
| A 10k batch with 500-character descriptions | About 8 MB, under the 32 MB cap |
| 1M Changes at under 1,000/s | `source_version` runs up to 1M ms (17 min) ahead of the clock, inside A14's 24 h slack. Over about 86M Changes it wouldn't be: `--changes` is capped at 10M |
| Two generators at once, or a rerun | Both post as one Merchant; the later `start_ms` makes the later run's versions newer. Overlapping in time, some Changes go stale, which the oracles handle |
| The e2e test's API on a full disk | `--min-free 0`: the guard never refuses; a write that truly fails is a 500, as today |

## Implementation decisions

1. **One module, stdlib only:** `multiprocessing`, `urllib.request`, `json`, `random`. No new dependency.
2. **Open-loop pacing** (each send due at a fixed time, late ones go at once) rather than waiting for each answer before scheduling the next. A closed loop would slow down with the API and hide its latency. Ceiling: a blocked process can't send, so a very slow API caps the rate at `processes / latency` (a `ponytail:` comment).
3. **Deterministic Changes from `(seed, i, start_ms)`**, so 7b's oracles can rebuild what was sent without the generator saving it.
4. **No retries, no adversarial mixes.** Out-of-order, duplicate retries, a delete then a late update, and poison Changes are 7b's chaos scenarios. They reuse the Change function from point 2.
5. **Real titles from the labeled set**, not lorem ipsum: free, already in the repo, and the shortlist and Jev behave as on real Listings.

## Testing decisions

- **Test points (seams), to confirm:**
  1. **The Change function** (new, red first, `tests/unit/test_load.py`). Covers:
     - the same seed gives the same Changes;
     - every batch passes `envelope.check_batch` with nothing rejected;
     - keys stay in `0..keys-1`, and sequential order hits each once;
     - `source_version` grows with `i`.
  2. **One process's send loop** (new, red first, same file, with injected `post`, `clock` and `sleep`). Covers:
     - sends are due at `t0 + n / rate`, and a late one goes at once;
     - statuses and errors are counted, never raised;
     - a 401 stops the loop;
     - `rate 0` never sleeps.
  3. **`python -m catalog.load` against the real system** (new, red first, `tests/integration/test_e2e.py`, its `System` fixture). 2 processes and 200 Changes:
     - every request gets a 202 and every Change an Outcome;
     - the summary's counts match;
     - the key is in no output;
     - an unset `CATALOG_API_KEY` exits 2.
  4. **`--min-free`** (new, red first, `tests/integration/test_api.py`, as `test_the_cli_serves_on_localhost_only`): `api.main` with `--min-free` set above the disk's free space serves an app that answers 503 to a post. The e2e rewrite's `--min-free 0` is covered by every e2e test.
- **The hand run** (after tests pass, numbers in the PR, nothing committed). A scratch system with a `fake`-classifier Procfile, as the e2e test builds it:
  - 60 s steady at 50/s;
  - one 10k bulk batch.
- **Coverage:** new lines covered by test points 1–4; the 90% gate holds.

## Questions (answered Oct 5)

1. **Keep `np.maximum.at`** (solution 6), given 0.08 s per batch today? `reduceat` is about 5 lines and 20× faster. I'd switch only when something shortlists whole batches. **Yes: keep it.**
2. **The hand run uses the `fake` classifier**, so it costs nothing. A `jev` run at 50/s for 60 s would be about 3,000 calls, about $0.19. Want one too, or leave paid runs to 7d? **`fake` only.**

## Outcome (Oct 5)

The hand run, on the `fake` classifier, with 4 workers on this Mac:

| Run | Sent | Got 202 | Changes/s | POST p50 / p99 / max |
|---|---|---|---|---|
| Steady, 4 processes | 3,000 | 3,000 | 25.8 | 156 / 233 / 1,327 ms |
| Steady, 16 processes | 3,000 | 3,000 | 49.7 | 156 / 298 / 343 ms |
| Bulk, one 10k batch | 10,000 | 10,000 | n/a | 372 ms |

All 16,000 Changes got an Outcome, all `written`. A batch-1 POST waits for the group commit (about 156 ms), so the open loop's ceiling of `processes / latency` held 4 processes to 25/s. `--processes` now defaults to 16. The first steady run took 17 minutes of wall time for 116 s of monotonic time: the Mac slept during it, and `time.monotonic` stops while it sleeps. Run long loads under `caffeinate -i`.

## Out of scope

- Adversarial mixes, kills and the three oracles: 7b.
- Freshness, lag and the other metrics, and the runbook: 7c.
- The 1M initial load at full size, and CI's `stress-smoke`: 7d.
- A shared rate limiter for Jev across workers (6f decision 2).

## Size

About 110 lines in `load.py`, 3 in `api.py`, 2 in `test_e2e.py`'s rewrite, 1 comment in `classify.py`, about 110 of tests.
