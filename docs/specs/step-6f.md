# Step 6f: Jev in the pipeline (mini PRD)

Status: approved Oct 4, with the split, decisions 1 to 7 and the test points as written; open questions answered below. Plan row: [plan-v1.md, PR steps, 6f](../plan-v1.md) ("Jev in the pipeline", with four open points: the batch budget, the key through the supervisor, cost, and the `Procfile` switch), and B4 (Jev about 250 ms a call, 80 requests/s, 1M Listings about 3.5 h and $62). Design: [Categorization](../design-commerce-ingestion-pipeline.md) (the `classify` contract; "step 6f moves the pipeline onto it") and lifecycle step 5 ("The timeout (200 ms) covers the whole batch"). Builds on [step-6e.md](step-6e.md) (the decision and `taxonomy_version` decision 6), [step-6b.md](step-6b.md) (`EmbeddingClassifier`, `budget`, `top`) and [step-6c.md](step-6c.md) (the Backfill reclassifies rows on another version). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

The eval chose Jev after an embedding shortlist of 50, at threshold 0.40, but `JevClassifier` is an eval candidate only: it has no threshold, no budget, calls one at a time with up to 5 retries (up to 15 s of backoff per Listing), and defaults to a shortlist of 10 and 500 description characters. The worker and the Backfill don't know a `jev` kind, the `Procfile` runs `fake`, and `taxonomy_version` names only the taxonomy and the embedding model, so changing the threshold or shortlist would leave stored rows on the old answers forever.

The design's 200 ms batch timeout can't hold: one Jev call alone takes about 250 ms.

## Solution: two PRs

| PR | What | Gate |
|---|---|---|
| **6f.1** | `JevClassifier` meets the worker's `classify` contract (threshold, batch budget, parallel calls under the rate limit, no retries); a `jev` kind for the worker and the Backfill; `taxonomy_version` names every setting; a missing key is a fatal exit; spend per batch in the `batch` event | None: tests inject `call`, no paid API |
| **6f.2** | The `Procfile` runs `--classifier jev` for the workers and the Backfill; the e2e test rewrites them to `fake`; README says to export the key and download the model; one supervised run by hand on this Mac with 20 Listings | Your key in `TYPESAFE_API_KEY` and your OK on the spend (20 calls, under a cent) |

### 6f.1: the classifier

1. **Settings** (constants in `jev.py`, the eval's winners): `SHORTLIST = 50`, `THRESHOLD = 0.40`, `DESCRIPTION = 200`. `JevClassifier`'s defaults become these; the eval passes `threshold=0` and `budget=inf` and keeps its behaviour (one call at a time, every answer raw).
2. **Threshold:** an answer below `THRESHOLD`, or a key outside the options, becomes `(Uncategorized, confidence)`. At threshold 0 (the eval) an unknown key stays as itself, so 6d still counts it `invalid`.
3. **Batch budget: 10 s** for the whole `classify` call (design change, question 1). Listings are shortlisted in chunks of 16 (`EmbeddingClassifier.top`), and their Jev calls go to a thread pool; no new call starts after the deadline; calls in flight finish (each bounded by a 10 s HTTP timeout); Listings never started answer `None`, so the worker stores them Uncategorized, flagged, and the Backfill retries them.
4. **Rate:** each worker starts at most `80 / workers` calls per second (20/s with 4 workers), paced by start time, with `ceil(rate × 0.5)` threads (10). The Backfill makes no Jev calls (it appends `op=reclassify`; the workers classify), so the 4 workers share the whole 80/s. No shared limiter across processes.
5. **No retries in the pipeline:** `http(attempts=1)`. The first failed call (429, 529, network, bad JSON) stops new calls for the batch; answers already in hand are kept, the rest answer `None`. If no Listing got an answer, `classify` raises the first error instead, so `classify_failed` carries it. The Backfill is the retry.
6. **`taxonomy_version`:** `{taxonomy}+bge-small-en-v1.5+jev-1.13.0+k50+d200+t0.40` (6e decision 6). `classify.taxonomy_version("jev")` builds it from the constants without loading the model or reading the key, for the Backfill. The `embedding` kind stays as it is (`{taxonomy}+bge-small-en-v1.5`): it is no longer the choice, and nothing stores rows under it outside tests.
7. **The key:** `jev.http()` raises `KeyMissing` (a `RuntimeError`) when `TYPESAFE_API_KEY` is unset or empty; the worker maps it to fatal exit code 7, so the supervisor stops instead of restarting forever. The supervisor already passes its environment to its children. The key lives only in the `call` closure; errors carry status codes and up to 300 bytes of the response body, never headers.
8. **Spend:** the worker's `batch` event gets `usd`, the classifier's spend during that batch, rounded to 6 places, when the classifier has a `usd` attribute. Summing it over the events gives the cost so far.

### 6f.2: the switch

- `Procfile`: `worker-*` and `backfill` lines say `--classifier jev`.
- `tests/integration/test_e2e.py` rewrites those lines to `--classifier fake` (one more `re.sub` with its `assert n == 4` / `n == 1`), so CI never calls the paid API or needs the model.
- README: `export TYPESAFE_API_KEY=…` and `python -m catalog.classify --download` before `python -m catalog.supervisor`.
- By hand: the supervisor on this Mac, 20 Listings posted through the API, then the Listing Store shows their categories and the events' `usd` sum. The PR states the result.

## User stories

1. As you, the workers classify with the classifier the eval chose, at its threshold, with no code path that can spend past 80 calls/s.
2. As you, changing the threshold, shortlist, description length or Jev model reclassifies every stored Listing through the Backfill, because the version names them all.
3. As you, a missing key stops the system at start with a message naming the variable, instead of a restart loop.
4. As you, a Jev outage never stalls ingestion: Listings go Uncategorized and flagged, and the Backfill catches them up.
5. As you, the events tell what Jev has cost so far.
6. As CI, no test calls the paid API.

## Failure scenarios

| Scenario | Expected |
|---|---|
| `TYPESAFE_API_KEY` unset or empty at worker start | `KeyMissing`, exit 7, the supervisor stops everything with code 7; no call made |
| The key leaking into an event, stderr or the Listing Store | Never: the key is only in the closure's header; `repr` of an error holds the code and body only |
| Jev answers 429 mid-batch (a bulk upload) | New calls stop; answered Listings keep their answers; the rest are Uncategorized, flagged; `classify_failed` counts them; the Backfill retries |
| Jev down for every call | `classify` raises the first error; the whole batch is Uncategorized and flagged (stored answers kept for unchanged Listings, design step 5); the worker moves on |
| A 1,000-Change batch | At 20/s, about 200 Listings get answers within the 10 s budget; the rest are flagged for the Backfill |
| A call hangs | Bounded by the 10 s HTTP timeout; the batch takes at most about 20 s, under the 60 s heartbeat limit (`STALE`) |
| Jev answers a key outside `"1"` to `"50"`, or no probability for its choice | Uncategorized with confidence 0, not flagged (a real answer, like any below the threshold) |
| A malformed response (missing `answers`, `usage`) | Counts as a failed call (decision 5) |
| A worker killed mid-batch with calls in flight | The batch replays (at least once); those calls are paid twice, at most one batch's worth (about $0.01) |
| All 4 workers calling at once | 4 × 20/s = 80/s, Jev's limit; a faster Jev doesn't change the pace, since it's set by start times |
| The model missing from `models/` | `ModelMissing`, exit 6 (6b), before any call |
| The Backfill under `--classifier jev` with no key | Runs: it needs only the version string, not the key |
| The threshold or a setting changes | A new `taxonomy_version`; the Backfill reclassifies every stored row (about $62 per 1M Listings) |
| e2e test run in CI | Its Procfile copy says `fake`; it fails loudly if the shipped lines change shape |

## Implementation decisions

1. **A 10 s budget for Jev**, against the design's 200 ms. 200 ms can't fit one call. 10 s gives about 200 answers per worker batch at 20/s, adds at most 10 s to freshness (target: 5 min at p99) and stays well under the 60 s heartbeat. Alternative: a shared classifier process with a queue, which the plan offered; more code (a new process, IPC, its own supervision) for the same 80/s ceiling.
2. **A static split of the rate limit** (`80 / workers` each), since the worker already knows `--workers`. Ceiling: an idle worker's share is wasted, so a bulk upload to few partitions runs below 80/s (`ponytail:` comment; a shared limiter if Phase 7 shows it matters).
3. **No in-call retries in the pipeline.** The Backfill already retries flagged rows, and a retry with backoff inside the batch would blow the budget and the heartbeat. The eval keeps its 5 attempts.
4. **Stop on the first failure** rather than skipping one Listing and calling on: a 429 means slow down, and an outage would otherwise spend the budget on calls that fail.
5. **Thresholding inside `JevClassifier`**, as the contract says (the classifier owns its threshold), with the eval passing 0.
6. **Spend on the `batch` event**, the cheapest place that already exists. A spend cap is out of scope.
7. **Two PRs**: 6f.1 is code under test with no gate; 6f.2 needs your key and spend, and changes what `make dev`-style runs do.

## Answered questions

1. **The design's 200 ms timeout becomes 10 s for Jev:** yes; the design doc says so in this PR.
2. **The `embedding` kind's version:** stays as is.
3. **The 20-Listing hand run in 6f.2:** yes.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`JevClassifier` in the pipeline** (new, red first, `tests/integration/test_jev.py`, injected `call`, `clock` and `sleep`): below 0.40 or an unknown key gives Uncategorized; at 0.40 exactly keeps the answer; past the budget the rest answer `None`; a failed call stops new calls, keeps earlier answers and makes the rest `None`; all calls failing raises the first error; starts are paced at `rate`; with `threshold=0` an unknown key stays as itself (the eval).
  2. **`http(attempts=1)` and `KeyMissing`** (changed, `tests/integration/test_jev.py`): one attempt, no sleep, on 429; an empty key raises `KeyMissing`.
  3. **`taxonomy_version("jev")`** (new, red first, `tests/integration/test_classify.py`): equals the classifier's own version, and each setting changes it.
  4. **Worker `--classifier jev` with no key** (new, red first, `tests/integration/test_worker.py`, via `main`): exits 7 before claiming partitions.
  5. **`batch` event `usd`** (new, red first, `tests/integration/test_worker.py`, a fake classifier with a `usd` attribute): the event holds the spend during that batch; absent for a classifier without one.
  6. **e2e** (6f.2, changed): the shipped Procfile's `jev` lines are rewritten to `fake`.
- **No real API, model or network in tests.** The thread pool runs for real with an injected `call`; timing is injected, not slept.
- **Coverage:** `jev.py` stays under the 90% gate through its seams.

## Out of scope

- A shared classifier process or a cross-process rate limiter.
- A spend cap or alert.
- Tuning the Backfill's `LIMIT` and `INTERVAL` for a 1M initial load (Phase 7 measures it).
- Classify latency as a metric (7c, from the 6b review).
- Prompt tuning, other shortlist sizes, other Jev models.

## Size

- 6f.1: about 90 lines of code, 140 of tests.
- 6f.2: about 10 lines, plus the README and the run's result.
