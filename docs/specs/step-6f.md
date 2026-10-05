# Step 6f: Jev in the pipeline (mini PRD)

Status: 6f.1 and 6f.2 done Oct 5 (see Outcome). 6f.3 done Oct 5 too (see Outcome); it was added and approved Oct 5 when you chose to fold step 6h's `deeper` shortlist into 6f. Approved Oct 4, with the split, decisions 1 to 7 and the test points as written; open questions answered below. Plan row: [plan-v1.md, PR steps, 6f](../plan-v1.md) ("Jev in the pipeline", with four open points: the batch budget, the key through the supervisor, cost, and the `Procfile` switch), and B4 (Jev about 250 ms a call, 80 requests/s, 1M Listings about 3.5 h and $62). Design: [Categorization](../design-commerce-ingestion-pipeline.md) (the `classify` contract; "step 6f moves the pipeline onto it") and lifecycle step 5 ("The timeout (200 ms) covers the whole batch"). Builds on [step-6e.md](step-6e.md) (the decision and `taxonomy_version` decision 6), [step-6b.md](step-6b.md) (`EmbeddingClassifier`, `budget`, `top`) and [step-6c.md](step-6c.md) (the Backfill reclassifies rows on another version). Terms follow [CONTEXT.md](../../CONTEXT.md).

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

## 6f.3: the `deeper` shortlist (addendum, Oct 5)

### Problem

Step 6h measured Jev with a shortlist of 50 built from Shopify's deeper Category names (the `deeper` recipe): 66.7% exact at threshold 0.40, against 54.0% for the bare paths the pipeline uses today, at the same cost per call ([step-6h.md](step-6h.md), [eval/report.md](../../eval/report.md)). The pipeline should use it.

Two things stop a one-line switch:

1. **Start time.** `deeper` embeds about 14,600 texts when the classifier is built. One process took about 49 s for the eval, and four workers starting together share the CPU. The supervisor kills a worker after 60 s without a beat, and the start counts as one. So the workers would be killed and restarted forever.
2. **Stored rows.** Rows classified with the old shortlist must be reclassified, or they keep the weaker answers forever.

### Solution: one PR

1. **`jev.TEXTS = "deeper"`**, a constant beside `SHORTLIST`, `DESCRIPTION` and `THRESHOLD`. `JevClassifier` gains a `texts` field (default `TEXTS`), and `jev.version` names it. The version becomes `{taxonomy}+bge-small-en-v1.5+deeper+jev-1.13.0+k50+d200+t0.40`, so every stored row is on an old version. Plan reclassifies a row when it changes, and the Backfill does the rest (6e decision 6, 6c). `classify.taxonomy_version("jev")` picks this up with no change, so the Backfill targets the new version.
2. **A disk cache of the text embeddings.** `EmbeddingClassifier` gains `cache: Path | None`.
   - With a cache directory, it loads the text vectors from `<cache>/<key>.npy`. The key is the SHA-256 of the model name and every (text, path) pair, so a new taxonomy, recipe or model never reads stale vectors.
   - A missing or unreadable file raises `ModelMissing`, the worker's existing fatal code 6, with a message naming `--download`. The supervisor stops instead of restarting a worker that can't start.
   - With no cache (the eval and the tests), it embeds as today.
3. **`python -m catalog.classify --download` also builds the cache** for Jev's recipe, once, after fetching the model. It writes atomically (a unique temp file, then `os.replace`), so a killed download leaves no half file. The README's setup line already runs it; the README says it now takes about 2 minutes (measured 100 s on this Mac).
4. **The worker** builds Jev's shortlist with `texts=classify.texts(tax, jev.TEXTS, taxonomy.load_deeper(tax))` and `cache=a.models / "texts"`. The `embedding` and `fake` kinds don't change.
5. **By hand, with your OK:** rerun 6f.2's supervised run (`spikes/run_6f2.py`, 20 Listings, under a cent). The PR states the time to the first beat, the answers and the spend.

### User stories

1. As you, the pipeline classifies with the shortlist that measured 66.7%, not 54.0%.
2. As you, every stored Listing is reclassified onto the new shortlist, by Plan when it changes and by the Backfill otherwise.
3. As the operator, four workers start within seconds, not tens of seconds, and a missing cache stops the system with a message naming the command to run.

### Failure scenarios

| Scenario | Expected |
|---|---|
| The cache file is missing (`--download` not rerun after this change) | `ModelMissing` naming `--download`; the worker exits 6 and the supervisor stops |
| The cache was built for another taxonomy, recipe or model | Its key differs, so the file isn't found: the same as missing, never stale vectors |
| The cache file is truncated or corrupt | `np.load` fails, so `ModelMissing` as above |
| The cache has the wrong shape (a bug, or a hand-copied file) | `ModelMissing`: the row count must equal the number of texts and the width the model's |
| `--download` is killed while writing the cache | The temp file is removed or left beside it under a unique name; the real file is absent or whole |
| Two `--download` runs at once | Each writes its own temp file; the last `os.replace` wins, and both are whole |
| Workers start before the cache exists | Each exits 6 at start; nothing is classified with the old shortlist |
| Stored rows on the old version | Reclassified when touched (Plan), else by the Backfill, through Jev. About $62 per 1M stored Listings |
| The Backfill and workers disagree on the version | Impossible: both build it from `jev.version` with the same constants |
| Memory | About 22 MB more per worker for the text vectors |

### Implementation decisions

1. **A fatal missing cache, not a fallback to computing it.** Computing it at start is exactly what gets the workers killed. Failing fast with the fix in the message matches how the missing model is already handled (6b decision 4).
2. **The key hashes the texts, not a version string.** The vectors depend on every text, so any change to the taxonomy file, the release or the recipe changes the key.
3. **The cache lives in `models/`**, which is gitignored and already holds the model, and is built by the same command.
4. **No new `--classifier` kind.** `jev` simply moves to the better shortlist; the old one stays reachable only in the eval (`--texts path`).

### Testing decisions

- **Test points (seams), to confirm:**
  1. **The cache** (new, red first, `tests/integration/test_classify.py`, with the hand-made `embed`). Covers:
     - a built cache gives the same answers as no cache, and `embed` isn't called for the texts;
     - a missing, corrupt or wrong-shape file raises `ModelMissing`;
     - changing one text changes the key;
     - the write is atomic (a failing `os.replace` leaves no file).
  2. **The version** (extend the existing `test_the_jev_kind_version_needs_no_model_or_key`): it names `deeper`, and `classify.taxonomy_version("jev")` equals a built `JevClassifier`'s.
  3. **The worker wiring** (extend the existing `--classifier jev` worker test): the shortlist gets the `deeper` texts and the cache directory, and an empty cache directory exits 6.
- The real `--download` cache build runs in the `model` CI job, which already downloads the model.

### Out of scope

- A shared embedding process across workers; each worker loads its own vectors.
- Changing `SHORTLIST`, `THRESHOLD` or the chooser; 6h's split says the chooser is now the bigger lever, which is a later step.

### Size

About 40 lines in `classify.py`, 10 in `jev.py` and `worker.py`, 80 of tests, a README line.

## Outcome

- **6f.1** ([#63](https://github.com/Hien-Trinh/Shopping/pull/63)): as decided, plus the review's fixes: calls queued in the pool don't start after a failure or past the budget; `JevClassifier.error` gives `classify_failed` the real cause when some Listings were answered; `usd` counts every billed call, and a failed tick's spend shows in the next `batch` event; a probability outside 0..1 is a failed call.
- **6f.3** ([#71](https://github.com/Hien-Trinh/Shopping/pull/71)): as decided. `--download` builds the cache in 100 s; a worker's shortlist then starts in 0.9 s. Hand run (`spikes/run_6f2.py`): 15 of 20 exact (12 on the old shortlist), $0.001250, no `classify_failed` or `process_exit`. Review fixes: every unusable cache (empty, truncated, `.npz`, NaN or non-float) is `ModelMissing`, naming the file and cause; `--download` is tested end to end. Deferred to plan 7a: the `deeper` shortlist's `np.maximum.at` scoring, about 1.2 s per 1,000-change batch.
- **6f.2** ([#66](https://github.com/Hien-Trinh/Shopping/pull/66)): the `Procfile` runs Jev. Hand run (`spikes/run_6f2.py`, kept to repeat it): 12 of 20 exact, $0.001236 (about $62 per 1M Listings), no `classify_failed`. A wrong key isn't fatal yet: plan 7c.

## Size

- 6f.1: about 90 lines of code, 140 of tests.
- 6f.2: about 10 lines, plus the README and the run's result.
