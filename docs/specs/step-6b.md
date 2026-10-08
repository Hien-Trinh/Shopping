# Step 6b: Embedding classifier (mini PRD)

Status: approved Oct 4, with the download, the test points, partial answers on the budget (decision 1, option B of: all-or-nothing, partial, a budget scaled to the batch, smaller batches) and the model in `taxonomy_version` (decision 3). Plan row: [plan-v1.md, PR steps, 6b](../plan-v1.md) ("embedding classifier with a batch timeout (asks before downloading the model). From 3e: its native calls must release the GIL"), Phase 6 ("`EmbeddingClassifier` (fastembed) with batch classification and a batch timeout"), and CI (the `model` job arrives in Phase 6). Design: [Categorization](../design-commerce-ingestion-pipeline.md) ("embedding similarity (fastembed) against Category paths, with the batch embedded in one call"; "below the confidence threshold the result is Uncategorized") and lifecycle step 5 ("the timeout (200 ms) covers the whole batch"). Builds on [step-6a.md](step-6a.md) (`taxonomy.load()`). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

The worker classifies with `FakeClassifier`, which invents `Fake > X` categories. Every Listing needs a real Primary Category from the Shopify taxonomy, within the design's 200 ms batch budget, falling back to Uncategorized when the classifier isn't confident or isn't available. The classifier must also never stall the supervisor watch: 3e found that a worker stuck in a native call that holds the GIL can't be stopped by the watch thread.

## Source (checked Oct 3)

- [fastembed](https://github.com/qdrant/fastembed) 0.8.1 (Sep 22), Apache-2.0, supports Python 3.14 (pulls `onnxruntime>=1.24.2`, which has `cp314` macOS arm64 and Linux wheels; also `huggingface-hub`, `tokenizers`, `numpy`, `loguru`, `mmh3`, `pillow`).
- Default model `BAAI/bge-small-en-v1.5`: 384 dimensions, MIT, a quantized ONNX file of about 67 MB from the Hugging Face repo `Qdrant/bge-small-en-v1.5-onnx-Q`. fastembed downloads it on first use into `cache_dir` (a temp folder by default).
- `TextEmbedding(model_name, cache_dir, threads, lazy_load, local_files_only=…)`; `embed(texts, batch_size=256)` yields unit-length numpy vectors.

## Solution

1. **A one-time download, by hand, with your OK:** `uv add fastembed`, then the model into `models/` (gitignored) with one command, `python -m catalog.classify --download`. Workers never download.
2. `src/catalog/classify.py` gains `EmbeddingClassifier`:
   - `EmbeddingClassifier(taxonomy, *, model_dir, threshold=THRESHOLD, budget=0.2, chunk=16, clock=time.monotonic)`. At construction it loads the model with `local_files_only=True` and embeds every Category path once (1,862 vectors, a 1,862 × 384 matrix).
   - `taxonomy_version`: the taxonomy's version plus the model (decision 3), e.g. `shopify-2026-08+bge-small-en-v1.5`.
   - `classify(listings)`: the text of each Listing is its title, then its description cut to 500 characters. It embeds the texts in chunks of `chunk` (16 after review: a 64-Listing chunk with 500-character descriptions took 0.7 s, against a 200 ms budget), and for each one takes the most similar Category path (cosine, a matrix product), with the similarity clamped to 0..1 as the confidence. Below `threshold` the answer is `(Uncategorized, confidence)`.
   - **The budget:** before each chunk it checks `clock()` against the start; once `budget` is spent, the remaining Listings get no answer (decision 1).
   - `THRESHOLD`: a provisional constant, marked as such. 6e picks the real one from the eval.
3. `worker._classify` accepts `None` in place of an answer: that Listing takes the failure path it takes today (its stored answer if its inputs are unchanged, else Uncategorized, flagged `needs_reclassify`), and the batch's `classify_failed` event counts the Listings left unanswered. A raised error still fails the whole call, as now.
4. `worker.main` gains `--classifier {fake,embedding}`, default `fake`, so the `Procfile`, the e2e test and CI stay offline. 6e switches the `Procfile` once the threshold is chosen.
5. CI: the `model` job (`macos-latest`), run only when the diff touches `classify*`, `taxonomy*`, `eval/`, `pyproject.toml` or `uv.lock` (a job-level `if`, as the plan says), with `models/` cached by `actions/cache` keyed on the model name. It runs `pytest -m model`.

## User stories

1. As a worker, I classify a whole batch in one call and get one Shopify Category (or Uncategorized) per Listing, each with a confidence.
2. As the operator, a bulk batch that would blow the 200 ms budget still finishes on time: whatever was classified keeps its answer, the rest are flagged for 6c's Backfill instead of stalling the partition.
3. As the operator, a worker started without the model exits at startup with a clear message, instead of eight workers racing to download it.
4. As the supervisor watch, I can still stop a worker that is in the middle of embedding.
5. As the operator, rows show which taxonomy and model classified them, so a change of either shows which rows to reclassify.

## Failure scenarios

| Scenario | Expected |
|---|---|
| A batch that fits the budget | Every Listing answered, in order |
| The budget runs out after some chunks (a 1,000-Change bulk batch) | Answered chunks keep their answers; the rest get `None`, so they are Uncategorized (or keep their stored answer) with `needs_reclassify`; one `classify_failed` event with the unanswered count |
| The budget is already spent before the first chunk ends (a slow machine) | At least the first chunk is answered, so a backlog always makes progress (decision 1) |
| Best similarity below `threshold` | `(Uncategorized, confidence)`, not flagged: a confident "don't know" is an answer |
| Negative cosine similarity | Confidence clamped to 0.0 |
| Empty description, or a 5,000-character one | Title alone; description cut to 500 characters |
| Title only punctuation or emoji | Still embedded; most likely below the threshold → Uncategorized |
| The model isn't in `models/` | Construction raises; the worker exits with a new fatal code, so the supervisor stops instead of restarting it forever |
| The model file is corrupt | Same as missing: fatal at startup |
| onnxruntime raises mid-batch | The whole call fails, as any classifier error does today: the failure path, flagged |
| A native call hangs forever | Heartbeats stop and the 60 s watchdog kills the worker (B1); the watch thread can still run, because onnxruntime and tokenizers release the GIL (test point 4) |
| `kill -9` mid-classify | Nothing written yet; the batch replays (at least once) |
| SIGTERM mid-classify | `stop` is checked between batches, so the batch finishes (it is bounded by the budget) |
| Eight workers start together | Each loads its own model (about 67 MB on disk, a few hundred MB in memory each, about 2 GB for 8), each embeds the taxonomy once at startup (a few seconds, covered by the start-time beat); none downloads |
| Taxonomy or model bump | `taxonomy_version` changes, so plan reclassifies those rows when touched, and 6c backfills the rest |

## Implementation decisions

1. **The budget answers what it can and flags the rest, instead of raising.** At about 1 to 2 ms per short text on a Mac CPU (to measure in 6e), a 1,000-Change batch takes about 1 to 2 s, so a 200 ms all-or-nothing timeout would send every bulk batch, and every 6c Backfill batch, to Uncategorized, and the Backfill would never catch up. Checking the clock between chunks also needs no thread: nothing has to be abandoned mid-call. The first chunk always runs, so even a slow machine makes progress. The cost is a small contract change: `classify` may answer `None`. This reads the design's "on a timeout, those Listings get Uncategorized" as per Listing. A batch can overrun the budget by up to one chunk (about 160 ms).
2. **Embed in the worker, one model per process.** 8 × bge-small fits easily in 16 GB; B5's shared classifier process is for Laya, if 6e picks it.
3. **`taxonomy_version` names the model too.** Plan already reclassifies rows stamped with another version, so a model swap reuses that path for free. The column name stays. The column now means taxonomy plus model.
4. **Workers load with `local_files_only=True`; the download is a separate command.** It keeps the network out of the worker and CI's `check` job, and avoids concurrent downloads into one cache.
5. **Path text as written, no query prefix or tuning.** Prompt prefixes, using the description at all, deeper-vs-shallower preference and the threshold are 6e's experiments; 6b ships the plain version.
6. **Fake stays the default.** No test outside `-m model` loads the model (Phase 6: "`FakeClassifier` is used everywhere except `tests/model`").

## Testing decisions

- **Test points (seams), confirmed:**
  1. **Ranking, threshold and budget, without the model** (new, red first, `tests/integration/test_classify.py`: `make mutate` runs `tests/unit` against the pure modules only): `EmbeddingClassifier` takes an injectable `embed` function, so tests pass tiny hand-made vectors and a fake clock. Covers: best path wins, threshold boundary (equal is kept), negative similarity clamps to 0, text built from title and cut description, chunks past the budget answer `None`, the first chunk always answered, order kept.
  2. **Worker with `None` answers** (new, red first, `tests/integration/test_worker.py`): a classifier answering `None` for some Listings stores Uncategorized (or keeps the stored answer when inputs are unchanged) with `needs_reclassify`, the answered ones store their answers, and `classify_failed` counts the unanswered.
  3. **Missing model is fatal** (new): `--classifier embedding` with an empty `models/` exits with the new fatal code, and the supervisor's `FATAL_CODES` includes it.
  4. **Real model** (new, `tests/model/test_embedding.py`, marked `model`): with the committed taxonomy, "Men's cotton crew-neck t-shirt" lands under `Apparel & Accessories`; confidences are in 0..1; and a thread counting in a loop keeps counting while a large `classify` call runs (the GIL is released).
- **Coverage:** `classify.py` keeps the 90% package gate through test point 1; the model-only lines (loading, `--download`) are covered by the `model` job, not the gate, and are kept to a few lines.

## Out of scope

- Choosing the threshold, model or text recipe (6e).
- Switching the `Procfile` to the embedding classifier (6e).
- The Backfill (6c).
- Laya, Jev and a shared classifier process (6e, B5).
- Caching Category embeddings on disk: a few seconds per worker start is fine.

## Size

About 60 lines in `classify.py`, 10 in `worker.py`, 25 of CI, about 120 of tests, plus the `uv.lock` change (generated).
