# Step 6m: the student in the pipeline (mini PRD)

Status: done Oct 7 (6m.1 #104, 6m.2 the Procfile switch). Question 1: **τ = 0, the student replaces Jev**. Question 2: measure (int8 against fp32, the rule in 6m.1 point 8). Question 3: the index built locally, downloads separate. Question 4: yes, plus the design doc's Runtime list line, which becomes "fastembed running the fine-tuned student (step 6m); Jev, the shortlist and Laya were the earlier choices". Question 5: the test points OK. The Oct 7 draft proposed a cascade (the student first, Jev below a threshold); this version is the student alone, and the cascade is a later step. Plan row: [plan-v1.md, PR steps, 6m](../plan-v1.md). Builds on [step-6l.3.md](step-6l.3.md) (the fine-tuned student meets the bar; "for 6m" in its Outcome), [step-6l.2.md](step-6l.2.md) (the τ rule and its review's deferral), [step-6f.md](step-6f.md) (how Jev entered the pipeline: the kind, the version, the Procfile switch) and [docs/research/classifier-hosting-options.md](../research/classifier-hosting-options.md) (keep the classifier in-process). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Every Listing goes to Jev: about $62 per 1M Listings, 250 ms a call, 80 calls a second across the workers, so a 1,000-Change batch gets about 200 answers in its 10 s budget and the rest wait for the Backfill; 66.9% exact on the 1,020. The fine-tuned student (`student-ft-knn`, k = 5) is an eval candidate only: 72.5% exact alone on the 1,020 and 60.9% on Shopify's 2,000 (Jev 61.8%) at about 7 ms a Listing ([eval/student.md](../../eval/student.md)). The Ingestion workers and the Backfill don't know it, nothing builds its index for them, and `taxonomy_version` can't name it.

Four points were left to this spec: the threshold τ needs a minimum count of held-out rows behind it (the 6l.2b review); int8 or fp32 for the encoder (int8 cost 1.7 points on the training held-out rows); where the weights and the 41k-vector index live; and whether the student's `--download` folds into `classify`'s.

### Why the student alone (decided Oct 7)

| | Jev alone (today) | Student alone, τ = 0 | Cascade, τ = 0.60 |
|---|---|---|---|
| Exact on the 1,020 | 66.9% | 72.5% | 75.2% |
| Exact on Shopify's 2,000 | 61.8% | 60.9% | 65.6% |
| Cost per 1M Listings | about $62 | $0 | about $12 |
| A worker's 1,000-Change batch | about 200 answered in 10 s, the rest to the Backfill | all answered in a few seconds | all answered in about 13 s |
| 1M bulk load | about 3.5 h, capped at 80 calls/s | minutes (measured in 7d.2b) | about 1 h |
| In the worker | the key, the rate split, the 10 s budget, spend events, exit 7, bge-small plus the shortlist | the student's encoder and index, nothing else | all of the above plus a wrapper |

The student alone beats Jev by 5.6 points on our Listings and matches it on the independent set, for nothing and with the whole Jev path out of the worker. The cascade buys 3 to 5 more points for about $12 per 1M and keeps every operational piece of Jev; it is its own later step if the Shopify gap ever matters. 6l.2's τ rule (the lowest τ whose kept held-out answers are right at least as often as Jev) picks τ = 0 for this student too: it beats Jev on average at every τ. Caveat carried from 6l.3: the 1,020's labels are Claude-made, so Shopify's parity is the honest floor.

## Solution: a `student` classifier kind, in two PRs

| PR | What | Gate |
|---|---|---|
| **6m.1** | `StudentClassifier` under the worker's `classify` contract from the shipped index; the `student` kind for the worker and the Backfill; the version names it; the index built by `--download`; the report's τ rule with a minimum support; the int8/fp32 measurement | None: tests use a fake `embed` and a toy index |
| **6m.2** | The `Procfile` runs `--classifier student`; the e2e test rewrites it to `fake`; design doc, runbook and README say so; one supervised run on this Mac with 20 Listings | None: no key, no spend |

### 6m.1: the kind

1. **The classifier** is `student.StudentClassifier` as the eval runs it: `Knn` (k = 5) over the fine-tuned vectors, `embed` from `student.embedder` (the fine-tuned encoder through fastembed, offline), the index from the file in point 3. Every Listing gets the nearest neighbours' label with the vote share as its confidence; no threshold, so the student never answers Uncategorized and never `None`. Batches are embedded in chunks of 256 (`CHUNK`), each ONNX run releasing the GIL (6b's rule).
2. **No budget.** A batch is at most 1,000 Changes at a few ms each, a few seconds in all, against the 60 s heartbeat; 7d.2b measures it. `ponytail:` note on the class: a budget like `EmbeddingClassifier`'s if a batch ever nears the heartbeat.
3. **The index** is `models/student/index-<revision12>-<int8|fp32>.npz`: `x` (41k × 384 float32, 61 MB) and `y` (the labels), written by `python -m catalog.student --download` after it fetches the encoder and head: it embeds every row of `train/` through the shipped encoder file (about 5 minutes on the Mac, like `classify --download`'s 2 minutes for the shortlist). The worker only loads it, never embeds at start (6f.3's rule); a missing, truncated, mis-shaped or non-finite file is `ModelMissing` naming the download (exit 6), and a label not in the taxonomy is a `ValueError` naming it (the taxonomy changed under the student: retrain, a later step).
4. **`--classifier student`** for the worker (`KINDS` gains it; `down` stays last): the taxonomy, the fine-tuned encoder and the index, nothing of Jev or bge-small. The `jev` kind stays as it is, for comparison and as the fallback the cascade step would use.
5. **The version:** `{taxonomy}+student-ft-knn@{revision12}+{int8|fp32}+k5+train-{sha8}`, where `revision12` is the Hub commit the encoder is pinned to, the file tag names the ONNX file, and `sha8` is the SHA-256 of the three `train/` files (the index's inputs). `classify.taxonomy_version("student")` builds it from constants and the committed files without loading a model, so the Backfill stamps the same string as the workers, as for `jev`. Changing any of them reclassifies every stored Listing through the Backfill, for nothing.
6. **Pin the Hub revision:** `student.FT_REVISION = "1970ef96…"` (the 6l.3 run). `--download` fetches that commit, the eval scores it, the version names it. A retrain is a new constant, which is a new version. Today the eval fetches whatever `main` on the Hub is.
7. **τ with a minimum support, in the report only.** `pick_tau` gains `min_kept=200`: a τ qualifies only if at least 200 Amazon held-out rows (of 1,007) are kept at it, so one or two rows can't choose it (the 6l.2b deferral). The report also prints, per τ, the accuracy of the band just above it, so a later cascade step reads its knee from the report. Nothing in the pipeline uses τ.
8. **int8 or fp32** (question 2): `ONNX` becomes the file the pipeline serves and the eval scores; 6m.1 reruns the report once with `model.onnx` (one constant; about 15 minutes on the Mac, the vector cache is keyed by file) and the PR records both. The rule: fp32 ships if it is at least 1 point better alone on the 1,020 (6l.3's line) and its per-Listing p99 is under twice int8's; otherwise int8. The index is built from whichever file ships, so queries and index come from the same encoder.
9. **Downloads stay separate:** `classify --download` (bge-small and the shortlist, as today, also CI's `model` job) and `student --download` (encoder, head, index). Folding them would add the Hub fetch and a 5-minute index build to every `model` CI run for nothing it tests. Each `ModelMissing` names its own download, as today.

### 6m.2: the switch

- `Procfile`: the `worker-*` and `backfill` lines say `--classifier student`; `chaos.procfile` rewrites them to `fake` (its `n == 4` / `n == 1` checks fail loudly if the lines change shape), so the e2e test and the chaos runner never load a model.
- Design doc, Categorization: a sentence that step 6m replaces Jev with the student (question 4). Lifecycle step 5's timeout sentence gains the student's seconds. The Runtime list's classifier line becomes "fastembed running the fine-tuned student (step 6m); Jev, the shortlist and Laya were the earlier choices".
- Runbook: `TYPESAFE_API_KEY` is no longer needed to start; exit 6 names both downloads; exit 7 stays for the `jev` kind. README's Run section says what the workers classify with and the two downloads.
- By hand: the supervisor on this Mac, 20 Listings through the API, the Listing Store shows their categories and versions, the events show `classify_ms`. The PR states the result. Then 7d.2b ([step-7d.md](step-7d.md), amended): the SLO run and the 10k bulk on the student.

## User stories

1. As you, the workers answer every Listing locally in milliseconds for nothing, at an accuracy above Jev's on our Listings and level with it on the independent set.
2. As you, starting the system needs no key and no spend, and a bulk upload is bounded by the Mac, not by a rate limit.
3. As you, the version names the encoder, its file, k and the training data, so changing any of them reclassifies every stored Listing, and a retrain is one constant.
4. As CI, nothing downloads the student or calls a paid API.

## Failure scenarios

| Scenario | Expected |
|---|---|
| The encoder or index missing from `models/` (the head is the eval's; the pipeline never loads it) | `ModelMissing` naming `python -m catalog.student --download`, exit 6, before claiming partitions |
| The index was built with the other ONNX file | Its name carries the file tag, so the worker looks for the right one and finds it missing (exit 6) |
| The index is truncated by a killed `--download` | Written atomically (`state.atomic`): whole or absent |
| `train/` changes (a new labeled file) | The version's `train-` hash changes; `--download` builds a new index; the Backfill reclassifies every row, for nothing |
| A new Hub revision is pushed | Nothing changes until `FT_REVISION` does; `--download` fetches the pinned commit |
| The taxonomy changes under the student | The index holds a label the taxonomy lacks: `ModelMissing` naming it (exit 6, so the supervisor stops instead of restarting), saying to retrain |
| A Category with no training rows | kNN can never answer it (2 of the 1,020's labels); counted in the report, accepted |
| A Listing the student is sure of but wrong | Stored with its confidence under the student's version; the eval's price (27.5% of the 1,020). Nothing goes Uncategorized by the student: no threshold |
| A batch that takes long (a bulk upload) | A few seconds for 1,000; measured in 7d.2b against the 60 s heartbeat |
| 4 workers embedding at once | Each ONNX run releases the GIL (6b); oversubscribed cores only slow the batch. `ponytail:` fastembed's `threads` per worker if 7d.2b shows it |
| Memory | Per worker about 100 MB of files (encoder 32 MB int8 or 133 MB fp32, index 61 MB) plus the runtime arena; 4 workers well under 16 GB |
| The Backfill under `--classifier student` with no models downloaded | Runs: the version needs only constants and the `train/` files |
| A worker killed mid-batch | The batch replays, as today; nothing is paid |

## Implementation decisions

1. **The student replaces Jev; no cascade.** τ = 0 is what 6l.2's rule picks and what the eval supports (the table above); the cascade is a later step. `jev.py` stays, as a kind.
2. **kNN over the fine-tuned vectors, k = 5,** the better student on both evals (72.5% against 70.8% for the head alone). The head stays in the repo for the eval. Rejected: the head alone, which is simpler to ship (no index) and 1.7 to 2.7 points worse.
3. **The index is built locally from `train/`, not pushed to the Hub,** so the index always matches the committed training rows and the shipped encoder file, and the Hub repo holds only the model. Rejected: a 61 MB file in the Hub repo (a second artifact to keep in step with `train/`) and committing it (61 MB in git).
4. **The weights live on the public Hub repo at a pinned revision,** fetched into `models/` as bge-small is. No secret anywhere; on Databricks later it is a volume path (the hosting research).
5. **Two PRs,** as 6f: 6m.1 is code under test; 6m.2 changes what a run does.

## Testing decisions (test points for your OK)

With a fake `embed` and a toy index, no model, no network:

1. **The index file** (new, red first, `tests/integration/test_student.py`): `--download` writes it (with a fake embedder and head) and the loader returns it; missing, truncated, wrong shape and non-finite files raise `ModelMissing` naming the student's download; a label outside the taxonomy is refused by name.
2. **The classifier from the index** (new, red first): every Listing gets a taxonomy path and a confidence in [0, 1]; never `None`, never Uncategorized; an empty batch is `[]`; a 1,000-Listing batch runs in chunks with one result per Listing in order.
3. **The version** (new, red first, `tests/integration/test_classify.py`): `taxonomy_version("student")` equals the classifier's own; the revision, the file tag, k and the training hash each change it; the Backfill's `--classifier student` stamps it.
4. **The worker's `student` kind** (new, red first, `tests/integration/test_worker.py`, via `main` as the `jev` tests do): a missing index exits 6 before claiming partitions; the `classify` event is emitted with the batch's size and ms.
5. **`pick_tau` with `min_kept`** (new, red first): a τ kept by fewer rows never wins; the report prints the per-band accuracy.
6. **e2e** (6m.2, changed): the shipped Procfile's `student` lines are rewritten to `fake`.

Not in CI: the real student in a worker (no download in CI; 6m.2's hand run covers it) and the int8/fp32 report rerun (recorded in the PR).

## Questions

1. **τ.** Decided Oct 7: **τ = 0, the student alone replaces Jev.** The cascade (the student first, Jev below τ; the Oct 7 draft's rule picked τ = 0.60 on the held-out rows: 75.2% at 80.0% kept on the 1,020, 65.6% on Shopify, about $12 per 1M) is a later step if wanted.
2. **int8 or fp32.** Measure as in 6m.1 point 8 and ship fp32 only if it earns 1 point on the 1,020 at under twice the latency; int8 otherwise. **Measure (Oct 7).**
3. **The index built locally by `student --download`** (decision 3), downloads kept separate from `classify --download` (point 9). **Agreed (Oct 7).**
4. **The design doc** gets the step 6m sentence in Categorization and the student's seconds in lifecycle step 5's timeout sentence, as 6f added Jev's. **Agreed (Oct 7), plus the Runtime list line:** "fastembed for the shortlist, TypeSafe Jev for the choice (step 6f)" becomes "fastembed running the fine-tuned student (step 6m); Jev, the shortlist and Laya were the earlier choices".
5. **The test points above.** **OK (Oct 7).**

## Outcome, 6m.1 (Oct 7)

- **The code**: `student.pipeline` (kNN, k = 5, over the index), `build_index` / `load_index` (`models/student/index-<revision12>-<int8|fp32>.npz`, written whole or not at all by `--download`, checked on load), `version` from constants and the training files' hash, the encoder fetched at `FT_REVISION` and loaded through fastembed's `specific_model_path` (so the eval and the pipeline score one Hub commit), `--classifier student` for the worker and the Backfill, the classifier embedding in chunks of 256, the report's τ needing 200 held-out rows behind it and printing the accuracy of each band above it, the vector cache keyed by the encoder file too.
- **int8 against fp32** (point 8), the report rerun once per file on this Mac, one Listing at a time:

| `student-ft-knn` (k = 5) | int8 (32 MB) | fp32 (133 MB) |
|---|---|---|
| Exact on the 1,020 | 72.5% | 73.4% |
| Exact on Shopify's 2,000 | 60.9% | 62.9% |
| Held-out exact | 63.4% | 64.7% |
| p50 / p99 per Listing | 6.7 / 27.3 ms | 5.8 / 12.5 ms |

  **fp32 ships.** The rule asked for a full point on the 1,020 before paying latency for fp32; it gains 0.9 there and 2.0 on the independent set, and costs no latency: on this Mac the fp32 file is faster than int8's dynamic quantization. The price is 133 MB per worker instead of 32, well inside the memory row above. [eval/student.md](../../eval/student.md) is the fp32 run.
- **Review round 1** (#104, 11 finders, 13 candidates, 12 held) fixes, each with a regression test: the index file is named by the training files' hash too, so a stale index after `train/` changes is "missing" (exit 6) instead of serving under the new version; a label the taxonomy lacks, an empty index and a malformed label array are `ModelMissing` (exit 6, the supervisor stops) rather than a `ValueError` the supervisor would restart forever; a missing training file at start is `ModelMissing` naming it, not a bare `FileNotFoundError`; the chunked embed is one shared `embed_chunks` for the cache and the classifier; the snapshot directory comes from the ONNX file's own path; the `ponytail:` notes on the budget and on fastembed's threads; the plan's code layout lists `student.py`; this table's head row (the pipeline never loads the head). Tests: the 200-row support ships and the report applies it (the toy report says "no τ"), the chunking needs several calls, `embedder` fetches every file at `FT_REVISION` and loads the snapshot, the Backfill stamps the student's version, the index guards. Refuted: an `embed` answering too few or NaN rows is caught by the worker's strict pairing and confidence check.
- **A real run on this Mac** (the fp32 encoder and the 41k-row index, `--download` 588 s the first time, then seconds from the vector cache): one worker on `--classifier student` classified three Listings in 1.6 s including its start (a cotton t-shirt to Clothing Tops at 1.00, a wooden train set to Play Vehicles at 0.59, a chef's knife to Kitchen Tools at 1.00), stamped `shopify-2026-08+student-ft-knn@1970ef96152e+fp32+k5+train-ce767c3a`, the string the Backfill builds without a model.
- **The report's τ rule** still picks τ = 0 on the Amazon held-out rows with the 200-row support, as the Oct 7 decision expects; the bands line shows the knee (the 0.35 to 0.60 bands are right 22 to 68% of the time) for a later cascade step.

## Out of scope

- The cascade (Jev behind a τ), a shortlist for Jev from the student's top 10, retiring `jev.py`, a retrain, the head alone as a lighter student, a fastembed `threads` setting, a Databricks or Azure port, the Opus fallback (labeling only).

## Outcome, 6m.2 (Oct 7)

- **The switch**: the `Procfile`'s four `worker-*` lines and the `backfill` line say `--classifier student`; `chaos.procfile` rewrites `student` to the caller's kind (`fake` for the e2e test, `down` for the chaos runner) and refuses a Procfile where any line still says `student`, so CI never loads the encoder or the index. The design doc (Categorization, lifecycle step 5's timeout sentence, the Runtime list), the runbook (First run, Start without a key, `classify_ms`, exits 6 and 7) and the README's Run section say so.
- **The hand run on this Mac** (the fp32 encoder and the 41k-row index in `models/`, the shipped Procfile, a fresh `data/` and `state/`): the supervisor started four workers with no key in the environment; the load generator posted 20 real Listings one at a time at 20/s, all accepted; every one is live in the Listing Store with a category, none Uncategorized, none flagged, all stamped `shopify-2026-08+student-ft-knn@1970ef96152e+fp32+k5+train-ce767c3a`. Eighteen `classify` events (two batches held two Listings): p50 9.5 ms, p99 60 ms per call, the first call of each worker 54 to 61 ms (the ONNX session's warm-up), then 5 to 20 ms; freshness p99 0.33 s. Confidences: 18 at 1.00, a sealer at 0.63 (Office Supplies > Impulse Sealers > Tabletop Impulse Sealers) and a halter dress at 0.59 (Wedding & Bridal Party Dresses). Judged by title, 19 of the 20 are right; a plastic drawstring threader went to Arts & Crafts, arguable.
- **Next**: 7d.2b ([step-7d.md](step-7d.md)): the SLO run and the 10k bulk on the student, where the batch time against the 60 s heartbeat is measured.
