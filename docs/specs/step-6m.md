# Step 6m: the student in the pipeline (mini PRD)

Status: draft Oct 7, for your approval (questions at the end). Plan row: [plan-v1.md, PR steps, 6m](../plan-v1.md). Builds on [step-6l.3.md](step-6l.3.md) (the fine-tuned student meets the bar; "for 6m" in its Outcome), [step-6l.2.md](step-6l.2.md) (the τ rule and its review's deferral), [step-6f.md](step-6f.md) (how Jev entered the pipeline: the kind, the version, the Procfile switch) and [docs/research/classifier-hosting-options.md](../research/classifier-hosting-options.md) (keep the classifier in-process). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

Every Listing goes to Jev: about $62 per 1M Listings, 250 ms a call, 80 calls a second across the workers, and 66.9% exact on the 1,020. The fine-tuned student (`student-ft-knn`, k = 5) is an eval candidate only: it gets 72.5% exact alone at about 7 ms a Listing, and with Jev taking the Listings it is unsure of, 75% at 80 to 87% kept local ([eval/student.md](../../eval/student.md)). The Ingestion workers and the Backfill don't know it, nothing builds its index for them, and `taxonomy_version` can't name it.

Four points were left to this spec: the threshold τ needs a minimum count of held-out rows behind it (the 6l.2b review); int8 or fp32 for the encoder (int8 cost 1.7 points on the training held-out rows); where the weights and the 41k-vector index live; and whether the student's `--download` folds into `classify`'s.

## Solution: a `student` classifier kind, in two PRs

| PR | What | Gate |
|---|---|---|
| **6m.1** | The cascade under the worker's `classify` contract; the `student` kind for the worker and the Backfill; the version names it; the index built by `--download`; the report's τ rule with a minimum support; the int8/fp32 measurement | None: tests use a fake `embed` and an injected Jev `call` |
| **6m.2** | The `Procfile` runs `--classifier student`; the e2e test rewrites it to `fake`; design doc, runbook and README say so; one supervised run on this Mac with 20 Listings | Your key in `TYPESAFE_API_KEY` and your OK on the spend (at most 20 Jev calls, under a cent) |

### 6m.1: the cascade

1. **`student.Cascade(student, jev, tau)`**, in `student.py`. `classify(listings)`: the student (`StudentClassifier` with `Knn`, k = 5, over the fine-tuned vectors) answers every Listing in one pass, chunked as the eval does; the Listings whose confidence is below τ go to `jev.classify` as one batch; its answers replace theirs. Jev's own threshold (0.40 → Uncategorized), budget (10 s) and rate apply to that batch unchanged, so a Listing Jev never reaches answers `None` and takes today's path: Uncategorized, flagged, retried by the Backfill. `usd` and `error` read through from Jev (the worker's `batch` and `classify_failed` events already use them); `kept`, how many the student answered in the last call, is new.
2. **Jev down:** an exception from `jev.classify` other than `KeyMissing` makes the unsure Listings `None` and sets `error`; the sure ones keep the student's answers. Today the whole batch would go Uncategorized. `KeyMissing` still propagates (exit 7).
3. **The student has no budget.** A batch is at most 1,000 Changes and the student embeds them in chunks of 256 at a few ms each, so a full batch is a few seconds; measured in 6m.2's run. `ponytail:` note on the class: a budget like `EmbeddingClassifier`'s if a batch ever nears the 60 s heartbeat.
4. **Jev's shortlist is unchanged:** the frozen bge-small over the `deeper` texts, from `models/texts`, as shipped in 6f.3. The cascade was scored against Jev's stored answers in that configuration, so changing it would void the eval. A shortlist from the student's own vectors is a later step (out of scope).
5. **`--classifier student`** for the worker (`KINDS` gains it; `down` stays last). The worker builds it as it builds `jev` today: the key first (`jev.http(attempts=1)`), then the taxonomy, bge-small and the shortlist from the cache, then the fine-tuned encoder (`student.embedder`, offline) and the index (6). The Backfill takes the kind and needs no model (7).
6. **The index** is `models/student/index-<revision12>-<int8|fp32>.npz`: `x` (41k × 384 float32, 61 MB) and `y` (the labels), written by `python -m catalog.student --download` after it fetches the encoder and head: it embeds every row of `train/` through the shipped encoder file (about 5 minutes on the Mac, like `classify --download`'s 2 minutes for the shortlist). The worker only loads it, never embeds at start (6f.3's rule); a missing, truncated, mis-shaped or non-finite file is `ModelMissing` naming the download (exit 6), and a label not in the taxonomy is a `ValueError` naming it (the taxonomy changed under the student: retrain, a later step).
7. **The version:** `{Jev's version}+student-ft-knn@{revision12}+{int8|fp32}+k5+t{τ}+train-{sha8}`, where `revision12` is the Hub commit the encoder is pinned to, the file tag names the ONNX file, and `sha8` is the SHA-256 of the three `train/` files (the index's inputs). `classify.taxonomy_version("student")` builds it from constants and the committed files without loading a model, so the Backfill stamps the same string as the workers, as for `jev`. Changing any of them reclassifies every stored Listing through the Backfill, at the cascade's price (about $12 per 1M, not $62).
8. **Pin the Hub revision:** `student.FT_REVISION = "1970ef96…"` (the 6l.3 run). `--download` fetches that commit, the eval scores it, the version names it. A retrain is a new constant, which is a new version. Today the eval fetches whatever `main` on the Hub is.
9. **τ with a minimum support** (question 1). `pick_tau` gains `min_kept=200`: a τ qualifies only if at least 200 Amazon held-out rows (of 1,007) are kept at it, so one or two rows can't choose it (the 6l.2b deferral). The report also prints, per τ, the accuracy of the band just above it, so the knee is visible. The shipped τ is a constant in `student.py` chosen from the report, as Jev's 0.40 was in 6e.
10. **int8 or fp32** (question 2): `ONNX` becomes the file the pipeline serves and the eval scores; 6m.1 reruns the report once with `model.onnx` (one constant; about 15 minutes on the Mac, the vector cache is keyed by file) and the PR records both. The rule: fp32 ships if it is at least 1 point better alone on the 1,020 (6l.3's line) and its per-Listing p99 is under twice int8's; otherwise int8. The index is built from whichever file ships, so queries and index come from the same encoder.
11. **Downloads stay separate:** `classify --download` (bge-small and the shortlist, as today, also CI's `model` job) and `student --download` (encoder, head, index). Folding them would add the Hub fetch and a 5-minute index build to every `model` CI run for nothing it tests. Each `ModelMissing` names its own download, as today.
12. **The `classify` event gains `local`** (the Listings the student kept, from `kept`), and `metrics` reports `classify_local`, the share kept local in the window: the number this step is about, beside `usd` in the `batch` events.

### 6m.2: the switch

- `Procfile`: the `worker-*` and `backfill` lines say `--classifier student`; `chaos.procfile` rewrites them to `fake` (its `n == 4` / `n == 1` checks fail loudly if the lines change shape), so the e2e test and the chaos runner never load a model or call Jev.
- Design doc, Categorization: a sentence that step 6m puts the student first and Jev behind τ (question 4). Lifecycle step 5's timeout sentence gains the student's seconds.
- Runbook: exit 6 names both downloads; the metrics table gets `classify_local`. README's Run section says what the workers classify with and the two downloads.
- By hand: the supervisor on this Mac, 20 Listings through the API, the Listing Store shows their categories and versions, the events show `local`, `usd` and `classify_ms`. The PR states the result.

### What it buys (from the eval, to be confirmed by 6m.2's run)

| | Today (Jev) | Cascade at τ = 0.60 | Cascade at τ = 0.45 |
|---|---|---|---|
| Exact on the 1,020 | 66.9% | 75.2% | 75.3% |
| Exact on Shopify's 2,000 | 61.8% | 65.6% | 64.2% |
| Kept local (1,020 / Shopify) | 0% | 80.0% / 68.5% | 87.3% / 76.8% |
| Jev cost per 1M Listings | about $62 | about $12 | about $8 |
| A worker's 1,000-Change batch | about 200 answered in the 10 s budget, the rest to the Backfill | all answered in about 13 s: the student in a few seconds, 200 Jev calls in 10 s at 20/s | same |
| A 1M bulk load | about 3.5 h, Jev-bound at 80/s | about 1 h | under 1 h |

## User stories

1. As you, the workers answer most Listings locally in milliseconds for nothing, and Jev only sees the ones the student is unsure of, at a system accuracy above Jev alone on both evals.
2. As you, a Jev outage or a spent budget costs only the unsure Listings, never the whole batch.
3. As you, the version names the encoder, its file, k, τ and the training data, so changing any of them reclassifies every stored Listing, and a retrain is one constant.
4. As you, the events and `metrics` show how much stays local and what Jev still costs.
5. As CI, nothing downloads the student or calls Jev.

## Failure scenarios

| Scenario | Expected |
|---|---|
| Jev down for every call | The sure Listings keep the student's answers; the unsure are Uncategorized and flagged, with `classify_failed` carrying the error; the Backfill retries them |
| Jev answers 429 mid-batch | As 6f: new calls stop, answered Listings keep their answers, the rest are flagged |
| Jev's budget runs out (more than 200 unsure in a batch) | The rest answer `None`: Uncategorized, flagged, retried; they take the student's path again and go to Jev again, since they are below τ |
| `TYPESAFE_API_KEY` unset | Exit 7 before any model loads, as today: Jev is still in the loop |
| The encoder, head or index missing from `models/` | `ModelMissing` naming `python -m catalog.student --download`, exit 6, before claiming partitions |
| The index was built with the other ONNX file | Its name carries the file tag, so the worker looks for the right one and finds it missing (exit 6) |
| The index is truncated by a killed `--download` | Written atomically (`state.atomic`): whole or absent |
| `train/` changes (a new labeled file) | The version's `train-` hash changes; `--download` builds a new index; the Backfill reclassifies every row |
| A new Hub revision is pushed | Nothing changes until `FT_REVISION` does; `--download` fetches the pinned commit |
| The taxonomy changes under the student | The index holds a label the taxonomy lacks: the worker refuses to start, naming it |
| A Category with no training rows | kNN can never answer it (2 of the 1,020's labels); Jev can, but only for an unsure Listing. Counted in the report, accepted |
| A Listing the student is sure of but wrong | Stored with the student's confidence under the student's version; the eval's price of the kept rate (kept accuracy 83% at τ = 0.60 on held-out rows) |
| 4 workers embedding at once | Each ONNX run releases the GIL (6b); oversubscribed cores only slow the batch. `ponytail:` fastembed's `threads` per worker if 6m.2's run shows it |
| Memory | Per worker about 150 MB of files (bge-small 34 MB, texts 21 MB, encoder 32 MB int8 or 133 MB fp32, index 61 MB) plus the runtime arenas; 4 workers well under 16 GB |
| The Backfill under `--classifier student` with no models downloaded | Runs: the version needs only constants and the `train/` files |
| A worker killed mid-batch with Jev calls in flight | The batch replays, as today; at most one batch's unsure calls paid twice |

## Implementation decisions

1. **A wrapper, not a new contract.** `Cascade` composes the two classifiers the pipeline already has under the one `classify` contract; the worker, the Backfill and the events don't change shape. Rejected: a `fallback` hook inside `JevClassifier`, which would couple the paid classifier to the student.
2. **kNN over the fine-tuned vectors, k = 5,** the better student on both evals (72.5% against 70.8% for the head alone). The head stays in the repo for the eval. Rejected: the head alone, which is simpler to ship (no index) and 1.7 to 2.7 points worse.
3. **The index is built locally from `train/`, not pushed to the Hub,** so the index always matches the committed training rows and the shipped encoder file, and the Hub repo holds only the model. Rejected: a 61 MB file in the Hub repo (a second artifact to keep in step with `train/`) and committing it (61 MB in git).
4. **The weights live on the public Hub repo at a pinned revision,** fetched into `models/` as bge-small is. No secret anywhere; on Databricks later it is a volume path (the hosting research).
5. **τ is a constant,** like Jev's 0.40, picked from the report under the rule in question 1, and named in the version.
6. **Jev's configuration is frozen** for this step: its stored answers are what the cascade's numbers rest on.
7. **Two PRs,** as 6f: 6m.1 is code under test; 6m.2 needs your key and changes what a run does.

## Testing decisions (test points for your OK)

With a fake `embed`, a toy index and an injected Jev `call`, no model, no network:

1. **`Cascade`** (new, red first, `tests/integration/test_student.py`): at or above τ the student's answer and confidence; below τ Jev's, including its Uncategorized; a Listing Jev doesn't reach stays `None`; order and length preserved; no unsure Listing means Jev is never called; an empty batch is `[]`.
2. **Jev failing inside the cascade** (new, red first): a raised error makes only the unsure `None` and sets `error`; `KeyMissing` propagates; `usd` reads through; `kept` counts the sure ones.
3. **The index file** (new, red first): `--download` writes it (with a fake embedder and head) and the loader returns it; missing, truncated, wrong shape and non-finite files raise `ModelMissing` naming the student's download; a label outside the taxonomy is refused by name.
4. **The version** (new, red first, `tests/integration/test_classify.py`): `taxonomy_version("student")` equals the cascade's own; the revision, the file tag, k, τ and the training hash each change it; the Backfill's `--classifier student` stamps it.
5. **The worker's `student` kind** (new, red first, `tests/integration/test_worker.py`, via `main` as the `jev` tests do): a missing key exits 7 before any model loads; a missing index exits 6; the `classify` event carries `local`.
6. **`pick_tau` with `min_kept`** (new, red first): a τ kept by fewer rows never wins; the report prints the per-band accuracy.
7. **`metrics`** (new): `classify_local` is the kept share over the window's `classify` events.
8. **e2e** (6m.2, changed): the shipped Procfile's `student` lines are rewritten to `fake`.

Not in CI: the real student in a worker (no download in CI; 6m.2's hand run covers it) and the int8/fp32 report rerun (recorded in the PR).

## Questions

1. **τ.** 6l.2's rule (the lowest τ whose kept held-out answers are right at least as often as Jev's 66.9%) picks τ = 0 for this student: it beats Jev on average at every τ, so everything stays local, 72.5% on the 1,020 for $0 and no Jev in the loop at all. But the least sure Listings are where Jev helps: on the Amazon held-out rows the bands between τ = 0.35 and 0.60 are right only 30 to 50% of the time. Proposed rule, with the minimum support: **the lowest τ, kept by at least 200 held-out rows, whose expected cascade accuracy (kept answers right, the rest at Jev's rate) is within 1 point of the best.** On the held-out rows it picks **τ = 0.60** (expected 79.9%; the best is 80.3% at 0.70). On the 1,020 that is 75.2% at 80.0% kept; on Shopify 65.6% at 68.5% kept. The hand-picked knee on the 1,020 is τ = 0.45 (75.3% at 87.3% kept, 64.2% on Shopify). I'd ship the rule's 0.60: reproducible, and 1.4 points better on the independent set for about $4 per 1M more. Keep 6l.2's rule (τ = 0, no Jev), the proposed rule (0.60), or the knee (0.45)?
2. **int8 or fp32.** Measure as in 6m.1 point 10 and ship fp32 only if it earns 1 point on the 1,020 at under twice the latency; int8 otherwise. OK, or ship int8 without the rerun (the measured bar) and leave fp32 as a later lever?
3. **The index built locally by `student --download`** (decision 3), downloads kept separate from `classify --download` (point 11). OK?
4. **The design doc** gets the step 6m sentence in Categorization and the student's seconds in lifecycle step 5's timeout sentence, as 6f added Jev's. OK?
5. **The test points above.** OK?

## Out of scope

- A shortlist for Jev from the student's top 10 instead of the embedding's top 50 (the hosting research's cheapest next gain; needs the student's top-10 recall measured first), a retrain, the head alone as a lighter student, a fastembed `threads` setting, a Databricks or Azure port, Jev's replacement, the Opus fallback (labeling only).
