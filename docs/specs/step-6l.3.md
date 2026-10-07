# Step 6l.3: fine-tune bge-small end-to-end as the student (mini PRD)

Status: approved Oct 6. Question 1: Colab first, by hand; Hugging Face Jobs for scripted reruns. Question 2: a public Hub model repo. Question 3: the bar OK. Question 4: the test points OK. Plan row: [plan-v1.md, PR steps, 6l.3](../plan-v1.md). Builds on [step-6l.md](step-6l.md) (the student and its eval), [step-6l.2.md](step-6l.2.md) (10k Amazon rows; the bar is met with kNN) and [docs/research/classifier-hosting-options.md](../research/classifier-hosting-options.md) (why this is the next lever). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

6l.2 met the bar with kNN over frozen bge-small vectors: the cascade gets 70.0% exact on the 1,020 with 83.7% kept local, against Jev's 66.9%; the student alone gets 65.7% (55.0% on Shopify's 2,000). Those vectors were never trained for our Categories. The closest published results (2,214 Amazon categories; 3,008 Rakuten categories) put a fine-tuned small encoder 5 to 10 points above frozen-vector and bag-of-words students, which is the largest gain left before 6m. A better student alone moves the whole curve: more Listings kept local at the same accuracy (less Jev), or more accuracy at the same kept rate.

The Mac is the wrong place to train it: PyTorch on MPS is little faster than CPU for BERT-sized models and has a history of wrong metrics, so a run is an hour or more and needs a cross-check. On a T4-class GPU the same run is about 5 to 10 minutes. So the training script runs in the cloud and only the result comes back.

## Solution: one training script, run anywhere, and a third student in the eval

### The model

bge-small-en-v1.5 (33M parameters, the fp32 checkpoint of the model the workers run as Qdrant's int8 ONNX) with a linear head, 384 × the Categories seen in training, trained end-to-end with cross-entropy on `train/` (41k rows, 1,7xx Categories). CLS pooling and unit normalization, as bge does, so the fine-tuned encoder is still an embedding model. Hyperparameters, to be fixed on the held-out rows: AdamW, encoder lr 5e-5 and head lr 1e-3, 10% linear warmup, batch 64, max length 128 tokens (the text is the title plus 200 description characters, about 64 tokens), 4 epochs, keep the epoch with the best held-out exact; seed 0; mixed precision on CUDA. Flat over leaves: the measured results have flat classifiers beating level-wise ones.

### `train/finetune.py` (torch, transformers; never imported by `src/`)

1. Reads `train/` through `catalog.student.load_training`, drops eval titles and holds out 10% of each source with `held_out(seed=0)`, so the split is 6l.2's.
2. Trains as above, logging held-out exact after each epoch.
3. Exports the best epoch's encoder to ONNX (`onnx/model.onnx`, output `last_hidden_state`, dynamic batch and length) and the head to `head.npz` (`classes`, `w`, `b`: the fields of `student.Softmax`). Before saving, a self-check: ONNX and torch vectors agree within 1e-3 on 100 held-out texts, or the run fails.
4. Quantizes a second copy to int8 with onnxruntime's dynamic quantization (`onnx/model_quantized.onnx`, about 34 MB against 133 MB), the format the workers already run.
5. Writes `train.json`: hyperparameters, git SHA, the SHA-256 of the training rows, device, library versions, held-out exact per epoch, wall time.
6. Pushes the folder to a public Hugging Face Hub model repo under your account (question 2), the same place bge-small comes from, so `python -m catalog.classify --download` can fetch it.

Dependencies go in a `finetune` dependency group (`torch`, `transformers`, `huggingface_hub`; `onnxruntime` is already installed through fastembed), not installed by `uv sync` or in CI. The script has a `--smoke` flag (200 rows, 1 epoch, no push) to prove the whole path on any machine in a minute.

### Where it trains (question 1)

The script is plain Python, so it runs in any of these; the artifact comes back through the Hub either way. Colab first, by hand; Jobs for scripted reruns.

| Where | GPU | Price for one run (about 10 min) | How it runs | Notes |
|---|---|---|---|---|
| **Google Colab (free)**, the first runs | T4 (varies, preemptible) | $0; Pro $9.99 a month if the free GPU isn't available | A notebook cell: clone the repo, `pip install`, `python train/finetune.py` | By hand; 90 min idle disconnect, GPU not guaranteed, so the script checkpoints each epoch |
| **Hugging Face Jobs, `t4-small`**, scripted reruns | T4 | about $0.07 ($0.40/h, per-minute billing); needs a positive credit balance, no free tier | `hf jobs uv run --flavor t4-small -s HF_TOKEN train/finetune.py`; `hf jobs wait` fails loudly | One command from the Mac, no notebook; pin the script to a commit and the image to a tag; the 30 min default timeout fits |
| Modal (Starter plan) | T4 | $0 ($30 credit a month, about 50 T4 hours) | `modal run` on a decorated function; `modal volume get` for files | Same script-and-CLI shape; a second account to manage |
| Kaggle | T4 ×2 / P100 | $0 (about 30 GPU hours a week) | Notebook, or `kaggle kernels push` | Phone verification; preinstalled versions drift |
| Azure ML command job, `NC4as_T4_v3` | T4 | about $0.09 on demand ($0.53/h), about $0.03 spot | `az ml job create`, compute scaled to zero | The right place once the project is on Azure; needs a GPU quota request first |
| The Mac (MPS or CPU) | M4 | $0 | `uv run --group finetune python train/finetune.py` | An hour or more; MPS results need a cross-check against CPU or CUDA; keep for `--smoke` |

### The eval: two more students in `evaluate run` and `eval/student.md`

1. `classify.fastembed` learns the custom model: `TextEmbedding.add_custom_model("student-ft", PoolingType.CLS, normalization=True, sources=ModelSource(hf=<repo>), dim=384, model_file="onnx/model_quantized.onnx")`, downloaded by `--download` beside bge-small. The report also runs the fp32 file once and says what int8 costs.
2. `student.vectors`' cache key gains the model name (today it is `classify.MODEL`), so fine-tuned and frozen vectors don't collide.
3. `--classifier student-ft`: `StudentClassifier(embed_ft, Softmax` loaded from `head.npz)`; confidence is the top probability, as today. `--classifier student-ft-knn`: kNN (k from `KS`) over the fine-tuned vectors, a free second candidate since the vectors are cached anyway; the papers found test-time kNN interpolation adds little, so this just records whether that holds here.
4. `python -m catalog.student` reports both beside softmax and kNN: alone, the cascade curves with Jev and Opus, τ from the Amazon held-out rows (6l.2's rule), p50/p99, unseen labels, and the bar.

### The bar (question 3)

6l.2's bar is already met, so this step needs its own. **6l.3 replaces kNN as 6m's student if, on the 1,020 with Jev below τ, the fine-tuned student alone beats kNN alone (65.7%) by 3 points or more, and its cascade is at least as accurate as 6l.2's point (70.0%) at a higher kept rate, or more accurate at 83.7% kept.** The eval's standard error is about 1.5 points, so 3 points is the smallest gain worth switching models for. Shopify's 2,000 are reported, not gated. If the bar is missed, 6m ships kNN and nothing else changes.

## User stories

1. As you, I know whether fine-tuning buys the 5 to 10 points the literature suggests on our Listings, for under a dollar of GPU time and no API calls.
2. As you, I can rerun the training with one command, from the Mac now and from Azure later, and the result is reproducible from `train.json`.
3. As you, the fine-tuned model is the same kind of artifact the workers already load (an int8 ONNX encoder through fastembed), so 6m's decision about where weights live is unchanged.

## Failure scenarios

| Scenario | Expected |
|---|---|
| Training diverges (NaN loss) | The script stops naming the step; nothing is exported or pushed |
| Held-out exact falls or stalls across epochs | The best epoch is kept and `train.json` shows the curve; the report says so |
| MPS gives a result that differs from CUDA/CPU | Not trusted: the spec's numbers come from a CUDA run; the Mac is for `--smoke` |
| ONNX export disagrees with torch (> 1e-3 on 100 texts) | The self-check fails before saving |
| int8 loses more than 1 point against fp32 on the 1,020 | The report says so; 6m's spec picks the file (133 MB fp32 fits 8 workers in 16 GB) |
| The cloud session dies mid-run (Colab disconnect, spot eviction) | Each epoch's checkpoint goes to the output folder; the run resumes from the last one |
| The model isn't in `models/` | `ModelMissing` naming `--download`, as today; in a worktree, `--models` |
| The Hub repo is private and no token is set | `--download` fails with the Hub's error; the runbook says where the token goes |
| An eval Category never occurs in training | The head can't answer it; counted and listed, as today |

## Implementation decisions

1. **Eval only**, like 6l and 6l.2: no worker, Backfill or `taxonomy_version` change. 6m decides which student ships.
2. **Train in the cloud, serve as today.** torch and transformers stay in an optional group; `src/` imports neither. The pipeline's dependency list doesn't change.
3. **The same split and the same threshold rule as 6l.2,** so the curves are comparable row for row.
4. **The artifact's home is the Hub,** because `--download` and the CI `model` job already fetch from it; a public repo needs no secret in CI (plan rule: no secrets in CI). The model is a derivative of bge-small (MIT) trained on Shopify's Apache-2.0 benchmark and our own labels, so publishing it is allowed; whether we want to is question 2.
5. **Flat leaf head, no hierarchy tricks, no label-text augmentation yet.** Measured gains for those are 1 to 2 points; they go in a later step if this one lands.

## Testing decisions (test points for your OK)

In `tests/integration/test_student.py`, with a fake `embed`, as today:

1. `Softmax` round-trips through `head.npz` (`save`, `load`) and predicts the same labels and confidences.
2. The vector cache is keyed by model name: two `embed` functions with different names over the same texts don't share a file.
3. `evaluate run --classifier student-ft` and `student-ft-knn` return valid taxonomy paths for every Listing when the head file is present, and `ModelMissing` names `--download` when it isn't.
4. The report lists the two new students with the same sections as the old ones.

Not in CI: the training script (torch isn't installed there). Its check is the ONNX self-check it runs on itself and the `--smoke` run, recorded in this spec's Outcome with its wall time per device.

## Questions

1. **Where to train.** Decided Oct 6: Colab first, by hand; Hugging Face Jobs (about $0.07 a run, one command) once reruns are needed. Modal is free but a second account.
2. **The artifact's home.** Decided Oct 6: a public Hub model repo under your account (no secret anywhere, CI can fetch it). Rejected: private (a token on the Mac and in CI, against the no-secrets rule) and a GitHub release asset loaded through onnxruntime directly.
3. **The bar above:** student alone +3 points over kNN on the 1,020, and the cascade no worse than 6l.2's point in both exact and kept. OK (Oct 6).
4. **The test points above.** OK (Oct 6).

## Out of scope

- The pipeline (6m), hierarchy-aware heads, label-text augmentation, a top-10 shortlist for Jev (a candidate for 6m's spec), images, non-English, a bigger encoder (ModernBERT, DeBERTa).
