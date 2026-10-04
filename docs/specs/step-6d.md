# Step 6d: Eval harness and report format (mini PRD)

Status: approved Oct 4, with decisions 1 to 6 and the test points as written. Plan row: [plan-v1.md, PR steps, 6d](../plan-v1.md) ("Eval harness and report format"), Phase 6 ("The eval harness scores all 4 options, then accuracy, p50/p99 latency and cost go into a report"; exit: "the eval report is committed and the threshold is chosen from it"), B4 (6e measures throughput on this Mac) and B5 (memory decides one shared classifier process). Design: [Categorization](../design-commerce-ingestion-pipeline.md) ("The threshold is chosen from the eval") and Future work ("On a 200-Listing eval set, compare (a) … (d) on accuracy, latency and cost"). Builds on [step-6b.md](step-6b.md) (the `classify` contract, `THRESHOLD`, `budget`). Terms follow [CONTEXT.md](../../CONTEXT.md).

## Problem

6e has to pick a classifier, a text recipe and a threshold, and today nothing measures any of them. `THRESHOLD = 0.5` is a guess, and the 6b review left two questions only a measurement answers: what the 500-character description buys for its 7× cost, and how fast the embedding path runs on the 16 GB Mac. 6e also compares four options, two of which (Laya, Jev) don't exist yet and may not run in the worker's environment. 6d builds the measuring tool and fixes the report's shape, so 6e only adds a labeled set and candidates.

## Solution

1. **Labeled set format:** `eval/labels.jsonl`, one Listing per line: `{"id", "title", "description", "category"}`, other fields ignored (6e may add the source ASIN or a note). `category` is a path in the committed taxonomy. 6d commits no labeled set; 6e does.
2. `src/catalog/evaluate.py`:
   - `load_labels(path, taxonomy) -> list[Labeled]`: refuses the whole file, naming the line, on bad JSON, a missing field, a duplicate `id`, a title or description the API would reject (`Content`'s limits), or a `category` not in the taxonomy. An empty file is refused too.
   - `run(classifier, labeled, *, batch, clock=time.perf_counter) -> Result`: one untimed warm-up call, then `classifier.classify` on `batch` Listings at a time, timing each call. Keeps every answer `(category, confidence)` or `None`, the call times, the label set's SHA-256, `taxonomy_version`, `batch`, the peak RSS, the candidate's spend (decision 4), the date and the machine (`platform.platform()`, CPU count).
   - `score(result, labels, thresholds) -> dict`: per threshold `t`, an answer counts only when its confidence is `≥ t` (else it is Uncategorized). Reports, per `t`: **accuracy** (exact path correct, over all Listings), **precision** (correct over answered), **Uncategorized rate**, and **level-1 and level-2 accuracy** (the answer's first one or two levels match the label's). Plus p50 and p99 ms per call, Listings per second, `None` count, and $ per 1M Listings.
   - `render(results) -> str`: `eval/report.md` (decision 5).
   - `main`: `python -m catalog.evaluate run --classifier {fake,embedding} [--labels eval/labels.jsonl] [--batch 1] [--name …]` writes `eval/results/<name>.json`; `python -m catalog.evaluate report` reads every file in `eval/results/` and writes `eval/report.md`.
3. Candidates are built with the confidence threshold at 0 and the budget off (`math.inf`), so every Listing gets its best path and raw confidence, and the sweep applies thresholds afterwards (decision 2). The `run` CLI builds `fake` and `embedding`; 6e adds Laya and Jev.

## User stories

1. As you in 6e, I run one command per candidate and one for the report, and the report shows accuracy, latency, cost and memory side by side.
2. As you in 6e, I pick the threshold from a table of accuracy, precision and Uncategorized rate per threshold, without re-running the model for each threshold.
3. As you verifying labels, a bad label (a typo'd path, a duplicate) fails the run with its line number, instead of quietly counting as a miss.
4. As you comparing recipes, results record the label set's hash and the `taxonomy_version`, so a report never mixes runs over different sets without saying so.
5. As 6e's Laya or Jev candidate, I only need the existing `classify` contract (plus an optional `usd` attribute) to be scored.

## Failure scenarios

| Scenario | Expected |
|---|---|
| A label path not in the taxonomy (typo, depth 4, Shopify's own Uncategorized) | Refused, naming the line and the path |
| Duplicate `id`, bad JSON, missing field | Refused, naming the line |
| A title over 150 characters (common in Amazon titles) | Refused, naming the line: the set holds only what the API accepts; 6e cuts titles when building it |
| Empty label file | Refused: no accuracy over zero Listings |
| The candidate answers `None` (a budget left on, or a partial service) | Counted as Uncategorized and reported as `None` count, so a budget leak is visible |
| The candidate raises mid-run | The run fails and writes no result file: a partial result would score as if complete |
| The candidate answers a path not in the taxonomy | Scored as a miss and counted apart (`invalid`), so a Laya or Jev parsing bug is visible |
| A wrong number of answers for a batch | The run fails: the contract is one answer per Listing |
| Results from different label sets in `eval/results/` | The report groups by label-set hash and says so in a line above the table |
| The first call loads lazily (ONNX, MLX compile) | The warm-up call isn't timed |
| `--batch` larger than the set | One call with every Listing |
| Running the `model` candidate in the `check` job | Not possible: tests use `fake` and an injected `embed`; the real model runs only by hand or in the `model` job |
| Killed mid-run | No result file (written once, at the end, through a temp file and rename); nothing to clean up |
| Re-running a candidate | Overwrites its `eval/results/<name>.json`; git shows the diff |

## Implementation decisions

1. **One process per candidate, results as JSON, then a separate report step.** Peak RSS is per process, so one candidate per run measures B5's memory honestly. It also lets 6e run Laya or Jev from another environment (MLX, a paid API) and still drop a JSON file into `eval/results/`. Alternative: score all four in one process, which mixes their memory and needs every dependency in one venv.
2. **Threshold sweep after the fact.** Candidates run at threshold 0, the harness applies `t = 0.30, 0.35, …, 0.90`. One model run gives the whole table, and 6e chooses from it. The report highlights no "best" threshold: that trade-off (accuracy against Uncategorized rate) is your call in 6e.
3. **Exact match is the headline, levels 1 and 2 are context.** A Listing filed one level too shallow or in a sibling Category is still wrong for the catalog, but the level columns show whether a candidate is roughly right or lost.
4. **Cost from an optional `usd` attribute.** A candidate that spends money (Jev) keeps a running total in `usd`; the harness reads it after the run and reports $ per 1M Listings. Local candidates have none, so $0. Alternative: a token counter in the harness, which would have to know each API's pricing.
5. **The report format** (`eval/report.md`, generated, committed by 6e): a header with the date, machine and label-set hash; a summary table with one row per candidate at `report --threshold` (default `classify.THRESHOLD`) (accuracy, precision, Uncategorized rate, L1, L2, p50 ms, p99 ms, Listings/s, $/1M, peak RSS MB, `None`, `invalid`); then one threshold table per candidate. Every miss, with its label, goes in its JSON, not the report, for you to check labels against.
6. **`src/catalog/evaluate.py`, not code under `eval/`.** It stays in the package, under the 90% coverage gate and ruff; `eval/` holds only data (labels, results, report). The plan's layout line ("eval/ labeled set, classifier experiment") still holds.

## Testing decisions

- **Test points (seams), confirmed:**
  1. **`load_labels`** (new, red first, `tests/integration/test_evaluate.py`, files in `tmp_path`, the committed taxonomy): a good file loads in order; each refusal in the table names its line.
  2. **`run`** (new, red first, same file): with `FakeClassifier` and a fake clock, one warm-up call untimed, calls of `batch` Listings, call times kept; a classifier answering the wrong count, or raising, fails the run.
  3. **`score`** (new, red first, same file; pure, so it could go in `tests/unit`, but `make mutate` covers only the six pure modules): hand-made answers and labels give exact, precision, Uncategorized rate, L1 and L2 at threshold boundaries (equal to `t` counts, as in 6b); `None` and invalid paths counted apart; p50/p99 over known times; `usd` turns into $ per 1M.
  4. **`main`** (new): `run --classifier fake` on a tiny label file writes a result JSON, `report` writes a Markdown file whose summary row names `fake-1`; a second label set in `results/` produces the mismatch line.
- **No model in these tests.** The `model` job gets no new test: the real run is 6e's, by hand.
- **Coverage:** `evaluate.py` under the 90% gate; not on the 100% list (it reads files and the clock).

## Out of scope

- The labeled set and its hand check, Laya, Jev, the text-recipe experiments, choosing the threshold, switching the `Procfile` (6e).
- Statistical intervals on 200 Listings (6e can add a bootstrap column if two candidates are close).
- Per-Category breakdowns and a confusion matrix: 200 Listings over 1,862 Categories is too thin.
- Running the eval in CI (plan, CI table: "run by hand").

## Size

About 120 lines in `evaluate.py`, about 130 of tests, an `eval/results/.gitkeep`.
