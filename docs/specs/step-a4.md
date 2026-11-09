# Step A4: classification on executors (mini PRD)

Status: approved Nov 8: the decisions, the test points and all three questions. Plan row: [plan-v2.md, PR steps, A4](../plan-v2.md). Decisions from the [streaming job][t137], [student on Spark][t132] and [CI][t162]. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

A2's batch runs the classifier inside the per-key plan, with `fake` fixed in `ingest.plan_key`. The student can't run there: it needs its encoder and index loaded once per Python worker, and its version, which hashes `train/`. Nine facts shape this step. "Probed" means local Spark on Nov 8 (PySpark 4.1.4, delta-spark 4.4.1, JDK 21, `local[2]`), with the files in `probe/local/` on the unmerged branch [`probe/a4`][probe], unless it names Free Edition, where it means [this run][fe-probe] from that branch on environment 5.

1. **A cache that the function names directly reloads on every task.** Probed: a classifier kept in a global that the pandas function itself names loaded 12 times in 12 tasks on 2 Python workers, since cloudpickle ships a nested function's globals by value. Kept behind a module-level function, it loaded once per worker: 2 loads over 3 actions. Spark [reuses Python workers][sparkconf] by default.
2. **A small batch reaches the classifier as one task.** Probed: 1,000 rows out of `applyInPandas` reached the next function in 1 partition, at 4 or at 200 shuffle partitions, since AQE coalesces them. `repartition(n)` gave n even partitions, and AQE [never coalesces][coalesce] a repartition by number.
3. **`mapInPandas` corrupts big integers.** Probed: a price of 2^53 + 1 in a batch that also held a null came back as 2^53, since pandas holds an integer column with nulls as floats. An iterator pandas UDF that takes only the classifier's columns kept the price exact.
4. **A MERGE runs its source once, and a count runs it again.** Probed: 100 source rows went through the UDF once in a MERGE, and a `count()` of the same source sent them through again (200). Delta [materializes a MERGE source][materialize] that holds a UDF, "even if the user defined function is marked as deterministic". `ingest.merge` counts its source today.
5. **The student loads from plain, read-only files.** Probed: the encoder as plain copies (no Hub symlinks) and the index, in a read-only folder, loaded with `local_files_only` and gave the expected categories through the UDF. `hf_hub_download` [reads `snapshots/<revision>/<file>`][hub] and writes nothing when the file is there. A Python worker's peak memory went from about 210 MB to about 500 MB once the student loaded (macOS), and serverless [caps user code at 1 GB][limits].
6. **The driver has no checkout either.** The paid job and the Free Edition run start our wheel as a wheel task, so the driver has no `train/`, which `student.version()` and `index_path()` hash (question 1).
7. **v1's worker test needs delta-rs 1.6.6.** Probed: in the Spark env, the three bge-small tests pass, and the worker CLI test fails with `to_pyarrow_dataset() got an unexpected keyword argument 'file_pruning_predicate'`, since the env holds Runtime 18's deltalake 1.1.4 (question 2).
8. **On Free Edition, the compiled pins install and the student runs on the executors.** Probed on Free Edition: environment 5 installed the wheel and all 38 pins of `requirements/databricks.txt` on aarch64, so risk B3's pins hold there. A pandas function on the executors loaded the encoder and the index from `/Volumes` with `local_files_only`, gave the 8 categories the Mac gave, loaded once in its Python worker over two actions, and stayed under 0.75 GB. `spark.sparkContext` raises `JVM_ATTRIBUTE_NOT_SUPPORTED` there, "not available on Standard access mode or Serverless compute", and says to switch to dedicated access mode. `spark.default.parallelism` isn't set, and `spark.sql.shuffle.partitions` is `auto`.
9. **CI's cache is shared one way.** A `pull_request` run saves its cache under the PR's merge ref, which [only re-runs of that PR][cache] restore. Every branch restores `main`'s. A job skipped by its `if` [reports success][skipped] to a required check.

## Solution

```text
each micro-batch (ingest.apply_batch), the new parts marked +:
  plan per Listing key (applyInPandas): Outcome rows and Write rows
  + a Write the plan marks for classifying carries needs_classify and its provisional answer:
  +   its unchanged answer, else Uncategorized, either way flagged needs_reclassify
  + count the Write rows, from the persisted plan
  + repartition the Write rows to the slot count, where Spark gives one
  + the iterator pandas UDF (classify_udf), on the executors:
  +   build the classifier once per Python worker, from a spec the driver made
  +   classify the marked rows in one call; if the call fails, one Listing at a time
  +   a Listing it can't classify keeps its provisional answer
  MERGE, then the Outcomes append, as in A2
```

## User stories

1. As the operator, I want the student to classify on every executor core, so that 50 Changes/s don't wait on one task.
2. As a Merchant, I want my Change written when the classifier fails, so that categorization never blocks ingestion (v1's Uncategorized path).
3. As the operator, I want a missing or broken model to stop the job, so that it doesn't file every Listing as Uncategorized (question 3).
4. As the developer, I want CI to run the real student on Linux in Runtime 18's libraries, so that a broken model path shows before a paid run.

## Failure scenarios

Seeded from lean-review's finder angles.

| Scenario | Expected |
|---|---|
| The classifier raises on a call | The UDF retries the call one Listing at a time. A Listing that still fails keeps its provisional answer, as v1's `_classify` did. Its Outcome stays `written` |
| One Listing makes the classifier raise | Only that Listing falls back (decision 4) |
| An answer breaks the contract: an empty category, a confidence outside 0..1 or NaN, a wrong count | It counts as a failure, through v1's `decide._answer` |
| The student's files are missing or broken | `ModelMissing`, an `entry.Fatal`, fails the task. A cluster retries the task first ([`spark.task.maxFailures`][sparkconf]). Nothing commits, and the query fails (question 3) |
| A Python worker dies mid-batch, such as over the 1 GB cap | The task fails, and the batch replays as in A2 |
| A crash between the plan and the MERGE | Nothing committed, so the batch replays (A2's crash tests). The classifier runs inside the MERGE, so it adds no crash point |
| The persisted plan is evicted | The plan recomputes against the pinned version (A2), and the UDF runs again on the same rows |
| A small batch | Repartitioned to the slot count, so it doesn't run as one task (probed) |
| No `sparkContext`, as on serverless | `start` catches `PySparkAttributeError` (probed on Free Edition) and doesn't repartition, so the platform picks the tasks. Free Edition checks answers, not speed |
| The classified source is counted | It would run the classifier again (probed), so the count comes from the persisted plan |
| A price past a float's precision | The UDF never sees it: it takes only the classifier's columns (probed) |
| Many Python workers load from one folder | They only read. `hf_hub_download` writes nothing when the file is there ([source][hub]), and fastembed's `mkdir` of an existing `cache_dir` [changes nothing][fastembed] (probed read-only) |
| `train/` changed but the index didn't | The index's name holds the training files' hash, so it's missing: `ModelMissing` (v1's rule, `student.index_path`) |
| The student's version changes between runs | Stored rows carry the old version, so the plan marks each on its next Change (v1's rule). A5's Backfill reclassifies the rest |
| The `model` job's cache misses | It downloads the models and builds the index: about 15 minutes for the student's on a runner (the probe's step), inside `timeout-minutes: 40`. Its cache then serves later runs of that PR, and `main`'s serves every branch ([docs][cache]) |
| A PR that touches no model path | `model` is skipped, which [reports success][skipped] to the required check |

## Implementation decisions

1. **`src/ingestion/classify_udf.py`** (new), the UDF module:
   - `Spec`, a frozen dataclass of what an executor needs: the kind (`student`, `fake` or `down`), the classifier's version and, for the student, the models folder and the index's path. The driver makes it: `Spec.student(models)` computes `student.version()` and `student.index_path()` from the `train/` beside `models/` (question 1). The UDF's closure carries it to the executors, since a broadcast variable needs the `sparkContext` that serverless lacks (fact 8).
   - `loaded(spec)`, module-level, keeps one classifier per Python worker (fact 1). A new spec replaces the old one.
   - `classified(writes, spec, slots)`: `writes.repartition(slots)` when `slots` is set, then the iterator pandas UDF over seven columns: `needs_classify`, `title`, `description` and the four provisional classification columns. It returns those four, replaced where it has an answer: the category and confidence, the spec's version, and `needs_reclassify` false.
   - The student loads with `threads=1`, one ONNX thread per task slot ([student on Spark][t132]).
2. **The plan marks; it doesn't classify.** `decide.planned(landed, stored, version)` is `decide` without the classifier, and `decide.decide` becomes `planned` plus v1's `_classify`. `decide.provisional(write, version)` is the answer a failure leaves: the unchanged answer flagged, else Uncategorized flagged. `_classify` uses it, and `ingest.plan_key` writes it into each marked Write row with `needs_classify` true. The output schema gains `needs_classify`.
3. **`ingest`:** `start` takes `classifier: Spec` and works out the slot count once: `sparkContext.defaultParallelism`, or none on serverless, where `sparkContext` raises (fact 8). `apply_batch` counts the Write rows from the persisted plan, then hands `classified(...)` to `merge`, which takes that count (fact 4). `main` gains `--classifier` (`student` by default, or `fake` or `down`) and `--models` (`models` by default).
4. **Errors:** the UDF classifies the marked rows of each Arrow batch in one call. If the call raises, or answers the wrong count or out of contract, it calls again one Listing at a time, so only the Listings the classifier can't classify keep their provisional answer. v1 failed the whole call. `entry.Fatal`, `ModelMissing` included, is never caught (question 3).
5. **`student`:** `serve(tax, models, index, version, *, threads)` builds the classifier from paths the driver named, and `pipeline` becomes `serve` over the checkout's. `version()` and `index_path()` take the training files' paths, the checkout's by default.
6. **The `model` job** runs on `ubuntu-latest` with a 40-minute timeout. It builds the Spark env (`make spark-env`, split out of `make spark`), computes the student's version there, and restores `models/` from a cache keyed by it. On a miss it runs `classifiers --download` and `student --download` there, so the index is built with the libraries Databricks runs. Then the bge-small tests (question 2) and `make spark TEST='tests/spark -m model'`, JDK 21 from the runner. The path filter gains `student`, `ingest`, `train/`, `requirements/`, the `Makefile` and the UDF's Spark tests (`classify_udf` already matches its `classif…` pattern). The macOS leg goes.
7. **`model` becomes required** once it has run green on `main`. ⏸ You OK the change to `main`'s protection, as you did for `spark` in G5.
8. **Two PRs** (Size): **A4.1** is decisions 1–7, on local Spark and CI. **A4.2** is the Free Edition check of the student (decision 9).
9. **A4.2, the Free Edition check:** `free_edition.main` gains `--models` and one check, "the student classifies on the executors": 8 Listings of known categories, landed and run through the ingest query on `Spec.student(models)`, each stored with its expected category and the student's version. `scripts/free-edition.py` uploads the student's encoder files and index under `models/` on the volume when they're missing (their paths hold the revision and the training hash), and `train/` on every run (its names hold no version). The environment's dependencies become the wheel plus every pin of `requirements/databricks.txt`. The workflow restores `models/` from the `model` job's cache, and builds it on a miss. The A3 checks stay on `fake`.
10. **plan-v2 changes, in the spec's PR:** the A4 row names this spec and its two PRs, and follows decision 1 (the closure, not a broadcast) and questions 2 and 3; the CI table's `model` row says where the bge-small tests run (question 2); the "Inferred, not measured" line drops the `/Volumes` read that the Free Edition probe settled.

## Testing decisions (test points for your OK)

- **`tests/unit/test_decide.py`,** in `make check`:
  1. `planned` gives `decide`'s Outcomes and its Writes before classifying, `needs_classify` still set. `provisional` keeps an unchanged answer, flagged, and gives a new Listing Uncategorized at the classifier's version, flagged. The existing `decide` tests still pass. Red: no function.
- **`tests/spark/test_classify_udf.py`** (new), in `make spark`, through the real query on `fake`:
  2. Only marked rows reach the classifier. A first batch on `fake` stores fake's answer. A second batch on `down`, at the same version, keeps the stored answer for a price-only change (not marked), and gives a changed title and a new Listing Uncategorized, flagged. Every Outcome is `written`. Red: classify every Write, or let the outage fail the batch.
  3. A Listing the classifier raises on fails alone: an empty title, which `fake` raises on, beside two good Listings in one batch. Red: v1's whole-call fallback.
  4. A student that can't load fails the batch: `Spec.student` over an empty models folder raises `ModelMissing` through the query, and the store and Outcomes stay empty. Red: catch it.
  5. One classifier per Python worker: after two batches, a test UDF on the same workers finds one entry in each worker's `classify_udf._loaded`, the same object after each batch. Red: a cache that the UDF names directly.
  6. `classified` with `slots=2` gives 2 partitions, and with `None` leaves them as they are.
  7. `Spec.student` over a copy of `train/` beside a models folder gives `student.version()` and `index_path()`'s values.
- **A2's tests,** unchanged, pass on `fake` through the UDF, the 2^53 + 1 price among them.
- **The real student,** marked `model`, in the `model` job: 8 Listings of known categories, landed in one commit and run through the query on `Spec.student(Path("models"))`, each stored with its expected category, the student's version and `needs_reclassify` false. Seen red on the runner: the PR's first push holds the test without the code.
- **A4.2,** in `tests/spark/test_free_edition.py`: in the `model` job, `main` with the real models prints only PASS lines; in `make spark`, the student check called on `fake` prints FAIL. On Free Edition, two throwaway runs show the new check red, one failing on every pass and one on the first pass only (plan-v2, Testing rules). Then the PR's branch runs green, cited in its Evidence.
- **The Free Edition workflow on A4.1's branch:** the A3 checks, on `fake`, through the new batch.

## Questions

1. **Where does the driver find `train/`?**

   Context: the student's version names everything that changes an answer, the training files included (`student.version()`). `index_path()` puts the same hash in the index's file name, so a stale index is missing, never served. Both read `train/` (21 MB) from the working folder. The plan says the driver computes them and sends them to the executors. On Databricks the driver runs our wheel, not a checkout, so it has no `train/` either.

   - **Option A (recommended): upload `train/` beside the model files.** The job takes one folder, `--models`, and reads the `train/` next to it, as a checkout has it. The driver computes both values, as the plan says. Cost: about 21 MB more to upload, and `version()` and `index_path()` take the training files' paths.
   - **Option B: the launcher computes them.** The workflow, and later C2's Terraform, computes the version and the index's path from its checkout and passes both to the job. Cost: two more job parameters that must match the files on the volume, and the plan row changes, since the driver computes nothing.

   **Answer (Nov 8): upload `train/` beside the model files.**

2. **Where do the bge-small tests run?**

   Context: the plan moves the whole `model` job into the Spark env. Three of the four bge-small tests pass there. The fourth runs v1's worker with the bge-small classifier, which needs delta-rs 1.6.6, and the Spark env has Runtime 18's 1.1.4 (probed). v2 runs neither bge-small nor v1's worker, and S2 deletes the worker.

   - **Option A (recommended): the bge-small tests in the uv env, the student's test in the Spark env.** The job keeps all four tests, now on Linux. Cost: the job syncs both envs.
   - **Option B: everything in the Spark env, without v1's worker test.** One env. Cost: v1's worker loses its only real-model test until S2 deletes it.

   **Answer (Nov 8): the bge-small tests in the uv env, the student's test in the Spark env.**

3. **What does the UDF do when the student can't load?**

   Context: the plan says the UDF "catches per-row errors and never raises, so v1's Uncategorized path holds". v1 had two paths. A classifier error or outage gave Uncategorized, flagged for the Backfill. A missing or broken model (`ModelMissing`, an `entry.Fatal`) stopped the worker with exit code 6, since no batch could succeed.

   - **Option A (recommended): a load failure fails the batch, as v1's exit 6 did.** Errors on Listings still never raise. Spark retries the task, then the query fails, and A6's retries and email take over. Cost: the job stops until someone fixes the model files.
   - **Option B: a load failure gives every marked Listing Uncategorized, flagged.** The job never stops for the classifier. Cost: a missing model files every new Listing as Uncategorized until someone notices, and A5's Backfill then reclassifies all of them.

   **Answer (Nov 8): a load failure fails the batch, as v1's exit 6 did.**

## Out of scope

- The Backfill tail: A5. It reclassifies the rows this step leaves flagged.
- Diagnostics: v1's `classify` and `classify_failed` events have no home in the job. A6's spec decides what the job logs (rule 6).
- The paid job's `--models` and the models step that fills the external volume: C2 and C3. They follow decision 9's layout: the encoder files and the index under `models/`, and `train/` beside it.
- The student's real rate per core, and `sparkContext` in dedicated access mode on Runtime 18: P3. Serverless's own error points to dedicated mode for it (fact 8), and the [dedicated limitations][dedicated] list no limit on it.
- Memory on the paid cluster: about 500 MB per Python worker (macOS), so about 2 GB on a 4-vCPU worker. P3 records it.
- Deleting `probe/a4` and `probe/free-edition`: when Phase A's probes end.

## Size

About 235 changed lines, tests left out, so two PRs. **A4.1, about 150:** `classify_udf.py` about 65, `ingest.py` about 30, `decide.py` about 15, `student.py` about 12, `ci.yml` and the Makefile about 25. **A4.2, about 85:** `free_edition.py` about 35, the script about 35, the workflow about 15. About 250 of tests. A1 to A3 came in 22–77% over their estimates, so A4.1 may reach about 260, still under the cap.

[t132]: https://github.com/Hien-Trinh/Shopping/issues/132
[t137]: https://github.com/Hien-Trinh/Shopping/issues/137
[t162]: https://github.com/Hien-Trinh/Shopping/issues/162
[probe]: https://github.com/Hien-Trinh/Shopping/tree/probe/a4
[fe-probe]: https://github.com/Hien-Trinh/Shopping/actions/runs/42503954774
[sparkconf]: https://spark.apache.org/docs/4.1.0/configuration.html
[coalesce]: https://github.com/apache/spark/blob/v4.1.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala
[materialize]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/main/scala/org/apache/spark/sql/delta/commands/merge/MergeIntoMaterializeSource.scala
[hub]: https://github.com/huggingface/huggingface_hub/blob/v1.2.4/src/huggingface_hub/file_download.py
[fastembed]: https://github.com/qdrant/fastembed/blob/v0.9.0/fastembed/common/utils.py
[limits]: https://docs.databricks.com/aws/en/compute/serverless/limitations
[dedicated]: https://docs.databricks.com/aws/en/compute/dedicated-limitations
[cache]: https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching
[skipped]: https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-jobs-with-conditions
