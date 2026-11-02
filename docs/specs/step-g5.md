# Step G5: the compiled requirements file and the `spark` job (mini PRD)

Status: draft, for your OK on the decisions, question 1 and the test points. Plan row: [plan-v2.md, PR steps, G5](../plan-v2.md). Decisions from [3.12 packaging][t141] and [CI][t162]. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

v2 categorizes and streams on Databricks Runtime 18 LTS, and nothing here runs Spark yet:

- The job cluster installs our wheel `--no-deps` ([#141][t141], decision 2), so fastembed and everything it pulls in need exact pins that fit the runtime's own libraries (numpy 2.1.3, pyarrow 21.0.0). No file holds them.
- No test runs Spark or delta-spark. The steps that bring the streaming job, the categorization UDF and the Spark `stress-smoke` legs have no env, no make target and no CI job to land in.

## Solution

One PR:

1. **The compiled requirements file.** `make requirements` compiles `requirements/databricks.in` (`fastembed==0.9.0`) to `requirements/databricks.txt`, with `requirements/runtime-18.txt`, Runtime 18's installed-library table, as constraints: 31 exact pins today, for Linux x86_64 and Python 3.12.
2. **The Spark env.** `.venv-spark` (gitignored) holds the compiled file and `requirements/spark.txt` (`pyspark[sql]==4.1.4`, `delta-spark==4.4.1`, `pytest`), all under the same constraints, plus the package itself with `--no-deps`, as on the cluster.
3. **`make spark`.** It builds or refreshes the env, then runs `pytest tests/spark` in it: one shared session with `snapshotPartitions=1`, no xdist. `make check` never collects `tests/spark`.
4. **The `spark` job.** It runs on every PR and every push to `main`, with no path filter, JDK 21 from the runner, a step that fails on a stale compiled file, the Delta jars cached by `requirements/spark.txt`, and a 10-minute timeout.
5. **A first Spark test:** a write, a MERGE and a change-feed read on a real local Delta table.

## User stories

1. As the operator of v2's job cluster, I want one file of exact pins for everything our wheel needs beyond the runtime, so that every cluster installs the same versions and none replaces a library Spark uses.
2. As a developer, I want CI to fail when that file no longer matches its input or its constraints, so that it can't drift, as `UV_LOCKED` stops `uv.lock` drifting.
3. As a developer, I want `make spark` to build an env with Runtime 18's libraries and run the Spark tests in one command, so that Spark code is tested before it reaches Databricks.
4. As a developer, I want `make check` unchanged, so that no JVM starts there and its time stays where it is.
5. As a reviewer, I want a required `spark` check on every PR, so that a change to shared code that breaks Spark can't merge.
6. As a developer, I want the Delta jars cached by version, so that only a cold runner or a version bump downloads them.

## Failure scenarios

| Scenario | Expected |
|---|---|
| `databricks.txt` edited by hand, or `databricks.in` or `runtime-18.txt` changed without recompiling | The stale step fails the `spark` job |
| A Runtime 18 maintenance update changes a library | Nothing notices: `runtime-18.txt` is refetched by hand, and its header says when it was fetched |
| pyspark missing from the Spark env (a broken install) | Nothing is collected, and pytest's exit code 5 fails the job: never a green run of zero tests |
| The runner's default JDK (17) is picked | The session fixture asserts JVM 21 and fails at once |
| Cold Ivy cache (a new runner or a version bump) | The first session downloads about 85 MB of jars (31 s on the Mac) and still fits the timeout |
| Maven Central down | The job fails; rerun it. No mirror |
| A test leaves a table behind | Every table lives under the test's `tmp_path` |

## Implementation decisions

1. **`requirements/`** holds the three inputs (`runtime-18.txt`, `databricks.in`, `spark.txt`) and the one output (`databricks.txt`).
2. **The compile:** `uv pip compile requirements/databricks.in -c requirements/runtime-18.txt --python-version 3.12 --python-platform x86_64-manylinux_2_39 -o requirements/databricks.txt`. manylinux 2.39 is Runtime 18's glibc (Ubuntu 24.04). aarch64 and the other x86_64 targets compile to the same 31 pins today.
3. **`runtime-18.txt` is the whole table as published:** 265 libraries, fetched Nov 1, with its source URL and date in the header (question 1).
4. **pyspark 4.1.4 and delta-spark 4.4.1,** the pair measured on the Mac for [#162][t162]. Runtime 18.0's Spark is 4.1.0, and 4.1.4 is the latest 4.1 patch. delta-spark 4.4.1 accepts pyspark 4.0.1 to 4.2.0, and its jar (`delta-spark_4.1_2.13`) follows the installed pyspark. delta-spark 4.2.0 is out: it caps pyspark at 4.1.1. The `sql` extra brings pandas and pyarrow at the runtime's versions under the constraints. The `connect` extra conflicts with the runtime's grpcio and zstandard, and nothing needs it.
5. **`tests/conftest.py` skips `tests/spark` when pyspark isn't importable,** so `make check` and a bare `uv run pytest` never import it.
6. **The session fixture** (`tests/spark/conftest.py`, session scope) sets:
   - `local[2]`;
   - the Delta extension and catalog;
   - `snapshotPartitions=1` and `spark.sql.shuffle.partitions=1`;
   - the UI off;
   - an assertion that the JVM is 21.
7. **CI:**
   - `JAVA_HOME=$JAVA_HOME_21_X64` in the run step, because runner variables aren't in the workflow's `env` context;
   - the stale step is `make requirements` then `git diff --exit-code requirements/`;
   - `actions/cache` on `~/.ivy2.5.2`, keyed on `hashFiles('requirements/spark.txt')`.
8. **Required after merge.** Once the job has run green on `main`, `spark` joins `main`'s required checks. That's a branch-protection change, and I ask before making it.

## Testing decisions (test points for your OK)

- **`tests/spark/test_delta.py`:** one test.
  1. Write two rows to a Delta table with the change data feed on.
  2. MERGE in one update and one insert.
  3. Read the feed from version 1. It holds exactly an `update_preimage`, an `update_postimage` and an `insert`, with their rows.

  This tests the env, not our code. The red is `make spark` not existing yet.
- **The stale step,** shown once in the PR's Evidence: a hand-edited pin in `databricks.txt` fails it.
- **`make check` unchanged:** the same test count with `tests/spark` present.
- **The job's time,** cold and cached, from its first runs. The target is under 5 minutes.

## Questions

1. **Keep `runtime-18.txt` whole, or trim it?**
   - Whole: 265 lines of copied data, so the PR is about 470 lines, about 75 of them hand-written.
   - Trimmed: only the 45 or so packages the two resolutions touch, so the PR stays under 300.

   I recommend whole. A trimmed file silently stops constraining any package a later step adds.

## Out of scope

- The cluster side: installing the compiled file and our wheel on a job cluster, and the Unity Catalog Volume (P phase).
- The `model` job's move to Linux in the Spark env, with the categorization UDF.
- The Spark `stress-smoke` legs, Azurite and the ADLS-path tests.
- An offline wheelhouse. It comes only if the paid smoke check shows that install time matters.
- Running `make spark` from the Mac's hooks.

## Size

About 75 hand-written lines (Makefile, `ci.yml`, two conftests, the test, the two small inputs, `.gitignore`, README), plus 126 compiled lines and 267 copied ones.

[t141]: https://github.com/Hien-Trinh/Shopping/issues/141
[t162]: https://github.com/Hien-Trinh/Shopping/issues/162
