# Step A3: the Free Edition workflow and its first run (mini PRD)

Status: approved Nov 6: the decisions, the test points and all three questions. Plan row: [plan-v2.md, PR steps, A3](../plan-v2.md). Decisions from [Free Edition][t129] and [CI][t162]. Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

A2's query has only run on local Spark with open-source Delta. Runtime 18's Delta fork, Spark Connect and serverless's limits could each break it, and a surprise found at the paid smoke run costs money. Free Edition can show it for free, but nothing reaches it yet.

Seven facts shape this step. "Probed" means on the real workspace on Nov 6, on environment 5, from the unmerged branch [`probe/free-edition`][probe] ([its last run][run]), unless it says local.

- **The query runs as it is.** Probed: `ingest.start` with `availableNow`, on a volume path, gave the store oracle's result and every Outcome, and a restart changed nothing. `ingest.py` needs no change. Of the items step-a2.md left for this run, `delta.exceptions` imports, `execute()` returns `num_affected_rows` and `deltalake` loads in a Python worker. `persist` works, as the docs now say from [environment 5][cache], though the Oct 12 research said it raised.
- **`os._exit` fails a run.** Probed: after every check passed, `entry.exit_with` ended it `RUN_EXECUTION_ERROR`, "The Python process exited unexpectedly". The same run returning normally ended `SUCCESS`.
- **A failed task runs twice.** Probed: raising, `sys.exit(1)` and `os._exit(0)` each left two execution marks, although the [API page][jobs] says submitted runs don't retry. The second pass meets the first's files: `DELTA_PATH_EXISTS`, and a second copy of any backlog.
- **`--no-deps` doesn't exist on Databricks.** A `dependencies` entry is a pip requirements line ([Jobs API][jobs]), and [that format][pip] has no such option. No page documents another way, on serverless or for a classic cluster's `whl` library. A wheel that lists no dependencies ran on environment 5's own deltalake 1.1.4, numpy 2.1.3, pandas 2.2.3, pyarrow 21.0.0 and pydantic 2.10.6 (probed), the pins in `requirements/runtime-18.txt`.
- **delta-rs can't commit on a volume.** Probed: `landing.append` fails with "Unable to rename file", "Operation not permitted". So Spark writes the Landing log here, and risk B1 stays with the local tests and P3.
- **A script can't run from a volume.** Probed: `python_file` at a `/Volumes` path ended `RESOURCE_NOT_FOUND`, "Cannot read the python file". A wheel task runs.
- **Errors name the run's identity.** Probed: `state_message` quotes the account's email, and these logs are public.

Your token reached the SCIM, Unity Catalog, Files and Jobs APIs, which settles risk B8.

## Solution

```text
gh workflow run free-edition.yml [--ref <branch>]
  build a wheel that lists no dependencies
  create the volume workspace.default.shopping if it's missing
  upload the wheel to wheels/<sha>/
  make one one-time serverless run, environment 5:
    wheel task ingestion-smoke --data /Volumes/workspace/default/shopping/smoke
      wipe --data
      create the Landing log and Outcomes with Spark, land a backlog
      ingest.start(availableNow), then the checks: one PASS or FAIL line each
      raise if any failed
  print the PASS and FAIL lines and the run's code; exit 1 unless SUCCESS
```

## User stories

1. As the developer, I want one command that runs our wheel on the real runtime, so that a Phase A surprise shows before a paid run.
2. As the developer, I want a pass or fail per check, so that I know what broke without opening the workspace.
3. As the repo's owner, I want only those lines in the public logs, so that no timing, token or identity leaks.

## Failure scenarios

Seeded from lean-review's finder angles.

| Scenario | Expected |
|---|---|
| A check fails | Its FAIL line prints, the later checks still run, and `main` raises. The platform runs the task again (probed), and the wipe gives that pass a clean start. The workflow prints the lines and exits 1 |
| A check fails on the first pass only | The run ends `SUCCESS`. Accepted: this workflow checks the runtime, and `make spark` is the gate for flakes |
| The Python process dies | `error` holds "Fatal error" (probed). The code, the message and the error print, with no check lines, and the workflow exits 1 |
| The token expired or was revoked, or the account went inactive | The first API call fails, and its status and error code print. The fix is a new token and `gh secret set FREE_EDITION_TOKEN`. An unused token is [revoked after 90 days][pat], and an account can go [inactive after 60 days without a login][aup] |
| The quota ran out, which [stops compute for the day][limits] | The run ends with an error code, which prints. Run it again tomorrow |
| An install fails | `LIBRARY_INSTALLATION_ERROR` prints, and the workflow exits 1 |
| The run hangs | `timeout_seconds` = 1800 ends it, and the workflow reports it like any other end. The job's `timeout-minutes` = 40 covers an API that stops answering |
| One poll fails with a network error or a 5xx | The script polls again, since stopping would leave the run writing to `--data` for the next dispatch to wipe. A 4xx stops it |
| `get-output` fails, with a status or a dropped connection | Its error prints, and so do the run's code and message. The workflow exits 1, even on `SUCCESS` |
| A run ends `SUCCESS` with no check lines | The workflow exits 1, since nothing shows the checks ran |
| The wipe fails | `main` raises before any check, rather than starting on leftover tables |
| Two dispatches at once | A `concurrency` group queues the second, since both use one `--data` |
| The workflow is cancelled mid-run | The Databricks run carries on until it ends. A dispatch started before then could wipe under it and report a false FAIL. Accepted: run it again |
| Same commit run again | The upload overwrites the same bytes (`overwrite` is the [default][files]) |
| New code, same wheel version | The path holds the commit, since the [docs warn][deps] a job may not pick up a changed package otherwise |
| A reformatted `pyproject.toml` defeats the wheel's `sed` | The next step finds `Requires-Dist` and fails the job before any upload |
| The volume exists | The create returns 400 `RESOURCE_ALREADY_EXISTS` (probed), which is fine. Any other failure stops the workflow |
| An error holds the email | The script replaces it with `<email>`, in check lines too, and cuts each line at 1500 characters only after that, so a cut can't strand part of an address |
| Spark's own output in `logs` | Only lines starting `PASS ` or `FAIL ` print, so nothing else from the driver reaches the public log |
| Serverless picks another architecture | The wheel is `py3-none-any`. The run probed was on aarch64, and [it can change][deps] |

## Implementation decisions

1. **`src/ingestion/free_edition.py`** (new; question 2) holds the checks. `main(argv)` takes `--data`, wipes it with `shutil.rmtree` (probed on a Delta table on a volume), runs them and raises `RuntimeError` if any failed. It returns normally otherwise, so it never goes through `entry.exit_with`. `[project.scripts]` names it `ingestion-smoke`. It imports pyspark, so `pyproject.toml`'s coverage `omit` gains it, as it has `ingest.py`.
2. **Spark creates the tables on Free Edition.** The Landing log with `landing.SCHEMA` and its properties, Outcomes with `tables.OUTCOMES`, `hour` and `OUTCOMES_PROPERTIES`. Two small splits keep one definition of each: `landing.rows(entries)` out of `append`, and `landing.PROPERTIES` out of `ensure`. Each commit is `spark.createDataFrame(landing.rows(entries)).write`, probed. A table made this way is reader 3 / writer 7 with deletion vectors (probed: this workspace's default), not delta-rs's reader 1 / writer 4.
3. **The checks,** each one PASS or FAIL line, never a time:
   - The backlog lands as `replay.expected_store` says. Two commits, covering `written`, `stale`, `already_applied`, `conflict`, `failed` and a Tombstone.
   - One Outcome per Change, exactly.
   - A restart adds no Outcome and no store version.
   - A later commit reads its stored row, in a second batch.
   - The store's layout: clustering, deletion vectors and the change feed, plus a deletion vector from that second batch's MERGE.

   Each later step of Phase A adds its own.
4. **The default cap and `availableNow`.** `start` already takes the trigger, and serverless [refuses `processingTime`][limits]. The query itself is unchanged.
5. **`scripts/free-edition.py`** (new), standard library only, so it needs no env. It creates the volume, uploads the wheel, submits the run (`POST /api/2.2/jobs/runs/submit`), polls every 15 s until `status.state` is `TERMINATED`, and reads `runs/get-output` by the task's run id, since the parent's id returns 400 (probed). It prints the `PASS ` and `FAIL ` lines from `logs`, which holds stdout on serverless (probed), then `termination_details.code` and, on a failure, the message and `error`. It prints no `*_time`, `*_duration`, `run_page_url` or `creator_user_name`. A failed call prints its method, path and status. Its exit code is 0 only on `SUCCESS` with at least one check line.
6. **Environment 5,** pinned in the script. Its libraries are Runtime 18's (probed above), where environment 6's [deltalake is 1.5.1][env6], and `persist` needs 5 or later.
7. **`.github/workflows/free-edition.yml`:** `workflow_dispatch` only, `contents: read`, one `concurrency` group, `timeout-minutes: 40`. It strips `dependencies` (question 1), runs `uv build --wheel`, fails if the wheel's `METADATA` has `Requires-Dist`, and runs the script with the secrets in its env.
8. **Secrets:** `FREE_EDITION_HOST` and `FREE_EDITION_TOKEN`, already set. GitHub masks the URL in logs.
9. **A stub workflow lands with this spec.** GitHub [dispatches a workflow only once its file is on the default branch][dispatch], and then from any branch, as `linux.yml` does. So the spec's PR adds a stub with one `echo` step, and the code PR's first run happens on its own branch, before you review it.
10. **Each later Phase A code PR that changes what runs on Spark runs this workflow on its branch** and cites the result in its Evidence. One line in plan-v2's Testing rules. It never runs on PRs by itself ([CI][t162]).
11. **plan-v2 changes, in the spec's PR:** B3 and the Phase A note say "our wheel with no dependencies" for `--no-deps`; B8 gets ✅; the A3 row names this spec, the wheel task and environment 5; C2 gains one clause, that the job's wheel task enters at `ingest.main` itself, since `os._exit` fails a run.

## Testing decisions (test points for your OK)

- **`tests/spark/test_free_edition.py`** (new), in `make spark`, on local Spark:
  1. On the real query, `main` prints only PASS lines, one per check, and returns. Red: no module.
  2. With `ingest.merge` replaced by a no-op, the store check prints FAIL, the later checks still print, and `main` raises. Red: a run that stops at the first failure, or one that doesn't raise. Each later check also fails on what it checks, called directly on tampered data. A wipe that fails stops the run.
  3. `main` twice on one `--data` passes both times, as a second pass must. Red: no wipe.
- **`tests/integration/test_free_edition_script.py`** (new), in `make check`: the script as a subprocess, as `test_red.py` runs `red.py`, against a stub HTTP server on a free port:
  4. On `SUCCESS` it exits 0 and prints the check lines and the code. It prints nothing else, although the stub's replies hold `start_time`, `run_duration`, `run_page_url`, `creator_user_name` and non-check log lines. The stub also asserts the requests: the volume, the upload under `wheels/<sha>/`, a submit with environment 5, the wheel task and `--data`, and get-output by the task's id.
  5. On a failed run it exits 1 and prints the code, the message and `error`, with the email replaced, in a check line too, and an email across the line's cut doesn't strand. It polls on through a running state, a dropped connection and a 5xx. `SUCCESS` with no check lines exits 1, and a failed `get-output` still prints the code.
  6. A volume that exists carries on. Any other failed call stops with its status and exits 1, submitting nothing.
- **The first run** is on the code PR's branch, and its lines go in the PR's Evidence. Only it shows the wheel step and the whole workflow.

## Questions

1. **How does our wheel leave out its dependencies on Databricks?** [Packaging][t141] chose `--no-deps`, which turns out not to exist there, so this changes how that decision is carried out.
   - Strip them when building: `sed` deletes the `dependencies` block from the checkout's `pyproject.toml` in the workflow, before `uv build`. One line plus its check, probed end to end. C3's workflow does the same for classic compute, so C2's `whl` library needs no flag. Local envs and the API's image still use the real file.
   - Leave them in and let pip resolve them. No extra code, but PyPI is reachable (probed), so it could install pyarrow 25 and deltalake 1.6.6 over the runtime's, which [packaging][t141] ruled out, and B3 would go untested.
   - A second `pyproject.toml` for Databricks. Two files to keep in step, for what one `sed` does.

   I recommend the first.

   **Answer (Nov 6): strip them when building.**

2. **Where does the checks' code live?**
   - In the wheel, as a wheel task. Probed. A wheel task is what the paid job can run too, and the code can't drift from the wheel it checks. About 110 inert lines ship in the API's image, next to `chaos.py` and `load.py`.
   - In `tests/spark/`, uploaded to `/Workspace/Shared/` through the Workspace API and run as a script. Keeps it out of the wheel, but takes one more API call and a path not probed, since a volume path failed.

   I recommend the first.

   **Answer (Nov 6): in the wheel, as a wheel task.**

3. **GitHub shows each step's elapsed time, and no setting hides it** ([docs][steptime]). The plan's "never a duration" can only bind what we print, and the [acceptable use policy][aup] forbids disclosing benchmark results.
   - Accept it. We print no duration, and the step's time is mostly queueing, the install and 15 s polls, so it measures no workload. That is my reading of the policy, and the risk is yours.
   - Submit, then read the result in a second dispatch. No elapsed time links the two, but every run takes two commands.

   I recommend accepting it.

   **Answer (Nov 6): accept it.**

## Found for later steps

- **C2:** the job's wheel task enters at `ingest.main`, not through `entry.exit_with`. The stripped wheel avoids the question of whether a classic `whl` library resolves dependencies, which no page answers.
- **A4:** PyPI is reachable from the driver (probed: `pip download fastembed==0.9.0`). Serverless ran on aarch64, while `requirements/databricks.txt` is compiled for x86_64, so A4's Free Edition run has to check fastembed's pins there. A Python UDF is [capped at 1 GB][udf] on serverless, which the student's load may reach.
- **Spark Connect's `forPath` is lazy.** Probed: no error on a missing path, where local Spark raises at once (probed locally). So on serverless `start`'s guard against lost Outcomes fires only when an action reads them. Classic compute, where it matters, is eager like local Spark, though only a paid run shows that.

## Out of scope

- Risk B1, a delta-rs Landing log read on Databricks: local tests and P3.
- Checks for A4 and later: each step adds its own.
- Any timing from Free Edition, and any required status: it never runs on PRs.
- Cancelling the Databricks run when the workflow is cancelled, which is rare and costs a rerun.
- Deleting old wheels (325 KB each, for at most a few dozen commits in Phase A) and the probe's few files on the volume.
- `probe/free-edition` stays until this step merges, so the spec's links hold.

## Size

About 230 changed lines were estimated. It came to about 400: `free_edition.py` about 190, the script about 150, and the workflow, `landing.py`, `pyproject.toml`, the README and plan-v2 about 50. About 290 of tests. Two PRs would have kept it under the cap: the checks on local Spark first, then the workflow and its script.

[t129]: https://github.com/Hien-Trinh/Shopping/issues/129
[t141]: https://github.com/Hien-Trinh/Shopping/issues/141
[t162]: https://github.com/Hien-Trinh/Shopping/issues/162
[probe]: https://github.com/Hien-Trinh/Shopping/tree/probe/free-edition
[run]: https://github.com/Hien-Trinh/Shopping/actions/runs/42271134443
[jobs]: https://docs.databricks.com/api/workspace/jobs/submit
[pip]: https://pip.pypa.io/en/stable/reference/requirements-file-format/
[cache]: https://docs.databricks.com/aws/en/compute/serverless/dataframe-cache
[deps]: https://docs.databricks.com/aws/en/compute/serverless/dependencies
[env6]: https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/six
[files]: https://docs.databricks.com/api/workspace/files/upload
[limits]: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
[pat]: https://docs.databricks.com/aws/en/dev-tools/auth/pat
[aup]: https://www.databricks.com/legal/acceptable-use-policy-fe
[udf]: https://docs.databricks.com/aws/en/udf/python
[dispatch]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows
[steptime]: https://docs.github.com/en/actions/monitoring-and-troubleshooting-workflows/monitoring-workflows/using-workflow-run-logs
