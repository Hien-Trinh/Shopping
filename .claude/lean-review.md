# Lean-review profile: Shopping

Read by the user-scoped `/lean-review` skill and its `reviewer-*` agents.

## Spec sources
Read from `main` (`git show main:<path>`):
- the PR's row in docs/plan-v1.md's "PR steps" table, and its phase section (tests, "Rules carried")
- for a v2 step (G1–P7): its row in docs/plan-v2.md's "PR steps" table, its phase under "Phases", and that plan's "Rules that hold across steps" and "Testing rules"
- for a step with a mini PRD: docs/specs/step-<id>.md, whose Out of scope names the step that owns each gap. A lane reports a gap only when the spec names no owner
- the sections of docs/design-commerce-ingestion-pipeline.md the diff touches
- docs/adr/
- GLOSSARY.md (Listing, Change, Tombstone, Submission, Outcome)

Unsourced-prose check (spec question 4) also covers report prose: docs/stress.md and specs' results sections, against the run's own files. It also covers a spec's claims about how Spark, Delta, Databricks or GitHub Actions behaves, its Failure scenarios rows included: each says `probed` or cites a source, and the probe ran the decision as written, so a spec PR's review intent names the folder holding its probes.

## Invariants
From docs/design-commerce-ingestion-pipeline.md and docs/adr/:
- exactly one writer per partition
- landing order is (commit version, seq)
- at-least-once delivery, with offsets saved only after the MERGE and events
- every process-crash point is safe (`kill -9` then restart), in a stream's first batch and a later one (docs/plan-v2.md, Testing rules)

`storage` is true when the diff touches storage, state, events or worker code.

## Scale
From docs/plan-v1.md: about 1M Listings in 64 partitions; about 50 changes/s with bulk batches of 10k items; worker batches up to 1,000 changes; status lookups over about 1M event lines per hour; p99 freshness under 5 minutes. Per-row Python where Arrow would do is waste at this scale.

A delta-rs append adds a file per commit, and commit cost grows with live files: the Submission log went from 10 to 90 ms per commit over 3,000 commits, and back to 14 ms after one compaction (#178). So every table the API appends to needs a named compactor, and every lane checks a new append path for one, since the performance lane runs only on larger diffs. The Submission log's is A10's to settle.

A Spark write makes one file per shuffle partition unless the table sets `delta.autoOptimize.optimizeWrite`: 174 files a batch per table at Spark's default 200 partitions (#181), where tests run at 4. So every lane counts files per batch on a new Spark write path.

## Data
Delta schema nullability/widening/column order; string vs string_view; content_hash and key derivation must match between writer and reader; export, snapshot and backfill must read today's data.

## Observability
docs/design-commerce-ingestion-pipeline.md, Observability: events carry submission_id, change_index, the Listing key, partition, the store version where relevant, and a timestamp. Status, freshness p99, lag per partition and the Uncategorized rate must be computable; crash replays must not double-count indistinguishably.

## Trust boundaries
Merchant-controlled values: f-strings into SQL or Delta predicates, merchant/product/submission IDs reaching the filesystem, cross-Merchant access (IDOR), constant-time key comparison, unbounded input reaching the classifier.

Public CI logs, such as the free-edition workflow's: text from a remote reply goes through `scrub` (scripts/free-edition.py) before any cut, and no duration, email, workspace host or run URL prints. Every lane checks a new print path, since the security lane runs only on larger diffs.

## Coding rules
There is no CODING_STANDARDS.md. The rules:
- docs/plan-v1.md "Code layout": functional core, imperative shell; decisions in pure functions; thin I/O modules; the module list
- docs/plan-v1.md "Testing rules": coverage gates, the 100% PURE list, property tests, no mocks of delta-rs, inject clock/classifier/paths, no `sleep` in unit tests
- docs/plan-v1.md Phase 3 "Rules carried" (e.g. every standalone entry point exits through `entry.exit_with`)
- conventions the ADRs set
Tests that exercise only FakeClassifier or monkeypatches, never the real contract, are weak.

## Gates
1. `make check`
2. If the diff touches `src/` or `tests/spark/`: `make spark`
3. If the diff touches a pure module (`source_paths` under `[tool.mutmut]` in pyproject.toml): `make mutate`; surviving mutants in changed lines not on plan-v1.md's equivalent list must be fixed or explained.

## Running scratch scripts
From the repo root with `uv run`, `PYTHONPATH=tests` for test helpers. Run a script's body through `entry.exit_with(main)`, which flushes and exits hard: a process that scanned a Delta table can hang at interpreter exit (spikes/NOTES.md, "Exit hang").

A Spark probe is a pytest file in your scratch dir. It gets the `spark` fixture, and `from test_ingest import land, commit, run` writes Landing logs with the API's delta-rs and runs the query: `JAVA_HOME=$(/usr/libexec/java_home -v 21) PYTHONPATH=tests/spark:tests .venv-spark/bin/pytest -p conftest <file>`. Lanes run side by side, and this skips the sync in `make spark`, which briefly removes the package from the env.

Once the lanes are done, the session running the review settles a claim that a mutation survives with `scripts/red.py FILE OLD NEW TEST...`. It swaps the file in place, so a lane marks such a claim unreproduced.

## Defer target, round record, PR body
- Defer findings to a named later step in docs/plan-v1.md.
- Record "Review: round N done" as a PR comment.
- PR body follows the `pr` skill (Summary visual, Evidence, Merge Danger); step-7 regression tests update Evidence.
- `/retro` exists here; its output goes in its own docs-or-tooling PR, never the step PR.
