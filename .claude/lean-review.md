# Lean-review profile: Shopping

Read by the user-scoped `/lean-review` skill and its `reviewer-*` agents.

## Spec sources
Read from `main` (`git show main:<path>`):
- the PR's row in docs/plan-v1.md's "PR steps" table, and its phase section (tests, "Rules carried")
- the sections of docs/design-commerce-ingestion-pipeline.md the diff touches
- docs/adr/
- GLOSSARY.md (Listing, Change, Tombstone, Submission, Outcome)

Unsourced-prose check (spec question 4) also covers report prose: docs/stress.md and specs' results sections, against the run's own files.

## Invariants
From docs/design-commerce-ingestion-pipeline.md and docs/adr/:
- exactly one writer per partition
- landing order is (commit version, seq)
- at-least-once delivery, with offsets saved only after the MERGE and events
- every process-crash point is safe (`kill -9` then restart)

## Scale
From docs/plan-v1.md: about 1M Listings in 64 partitions; about 50 changes/s with bulk batches of 10k items; worker batches up to 1,000 changes; status lookups over about 1M event lines per hour; p99 freshness under 5 minutes. Per-row Python where Arrow would do is waste at this scale.

## Data
Delta schema nullability/widening/column order; string vs string_view; content_hash and key derivation must match between writer and reader; export, snapshot and backfill must read today's data.

## Observability
docs/design-commerce-ingestion-pipeline.md, Observability: events carry submission_id, change_index, the Listing key, partition, the store version where relevant, and a timestamp. Status, freshness p99, lag per partition and the Uncategorized rate must be computable; crash replays must not double-count indistinguishably.

## Trust boundaries
Merchant-controlled values: f-strings into SQL or Delta predicates, merchant/product/submission IDs reaching the filesystem, cross-Merchant access (IDOR), constant-time key comparison, unbounded input reaching the classifier.

## Coding rules
There is no CODING_STANDARDS.md. The rules:
- docs/plan-v1.md "Code layout": functional core, imperative shell; decisions in pure functions; thin I/O modules; the module list
- docs/plan-v1.md "Testing rules": coverage gates, the 100% PURE list, property tests, no mocks of delta-rs, inject clock/classifier/paths, no `sleep` in unit tests
- docs/plan-v1.md Phase 3 "Rules carried" (e.g. every standalone entry point exits through `entry.exit_with`)
- conventions the ADRs set
Tests that exercise only FakeClassifier or monkeypatches, never the real contract, are weak.

## Gates
1. `make check`
2. If the diff touches a pure module (`source_paths` under `[tool.mutmut]` in pyproject.toml): `make mutate`; surviving mutants in changed lines not on plan-v1.md's equivalent list must be fixed or explained.

## Running scratch scripts
From the repo root with `uv run`, `PYTHONPATH=tests` for test helpers.

## Defer target, round record, PR body
- Defer findings to a named later step in docs/plan-v1.md.
- Record "Review: round N done" as a PR comment.
- PR body follows the `pr` skill (Summary visual, Evidence, Merge Danger); step-7 regression tests update Evidence.
- `/retro` exists here; its output goes in its own docs-or-tooling PR, never the step PR.
