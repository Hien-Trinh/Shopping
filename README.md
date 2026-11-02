# Shopping — commerce ingestion pipeline

A learning project: merchants push product Listings through one API, and the pipeline categorizes them and keeps a correct current catalog under out-of-order, duplicate and crash-replayed changes. It runs locally on one Mac (Python 3.12, Delta Lake via delta-rs, DuckDB for queries).

- [Design doc](docs/design-commerce-ingestion-pipeline.md) · [v1 build plan](docs/plan-v1.md) · [Runbook](docs/runbook.md) · [Glossary](GLOSSARY.md) · [ADRs](docs/adr/)

## Develop

```bash
uv sync
make hooks   # once per clone: runs ruff before every push, and refreshes graphify-out/ after a pull
make check   # ruff + tests + coverage gates, same as CI
make spark   # the Spark tests, in Runtime 18's libraries: needs JDK 21, and the first run fetches ~85 MB of jars
make repeat TEST=tests/x.py::test_y N=25 J=4   # reproduce a flake: stops at the first failure, keeps its log and temp dir
gh workflow run linux.yml --ref <branch> -f test=tests/x.py::test_y   # the same on Linux (3.12), for what the Mac can't show
```

## Run

The workers categorize with the fine-tuned student (step 6m): kNN over a bge-small fine-tuned on the training rows, locally, a few ms a Listing, no key. `--classifier jev` (an embedding shortlist and TypeSafe's paid Jev API, about $62 per 1M Listings) stays available and needs `TYPESAFE_API_KEY` exported and `ingestion.classifiers --download`.

```bash
uv run python -m ingestion.student --download    # once: the student's encoder and its index (about 10 min the first time), into models/
uv run python -m ingestion.merchants create --currency USD   # once: the API won't start without a merchant; prints its key once
uv run python -m ingestion.supervisor             # runs the Procfile
```

Stop, rescale, recover, reset and read metrics: see the [runbook](docs/runbook.md). It was written for v1, before the package was renamed: read its `catalog` as `ingestion`, or its `pgrep` and `pkill` lines match nothing.

## Data

- `eval/labels-shopify.jsonl` and `train/shopify.jsonl.gz` are built from [Shopify/product-catalogue](https://huggingface.co/datasets/Shopify/product-catalogue) (Apache-2.0, © Shopify) by `python -m ingestion.benchmark`: text and Category only, English rows, Categories mapped to our taxonomy.
- Amazon Listings in `eval/` and `train/` come from [Amazon Reviews '23](https://amazon-reviews-2023.github.io/) (McAuley Lab).

Changes reach `main` only through a PR with a green `check` job.
