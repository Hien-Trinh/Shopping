# Shopping — commerce ingestion pipeline

A learning project: merchants push product Listings through one API, and the pipeline categorizes them and keeps a correct current catalog under out-of-order, duplicate and crash-replayed changes. It runs locally on one Mac (Python 3.14, Delta Lake via delta-rs, DuckDB for queries).

- [Design doc](docs/design-commerce-ingestion-pipeline.md) · [v1 build plan](docs/plan-v1.md) · [Glossary](CONTEXT.md) · [ADRs](docs/adr/)

## Develop

```bash
uv sync
make hooks   # once per clone: runs `make check` before every push, and refreshes graphify-out/ after a pull
make check   # ruff + tests + coverage gates, same as CI
```

## Run

The workers categorize with an embedding shortlist and TypeSafe's paid Jev API (about $62 per 1M Listings).

```bash
uv run python -m catalog.classify --download   # once: the embedding model and the shortlist vectors (about 2 min), into models/
uv run python -m catalog.merchants create --currency USD   # once: the API won't start without a merchant; prints its key once
export TYPESAFE_API_KEY=...                     # your TypeSafe key; never logged
uv run python -m catalog.supervisor             # runs the Procfile
```

Changes reach `main` only through a PR with a green `check` job.
