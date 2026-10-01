# Pure modules (comma-separated) must stay at 100% branch coverage (plan-v1.md, Testing rules).
# A stale uv.lock fails the gate instead of being re-resolved silently (here and in CI).
export UV_LOCKED := 1

PURE := src/catalog/keys.py,src/catalog/envelope.py,src/catalog/plan.py,src/catalog/replay.py,src/catalog/status.py,src/catalog/collapse.py

.PHONY: check lint test hooks mutate

check: lint test

lint:
	uv run ruff check .
	uv run ruff format --check .

test:
	uv run pytest --cov --cov-report=term-missing
	uv run coverage report --include='$(PURE)' --fail-under=100

hooks:
	git config core.hooksPath .githooks

# Mutation testing of the pure modules (~1 min). Not part of `check`; CI reports it without blocking.
mutate:
	uv run mutmut run
	uv run mutmut results
