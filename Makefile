# Pure modules (comma-separated) must stay at 100% branch coverage (plan-v1.md, Testing rules).
PURE := src/catalog/keys.py,src/catalog/envelope.py,src/catalog/plan.py,src/catalog/replay.py,src/catalog/status.py,src/catalog/collapse.py

.PHONY: check lint test hooks

check: lint test

lint:
	uv run ruff check .
	uv run ruff format --check .

test:
	uv run pytest --cov --cov-report=term-missing
	uv run coverage report --include='$(PURE)' --fail-under=100

hooks:
	git config core.hooksPath .githooks
