# Pure modules (comma-separated) must stay at 100% branch coverage (plan-v1.md, Testing rules).
# A stale uv.lock fails the gate instead of being re-resolved silently (here and in CI).
export UV_LOCKED := 1

PURE := src/ingestion/keys.py,src/ingestion/envelope.py,src/ingestion/plan.py,src/ingestion/replay.py,src/ingestion/status.py,src/ingestion/collapse.py

.PHONY: check lint test hooks mutate repeat bench requirements spark

check: lint test

lint:
	uv run ruff check .
	uv run ruff format --check .

test:
	uv run pytest -n auto --cov --cov-report=term-missing  # one worker per core (pytest-xdist)
	uv run coverage report --include='$(PURE)' --fail-under=100

hooks:
	git config core.hooksPath .githooks

# The job cluster's pins: fastembed under Runtime 18's libraries, for Linux x86_64 (step-g5.md).
# --upgrade and --exclude-newer make the pins depend on the inputs and that date alone, so CI's
# stale step catches any hand edit. A fastembed newer than the date needs the date moved.
requirements:
	uv pip compile -q requirements/databricks.in -c requirements/runtime-18.txt --python-version 3.12 --python-platform x86_64-manylinux_2_39 --upgrade --exclude-newer 2026-11-01T00:00:00Z -o requirements/databricks.txt

# The Spark tests, in Runtime 18's libraries with the package --no-deps, as on the cluster (JDK 21).
# --exact also removes what the files no longer list, so a reused env matches a fresh one.
spark:
	uv venv -q --allow-existing --python 3.12 .venv-spark
	uv pip install -q --exact -p .venv-spark -r requirements/databricks.txt -r requirements/spark.txt -c requirements/runtime-18.txt
	uv pip install -q -p .venv-spark --no-deps -e .
	.venv-spark/bin/pytest tests/spark

# Mutation testing of the pure modules (~1 min). Not part of `check`; CI reports it without blocking.
mutate:
	uv run mutmut run
	uv run mutmut results

# Reproduce a flaky test: make repeat TEST=<node id or -k expr> N=<runs per loop> J=<parallel loops>.
# Stops at the first failure and prints that run's log and pytest temp dir.
N ?= 10
J ?= 1
repeat:
	scripts/repeat.sh $(N) $(J) $(TEST)

# Time the student's encoder per CHUNK and ONNX threads, waiting for a quiet machine; needs models/.
# make bench ARGS="--chunk 256,16 --threads 0,2 --procs 4,1" (see scripts/bench-encode.py)
bench:
	PYTHONPATH=src uv run python scripts/bench-encode.py $(ARGS)
