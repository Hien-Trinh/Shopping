from importlib.util import find_spec

# tests/spark runs in the Spark env (make spark); the uv env has no pyspark (docs/specs/step-g5.md).
collect_ignore = [] if find_spec("pyspark") else ["spark"]
