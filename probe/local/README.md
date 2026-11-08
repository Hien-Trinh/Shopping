# A4's local Spark probes (never merged)

From a checkout of this branch, after `make spark` has built `.venv-spark`, with a plain-file,
read-only copy of the student's encoder and index in `$A4_MODELS_RO`:

    A4_MODELS_RO=... JAVA_HOME=$(/usr/libexec/java_home -v 21) \
      PYTHONPATH=probe/local:tests/spark:tests .venv-spark/bin/pytest -s -q probe/local

They print the lines step-a4.md's facts 1 to 5 quote (P1 to P5, Q1 and Q2).
