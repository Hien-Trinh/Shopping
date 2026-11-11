"""Probe for step A6, part 2: a batch hung inside a Spark job (an executor that never answers),
and how often an idle query reports."""

import threading
import time

from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from support import up
from test_ingest import commit, land

from ingestion import classify_udf, ingest


def start(spark, data):
    return ingest.start(
        spark, str(data), trigger={"processingTime": "2 seconds"}, classifier=classify_udf.FAKE
    )


def test_a_batch_hung_in_a_spark_job(spark, tmp_path, monkeypatch):
    def hung(data, classifier, slots, clock, batch, batch_id):
        @F.udf(LongType())
        def never(x):
            import time as t

            t.sleep(120)
            return x

        batch.sparkSession.range(1).select(never("id")).collect()

    monkeypatch.setattr(ingest, "apply_batch", hung)
    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    query = start(spark, tmp_path)
    time.sleep(8)
    print("PROBE hung-in-job status:", query.status, "lastProgress:", query.lastProgress)
    stopped = []

    def stop():
        started = time.monotonic()
        try:
            query.stop()
            stopped.append(("returned", round(time.monotonic() - started, 1)))
        except Exception as e:
            stopped.append((type(e).__name__, str(e)[:200], round(time.monotonic() - started, 1)))

    t = threading.Thread(target=stop, daemon=True)
    t.start()
    t.join(30)
    print("PROBE stop on a batch hung in a job:", stopped or "still blocked after 30 s")
    try:
        print("PROBE awaitTermination:", query.awaitTermination(10), "exception:", query.exception())
    except Exception as e:
        print("PROBE awaitTermination raised:", type(e).__name__, str(e)[:300])


def test_how_often_an_idle_query_reports(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    query = start(spark, tmp_path)
    seen = []
    for _ in range(22):
        query.awaitTermination(2)
        last = query.lastProgress
        stamp = last and (last["batchId"], last["timestamp"][11:19], last["numInputRows"])
        if stamp and stamp not in seen:
            seen.append(stamp)
    print("PROBE idle reports over 44 s:", seen, "status:", query.status)
    query.stop()
