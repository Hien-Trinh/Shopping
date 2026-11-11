"""Probe for step A5 (not merged): does a trigger with no new input run a batch, and would a
`rate` source unioned to the change feed make one run?"""

import time

from pyspark.sql import functions as F
from support import up
from test_ingest import commit, land

from ingestion import classify_udf, ingest


def test_a_trigger_with_no_new_input_runs_no_batch(spark, tmp_path, monkeypatch):
    calls = []
    real = ingest.apply_batch

    def counted(*args):
        calls.append(time.monotonic())
        return real(*args)

    monkeypatch.setattr(ingest, "apply_batch", counted)
    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    query = ingest.start(
        spark, str(tmp_path), trigger={"processingTime": "2 seconds"}, classifier=classify_udf.FAKE
    )
    time.sleep(14)
    query.stop()
    print("PROBE idle: foreachBatch calls =", len(calls))
    assert len(calls) == 1


def _unioned(spark, log: str):
    feed = spark.readStream.format("delta").option("readChangeFeed", "true")
    feed = feed.option("startingVersion", 0).load(log).select("merchant_product_id")
    rate = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    tick = rate.select(F.lit(None).cast("string").alias("merchant_product_id"))
    return feed.unionByName(tick)


def test_a_rate_source_unioned_in_runs_a_batch_each_trigger(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    seen = []

    def batch(df, batch_id):
        rows = [r.merchant_product_id for r in df.collect()]
        seen.append((batch_id, sum(r is not None for r in rows), len(rows)))

    writer = _unioned(spark, f"{tmp_path}/landing_log").writeStream.foreachBatch(batch)
    writer = writer.option("checkpointLocation", f"{tmp_path}/cp")
    query = writer.trigger(processingTime="2 seconds").start()
    time.sleep(14)
    query.stop()
    print("PROBE rate, processingTime:", seen)
    assert len(seen) >= 4
    assert sum(changes for _, changes, _ in seen) == 1

    seen.clear()
    writer = _unioned(spark, f"{tmp_path}/landing_log").writeStream.foreachBatch(batch)
    writer = writer.option("checkpointLocation", f"{tmp_path}/cp")
    started = time.monotonic()
    writer.trigger(availableNow=True).start().awaitTermination(60)
    print("PROBE rate, availableNow:", seen, "s =", round(time.monotonic() - started, 1))
    assert sum(changes for _, changes, _ in seen) == 0  # the checkpoint held the one Change
