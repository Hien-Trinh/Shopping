"""Probe for step A6 (not merged): what a driver sees of an idle query and a hung batch, what
stop() does to a hung batch, and what a Python StreamingQueryListener gets."""

import json
import threading
import time

from pyspark.sql.streaming import StreamingQueryListener
from support import up
from test_ingest import commit, land

from ingestion import classify_udf, ingest


class Lines(StreamingQueryListener):
    def __init__(self):
        self.lines = []

    def onQueryStarted(self, event):
        self.lines.append(("started", str(event.id)))

    def onQueryProgress(self, event):
        self.lines.append(("progress", event.progress.json))

    def onQueryIdle(self, event):
        self.lines.append(("idle", event.timestamp))

    def onQueryTerminated(self, event):
        self.lines.append(("terminated", event.exception))


def start(spark, data):
    return ingest.start(
        spark, str(data), trigger={"processingTime": "2 seconds"}, classifier=classify_udf.FAKE
    )


def test_an_idle_query(spark, tmp_path):
    listener = Lines()
    spark.streams.addListener(listener)
    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    query = start(spark, tmp_path)
    seen = []
    for _ in range(13):
        done = query.awaitTermination(2)
        last = query.lastProgress
        seen.append((done, last and (last["batchId"], last["timestamp"], last["numInputRows"])))
    print("PROBE idle lastProgress:", seen)
    print("PROBE idle status:", query.status)
    print("PROBE recentProgress count:", len(query.recentProgress))
    query.stop()
    time.sleep(1)
    spark.streams.removeListener(listener)
    kinds = [k for k, _ in listener.lines]
    print("PROBE listener kinds:", kinds)
    first = next(v for k, v in listener.lines if k == "progress")
    parsed = json.loads(first)
    print("PROBE progress keys:", sorted(parsed))
    print("PROBE progress source:", json.dumps(parsed["sources"][0])[:600])
    print("PROBE progress durationMs:", parsed["durationMs"])
    print("PROBE terminated:", [v for k, v in listener.lines if k == "terminated"])


def test_a_hung_batch(spark, tmp_path, monkeypatch):
    release = threading.Event()
    real = ingest.apply_batch

    def hung(*args):
        release.wait(60)
        return real(*args)

    monkeypatch.setattr(ingest, "apply_batch", hung)
    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    query = start(spark, tmp_path)
    time.sleep(6)
    print("PROBE hung status:", query.status, "lastProgress:", query.lastProgress)
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
    t.join(20)
    print("PROBE stop on a hung batch:", stopped or "still blocked after 20 s", "active:", query.isActive)
    release.set()
    t.join(30)
    print("PROBE after release:", stopped, "active:", query.isActive)
    try:
        print("PROBE awaitTermination:", query.awaitTermination(10))
    except Exception as e:
        print("PROBE awaitTermination raised:", type(e).__name__, str(e)[:300])
