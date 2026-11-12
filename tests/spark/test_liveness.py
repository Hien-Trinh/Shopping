"""The watchdog and the job's log lines, through `ingest.main` on real Spark
(docs/specs/step-a6.md)."""

import json
import threading
import time

import pytest
from pyspark.sql.streaming import StreamingQueryManager
from support import up
from test_ingest import commit, land, outcomes, store_rows

from ingestion import ingest, liveness


@pytest.fixture(autouse=True)
def listeners(spark, monkeypatch):
    """main adds a listener to the one shared session: take each off again."""
    added, add = [], StreamingQueryManager.addListener
    monkeypatch.setattr(
        StreamingQueryManager, "addListener", lambda self, it: (added.append(it), add(self, it))[1]
    )
    yield
    for listener in added:
        spark.streams.removeListener(listener)


def main(data, *more: str) -> None:
    ingest.main(["--data", str(data), "--classifier", "fake", *more])


def wait_until(done, seconds: float = 30.0) -> None:
    end = time.monotonic() + seconds
    while not done():
        assert time.monotonic() < end, "waited too long"
        time.sleep(0.2)


def test_a_hung_batch_fails_the_run_and_a_restart_lands_it(spark, tmp_path, monkeypatch):
    hung, plain = tmp_path / "hung", tmp_path / "plain"
    for data in (hung, plain):
        land(data, ["init"], commit(0, [up("a", 1), up("b", 1)]))
    release = threading.Event()

    def hanging(*args):
        release.wait()
        raise RuntimeError("released after the stall")  # never the real batch, in a stopped query

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ingest, "apply_batch", hanging)
        patch.setattr(ingest, "TRIGGER", {"processingTime": "2 seconds"})
        started = time.monotonic()
        with pytest.raises(liveness.Stalled):
            main(hung, "--watchdog", "3")
        assert time.monotonic() - started < 15
        assert store_rows(spark, hung) == [] and outcomes(spark, hung) == []
        release.set()
        wait_until(lambda: not spark.streams.active)  # the watchdog's stop() returns now
    monkeypatch.setattr(ingest, "TRIGGER", {"availableNow": True})
    main(hung)  # the restart, from the checkpoint
    main(plain)
    assert len(store_rows(spark, hung)) == 2
    assert store_rows(spark, hung) == store_rows(spark, plain)
    assert outcomes(spark, hung) == outcomes(spark, plain)


def test_a_run_prints_each_batchs_progress_its_batch_line_and_the_querys_end(
    spark, tmp_path, monkeypatch, capsys
):
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1)]))
    monkeypatch.setattr(ingest, "TRIGGER", {"availableNow": True})
    main(tmp_path)
    lines: list[dict] = []

    def ended() -> bool:  # the listener's events come after the query ends
        printed = capsys.readouterr().out.splitlines()
        lines.extend(json.loads(line) for line in printed if line.startswith("{"))
        return any(line.get("type") == "terminated" for line in lines)

    wait_until(ended)
    progress = [line for line in lines if line.get("numInputRows")]
    assert len(progress) == 1  # one batch held the two Changes
    # 4 for the two Changes: the batch reads its input twice (the keys, the plan), and Spark
    # seems to count each read. A size to read by eye, not a fact (rule 6).
    assert progress[0]["batchId"] == 0 and progress[0]["numInputRows"] >= 2
    assert progress[0]["durationMs"]["addBatch"] > 0
    assert [line for line in lines if line.get("type") == "batch"] == [
        {"type": "batch", "batch_id": 0, "writes": 2, "round": 0}
    ]
    ends = [line for line in lines if line.get("type") == "terminated"]
    assert ends == [{"type": "terminated", "id": progress[0]["id"], "exception": None}]
