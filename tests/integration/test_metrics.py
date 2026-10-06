import json
from collections import Counter
from datetime import UTC, datetime

import pytest
from support import listing

from catalog import events, metrics, status, store
from catalog.classify import UNCATEGORIZED
from catalog.events import EventLog
from catalog.plan import Classification, Write

SINCE, NOW = datetime.fromtimestamp(50, UTC), datetime.fromtimestamp(350, UTC)


def emit(root, t, *logged):
    """Events stamped `t` seconds, as a process would have written them."""
    EventLog(root, "test", clock=lambda: t).emit(logged)


def change(kind, submission, index):
    return {"type": kind, "submission_id": submission, "change_index": index, "merchant_id": "m_1"}


def test_freshness_and_rates_count_each_change_once_with_its_best_outcome(tmp_path):
    root = tmp_path / "events"
    emit(
        root,
        100,
        change("accepted", "s1", 0),
        change("accepted", "s1", 1),
        change("accepted", "s2", 0),
        change("accepted", "s3", 0),
        change("rejected", "s4", 0),
    )  # never landed: not a Change the pipeline handled
    emit(root, 103, change("written", "s1", 0))
    emit(root, 110, change("stale", "s1", 1))
    emit(root, 120, change("failed", "s2", 0))
    emit(root, 130, change("conflict", "s3", 0), change("reclassified", "backfill-x", 0))
    emit(root, 200, change("already_applied", "s1", 0))  # a crash replay: not a second Change
    got = metrics.compute(tmp_path, SINCE, NOW)
    assert got["freshness_s"] == {"changes": 4, "p50": 15.0, "p99": 29.7}  # 3, 10, 20 and 30 s
    assert got["outcomes"] == {"changes": 4, "stale": 0.25, "conflict": 0.25, "failed": 0.25}
    # the same verdicts as GET /submissions folds them
    found = events.read(root)
    folded = Counter(o for s in ("s1", "s2", "s3") for o in status.fold(s, found).outcomes.values())
    assert {k: folded[k] / 4 for k in ("stale", "conflict", "failed")} == {
        k: got["outcomes"][k] for k in ("stale", "conflict", "failed")
    }  # fmt: skip


def test_a_change_first_seen_before_the_window_is_left_out(tmp_path):
    root = tmp_path / "events"
    emit(root, 10, change("accepted", "s1", 0))
    emit(root, 20, change("written", "s1", 0))  # its Outcome came before the window
    emit(root, 100, change("already_applied", "s1", 0))  # only a replay inside it
    assert metrics.compute(tmp_path, SINCE, NOW)["outcomes"]["changes"] == 0


def test_a_replay_of_a_change_older_than_the_read_range_is_not_a_new_change(tmp_path):
    root, later = tmp_path / "events", 4 * 3600  # PR #80 review
    emit(root, 0, change("accepted", "s1", 0))
    emit(root, 5, change("written", "s1", 0))  # hours before what the window reads
    emit(root, later, change("already_applied", "s1", 0))  # a crash replay inside the window
    since, now = (datetime.fromtimestamp(t, UTC) for t in (later - 60, later + 60))
    assert metrics.compute(tmp_path, since, now)["outcomes"]["changes"] == 0


def test_lag_reads_each_partitions_latest_batch(tmp_path):
    root = tmp_path / "events"
    emit(root, 100, {"type": "batch", "worker": "worker-0", "changes": 5, "ms": 30, "head": 9,
                     "next": {"3": [5, 0]}})  # fmt: skip
    emit(root, 150, {"type": "batch", "worker": "worker-0", "changes": 9, "ms": 60, "head": 12,
                     "next": {"3": [13, 0], "7": [2, 4]}})  # fmt: skip
    got = metrics.compute(tmp_path, SINCE, NOW)
    assert got["lag_commits"] == {"3": 0, "7": 11}  # head + 1 - next[p][0]
    assert got["utilization"] == {"worker-0": 0.0003}  # 90 ms of the 300 s window
    # worker-0 stalls; worker-1 goes on to head 512 (PR #80 review)
    emit(root, 160, {"type": "batch", "worker": "worker-1", "changes": 1, "ms": 5, "head": 512,
                     "next": {"20": [513, 0]}})  # fmt: skip
    lag = metrics.compute(tmp_path, SINCE, NOW)["lag_commits"]
    assert lag == {"3": 500, "7": 511, "20": 0}  # behind the newest head any worker read


def test_classify_appends_and_refusals(tmp_path):
    root = tmp_path / "events"
    for t, ms in [(100, 100), (101, 200), (102, 300)]:
        emit(root, t, {"type": "classify", "listings": 10, "ms": ms})
    emit(root, 103, {"type": "append_retry", "rows": 4, "ms": 40})
    emit(root, 104, {"type": "refused", "status": 401, "reason": "unknown_key"},
         {"type": "refused", "status": 503, "reason": "low_disk"},
         {"type": "refused", "status": 401, "reason": "unknown_key"})  # fmt: skip
    got = metrics.compute(tmp_path, SINCE, NOW)
    assert got["classify_ms"] == {"calls": 3, "listings": 30, "p50": 200.0, "p99": 298.0}
    assert got["append_retries"] == {"count": 1, "mean_ms": 40.0}
    assert got["refused"] == {"low_disk": 1, "unknown_key": 2}


def test_an_empty_window_gives_nulls(tmp_path):
    got = metrics.compute(tmp_path, SINCE, NOW)
    assert got == {
        "since": "1970-01-01T00:00:50+00:00",
        "freshness_s": {"changes": 0, "p50": None, "p99": None},
        "lag_commits": {},
        "classify_ms": {"calls": 0, "listings": None, "p50": None, "p99": None},
        "outcomes": {"changes": 0, "stale": None, "conflict": None, "failed": None},
        "uncategorized": None,  # no Listing Store yet
        "utilization": {},
        "append_retries": {"count": 0, "mean_ms": None},
        "refused": {},
    }
    json.dumps(got)  # printable as it is


def test_the_uncategorized_rate_is_the_share_of_live_listings(tmp_path):
    dt = store.ensure(str(tmp_path / "listing_store"))
    rows = [("a", "Apparel", False), ("b", UNCATEGORIZED, True), ("c", UNCATEGORIZED, True)]
    writes = [Write(("m_1", k), 1, listing(), Classification(c, 0.5, "v", f), False)
              for k, c, f in rows]  # fmt: skip
    writes.append(Write(("m_1", "d"), 2, None, None, False))  # a tombstone: not live
    store.merge(dt, writes, NOW)
    assert metrics.compute(tmp_path, SINCE, NOW)["uncategorized"] == {"live": 3, "share": 2 / 3}


def test_the_cli_prints_one_json_object(tmp_path, capsys):
    emit(tmp_path / "events", datetime.now(UTC).timestamp(), {"type": "classify", "listings": 2,
                                                              "ms": 7})  # fmt: skip
    metrics.main(["--data", str(tmp_path), "--since", "60"])
    assert json.loads(capsys.readouterr().out)["classify_ms"]["calls"] == 1


def test_since_takes_a_duration_and_must_be_positive(tmp_path, capsys):
    metrics.main(["--data", str(tmp_path), "--since", "10m"])
    assert json.loads(capsys.readouterr().out)["since"]
    for bad in ("0", "-5", "soon"):
        with pytest.raises(SystemExit) as stopped:
            metrics.main(["--data", str(tmp_path), "--since", bad])
        assert stopped.value.code == 2
    assert (
        metrics.duration("90s") == metrics.duration("1.5m") == 90 and metrics.duration("2h") == 7200
    )


def test_a_corrupt_listing_store_is_an_error_not_a_missing_one(tmp_path):
    log = tmp_path / "listing_store" / "_delta_log"
    log.mkdir(parents=True)
    (log / "00000000000000000000.json").write_text("garbage")
    with pytest.raises(Exception, match="(?i)json|parse|invalid|delta"):
        metrics.compute(tmp_path, SINCE, NOW)
