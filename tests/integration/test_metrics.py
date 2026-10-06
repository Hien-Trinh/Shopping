import json
from collections import Counter
from datetime import UTC, datetime

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
    emit(root, 100, change("accepted", "s1", 0), change("accepted", "s1", 1),
         change("accepted", "s2", 0))  # fmt: skip
    emit(root, 103, change("written", "s1", 0))
    emit(root, 110, change("stale", "s1", 1))
    emit(root, 120, change("failed", "s2", 0))
    emit(root, 200, change("already_applied", "s1", 0))  # a crash replay: not a second Change
    got = metrics.compute(tmp_path, SINCE, NOW)
    assert got["freshness_s"] == {"changes": 3, "p50": 10.0, "p99": 19.8}  # 3, 10 and 20 s
    # the same verdicts as GET /submissions folds them
    found = events.read(root)
    folded = [o for s in ("s1", "s2") for o in status.fold(s, found).outcomes.values()]
    kinds = Counter(folded)
    assert got["outcomes"] == {"changes": 3, "stale": kinds["stale"] / 3, "conflict": 0.0,
                               "failed": kinds["failed"] / 3}  # fmt: skip


def test_a_change_first_seen_before_the_window_is_left_out(tmp_path):
    root = tmp_path / "events"
    emit(root, 10, change("accepted", "s1", 0))
    emit(root, 20, change("written", "s1", 0))  # its Outcome came before the window
    emit(root, 100, change("already_applied", "s1", 0))  # only a replay inside it
    assert metrics.compute(tmp_path, SINCE, NOW)["outcomes"]["changes"] == 0


def test_lag_reads_each_partitions_latest_batch(tmp_path):
    root = tmp_path / "events"
    emit(root, 100, {"type": "batch", "worker": "worker-0", "changes": 5, "ms": 30, "head": 9,
                     "next": {"3": [5, 0]}})  # fmt: skip
    emit(root, 150, {"type": "batch", "worker": "worker-0", "changes": 9, "ms": 60, "head": 12,
                     "next": {"3": [13, 0], "7": [2, 4]}})  # fmt: skip
    got = metrics.compute(tmp_path, SINCE, NOW)
    assert got["lag_commits"] == {"3": 0, "7": 11}  # head + 1 - next[p][0]
    assert got["utilization"] == {"worker-0": 0.0003}  # 90 ms of the 300 s window


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
