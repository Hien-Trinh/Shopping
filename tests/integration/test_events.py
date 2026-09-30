from datetime import UTC, datetime

from catalog import events
from catalog.events import EventLog

T0 = datetime(2026, 9, 30, 12, 30, tzinfo=UTC).timestamp()


def clock(*times):
    it = iter(times)
    return lambda: next(it)


def test_round_trip_across_processes_and_hours(tmp_path):
    EventLog(tmp_path, "api", clock(T0)).emit([{"type": "accepted", "submission_id": "s1"}])
    EventLog(tmp_path, "worker-0", clock(T0 + 3600)).emit(
        [{"type": "written", "submission_id": "s1"}, {"type": "exported"}]
    )
    got = events.read(tmp_path)
    assert [(e["type"], e["ts"]) for e in got] == [
        ("accepted", int(T0 * 1000)),
        ("written", int((T0 + 3600) * 1000)),
        ("exported", int((T0 + 3600) * 1000)),
    ]
    assert "submission_id" not in got[2]  # absent fields are not padded with None
    assert sorted(p.name for p in tmp_path.iterdir()) == ["2026-09-30T12", "2026-09-30T13"]


def test_filters(tmp_path):
    log = EventLog(tmp_path, "api", clock(T0, T0 + 3600))
    log.emit([{"type": "accepted", "submission_id": "old"}])
    log.emit(
        [{"type": "accepted", "submission_id": "s1"}, {"type": "accepted", "submission_id": "s2"}]
    )
    since = datetime(2026, 9, 30, 13, 59, tzinfo=UTC)
    assert [e["submission_id"] for e in events.read(tmp_path, since=since)] == ["s1", "s2"]
    assert [e["submission_id"] for e in events.read(tmp_path, submission_id="s2")] == ["s2"]


def test_half_written_line_is_skipped(tmp_path):
    EventLog(tmp_path, "worker-0", clock(T0)).emit([{"type": "written", "change_index": 0}])
    with open(tmp_path / "2026-09-30T12" / "worker-0.jsonl", "a") as f:
        f.write('{"ts": 1, "type": "written", "submission_id": "s1", "chan')  # mid-append
    assert events.read(tmp_path) == [{"ts": int(T0 * 1000), "type": "written", "change_index": 0}]


def test_line_torn_by_a_crash_does_not_swallow_the_next_event(tmp_path):
    log = EventLog(tmp_path, "worker-0", clock(T0, T0 + 1))
    log.emit([{"type": "written", "change_index": 0}])
    with open(tmp_path / "2026-09-30T12" / "worker-0.jsonl", "a") as f:
        f.write('{"ts": 1, "type": "wri')  # killed mid-write
    log.emit([{"type": "written", "change_index": 1}])  # restarted process appends
    assert [e["change_index"] for e in events.read(tmp_path)] == [0, 1]


def test_empty_cases(tmp_path):
    assert events.read(tmp_path) == []
    EventLog(tmp_path, "api", clock(T0)).emit([])
    assert list(tmp_path.iterdir()) == []
    EventLog(tmp_path, "api", clock(T0)).emit([{"type": "heartbeat"}])
    assert events.read(tmp_path, submission_id="s1") == []
