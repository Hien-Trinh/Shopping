import os
import re
import threading
from datetime import UTC, datetime

import pytest

from catalog import events
from catalog.events import EventLog

T0 = datetime(2026, 9, 30, 12, 30, tzinfo=UTC).timestamp()
HOUR = "2026-09-30T12"


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
    assert "submission_id" not in got[2]
    assert sorted(p.name for p in tmp_path.iterdir()) == [HOUR, "2026-09-30T13"]


def test_one_file_per_process_run(tmp_path):
    EventLog(tmp_path, "worker-0", clock(T0)).emit([{"type": "x"}])
    (name,) = [f.name for f in (tmp_path / HOUR).iterdir()]
    assert re.fullmatch(rf"worker-0-{os.getpid()}-[0-9a-f]{{8}}\.jsonl", name)


def test_the_shared_timestamp_wins(tmp_path):
    EventLog(tmp_path, "api", clock(T0)).emit([{"type": "x", "ts": "yesterday"}])
    assert events.read(tmp_path)[0]["ts"] == int(T0 * 1000)


def test_filters(tmp_path):
    log = EventLog(tmp_path, "api", clock(T0, T0 + 3600))
    log.emit([{"type": "accepted", "submission_id": "old"}])
    log.emit(
        [{"type": "accepted", "submission_id": "s1"}, {"type": "accepted", "submission_id": "s2"}]
    )
    since = datetime(2026, 9, 30, 13, 59, tzinfo=UTC)
    assert [e["submission_id"] for e in events.read(tmp_path, since=since)] == ["s1", "s2"]
    assert [e["submission_id"] for e in events.read(tmp_path, submission_id="s2")] == ["s2"]


def test_submission_filter_is_exact(tmp_path):
    tricky = 's"1é'
    EventLog(tmp_path, "api", clock(T0)).emit(
        [
            {"type": "accepted", "submission_id": tricky},
            {"type": "accepted", "submission_id": "s1-other"},
            {"type": "note", "text": '"submission_id":"s1"'},  # matches the bytes, not the field
            {"type": "nested", "meta": {"submission_id": "s1"}},  # same bytes, not top level
            {"type": "accepted", "submission_id": "s1"},
        ]
    )
    assert [e["type"] for e in events.read(tmp_path, submission_id="s1")] == ["accepted"]
    assert len(events.read(tmp_path, submission_id=tricky)) == 1


def test_naive_since_is_refused(tmp_path):
    with pytest.raises(ValueError, match="timezone-aware"):
        events.read(tmp_path, since=datetime(2026, 9, 30, 12, 29))


def test_unterminated_and_corrupt_lines_are_skipped(tmp_path):
    EventLog(tmp_path, "worker-0", clock(T0)).emit([{"type": "written", "change_index": 0}])
    (tmp_path / HOUR / "dead-1.jsonl").write_text('not json\n{"ts": 1, "type": "wri')  # torn
    assert events.read(tmp_path) == [{"type": "written", "change_index": 0, "ts": int(T0 * 1000)}]


def test_a_file_removed_mid_scan_is_skipped(tmp_path):
    EventLog(tmp_path, "api", clock(T0)).emit([{"type": "accepted"}])
    (tmp_path / HOUR / "gone.jsonl").symlink_to(tmp_path / "missing.jsonl")  # listed, can't open
    assert [e["type"] for e in events.read(tmp_path)] == ["accepted"]


def test_empty_cases(tmp_path):
    assert events.read(tmp_path) == []
    EventLog(tmp_path, "api", clock(T0)).emit([])
    assert list(tmp_path.iterdir()) == []
    EventLog(tmp_path, "api", clock(T0)).emit([{"type": "heartbeat"}])
    assert events.read(tmp_path, submission_id="s1") == []


def test_a_restart_with_the_same_pid_never_appends_to_a_torn_file(tmp_path):
    EventLog(tmp_path, "worker-0", clock(T0)).emit([{"type": "a"}])
    (torn,) = (tmp_path / HOUR).iterdir()
    with torn.open("a") as f:
        f.write('{"type":"b","sta')  # killed mid-write
    EventLog(tmp_path, "worker-0", clock(T0 + 1)).emit([{"type": "c"}])  # same pid
    assert [e["type"] for e in events.read(tmp_path)] == ["a", "c"]


def test_a_failed_write_never_leaves_the_next_events_after_a_torn_line(tmp_path, monkeypatch):
    log = EventLog(tmp_path, "worker-0", clock(T0, T0 + 1, T0 + 2))
    log.emit([{"type": "a"}])
    (first,) = (tmp_path / HOUR).iterdir()
    with first.open("a") as f:
        f.write('{"type":"b","sta')  # the disk filled mid-write

    def full(*_):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(events, "open", full, raising=False)
    with pytest.raises(OSError):
        log.emit([{"type": "b"}])
    monkeypatch.undo()
    log.emit([{"type": "c"}])  # the retry
    assert [e["type"] for e in events.read(tmp_path)] == ["a", "c"]


def test_a_stop_event_says_why_the_watch_stopped_the_process(tmp_path):  # step 7c.1
    stop = threading.Event()
    stop.reason = "supervisor_gone"  # as worker.watch sets it
    with events.stopping(EventLog(tmp_path, "export"), stop):
        pass
    [stop_event] = events.read(tmp_path)
    assert (stop_event["type"], stop_event["reason"]) == ("export_stop", "supervisor_gone")
    assert "error" not in stop_event
