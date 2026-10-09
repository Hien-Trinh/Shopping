import argparse
import os
import signal
import sys
import threading

import pytest

from catalog import entry, events, state


@pytest.fixture
def exits(monkeypatch):
    codes = []
    monkeypatch.setattr(os, "_exit", codes.append)  # what the process would have exited with
    return codes


def test_exit_with_uses_mains_return_value(exits):
    entry.exit_with(lambda: 3)
    entry.exit_with(lambda: None)
    assert exits == [3, 0]


def test_exit_with_maps_an_exception_to_its_code_else_1(exits, capsys):
    def taken():
        raise state.PartitionTaken("partition 0")

    def bug():
        raise KeyError("x")

    for main in (taken, bug):
        entry.exit_with(main, {state.PartitionTaken: 3})
    assert exits == [3, 1]
    assert capsys.readouterr().err.count("Traceback") == 2


def test_exit_with_keeps_argparses_code_and_survives_closed_streams(exits, monkeypatch):
    def bad_flag():
        raise SystemExit(2)

    monkeypatch.setattr(sys, "stdout", None)  # e.g. started with stdout closed
    entry.exit_with(bad_flag)
    assert exits == [2]


def test_a_watched_process_stops_when_its_supervisor_dies_and_exits_hard_if_stuck():
    stop, seen, checks = threading.Event(), [], iter([True, True, False])
    entry._watch(
        lambda: next(checks), stop, deadline=1.5,
        sleep=lambda s: seen.append((s, stop.is_set())), exit=seen.append,
    )  # fmt: skip
    # Checked each second while it lived. Once it's gone, stop is set so the tick in progress
    # can finish; a process still running a deadline later is stuck in a native call: exit 1.
    assert seen == [(1, False), (1, False), (1.5, True), 1]


def test_the_watch_says_why_it_stopped_and_leaves_a_trace_of_its_hard_exit(tmp_path):  # 3e
    stop, exits, slept = threading.Event(), [], []

    def sleep(seconds):  # the trace is written only once the deadline has passed
        slept.append((seconds, events.read(tmp_path)))

    entry._watch(lambda: False, stop, tmp_path, "worker-2", deadline=30,
                 sleep=sleep, exit=exits.append, clock=lambda: 1_000.0)  # fmt: skip
    assert stop.is_set() and stop.reason == "supervisor_gone" and exits == [1]
    assert slept == [(30, [])]
    assert events.read(tmp_path) == [{"type": "watch_exit", "process": "worker-2",
        "pid": os.getpid(), "deadline": 30, "ts": 1_030_000}]  # the planned exit time  # fmt: skip


def test_the_hard_exit_happens_even_when_its_trace_cant_be_written(tmp_path):
    tmp_path.chmod(0o500)  # read-only, as good as a full disk here
    try:
        exits = []
        entry._watch(lambda: False, threading.Event(), tmp_path, "api",
                     sleep=lambda s: None, exit=exits.append)  # fmt: skip
    finally:
        tmp_path.chmod(0o700)
    assert exits == [1] and not list(tmp_path.iterdir())


@pytest.mark.parametrize("pid", ["abc", "0", "-1", " 7", str(2**31)])
def test_a_malformed_supervisor_pid_is_refused_as_a_bad_flag(monkeypatch, capsys, pid):
    monkeypatch.setenv("CATALOG_SUPERVISOR", pid)  # 0 or -1 would make the watch signal a group
    with pytest.raises(SystemExit) as stopped:
        entry.supervisor_pid(argparse.ArgumentParser())
    assert stopped.value.code == 2  # fatal, like any bad flag: no restart can fix it
    assert "CATALOG_SUPERVISOR" in capsys.readouterr().err


def test_a_supervisor_run_by_another_user_still_counts_as_alive():
    assert entry._alive(1)  # init: signalling it is refused (EPERM), but it runs


def test_a_second_signal_does_not_start_a_second_stopper(tmp_path, monkeypatch):
    handlers, started = {}, []
    monkeypatch.setattr(signal, "signal", lambda sig, handler: handlers.update({sig: handler}))

    class Thread:
        def __init__(self, target):
            self.target = target

        def start(self):
            started.append(self)
            self.target()

    monkeypatch.setattr(entry.threading, "Thread", Thread)
    stop = entry.watch_supervisor(None, tmp_path, "export")
    for h in handlers.values():
        h(0, None)
    assert stop.is_set() and len(started) == 1
