import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from catalog import events, state, supervisor
from catalog.supervisor import STALE, parse_procfile, run, verdict


def test_parse_procfile():
    text = "# workers\nworker-0: python -m x --name 'a b'\n\n  api :uvicorn app:app\n"
    assert parse_procfile(text) == {
        "worker-0": ["python", "-m", "x", "--name", "a b"],
        "api": ["uvicorn", "app:app"],
    }
    for bad in ["worker-0", ": cmd", "w:  ", "w: cmd\nw: cmd"]:
        with pytest.raises(ValueError, match="Procfile line"):
            parse_procfile(bad)


@pytest.mark.parametrize(
    "code, now, started, beat, watched, want",
    [
        (None, STALE + 1, 0, None, True, "stale"),  # hung before its first beat: from its start
        (None, STALE, 0, None, True, None),  # not yet: only over STALE
        (None, 100, 0, 100 - STALE - 1, True, "stale"),  # beat, then went quiet
        (None, 100, 50, 10, True, None),  # a beat before the start is the predecessor's, so a
        (None, 200, 100, 10, True, "stale"),  # restarted worker gets its own minute, no more
        (None, 100, 0, 500, True, "stale"),  # a beat after now is another clock's: ignored
        (None, 1000, 0, None, False, None),  # a process that doesn't beat is never stale
        (1, 5, 0, 1, True, "exit"),  # gave up: a restart may help
        (2, 5, 0, 1, True, "fatal"),
    ],
)
def test_verdict(code, now, started, beat, watched, want):
    assert verdict(code, now, started, beat, watched) == want


class Child:
    """Stands in for a Popen."""

    def __init__(self, pid, name):
        self.pid, self.name, self.returncode, self.doomed, self.deaf = pid, name, None, False, False

    def poll(self):
        return self.returncode

    def terminate(self):
        if self.returncode is None and not self.deaf:
            self.returncode = 0

    def kill(self):
        self.doomed = True  # gone only once waited for, as with a real kill

    def wait(self, timeout=None):
        self.returncode = -9 if self.doomed else self.returncode
        if self.returncode is None:
            raise subprocess.TimeoutExpired("child", timeout)
        return self.returncode


class World:
    """Simulated time, sleep and processes: nothing really waits. `script` plays the processes."""

    def __init__(self, tmp_path, until, script=lambda w: None):
        self.t, self.until, self.script, self.tmp = 0.0, until, script, tmp_path
        self.kids, self.started, self.stopping = [], [], []  # started: when each was spawned

    def sleep(self, seconds):
        self.t += seconds
        self.script(self)
        self.stopping += [True] * (self.t >= self.until)

    def spawn(self, argv):
        # Never while its predecessor lives: the new process would find its partitions locked.
        assert all(k.returncode is not None for k in self.kids if k.name == argv[1])
        self.kids.append(Child(100 + len(self.kids), argv[1]))
        self.started.append(self.t)
        return self.kids[-1]

    def run(self, *names):
        log = events.EventLog(self.tmp / "events", "supervisor")
        return run({n: ["cmd", n] for n in names}, self.tmp / "state", log, stopping=self.stopping,
                   sleep=self.sleep, clock=lambda: self.t, spawn=self.spawn)  # fmt: skip

    def exits(self):  # what the supervisor logged: (process, reason, code)
        return [(e["process"], e["reason"], e["code"]) for e in events.read(self.tmp / "events")]


def test_a_hung_worker_is_restarted_a_minute_after_its_last_sign_of_life(tmp_path):
    def script(w):  # worker-1 beats until t=5; worker-0 never does; the api doesn't beat at all
        if w.t <= 5:
            state.beat(tmp_path / "state", "worker-1", w.t)

    w = World(tmp_path, 130, script)
    assert w.run("worker-0", "worker-1", "api") == 0  # stopped by `stopping`
    # worker-0 never beat, so it's killed 61 s after each start. worker-1 61 s after its last beat,
    # then 61 s after its restart: the predecessor's beat (t=5) doesn't count for the new run.
    assert w.started == [0, 0, 0, 61, 66, 122, 127]


def test_an_exit_restarts_a_process_but_a_fatal_one_stops_everything(tmp_path):
    def script(w):
        if w.t == 1:
            w.kids[0].returncode = 1  # worker-0 gave up
        if w.t == 2:
            w.kids[1].returncode = 3  # worker-1 found its partitions taken
            w.kids[2].deaf = True  # and the api ignores SIGTERM

    w = World(tmp_path, 100, script)
    assert w.run("worker-0", "worker-1", "api") == 3
    assert w.started == [0, 0, 0]  # worker-0's restart was due at t=6, after the fatal exit
    assert w.exits() == [("worker-0", "exit", 1), ("worker-1", "fatal", 3)]
    # The others were stopped: asked politely, and killed after GRACE if they ignored it.
    assert [k.returncode for k in w.kids] == [1, 3, -9]


def wait_for(condition, timeout=30):
    deadline = time.monotonic() + timeout
    while not (found := condition()):
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.05)
    return found


def test_the_supervisor_restarts_a_killed_process_and_stops_cleanly(tmp_path):
    cmd = "echo $$ > pid.tmp; mv pid.tmp pid; exec sleep 60"  # records its pid, atomically
    (tmp_path / "Procfile").write_text(f"worker-0: sh -c '{cmd}'\n")
    sup = subprocess.Popen([sys.executable, "-m", "catalog.supervisor"], cwd=tmp_path)

    def pid():
        return (tmp_path / "pid").exists() and int((tmp_path / "pid").read_text())

    try:
        first = wait_for(pid)
        assert os.getsid(first) == first  # its own session: a Ctrl-C reaches only the supervisor
        os.kill(first, signal.SIGKILL)
        second = wait_for(lambda: pid() != first and pid())
        sup.send_signal(signal.SIGTERM)
        assert sup.wait(timeout=30) == 0
    finally:
        sup.kill()
    assert [e["pid"] for e in events.read(tmp_path / "data" / "events")] == [first]
    with pytest.raises(ProcessLookupError):  # the replacement went down with it
        os.kill(second, 0)


def test_a_crash_looping_process_restarts_after_a_delay(tmp_path):
    def script(w):  # the api dies 1 s into every run; the worker after a long one
        for kid in w.kids:
            ran = w.t - w.started[w.kids.index(kid)]
            if kid.returncode is None and (ran >= 1 if kid.name == "api" else ran >= 30):
                kid.returncode = 1

    w = World(tmp_path, 40, script)
    w.run("api", "worker-0")
    starts = sorted(zip(w.started, (k.name for k in w.kids), strict=True))
    assert [t for t, n in starts if n == "api"] == [0, 6, 12, 18, 24, 30, 36]  # RETRY after 1 s
    assert [t for t, n in starts if n == "worker-0"] == [0, 30]  # ran long: back at once


def test_a_beat_landing_while_the_supervisor_reads_it_still_counts(tmp_path, monkeypatch):
    w = World(tmp_path, 300)
    real = state.last_beat

    def beating_now(state_dir, name):  # the worker beats just as the supervisor reads its file
        w.t += 0.5
        state.beat(state_dir, name, w.t)
        return real(state_dir, name)

    monkeypatch.setattr(state, "last_beat", beating_now)
    w.run("worker-0")
    assert len(w.started) == 1  # never killed: sampling the clock first would see a future beat


def test_main_reads_the_procfile_and_stops_on_sigterm_or_sigint(tmp_path, monkeypatch):
    handlers, seen = {}, {}
    (tmp_path / "Procfile").write_text("worker-0: python -m catalog.worker --index 0\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(signal, "signal", lambda sig, handler: handlers.update({sig: handler}))

    def fake_run(procs, state_dir, events, *, stopping):
        seen.update(procs=procs, state=state_dir, events=events.root)
        handlers[signal.SIGINT](signal.SIGINT, None)
        return len(stopping)

    monkeypatch.setattr(supervisor, "run", fake_run)
    assert supervisor.main() == 1  # stopping was set by the handler
    assert seen == {
        "procs": {"worker-0": ["python", "-m", "catalog.worker", "--index", "0"]},
        "state": Path("state"),
        "events": Path("data") / "events",
    }
    assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
