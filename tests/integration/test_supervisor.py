import contextlib
import fcntl
import inspect
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from catalog import events, state, supervisor, worker
from catalog.supervisor import GRACE, STALE, parse_procfile, run, verdict

REPO = Path(__file__).parents[2]


@pytest.fixture(params=["working", "broken"])
def stderr(request, monkeypatch):
    """Call first in the test (capsys swaps sys.stderr in only then); True if it works.

    Broken: a pipe whose reader died, as when the supervisor runs under `| tee` and tee dies.
    """
    read, write = os.pipe()
    os.close(read)
    # Closing flushes what the test couldn't write, so it raises BrokenPipeError too.
    with contextlib.suppress(OSError), open(write, "w", buffering=1) as broken:  # line-buffered

        def use():
            if request.param == "broken":
                monkeypatch.setattr(sys, "stderr", broken)
            return request.param == "working"

        yield use


def test_parse_procfile():
    text = "# workers\nworker-0: python -m x --name 'a b'\n\n  api :uv run python app\n"
    assert parse_procfile(text) == {
        "worker-0": [sys.executable, "-m", "x", "--name", "a b"],  # this interpreter, not PATH's
        "api": ["uv", "run", "python", "app"],
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
        (-9, 5, 0, 1, True, "exit"),  # killed by a signal
        (8, 5, 0, 1, True, "exit"),  # not one of a worker's fatal codes
        *[(code, 5, 0, 1, True, "fatal") for code in (2, 3, 4, 5, 6, 7)],
        (3, 5, 0, None, False, "exit"),  # another process's code 3 means something else
    ],
)
def test_verdict(code, now, started, beat, watched, want):
    assert verdict(code, now, started, beat, watched) == want


class Child:
    """Stands in for a Popen. A deaf one ignores SIGTERM, as a hung worker does; a slow one takes
    2 s to die after SIGKILL, as one stuck in I/O might."""

    def __init__(self, world, pid, name, deaf, slow):
        self.world, self.pid, self.name, self.deaf, self.slow = world, pid, name, deaf, slow
        self.returncode, self.dies_at, self.kills = None, None, 0

    def poll(self):
        if self.dies_at is not None and self.world.t >= self.dies_at:
            self.returncode = -9
        return self.returncode

    def terminate(self):
        if self.returncode is None and not self.deaf:
            self.returncode = -15

    def kill(self):
        self.kills += 1
        if self.returncode is None:
            self.dies_at = self.world.t + 2 if self.slow else self.world.t

    def wait(self, timeout=None):
        self.poll()
        if self.dies_at is not None:  # a kill always lands, eventually
            self.returncode = -9
        if self.returncode is None:
            assert timeout is not None, "wait() on a live child would block forever"
            self.world.t += timeout  # blocked that long, for nothing
            raise subprocess.TimeoutExpired("child", timeout)
        return self.returncode


class World:
    """Simulated time, sleep and processes: nothing really waits. `script` plays the processes."""

    def __init__(self, tmp_path, until, script=lambda w: None, deaf=(), slow=()):
        self.t, self.until, self.script, self.tmp = 0.0, until, script, tmp_path
        self.deaf, self.slow = deaf, slow
        self.kids, self.started, self.stopping, self.refusing = [], [], [], set()

    def sleep(self, seconds):
        self.t += seconds
        self.script(self)
        self.stopping += [True] * (self.t >= self.until)

    def spawn(self, argv):
        name = argv[1]
        if name in self.refusing:
            raise OSError(35, "Resource temporarily unavailable")
        # Never while its predecessor lives: the new process would find its partitions locked.
        assert all(k.returncode is not None for k in self.kids if k.name == name)
        self.kids.append(
            Child(self, 100 + len(self.kids), name, name in self.deaf, name in self.slow)
        )
        self.started.append(self.t)
        return self.kids[-1]

    def run(self, *names, log=None):
        log = log or events.EventLog(self.tmp / "events", "supervisor")
        return run({n: ["cmd", n] for n in names}, self.tmp / "state", log, stopping=self.stopping,
                   sleep=self.sleep, clock=lambda: self.t, spawn=self.spawn)  # fmt: skip

    def exits(self):  # what the supervisor logged: (process, reason, code)
        logged = events.read(self.tmp / "events")
        return [(e["process"], e["reason"], e.get("code")) for e in logged if "process" in e]


def test_a_hung_worker_is_killed_a_minute_after_its_last_sign_of_life(tmp_path):
    (tmp_path / "state" / "heartbeat").mkdir(parents=True)
    (tmp_path / "state" / "heartbeat" / "worker-0.json").write_text("{")  # torn: counts as none

    def script(w):  # worker-1 beats until t=5; worker-0 never does; the api doesn't beat at all
        if w.t <= 5:
            state.beat(tmp_path / "state", "worker-1", w.t)

    w = World(tmp_path, 130, script, deaf={"worker-0", "worker-1"})  # hung: deaf to SIGTERM
    assert w.run("worker-0", "worker-1", "api") == 0  # stopped by `stopping`
    # worker-0 is SIGKILLed 61 s after each start, and restarted on the next tick, once reaped.
    # worker-1 61 s after its last beat (t=5), then 61 s after its restart: the predecessor's beat
    # doesn't count for the new run.
    assert w.started == [0, 0, 0, 62, 67, 124, 129]
    assert w.exits() == [(n, "stale", -9) for n in ("worker-0", "worker-1") * 2]


def test_a_stale_worker_is_killed_once_and_restarted_only_once_gone(tmp_path):
    w = World(tmp_path, 70, deaf={"worker-0"}, slow={"worker-0"})
    w.run("worker-0")
    assert w.kids[0].kills == 1  # SIGKILLed at t=61, still dying at t=62: not killed again
    assert w.started == [0, 63]  # restarted once reaped, never while it still held its locks


def test_an_exit_restarts_a_process_but_a_fatal_one_stops_everything(tmp_path):
    def script(w):
        if w.t == 1:
            w.kids[0].returncode = 1  # worker-0 gave up
        if w.t == 2:
            w.kids[1].returncode = 3  # worker-1 found its partitions taken

    w = World(tmp_path, 100, script, deaf={"api"})
    assert w.run("worker-0", "worker-1", "api") == 3
    assert w.started == [0, 0, 0]  # worker-0's restart was due at t=6, after the fatal exit
    assert w.exits() == [("worker-0", "exit", 1), ("worker-1", "fatal", 3)]
    assert [k.returncode for k in w.kids] == [1, 3, -9]  # the deaf api was killed after GRACE


def test_only_a_workers_exit_code_can_be_fatal(tmp_path):
    def script(w):
        if w.t == 1:
            w.kids[0].returncode = 3  # the api's own code 3 means something else

    w = World(tmp_path, 3, script)
    assert w.run("api", "worker-0") == 0
    assert w.exits() == [("api", "exit", 3)]


def test_shutdown_gives_everyone_one_grace_then_kills_the_deaf(tmp_path):
    w = World(tmp_path, 1, deaf={"a", "b", "c"})
    w.run("a", "b", "c", "d")
    assert w.t == 1 + GRACE  # one GRACE shared by all, not one each
    assert [k.returncode for k in w.kids] == [-9, -9, -9, -15]


def test_a_bug_in_the_loop_still_stops_every_child(tmp_path):
    def script(w):
        raise RuntimeError("a bug")

    w = World(tmp_path, 100, script)
    with pytest.raises(RuntimeError):
        w.run("worker-0", "api")
    assert [k.returncode for k in w.kids] == [-15, -15]


class FullDisk:
    def emit(self, events):
        raise OSError(28, "No space left on device")


def test_a_full_disk_never_stops_the_supervisor(tmp_path, capsys, stderr):
    def script(w):
        if w.t == 1:
            w.kids[0].returncode = 1
        if w.t == 20:
            w.kids[1].returncode = 3

    works, w = stderr(), World(tmp_path, 100, script)
    assert w.run("worker-0", "worker-1", log=FullDisk()) == 3  # the fatal code still comes out
    assert w.started == [0, 0, 6]  # worker-0 was restarted though its exit couldn't be logged
    assert ("No space left on device" in capsys.readouterr().err) == works


def test_a_failed_restart_is_retried_and_spares_the_others(tmp_path):
    def script(w):
        if w.t == 1:
            w.kids[0].returncode = 1
            w.refusing.add("worker-0")  # say the machine is out of processes
        if w.t == 7:
            w.refusing.clear()

    w = World(tmp_path, 15, script)
    assert w.run("worker-0", "worker-1") == 0
    assert w.started == [0, 0, 11]  # the restart failed at t=6 and was retried RETRY later
    assert w.exits() == [("worker-0", "exit", 1), ("worker-0", "spawn", None)]

    w = World(tmp_path / "startup", 5)
    w.refusing.add("api")
    with pytest.raises(OSError):  # a command that can't start at all fails fast...
        w.run("worker-0", "api")
    assert w.kids[0].returncode == -15  # ...and stops what it had started


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


def test_workers_and_the_supervisor_agree_on_clock_names_and_directories():
    # A mismatch on any of these makes the supervisor kill every healthy worker each minute.
    assert state.CLOCK is time.monotonic  # beats survive laptop sleep and NTP steps
    for fn in (worker.run, supervisor.run):
        assert inspect.signature(fn).parameters["clock"].default is state.CLOCK
    procs = parse_procfile((REPO / "Procfile").read_text())
    workers = {n: argv for n, argv in procs.items() if n.startswith(state.WORKER)}
    assert workers
    for name, argv in workers.items():
        assert argv[1:4] == ["-m", "catalog.worker", "--index"]
        assert name == f"{state.WORKER}{argv[4]}"  # its heartbeat file is the one watched
        assert "--state" not in argv and "--data" not in argv  # the supervisor's directories


def test_main_wires_the_procfile_and_stops_on_sigterm_sigint_or_sighup(tmp_path, monkeypatch):
    handlers, seen = {}, {}
    (tmp_path / "Procfile").write_text("worker-0: python -m catalog.worker --index 0\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(signal, "signal", lambda sig, handler: handlers.update({sig: handler}))

    def fake_run(procs, state_dir, events, *, stopping):
        seen.update(procs=procs, state=state_dir, events=events.root)
        handlers[signal.SIGHUP](signal.SIGHUP, None)  # a closed terminal
        return len(stopping)

    monkeypatch.setattr(supervisor, "run", fake_run)
    assert supervisor.main() == 1  # stopping was set by the handler
    assert seen == {
        "procs": {"worker-0": [sys.executable, "-m", "catalog.worker", "--index", "0"]},
        "state": state.STATE,
        "events": state.DATA / "events",
    }
    assert set(handlers) == {signal.SIGTERM, signal.SIGINT, signal.SIGHUP}


def test_a_second_supervisor_exits_at_once(tmp_path, monkeypatch, capsys, stderr):
    works = stderr()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(supervisor, "run", lambda *a, **k: pytest.fail("a second one ran"))
    (tmp_path / "state").mkdir()
    held = os.open(tmp_path / "state" / "supervisor.lock", os.O_CREAT | os.O_RDWR)
    fcntl.flock(held, fcntl.LOCK_EX)  # the first supervisor
    try:
        assert supervisor.main() == supervisor.ANOTHER
    finally:
        os.close(held)
    assert ("another one holds" in capsys.readouterr().err) == works


def wait_for(condition, timeout=30):
    deadline = time.monotonic() + timeout
    while not (found := condition()):
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.05)
    return found


def read_pid(path):
    return path.exists() and int(path.read_text())


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_a_group_is_signalled_whole_and_never_after_its_reaped(tmp_path):
    cmd = f"sleep 30 & echo $! > {tmp_path}/pid.tmp; mv {tmp_path}/pid.tmp {tmp_path}/pid; wait"
    child = supervisor.Group(["sh", "-c", cmd])
    grandchild = wait_for(lambda: read_pid(tmp_path / "pid"))
    assert os.getsid(child.pid) == child.pid  # its own session, and so its own group
    child.terminate()
    assert child.wait(timeout=10) == -signal.SIGTERM
    wait_for(lambda: not alive(grandchild))  # the wrapper's child went with it
    child.kill()  # reaped already: a no-op, never a signal to whatever reuses its pid


def test_the_supervisor_restarts_a_killed_process_and_stops_cleanly(tmp_path):
    cmd = "echo $$ > pid.tmp; mv pid.tmp pid; exec sleep 60"  # records its pid, atomically
    (tmp_path / "Procfile").write_text(f"worker-0: sh -c '{cmd}'\n")
    sup = subprocess.Popen([sys.executable, "-m", "catalog.supervisor"], cwd=tmp_path)
    try:
        pid = tmp_path / "pid"
        first = wait_for(lambda: read_pid(pid))
        assert os.getsid(first) == first  # its own session: a Ctrl-C reaches only the supervisor
        os.kill(first, signal.SIGKILL)
        second = wait_for(lambda: read_pid(pid) != first and read_pid(pid))
        sup.send_signal(signal.SIGTERM)
        assert sup.wait(timeout=30) == 0
    finally:
        sup.kill()
    logged = events.read(tmp_path / "data" / "events")
    assert [e["pid"] for e in logged if e["type"] == "process_exit"] == [first]
    assert not alive(second)  # the replacement went down with it


def test_a_closed_terminal_stops_everything_even_a_wrappers_child(tmp_path):
    cmd = "sleep 60 & echo $! > pid.tmp; mv pid.tmp pid; wait"  # the sleep is the shell's child
    (tmp_path / "Procfile").write_text(f"api: sh -c '{cmd}'\n")
    sup = subprocess.Popen([sys.executable, "-m", "catalog.supervisor"], cwd=tmp_path)
    try:
        grandchild = wait_for(lambda: read_pid(tmp_path / "pid"))
        sup.send_signal(signal.SIGHUP)
        assert sup.wait(timeout=30) == 0
    finally:
        sup.kill()
    wait_for(lambda: not alive(grandchild))  # signalled with its group, not left holding locks
