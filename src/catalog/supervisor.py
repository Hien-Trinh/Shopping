"""Supervisor: runs the Procfile's processes, restarting any that exit or stop beating (plan B1).

A `worker-*` process must beat (state.beat); the others are restarted only when they exit. A
worker's fatal exit code (FATAL_CODES) is one no restart can fix: everything stops, with that code.
"""

import contextlib
import fcntl
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

from catalog import entry, state, worker
from catalog.events import EventLog

STALE = 60.0  # seconds without a sign of life (plan B1)
TICK = 1.0  # seconds between checks, so also the fastest a crash loop can spin
GRACE = 10.0  # seconds all processes together get to stop after SIGTERM, on shutdown
STABLE = 10.0  # a process that exits sooner is crash looping...
RETRY = 5.0  # ...so it restarts after this; each start costs a new events file
FATAL_CODES = {2, *worker.FATAL.values()}  # a worker's bad flag, or a fatal startup error
ANOTHER = 3  # exit code when another supervisor holds state/supervisor.lock


class Group(subprocess.Popen):
    """A child in its own session, so a Ctrl-C or a closed terminal reaches only the supervisor.

    Its signals go to the whole process group, so a wrapper command's own child (sh -c, uv run)
    is stopped too, and never left holding partition locks. Its environment carries this
    process's pid, so a worker stops by itself if the supervisor dies without stopping it
    (worker.watch).
    """

    def __init__(self, argv: list[str]):
        env = os.environ | {worker.SUPERVISOR: str(os.getpid())}
        super().__init__(argv, start_new_session=True, env=env)

    def send_signal(self, sig: int) -> None:  # terminate() and kill() come through here
        if self.poll() is None:  # not reaped, so its pid still names its group
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pid, sig)


def parse_procfile(text: str) -> dict[str, list[str]]:
    """`name: command` per line, in order; blank lines and #comments are skipped.

    A bare `python` runs as this interpreter: the one on PATH may not have catalog installed.
    """
    procs = {}
    for n, line in enumerate(text.splitlines(), 1):
        if line.strip() and not line.lstrip().startswith("#"):
            name, _, command = line.partition(":")
            if not (name := name.strip()) or not command.strip() or name in procs:
                raise ValueError(f"Procfile line {n}: want a new `name: command`, got {line!r}")
            argv = shlex.split(command)
            procs[name] = [sys.executable, *argv[1:]] if argv[0] == "python" else argv
    return procs


def verdict(
    code: int | None, now: float, started: float, beat: float | None, watched: bool
) -> str | None:
    """What to do: "exit" (restart it), "fatal" (stop all), "stale" (kill, restart it), or None.

    The start counts as a beat. A beat from before it is the predecessor's, and one from after
    `now` another clock's (a reboot): neither keeps this run alive. Only a worker's exit code
    can be fatal; another process means something else by its codes.
    """
    if code is not None:
        return "fatal" if watched and code in FATAL_CODES else "exit"
    last = beat if beat is not None and started <= beat <= now else started
    return "stale" if watched and now - last > STALE else None


def run(procs, state_dir, events, *, stopping, sleep=time.sleep, clock=state.CLOCK,
        spawn=Group) -> int:  # fmt: skip
    """Run `procs` until `stopping` is non-empty (returns 0) or a worker fails fatally (its code).

    Every exit is logged: the rest of the pass that saw a fatal one, and each child stopped at
    shutdown. `clock` must be the workers' beat clock.
    """
    live, due, killed = {}, {}, set()  # name -> (child, start); name -> restart time; stale kills
    fatal = None

    def note(event):  # best effort: a full disk must not stop the supervisor and its workers
        try:
            events.emit([event])
        except OSError as e:
            with contextlib.suppress(OSError):  # and stderr may be a broken pipe
                print(f"supervisor: couldn't log {event}: {e!r}", file=sys.stderr)

    def start(name, first=False):
        started = clock()  # before the spawn, so no beat of the new run can predate it
        try:
            live[name] = spawn(procs[name]), started
        except OSError as e:
            if first:
                raise  # a command that can't start at all: fail fast
            due[name] = started + RETRY  # e.g. EAGAIN under memory pressure: try again later
            error = repr(e)[:500]
            note({"type": "process_exit", "process": name, "reason": "spawn", "error": error})

    note({"type": "supervisor_start", "processes": list(procs), "pid": os.getpid()})
    try:
        for name in procs:
            start(name, first=True)
        while not stopping and fatal is None:
            for name in procs:
                if name in due:
                    if fatal is None and clock() >= due[name]:
                        del due[name]
                        start(name)
                    continue
                child, started = live[name]
                watched = name.startswith(state.WORKER)
                beat = state.last_beat(state_dir, name) if watched else None
                now = clock()  # after the read: a beat landing in between must not look future
                why = verdict(child.poll(), now, started, beat, watched)
                if why == "stale":
                    if name not in killed:  # SIGTERM isn't heard while a native MERGE blocks
                        child.kill()
                        killed.add(name)
                    continue  # restarted once reaped: its partition locks are free only then
                if why:
                    if name in killed:
                        why = "stale"
                        killed.discard(name)
                    pid, code = child.pid, child.returncode
                    note({"type": "process_exit", "process": name, "reason": why, "code": code,
                          "pid": pid})  # fmt: skip
                    del live[name]  # logged: shutdown won't log it again
                    if why == "fatal":
                        fatal = fatal or code  # the first one's: fatal codes are never 0
                    if fatal is not None:  # the pass goes on, only to log the others' exits
                        continue
                    if now - started < STABLE:
                        due[name] = now + RETRY
                    else:
                        start(name)
            if fatal is None:
                sleep(TICK)
    finally:  # also on a bug here: never leave workers running unsupervised
        stopped = [(name, live[name][0]) for name in procs if name in live]
        for name, why, child in _stop(stopped, clock):
            why = "stale" if name in killed else why  # killed for silence, not yet reaped
            note({"type": "process_exit", "process": name, "reason": why,
                  "code": child.returncode, "pid": child.pid})  # fmt: skip
    note({"type": "supervisor_stop", "code": fatal or 0})
    return fatal or 0


def _stop(children, clock) -> list[tuple[str, str, subprocess.Popen]]:
    """SIGTERM every (name, child), then SIGKILL any still running once one shared GRACE has
    passed. Returns (name, "exit" if it had already exited else "shutdown", child) for each."""
    why = [(name, "exit" if child.poll() is not None else "shutdown", child)
           for name, child in children]  # fmt: skip
    for _, child in children:
        child.terminate()
    deadline = clock() + GRACE
    for _, child in children:
        try:
            child.wait(max(0.0, deadline - clock()))
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
    return why


def _only_supervisor(state_dir: Path) -> bool:
    """Hold state/supervisor.lock for this process's life; False if another supervisor has it."""
    state_dir.mkdir(parents=True, exist_ok=True)
    fd = os.open(state_dir / "supervisor.lock", os.O_CREAT | os.O_RDWR)  # children don't inherit
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return False
    return True  # fd stays open: the lock lasts until this process ends


def main() -> int:
    """Run ./Procfile against the workers' default directories."""
    if not _only_supervisor(state.STATE):
        with contextlib.suppress(OSError):  # stderr may be a broken pipe: still exit ANOTHER
            print(f"supervisor: another one holds {state.STATE}/supervisor.lock", file=sys.stderr)
        return ANOTHER
    procs = parse_procfile(Path("Procfile").read_text())
    stopping = []
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):  # list.append can't deadlock
        signal.signal(sig, lambda *_: stopping.append(True))
    events = EventLog(state.DATA / "events", "supervisor")
    return run(procs, state.STATE, events, stopping=stopping)


if __name__ == "__main__":
    entry.exit_with(main)
