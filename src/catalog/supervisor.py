"""Supervisor: runs the Procfile's processes, restarting any that exit or stop beating (plan B1).

A `worker-*` process must beat (state.beat); the others are restarted only when they exit. An exit
code over 1 (worker.FATAL) is one no restart can fix: everything stops, with that exit code.
"""

import functools
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

from catalog import state
from catalog.events import EventLog

STALE = 60.0  # seconds without a sign of life (plan B1)
TICK = 1.0  # seconds between checks, so also the fastest a crash loop can spin
GRACE = 10.0  # seconds a process gets to stop after SIGTERM, on shutdown
STABLE = 10.0  # a process that exits sooner is crash looping...
RETRY = 5.0  # ...so it restarts after this; each start costs a new events file
_OWN_SESSION = functools.partial(subprocess.Popen, start_new_session=True)


def parse_procfile(text: str) -> dict[str, list[str]]:
    """`name: command` per line, in order; blank lines and #comments are skipped."""
    procs = {}
    for n, line in enumerate(text.splitlines(), 1):
        if line.strip() and not line.lstrip().startswith("#"):
            name, _, command = line.partition(":")
            if not (name := name.strip()) or not command.strip() or name in procs:
                raise ValueError(f"Procfile line {n}: want a new `name: command`, got {line!r}")
            procs[name] = shlex.split(command)
    return procs


def verdict(
    code: int | None, now: float, started: float, beat: float | None, watched: bool
) -> str | None:
    """What to do: "exit" (restart it), "fatal" (stop all), "stale" (kill, restart it), or None.

    The start counts as a beat. A beat from before it is the predecessor's, and one from after
    `now` another clock's (a reboot): neither keeps this run alive.
    """
    if code is not None:
        return "fatal" if code > 1 else "exit"
    last = beat if beat is not None and started <= beat <= now else started
    return "stale" if watched and now - last > STALE else None


def run(procs, state_dir, events, *, stopping, sleep=time.sleep, clock=time.monotonic,
        spawn=_OWN_SESSION) -> int:  # fmt: skip
    """Run `procs` until `stopping` is non-empty (returns 0) or one fails fatally (its code).

    `clock` is the one the workers beat with. Children get their own session, so a Ctrl-C reaches
    only the supervisor, which then stops them in order.
    """
    live, due = {}, {}  # name -> (child, its start time); name -> when a crash looper restarts

    def start(name):
        started = clock()  # before the spawn, so no beat of the new run can predate it
        live[name] = spawn(procs[name]), started

    try:
        for name in procs:
            start(name)
        while not stopping:
            for name in procs:
                if name in due:
                    if clock() >= due[name]:
                        del due[name]
                        start(name)
                    continue
                child, started = live[name]
                watched = name.startswith("worker-")
                beat = state.last_beat(state_dir, name) if watched else None
                now = clock()  # after the read: a beat landing in between must not look future
                why = verdict(child.poll(), now, started, beat, watched)
                if why == "stale":
                    child.kill()  # SIGTERM isn't heard while a native MERGE blocks the main thread
                    child.wait()  # its partition locks are free only once it's gone
                if why:
                    event = {"type": "process_exit", "process": name, "reason": why}
                    events.emit([event | {"code": child.returncode, "pid": child.pid}])
                    if why == "fatal":
                        return child.returncode
                    if now - started < STABLE:
                        due[name] = now + RETRY
                    else:
                        start(name)
            sleep(TICK)
    finally:  # also on a bug here: never leave workers running unsupervised
        children = [child for child, _ in live.values()]
        for child in children:
            child.terminate()
        for child in children:
            try:
                child.wait(GRACE)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
    return 0


def main(procfile: str = "Procfile") -> int:
    # ponytail: ./data and ./state, the workers' defaults. A flag when something needs others.
    procs = parse_procfile(Path(procfile).read_text())
    stopping = []
    for sig in (signal.SIGTERM, signal.SIGINT):  # append can't deadlock the way Event.set can
        signal.signal(sig, lambda *_: stopping.append(True))
    events = EventLog(Path("data") / "events", "supervisor")
    return run(procs, Path("state"), events, stopping=stopping)


if __name__ == "__main__":
    os._exit(main(*sys.argv[1:2]))  # not sys.exit: the entry-point rule (Arrow's exit hang)
