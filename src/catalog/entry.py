"""How a standalone entry point starts and stops.

It refuses a malformed supervisor pid, stops on SIGTERM or SIGINT or when its supervisor dies, and
exits by flushing, then os._exit (plan-v1, Phase 3 carried rules): Arrow can hang at process exit
after a Delta scan (spikes/NOTES.md, "Exit hang"), so no entry point leaves through normal
interpreter shutdown.
"""

import argparse
import contextlib
import os
import signal
import sys
import threading
import time
import traceback
from collections.abc import Callable, Mapping
from pathlib import Path

from catalog.events import prepared

SUPERVISOR = "CATALOG_SUPERVISOR"  # set to the supervisor's pid in its children's environment


def exit_with(main: Callable[[], int | None], codes: Mapping[type, int] | None = None) -> None:
    """Run `main` and exit with its return value: argparse's code for a bad flag, the `codes`
    entry for an exception, else 1."""
    try:
        code = main() or 0
    except SystemExit as e:  # argparse: --help or a bad flag
        code = e.code or 0
    except BaseException as e:
        # OSError only: a broken or closed stderr fd raises it, and one closed at startup is None,
        # which print handles. A ValueError needs our own code to close sys.stderr; none does.
        with contextlib.suppress(OSError):  # a broken pipe: still os._exit, with e's code
            traceback.print_exc()
        code = (codes or {}).get(type(e), 1)
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):  # closed, or a broken pipe: exit anyway
            stream.flush()
    os._exit(code)


def supervisor_pid(parser: argparse.ArgumentParser) -> int | None:
    """The pid of the supervisor that started this process, or None if none did.

    A malformed one is a bad flag: parser.error exits 2, which the supervisor treats as fatal.
    """
    pid = os.environ.get(SUPERVISOR, "")
    if pid and not (pid.isdecimal() and 0 < int(pid) < 2**31):  # 0 or -1 would name a group
        parser.error(f"{SUPERVISOR} must be the pid of the supervisor that started it, got {pid!r}")
    return int(pid) if pid else None


def watch_supervisor(pid: int | None, events_root: Path, process: str, stop=None):
    """Watch supervisor `pid` in a daemon thread, so a kill -9ed supervisor leaves nothing behind;
    no watch when no supervisor started us. Returns `stop`.

    Without a `stop`, it's an Event set by SIGTERM (the supervisor's stop) or SIGINT. A caller's
    own `stop` needs only a set() method and to take a `reason` attribute.
    """
    if stop is None:
        stop = _stop_on_signals()
    if pid:
        args = (lambda: _alive(pid), stop, events_root, process)
        threading.Thread(target=_watch, args=args, daemon=True).start()
    return stop


def _watch(
    alive: Callable[[], bool],
    stop,
    events_root: Path | None = None,
    process: str = "",
    deadline: float = 30.0,
    *,
    sleep: Callable[[float], None] = time.sleep,
    exit: Callable[[int], None] = os._exit,
    clock: Callable[[], float] = time.time,
) -> None:
    """Check `alive()` every second; once it's False, set `stop` (with `stop.reason`), so the tick
    in progress finishes and the claims are released, and exit 1 if the process still runs
    `deadline` seconds later.

    Run it in a daemon thread: a process that stops in time has exited by then. One that hasn't is
    stuck in a native call, and would otherwise hold its locks forever. So `exit` is os._exit with
    no flush, after a `watch_exit` event written by events.prepared: anything more could block on
    what the stuck thread holds.
    """
    while alive():
        sleep(1)
    stop.reason = "supervisor_gone"  # before set(): the stopped thread reads it once set
    stop.set()
    event = {"type": "watch_exit", "process": process, "pid": os.getpid(), "deadline": deadline}
    # stamped with the planned exit time; skipped when there's no events dir (tests)
    trace = events_root and prepared(events_root, process, event, clock() + deadline)
    sleep(deadline)
    if trace:
        trace()
    exit(1)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)  # a killed supervisor exists until reaped, which a shell does at once
    except ProcessLookupError:
        return False
    except PermissionError:  # it exists, as another user's: a wrapper dropped our privileges
        pass
    return True


def _stop_on_signals() -> threading.Event:
    """An Event set by SIGTERM (the supervisor's stop) or SIGINT."""
    stop, stopping = threading.Event(), []

    def on_signal(*_):  # Event.set takes a lock the interrupted thread may hold: set it elsewhere
        if not stopping:  # once: a second signal mid-Thread.start would re-enter its lock
            stopping.append(True)
            threading.Thread(target=stop.set).start()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    return stop
