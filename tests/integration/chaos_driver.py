"""Test only: the worker CLI with one catalog function wrapped to go wrong at its Nth call.

    python chaos_driver.py [MODULE.FUNCTION WHEN N] -- WORKER_ARGS...

WHEN is before or after: SIGKILL itself just before or after the call; a number: SIGKILL that
many ms into it (delta-rs releases the GIL); slow: the call takes 2 s longer; hang: it never
returns, like one stuck in a native call, and the worker exits 1 s after its supervisor dies
rather than 30. The worker compacts after every busy batch rather than every 100.
"""

import functools
import importlib
import itertools
import os
import signal
import sys
import threading
import time

from catalog import entry, worker


def die():
    os.kill(os.getpid(), signal.SIGKILL)


def wrap(target: str, when: str, n: int) -> None:
    if when not in ("before", "after", "slow", "hang") and not when.isdecimal():
        raise ValueError(f"unknown WHEN {when!r}")  # a typo must not quietly test nothing
    module, name = target.rsplit(".", 1)
    module = importlib.import_module(f"catalog.{module}")
    real, calls = getattr(module, name), itertools.count(1)

    def wrapped(*args, **kwargs):
        nth = next(calls) == n
        if nth and when == "before":
            die()
        if nth and when.isdecimal():
            threading.Timer(int(when) / 1000, die).start()
        if nth and when == "slow":
            time.sleep(2)
        if nth and when == "hang":
            print(f"hung in {target}", file=sys.stderr, flush=True)
            time.sleep(3600)
        result = real(*args, **kwargs)
        if nth and when == "after":
            die()
        return result

    setattr(module, name, wrapped)
    if when == "hang":
        entry._watch = functools.partial(entry._watch, deadline=1)


if __name__ == "__main__":
    split = sys.argv.index("--")
    if split > 1:
        target, when, n = sys.argv[1:split]
        wrap(target, when, int(n))
    worker.run = functools.partial(worker.run, compact_every=1)
    entry.exit_with(lambda: worker.main(sys.argv[split + 1 :]), worker.FATAL)
