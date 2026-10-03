"""Test only: the worker CLI with one catalog function wrapped to go wrong at its Nth call.

    python chaos_driver.py [MODULE.FUNCTION WHEN N] -- WORKER_ARGS...

WHEN is before or after: SIGKILL itself just before or after the call; during: a few ms into
it (delta-rs releases the GIL); hang: the call never returns, like one stuck in a native call.
The worker compacts after every busy batch rather than every 100, and exits 1 s after its
supervisor dies rather than 30.
"""

import functools
import importlib
import itertools
import os
import random
import signal
import sys
import threading
import time

from catalog import entry, worker


def die():
    os.kill(os.getpid(), signal.SIGKILL)


def wrap(target: str, when: str, n: int) -> None:
    module, name = target.rsplit(".", 1)
    module = importlib.import_module(f"catalog.{module}")
    real, calls = getattr(module, name), itertools.count(1)

    def wrapped(*args, **kwargs):
        nth = next(calls) == n
        if nth and when == "before":
            die()
        if nth and when == "during":
            threading.Timer(random.uniform(0.002, 0.01), die).start()
        if nth and when == "hang":
            time.sleep(3600)
        result = real(*args, **kwargs)
        if nth and when == "after":
            die()
        return result

    setattr(module, name, wrapped)


if __name__ == "__main__":
    split = sys.argv.index("--")
    if split > 1:
        target, when, n = sys.argv[1:split]
        wrap(target, when, int(n))
    worker.run = functools.partial(worker.run, compact_every=1)
    worker.watch = functools.partial(worker.watch, deadline=1)
    entry.exit_with(lambda: worker.main(sys.argv[split + 1 :]), worker.FATAL)
