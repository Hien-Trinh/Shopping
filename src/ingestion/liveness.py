"""The streaming job's watchdog: Spark has no task timeout, so a batch that never returns would
hold the job until its run's deadline (docs/specs/step-a6.md). No pyspark import: `queries` are
anything with a StreamingQuery's awaitTermination, lastProgress, status, stop, name and id.
"""

import threading
import time
from collections.abc import Callable, Sequence
from typing import Any


class Stalled(RuntimeError):
    """A query's trigger stayed active past the limit with no finished batch."""


def stalled(
    since: float, mark: Any, seen: Any, active: bool, now: float, limit: float
) -> tuple[float, Any, bool]:
    """One query's timer after a poll, as (since, seen, expired). `mark` names its last finished
    batch. A new mark or no active trigger restarts the timer."""
    if mark != seen or not active:
        return now, mark, False
    return since, seen, now - since > limit


def watch(
    queries: Sequence[Any],
    *,
    limit: float,
    poll: float = 10.0,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Returns when a query ends, raises its error if it failed, and raises Stalled when one
    stalls."""
    timers = {query.id: (clock(), None) for query in queries}
    while True:
        for query in queries:
            if query.awaitTermination(poll / len(queries)):  # the wait between polls
                return
            progress = query.lastProgress
            mark = progress and (progress["batchId"], progress["timestamp"])
            since, seen = timers[query.id]
            active = query.status["isTriggerActive"]
            since, seen, expired = stalled(since, mark, seen, active, clock(), limit)
            timers[query.id] = since, seen
            if expired:
                for each in queries:  # stop() blocks on a batch stuck in Python: never wait for it
                    threading.Thread(target=each.stop, daemon=True).start()
                name = query.name or query.id
                raise Stalled(
                    f"query {name}: no batch finished in {limit:g} s of an active trigger"
                )
