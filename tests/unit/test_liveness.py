"""The watchdog's rule on fake queries and a fake clock (docs/specs/step-a6.md). No test sleeps:
a fake query's awaitTermination moves the clock."""

import threading

import pytest

from ingestion import liveness

LIMIT = 100.0


class Clock:
    now = 0.0

    def __call__(self) -> float:
        return self.now


class Query:
    """A streaming query's face. `batch_at(t)` is the last finished batch's id at time t, `active`
    whether a trigger runs, `ends` when the query ends, `error` what it ends with."""

    def __init__(self, clock, name, *, batch_at=lambda t: None, active=True, ends=None, error=None):
        self.clock, self.name, self.id = clock, name, f"id-{name}"
        self.batch_at, self.active, self.ends, self.error = batch_at, active, ends, error
        self.stopped = threading.Event()

    def awaitTermination(self, timeout: float) -> bool:
        self.clock.now += timeout
        if self.ends is None or self.clock.now < self.ends:
            return False
        if self.error:
            raise self.error
        return True

    @property
    def lastProgress(self):
        batch = self.batch_at(self.clock.now)
        return None if batch is None else {"batchId": batch, "timestamp": f"t{batch}"}

    @property
    def status(self) -> dict:
        return {"isTriggerActive": self.active}

    def stop(self) -> None:
        self.stopped.set()
        threading.Event().wait()  # a batch stuck in Python: stop() never returns (fact 3)


def test_a_trigger_active_past_the_limit_with_no_batch_raises_without_waiting_for_stop():
    clock = Clock()
    query = Query(clock, "ingest")
    with pytest.raises(liveness.Stalled, match="ingest"):
        liveness.watch([query], limit=LIMIT, clock=clock)
    assert LIMIT < clock.now <= LIMIT + 10
    assert query.stopped.wait(5)


@pytest.mark.parametrize(
    "shape",
    [{"batch_at": int}, {"active": False}, {"batch_at": lambda t: 7, "active": False}],
    ids=["a batch at every poll", "idle before any batch", "idle after a batch"],
)
def test_a_query_that_progresses_or_idles_is_never_stalled(shape):
    clock = Clock()
    query = Query(clock, "ingest", ends=10 * LIMIT, **shape)
    assert liveness.watch([query], limit=LIMIT, clock=clock) is None
    assert clock.now == 10 * LIMIT
    assert not query.stopped.is_set()


def test_a_batch_that_finishes_just_under_the_limit_restarts_the_timer():
    clock = Clock()
    query = Query(clock, "ingest", batch_at=lambda t: 0 if t >= 90 else None)
    with pytest.raises(liveness.Stalled):
        liveness.watch([query], limit=LIMIT, clock=clock)
    assert 190 < clock.now <= 200  # the limit after the batch at 90, not after the start


def test_one_stalled_query_fires_while_the_other_finishes_batches():
    clock = Clock()
    stuck, busy = Query(clock, "export"), Query(clock, "ingest", batch_at=int)
    with pytest.raises(liveness.Stalled, match="export"):
        liveness.watch([busy, stuck], limit=LIMIT, clock=clock)
    assert stuck.stopped.wait(5) and busy.stopped.wait(5)  # every query is stopped


def test_a_failed_querys_error_comes_out_of_watch():
    clock = Clock()
    query = Query(clock, "ingest", batch_at=int, ends=50, error=ValueError("the batch failed"))
    with pytest.raises(ValueError, match="the batch failed"):
        liveness.watch([query], limit=LIMIT, clock=clock)


def test_the_rule_restarts_on_a_new_mark_or_no_trigger_and_expires_past_the_limit():
    assert liveness.stalled(0.0, None, None, True, 100.0, LIMIT) == (0.0, None, False)
    assert liveness.stalled(0.0, None, None, True, 100.5, LIMIT) == (0.0, None, True)
    assert liveness.stalled(0.0, (1, "t"), None, True, 500.0, LIMIT) == (500.0, (1, "t"), False)
    assert liveness.stalled(0.0, None, None, False, 500.0, LIMIT) == (500.0, None, False)
