"""The load generator's Changes and one process's send loop (docs/specs/step-7a.md)."""

import queue
import threading
from http.client import IncompleteRead
from types import SimpleNamespace

import pytest

from catalog import envelope, load

START_MS = 1_790_000_000_000
TEXTS = load.texts()


def changes(n, **options):
    options = {"seed": 7, "start_ms": START_MS, "keys": 50, "order": "random"} | options
    return [load.change(i, texts=TEXTS, currency="USD", **options) for i in range(n)]


def test_the_same_seed_gives_the_same_changes():
    assert changes(20) == changes(20)
    assert changes(20) != changes(20, seed=8)


def test_a_change_is_pinned_so_every_process_and_run_agrees():
    # Not just equal twice in one process: a hash-randomized seed would pass that.
    got = load.change(0, seed=7, start_ms=1, keys=50, order="random", texts=[("t", "d")],
                      currency="USD")  # fmt: skip
    assert got["merchant_product_id"] == "p44"
    assert got["listing"]["price_micros"] == 775063189


def test_every_change_passes_the_envelope():
    checked = envelope.check_batch(
        {"changes": changes(len(TEXTS) + 1)}, merchant_id="m_a", currency="USD", now_ms=START_MS
    )
    assert checked.rejected == []
    assert len(checked.accepted) == len(TEXTS) + 1


def test_keys_stay_in_the_key_space_and_sequential_order_hits_each_once():
    assert {c["merchant_product_id"] for c in changes(500)} <= {f"p{n}" for n in range(50)}
    sequential = [c["merchant_product_id"] for c in changes(50, order="sequential")]
    assert sequential == [f"p{n}" for n in range(50)]


def test_source_versions_grow_with_i():
    assert [c["source_version"] for c in changes(5)] == [START_MS + i for i in range(5)]


def test_deletes_come_at_the_asked_fraction_from_the_seed():
    got = changes(10_000, deletes=0.1)
    assert got == changes(10_000, deletes=0.1)
    deletes = [c for c in got if c["op"] == "delete"]
    assert 900 < len(deletes) < 1100
    assert all("listing" not in c for c in deletes)
    assert [c["op"] for c in changes(100)] == ["upsert"] * 100  # default 0: none


def test_deletes_pass_the_envelope():
    checked = envelope.check_batch(
        {"changes": changes(200, deletes=0.5)}, merchant_id="m_a", currency="USD",
        now_ms=START_MS,
    )  # fmt: skip
    assert checked.rejected == []
    assert {c.op for _, c in checked.accepted} == {"upsert", "delete"}


def test_start_ms_fixes_every_source_version(monkeypatch, capsys):
    seen = {}
    monkeypatch.setenv("CATALOG_API_KEY", "k")
    monkeypatch.setattr(load, "collect", lambda procs, *_: seen.setdefault("procs", procs)
                        and ({}, []))  # fmt: skip
    monkeypatch.setattr(load.multiprocessing, "get_context", lambda _: Context(seen))
    load.main(["--changes", "1", "--start-ms", "123", "--deletes", "0.25"])
    (options,) = seen["options"]
    assert (options["start_ms"], options["deletes"]) == (123, 0.25)
    assert '"start_ms": 123' in capsys.readouterr().out


class Context:
    """multiprocessing's spawn context, recording each Process's options instead of starting it."""

    def __init__(self, seen):
        self.seen = seen

    def Event(self):  # noqa: N802
        return threading.Event()

    def Queue(self):  # noqa: N802
        return queue.Queue()

    def Process(self, target, args):  # noqa: N802
        self.seen.setdefault("options", []).append(args[1])
        return SimpleNamespace(start=lambda: None, join=lambda: None, exitcode=0)


def test_batches_cover_a_process_share_in_order():
    got = [
        [c["source_version"] - START_MS for c in body["changes"]]
        for body in load.batches(
            1,
            2,
            changes=7,
            batch=2,
            seed=7,
            start_ms=START_MS,
            keys=50,
            order="random",
            texts=TEXTS,
            currency="USD",
        )  # fmt: skip
    ]
    assert got == [[2, 3], [6]]


class Clock:
    def __init__(self):
        self.now, self.slept = 100.0, []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def batches_of(*sizes):
    return [{"changes": [{}] * n} for n in sizes]


def test_sends_are_due_at_t0_plus_changes_over_rate_and_a_late_one_goes_at_once():
    clock, sent = Clock(), []

    def post(body):
        sent.append(clock.now)
        if len(sent) == 2:
            clock.now += 5  # a slow answer: the third send is late
        return 202, {"accepted": len(body["changes"])}

    tally = load.send(batches_of(2, 2, 2, 2), post, rate=1, clock=clock, sleep=clock.sleep)
    assert sent == [100, 102, 107, 107]  # the 4th, due at 106, goes at once
    assert tally["late"] == pytest.approx(3)
    assert (tally["sent"], tally["accepted"], tally["statuses"]) == (8, 8, {202: 4})


def test_statuses_and_errors_are_counted_never_raised():
    cut_off, not_json = IncompleteRead(b""), ValueError("Expecting value")
    answers = iter([(503, {}), OSError("refused"), TimeoutError(), cut_off, not_json,
                    (202, {"accepted": 1})])  # fmt: skip

    def post(_):
        answer = next(answers)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    tally = load.send(batches_of(1, 1, 1, 1, 1, 1), post, rate=0)
    assert (tally["statuses"], tally["errors"]) == ({503: 1, 202: 1}, 4)
    assert (tally["sent"], tally["accepted"]) == (6, 1)
    assert len(tally["latencies"]) == 6


def test_a_401_stops_the_loop():
    calls = []
    tally = load.send(batches_of(1, 1, 1), lambda b: calls.append(b) or (401, {}), rate=0)
    assert len(calls) == 1
    assert tally["statuses"] == {401: 1}


def test_rate_zero_never_sleeps():
    def no_sleep(_):
        pytest.fail("slept")

    load.send(batches_of(1, 1), lambda b: (202, {"accepted": 1}), rate=0, sleep=no_sleep)


def test_a_stop_ends_the_loop_before_the_next_send():
    calls = []
    load.send(batches_of(1, 1), lambda b: calls.append(b) or (202, {}), rate=0, stop=lambda: True)
    assert calls == []


def tally(*latencies):
    return {"sent": 1, "accepted": 1, "errors": 0, "late": 0.0, "latencies": list(latencies),
            "statuses": {202: 1}}  # fmt: skip


def test_latency_percentiles_are_nearest_rank():
    got = load.summary([tally(*(ms / 1000 for ms in range(100, 0, -1)))], 1.0, START_MS)
    assert got["latency_ms"] == {"p50": 50, "p99": 99, "max": 100}
    two = load.summary([tally(0.001, 0.002)], 1.0, START_MS)["latency_ms"]
    assert (two["p50"], two["p99"]) == (1, 2)
    assert load.summary([], 1.0, START_MS)["latency_ms"] is None


class Stopping:
    def __init__(self):
        self.stopped = False

    def set(self):
        self.stopped = True


def test_a_child_that_dies_without_a_tally_is_reported_not_waited_on():
    results = queue.Queue()
    results.put((0, tally(0.1)))
    procs = [SimpleNamespace(exitcode=0), SimpleNamespace(exitcode=-9)]  # 1: kill -9ed
    got = []
    stopping = Stopping()
    waiter = threading.Thread(
        target=lambda: got.append(load.collect(procs, results, stopping, poll=0.01)), daemon=True
    )
    waiter.start()
    waiter.join(timeout=5)
    assert got, "collect waited forever on the dead child"
    tallies, failed = got[0]
    assert (list(tallies), failed) == ([0], [1])


def test_ctrl_c_sets_stopping_and_still_collects_every_tally():
    class Interrupted(queue.Queue):
        def get(self, timeout=None):
            if not self.hit:
                self.hit = True
                raise KeyboardInterrupt
            return super().get(timeout=timeout)

    results, stopping = Interrupted(), Stopping()
    results.hit = False
    results.put((0, tally(0.1)))
    tallies, failed = load.collect([SimpleNamespace(exitcode=None)], results, stopping)
    assert stopping.stopped
    assert (list(tallies), failed) == ([0], [])
