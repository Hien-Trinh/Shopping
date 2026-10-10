"""The load generator's Changes and one process's send loop (docs/specs/step-7a.md)."""

import math
import queue
import threading
import time
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


def test_unique_text_gives_every_listing_its_own_description_ending_in_its_key():
    # 2,000 keys each once: the labeled set has about 1,000 texts, so without the flag many
    # Listings share one; with it each description ends in the Listing's key and is its own.
    plain = changes(2000, keys=2000, order="sequential")
    unique = changes(2000, keys=2000, order="sequential", unique_text=True)
    assert len({c["listing"]["description"] for c in plain}) < 2000
    assert len({c["listing"]["description"] for c in unique}) == 2000
    assert all(c["listing"]["description"].endswith(" " + c["merchant_product_id"]) for c in unique)
    checked = envelope.check_batch(
        {"changes": unique[:1000]}, merchant_id="m_a", currency="USD", now_ms=START_MS
    )
    assert checked.rejected == []  # still inside the description limit


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


def test_seconds_alone_sends_until_the_time_limit(monkeypatch, capsys):
    seen = {}
    monkeypatch.setenv("CATALOG_API_KEY", "k")
    monkeypatch.setattr(load, "collect", lambda procs, *_: seen.setdefault("procs", procs)
                        and ({}, []))  # fmt: skip
    monkeypatch.setattr(load.multiprocessing, "get_context", lambda _: Context(seen))
    clock = SimpleNamespace(monotonic=lambda: 1000.0, time_ns=time.time_ns)
    monkeypatch.setattr(load, "time", clock)  # load's clock only, not pytest's
    load.main(["--seconds", "1800", "--processes", "2"])
    options = seen["options"][0]
    assert (options["changes"], options["deadline"]) == (load.MAX_CHANGES, 2800.0)
    assert "seconds" not in options  # batches() takes every other option


def test_no_seconds_means_no_deadline_and_3000_changes(monkeypatch, capsys):
    seen = {}
    monkeypatch.setenv("CATALOG_API_KEY", "k")
    monkeypatch.setattr(load, "collect", lambda procs, *_: seen.setdefault("procs", procs)
                        and ({}, []))  # fmt: skip
    monkeypatch.setattr(load.multiprocessing, "get_context", lambda _: Context(seen))
    load.main(["--processes", "1"])
    (options,) = seen["options"]
    assert (options["changes"], options["deadline"]) == (3000, math.inf)


@pytest.mark.parametrize("seconds", ["0", "-1", "nan", "inf"])
def test_a_seconds_that_isnt_positive_and_finite_is_refused(seconds, monkeypatch, capsys):
    monkeypatch.setenv("CATALOG_API_KEY", "k")
    with pytest.raises(SystemExit) as stopped:
        load.main(["--seconds", seconds])
    assert stopped.value.code == 2
    assert "--seconds" in capsys.readouterr().err


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


def test_the_deadline_ends_the_loop_before_a_send_due_at_or_after_it():
    clock, sent = Clock(), []
    post = lambda body: sent.append(clock.now) or (202, {"accepted": 1})  # noqa: E731
    tally = load.send(batches_of(1, 1, 1, 1, 1), post, rate=1, clock=clock, sleep=clock.sleep,
                      deadline=102.5)  # fmt: skip
    assert sent == [100, 101, 102]  # the 4th is due at 103: not slept toward, not sent
    assert clock.slept == [1, 1]
    assert (tally["sent"], tally["timed_out"]) == (3, True)


def test_an_unpaced_loop_checks_the_deadline_on_the_clock():
    clock = Clock()

    def post(body):
        clock.now += 1
        return 202, {"accepted": 1}

    tally = load.send(batches_of(1, 1, 1, 1), post, rate=0, clock=clock, deadline=102)
    assert (tally["sent"], tally["timed_out"]) == (2, True)
    assert load.send(batches_of(1), post, rate=0, clock=clock)["timed_out"] is False


def test_a_late_loop_stops_at_the_deadline_though_its_sends_were_due_before_it():
    clock = Clock()

    def post(body):
        clock.now += 5  # each answer takes 5 s at 1/s: every send after the first is late
        return 202, {"accepted": 1}

    tally = load.send(batches_of(1, 1, 1, 1), post, rate=1, clock=clock, sleep=clock.sleep,
                      deadline=107)  # fmt: skip
    assert (tally["sent"], tally["timed_out"]) == (2, True)  # sends at 100 and 105, not 110


def tally(*latencies, timed_out=False):
    return {"sent": 1, "accepted": 1, "errors": 0, "late": 0.0, "latencies": list(latencies),
            "statuses": {202: 1}, "timed_out": timed_out}  # fmt: skip


def test_the_summary_says_whether_the_time_limit_or_the_changes_ended_the_run():
    assert load.summary([tally(0.1), tally(0.1)], 1.0, START_MS)["ended_by"] == "changes"
    timed = load.summary([tally(0.1), tally(0.1, timed_out=True)], 1.0, START_MS)
    assert timed["ended_by"] == "seconds"
    refused = tally(0.1) | {"statuses": {401: 1}}
    assert load.summary([tally(0.1), refused], 1.0, START_MS)["ended_by"] == "401"
    assert load.summary([tally(0.1)], 1.0, START_MS, stopped=True)["ended_by"] == "ctrl_c"


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
