"""The load generator's Changes and one process's send loop (docs/specs/step-7a.md)."""

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
    answers = iter([(503, {}), OSError("refused"), TimeoutError(), (202, {"accepted": 1})])

    def post(_):
        answer = next(answers)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    tally = load.send(batches_of(1, 1, 1, 1), post, rate=0)
    assert (tally["statuses"], tally["errors"]) == ({503: 1, 202: 1}, 2)
    assert (tally["sent"], tally["accepted"]) == (4, 1)
    assert len(tally["latencies"]) == 4


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
