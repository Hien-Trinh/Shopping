import itertools

import pytest
from support import listing, poison, product_in, reclassify, up

from catalog import jev, store
from catalog.classify import UNCATEGORIZED, FakeClassifier
from catalog.decide import decide
from catalog.keys import partition
from catalog.landing import Landed
from catalog.plan import Classification, Stored

pytestmark = pytest.mark.filterwarnings("ignore::UserWarning")  # pydantic, on poison content

A, B, C = product_in(3), product_in(40), product_in(5)
SHIRT, HAT = Classification("Fake > S", 0.9, "fake-1"), Classification("Fake > H", 0.9, "fake-1")


def landed(*changes, submission="s1") -> list[Landed]:
    return [Landed(c, submission, i, partition(*c.key), (1, i)) for i, c in enumerate(changes)]


def clock():
    """A counter clock: each classifier call takes 250 ms."""
    return itertools.count(10.0, 0.25).__next__


def run(changes, stored=None, classifier=None):
    return decide(landed(*changes), stored or {}, classifier or FakeClassifier(), clock())


def event(i, c, type, **more) -> dict:
    return {
        "submission_id": "s1",
        "change_index": i,
        "merchant_id": c.merchant_id,
        "merchant_product_id": c.merchant_product_id,
        "partition": partition(*c.key),
        "type": type,
        **more,
    }


def written(i, c, type="written"):
    return event(i, c, type, store_version=42)


def classify(listings):
    return {"type": "classify", "listings": listings, "ms": 250}


def failed_note(listings, partitions, reason, batch, error):
    return {
        "type": "classify_failed",
        "listings": listings,
        "partitions": partitions,
        "reason": reason,
        "batch": batch,
        "error": error,
    }


class Stub:
    """A classifier answering `answer(listings)`, and saying why it missed some with `error`."""

    taxonomy_version = "fake-1"

    def __init__(self, answer, error=None):
        self.answer, self.error, self.calls = answer, error, []

    def classify(self, listings):
        self.calls.append(len(listings))
        return self.answer(listings)


def classification(d, mpid):
    (w,) = [w for w in d.writes if w.key == ("m_1", mpid)]
    return w.classification


def test_answers_go_to_their_own_listings():
    changes = [up(A, 1, listing("Shirt")), up(B, 1, listing("Hat"))]
    d = run(changes)
    assert (classification(d, A), classification(d, B)) == (SHIRT, HAT)
    assert not any(w.needs_classify for w in d.writes)
    assert d.events(42) == [classify(2), written(0, changes[0]), written(1, changes[1])]


@pytest.mark.parametrize(
    "answer",
    [[], [("Fake > S", 0.9)] * 2, [(None, 0.5)], [("", 0.5)], [("x", float("nan"))], [("x", 1.5)],
     [("x", -0.1)], [("x", None)], ["x"]],
)  # fmt: skip
def test_a_bad_answer_counts_as_a_failure(answer):
    changes = [up(A, 1)]
    d = run(changes, classifier=Stub(lambda _: answer))
    assert classification(d, A) == Classification(UNCATEGORIZED, 0.0, "fake-1", True)
    (failed,) = [e for e in d.events(42) if e["type"] == "classify_failed"]
    assert (failed["listings"], failed["partitions"], failed["reason"]) == (1, [3], "error")


def test_each_classifier_call_logs_its_size_and_time():  # step 7c.2
    changes = [up(A, 1, listing("Shirt")), up(B, 1, listing("Hat"))]
    assert run(changes).events(42)[0] == classify(2)
    again = [up(A, 1, listing("Shirt"))]  # already applied: nothing to classify
    d = run(again, {("m_1", A): Stored(1, listing("Shirt"), SHIRT)})
    assert d.events(42) == [event(0, again[0], "already_applied")]


def test_an_outage_keeps_a_good_answer_whose_inputs_are_unchanged():
    stored = {
        ("m_1", A): Stored(1, listing("Shirt"), SHIRT),
        ("m_1", B): Stored(1, listing("Hat"), HAT),
    }
    changes = [reclassify(A), up(B, 2, listing("Cap"))]
    d = run(changes, stored, FakeClassifier(fail=True))
    assert classification(d, A) == Classification("Fake > S", 0.9, "fake-1", True)
    assert classification(d, B) == Classification(UNCATEGORIZED, 0.0, "fake-1", True)
    assert d.events(42) == [
        classify(2),
        failed_note(2, [3, 40], "error", 2, "RuntimeError('classifier unavailable')"),
        written(0, changes[0], "reclassified"),
        written(1, changes[1]),
    ]


def test_a_huge_classifier_error_is_truncated():
    def boom(listings):
        raise RuntimeError("x" * 10_000)

    (_, failed) = run([up(A, 1)], classifier=Stub(boom)).events(42)[:2]
    assert len(failed["error"]) == 500
    assert (failed["reason"], failed["batch"]) == ("error", 1)  # step 7c.2: no text matching


def test_a_refused_key_is_raised_not_stored_uncategorized():  # the worker exits 7
    def refused(listings):
        raise jev.KeyMissing("TYPESAFE_API_KEY was refused (401)")

    with pytest.raises(jev.KeyMissing):
        run([up(A, 1)], classifier=Stub(refused))


def test_listings_the_classifier_did_not_reach_take_the_failure_path():
    reached = {"Hat": ("Fake > H", 0.8)}  # the budget ran out before Shirt and Cup
    stub = Stub(lambda ls: [reached.get(x.title) for x in ls])
    changes = [up(B, 1, listing("Hat")), reclassify(A), up(C, 1, listing("Cup"))]
    d = run(changes, {("m_1", A): Stored(1, listing("Shirt"), SHIRT)}, stub)
    assert classification(d, B) == Classification("Fake > H", 0.8, "fake-1")
    assert classification(d, A) == Classification("Fake > S", 0.9, "fake-1", True)
    assert classification(d, C) == Classification(UNCATEGORIZED, 0.0, "fake-1", True)
    assert d.events(42) == [
        classify(3),
        failed_note(2, [3, 5], "budget", 3, "budget spent"),  # step 7c.2
        written(0, changes[0]),
        written(1, changes[1], "reclassified"),
        written(2, changes[2]),
    ]


def test_classify_failed_names_the_classifiers_error():  # PR #63
    error = "RuntimeError('Jev answered 429')"
    d = run([up(A, 1)], classifier=Stub(lambda ls: [None] * len(ls), error))
    (failed,) = [e for e in d.events(42) if e["type"] == "classify_failed"]
    assert (failed["error"], failed["reason"]) == (error, "error")  # it said why: not the budget


def test_a_poison_change_is_isolated_and_skipped():
    changes = [up(A, 1), up(B, 1), poison(A, 2), up(B, 2), poison(B, 3), up(A, 3, listing("New"))]
    d = run(changes)
    assert [(w.key[1], w.source_version) for w in d.writes] == [(A, 3), (B, 2)]
    (error,) = set(store.unstorable([poison(A, 2).listing]))
    assert "free" in error
    assert d.events(42) == [
        classify(2),
        *(written(i, changes[i]) for i in (0, 1)),
        event(2, changes[2], "failed", error=error),
        written(3, changes[3]),
        event(4, changes[4], "failed", error=error),
        written(5, changes[5]),
    ]


@pytest.mark.parametrize(
    ("bad", "error"),
    [
        ({"price_micros": "free"}, "ArrowInvalid"),
        ({"title": 123}, "ArrowTypeError"),
        ({"price_micros": 2**70}, "OverflowError"),
    ],
)
def test_every_kind_of_unstorable_data_fails_only_its_change(bad, error):
    d = run([up(A, 1), poison(B, 1, **bad), up(B, 2)])
    outcomes = [e for e in d.events(42) if "change_index" in e]
    assert [e["type"] for e in outcomes] == ["written", "failed", "written"]
    assert outcomes[1]["error"].startswith(error)


def test_a_poison_change_never_reaches_the_classifier():
    stub = Stub(FakeClassifier().classify)
    run([up(A, 1), poison(B, 1), up(B, 2, listing("Hat"))], classifier=stub)
    assert stub.calls == [2]  # one call, for the two good Listings


def test_a_long_error_is_truncated():
    changes = [poison(A, 1, price_micros="x" * 5000)]  # Arrow echoes the value in its message
    (failed,) = run(changes).events(42)
    assert len(failed["error"]) == 500
