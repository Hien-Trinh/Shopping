import random
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from support import delete, listing, product_in, reclassify, up

from catalog import events, landing, state, store, worker
from catalog.classify import UNCATEGORIZED, FakeClassifier
from catalog.envelope import Change, Content
from catalog.events import EventLog
from catalog.keys import PARTITIONS
from catalog.landing import START
from catalog.replay import diff, expected_store
from catalog.status import fold

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
pytestmark = pytest.mark.filterwarnings("ignore::UserWarning")  # pydantic, on poison content


def poison(mpid, sv, **bad) -> Change:
    """An upsert whose content slipped past validation: storing it fails on the data itself."""
    content = Content.model_construct(**listing().model_dump() | (bad or {"price_micros": "free"}))
    return Change("m_1", mpid, sv, "upsert", content)


def is_poison(c: Change) -> bool:  # the default poison above
    return c.listing is not None and c.listing.price_micros == "free"


A, B = product_in(3), product_in(40)


class Env:
    def __init__(self, tmp_path, classifier=None):
        self.tmp = tmp_path
        self.landing = landing.ensure(str(tmp_path / "landing"))
        self.store = store.ensure(str(tmp_path / "store"))
        self.state, self.events_root = tmp_path / "state", tmp_path / "events"
        self.log = EventLog(self.events_root, "worker-0", clock=lambda: NOW.timestamp())
        self.classifier = classifier or FakeClassifier()
        self.landed = []
        self.offsets = self.load_offsets()

    def load_offsets(self):
        table = landing.table_id(self.landing)
        return state.load_offsets(self.state, range(PARTITIONS), table)

    def land(self, *changes, submission="s1"):
        entries = [(submission, len(self.landed) + i, c) for i, c in enumerate(changes)]
        landing.append(self.landing, entries, NOW)
        self.landed += changes

    def run(self, limit=1000):
        self.offsets = worker.process_batch(
            self.landing, self.store, self.classifier, self.log, self.state, self.offsets,
            limit=limit, now=lambda: NOW,
        )  # fmt: skip

    def drain(self, limit=1000):
        for _ in range(100):
            before = self.offsets
            self.run(limit)
            if self.offsets == before:
                return
        raise AssertionError("worker never caught up")

    def outcomes(self, submission="s1"):
        return fold(submission, events.read(self.events_root)).outcomes


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def test_one_batch_end_to_end(env):
    env.land(up(A, 1, listing("Shirt")), up(B, 1), delete(B, 2), up(A, 1, listing("Other")))
    env.run()
    rows = store.read(env.store, [("m_1", A), ("m_1", B)])
    assert rows[("m_1", A)].classification.category == "Fake > S"
    assert rows[("m_1", B)].listing is None  # Tombstone
    assert env.outcomes() == {0: "written", 1: "written", 2: "written", 3: "conflict"}
    assert env.load_offsets() == env.offsets  # saved, and the next run starts there
    written = [e for e in events.read(env.events_root) if e["type"] == "written"]
    assert {e["store_version"] for e in written} == {env.store.version()}


def test_idle_offsets_move_past_commits_of_other_partitions(env):
    env.land(up(A, 1))
    env.offsets = worker.process_batch(
        env.landing, env.store, env.classifier, env.log, env.state, {40: START}, now=lambda: NOW
    )
    assert env.offsets == {40: (env.landing.version() + 1, 0)}
    assert store.read(env.store, [("m_1", A)]) == {}  # partition 3 isn't ours


def test_classifier_failure_marks_uncategorized_then_backfill_fixes_it(tmp_path):
    env = Env(tmp_path, FakeClassifier(fail=True))
    env.land(up(A, 1, listing("Shirt")))
    env.run()
    cls = store.read(env.store, [("m_1", A)])[("m_1", A)].classification
    assert (cls.category, cls.needs_reclassify) == (UNCATEGORIZED, True)
    assert [e["listings"] for e in events.read(env.events_root) if "listings" in e] == [1]
    env.classifier.fail = False
    env.land(reclassify(A), submission="backfill")
    env.run()
    cls = store.read(env.store, [("m_1", A)])[("m_1", A)].classification
    assert (cls.category, cls.needs_reclassify) == ("Fake > S", False)
    assert env.outcomes("backfill") == {1: "reclassified"}


def category(env, mpid):
    return store.read(env.store, [("m_1", mpid)])[("m_1", mpid)].classification


def test_answers_go_to_their_own_listings(env):
    env.land(up(A, 1, listing("Shirt")), up(B, 1, listing("Hat")))
    env.run()
    assert (category(env, A).category, category(env, B).category) == ("Fake > S", "Fake > H")


@pytest.mark.parametrize(
    "answer",
    [[], [("Fake > S", 0.9)] * 2, [(None, 0.5)], [("", 0.5)], [("x", float("nan"))], [("x", 1.5)],
     [("x", -0.1)], [("x", None)], ["x"]],
)  # fmt: skip
def test_a_bad_answer_counts_as_a_failure(env, monkeypatch, answer):
    monkeypatch.setattr(env.classifier, "classify", lambda listings: answer)
    env.land(up(A, 1))
    env.run()
    assert (category(env, A).category, category(env, A).needs_reclassify) == (UNCATEGORIZED, True)
    (failed,) = [e for e in events.read(env.events_root) if e["type"] == "classify_failed"]
    assert (failed["listings"], failed["partitions"]) == (1, [3])


def test_an_outage_keeps_a_good_answer_whose_inputs_are_unchanged(env):
    env.land(up(A, 1, listing("Shirt")), up(B, 1, listing("Hat")))
    env.run()
    env.classifier.fail = True
    env.land(reclassify(A), up(B, 2, listing("Cap")), submission="s2")
    env.run()
    assert (category(env, A).category, category(env, A).needs_reclassify) == ("Fake > S", True)
    assert (category(env, B).category, category(env, B).needs_reclassify) == (UNCATEGORIZED, True)


def test_a_huge_classifier_error_is_truncated(env, monkeypatch):
    def boom(listings):
        raise RuntimeError("x" * 10_000)

    monkeypatch.setattr(env.classifier, "classify", boom)
    env.land(up(A, 1))
    env.run()
    (failed,) = [e for e in events.read(env.events_root) if e["type"] == "classify_failed"]
    assert len(failed["error"]) == 500


def test_only_moved_offsets_are_saved(env, monkeypatch):
    env.land(up(A, 1))
    env.run()
    saved = []
    monkeypatch.setattr(state, "save_offsets", lambda _dir, offsets, _table: saved.append(offsets))
    env.run()  # nothing new: no partition moves
    assert saved == [{}]


def failed_indexes(env):
    return {e["change_index"] for e in events.read(env.events_root) if e["type"] == "failed"}


def test_a_poison_change_is_isolated_and_skipped(env):
    env.land(up(A, 1), up(B, 1), poison(A, 2), up(B, 2), poison(B, 3), up(A, 3, listing("New")))
    env.run()
    assert env.outcomes() == {0: "written", 1: "written", 2: "failed", 3: "written",
                              4: "failed", 5: "written"}  # fmt: skip
    assert failed_indexes(env) == {2, 4}
    assert diff(expected_store(env.landed, {2, 4}), store.fingerprints(env.store)) == []
    assert env.load_offsets()[3] != START  # the partition keeps moving
    (error,) = {e["error"] for e in events.read(env.events_root) if e["type"] == "failed"}
    assert "free" in error


@pytest.mark.parametrize(
    ("bad", "error"),
    [
        ({"price_micros": "free"}, "ArrowInvalid"),
        ({"title": 123}, "ArrowTypeError"),
        ({"price_micros": 2**70}, "OverflowError"),
    ],
)
def test_every_kind_of_unstorable_data_fails_only_its_change(env, bad, error):
    env.land(up(A, 1), poison(B, 1, **bad), up(B, 2))
    env.run()
    assert env.outcomes() == {0: "written", 1: "failed", 2: "written"}
    (failed,) = [e for e in events.read(env.events_root) if e["type"] == "failed"]
    assert failed["error"].startswith(error)


@pytest.mark.parametrize("limit", [1, 2, 10])
def test_a_superseded_poison_change_fails_whatever_the_batch_size(tmp_path, limit):
    env = Env(tmp_path)
    env.land(poison(A, 2), up(A, 3))  # plan would fold both into A@3, hiding the poison
    env.drain(limit)
    assert env.outcomes() == {0: "failed", 1: "written"}


def test_a_poison_change_never_reaches_the_classifier(env, monkeypatch):
    calls = []
    real = env.classifier.classify
    monkeypatch.setattr(env.classifier, "classify", lambda ls: calls.append(len(ls)) or real(ls))
    env.land(up(A, 1), poison(B, 1), up(B, 2, listing("Hat")))
    env.run()
    assert calls == [2]  # one call, for the two good Listings


def test_a_long_error_is_truncated(env):
    env.land(poison(A, 1, price_micros="x" * 5000))  # Arrow echoes the value in its message
    env.run()
    (failed,) = [e for e in events.read(env.events_root) if e["type"] == "failed"]
    assert len(failed["error"]) == 500


def assert_nothing_advanced(env):
    assert env.load_offsets() == {p: START for p in range(PARTITIONS)}
    assert [e for e in events.read(env.events_root) if "change_index" in e] == []


def test_a_storage_error_never_fails_changes_or_moves_offsets(env, monkeypatch):
    calls = []

    def disk_full(*args):
        calls.append(args)
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(store, "merge", disk_full)
    env.land(up(A, 1), up(B, 1))
    with pytest.raises(OSError):
        env.run()
    assert len(calls) == 1  # tried once: it isn't the data's fault
    assert_nothing_advanced(env)


def test_a_corrupt_store_file_is_a_storage_error_not_bad_data(env):
    env.land(up(A, 1))
    env.run()
    for f in (env.tmp / "store" / "partition=3").glob("*.parquet"):
        f.write_bytes(b"\0" * f.stat().st_size)  # pyarrow raises ArrowInvalid reading it
    saved = env.load_offsets()
    env.land(up(A, 2), submission="s2")
    with pytest.raises(pa.ArrowInvalid):
        env.run()
    assert env.load_offsets() == saved
    assert failed_indexes(env) == set()


def test_a_bug_in_our_code_is_not_blamed_on_the_data(env):
    env.land(up(A, 1), up(B, 1))
    with pytest.raises(ValueError, match="timezone-aware"):
        worker.process_batch(
            env.landing, env.store, env.classifier, env.log, env.state, env.offsets,
            now=lambda: datetime(2026, 9, 30, 12),  # naive: store.merge refuses it
        )  # fmt: skip
    assert_nothing_advanced(env)


def test_being_overtaken_by_another_writer_stops_the_batch(env, monkeypatch):
    monkeypatch.setattr(store, "merge", lambda *a: store.Merged(0, 1))
    env.land(up(A, 1))
    with pytest.raises(worker.Overtaken, match=r"applied 0 of 1 rows \(partitions \[3\]\)"):
        env.run()
    assert_nothing_advanced(env)


def test_crash_before_offsets_replays_safely(env, monkeypatch):
    env.land(up(A, 1), up(B, 1))

    def killed(*_):
        raise OSError("killed before saving offsets")

    monkeypatch.setattr(state, "save_offsets", killed)
    with pytest.raises(OSError):
        env.run()
    monkeypatch.undo()
    assert env.load_offsets()[3] == START  # nothing saved: the batch replays
    env.run()
    replayed = [e["type"] for e in events.read(env.events_root)][-2:]
    assert replayed == ["already_applied", "already_applied"]
    assert env.outcomes() == {0: "written", 1: "written"}  # best outcome survives the replay


@pytest.mark.parametrize("seed", range(5))
def test_replay_oracle_for_any_batching(env, seed):
    rng = random.Random(seed)
    keys = [product_in(p) for p in (3, 3, 17, 40)] + [product_in(3, prefix="alt")]
    for _ in range(6):  # several commits of mixed changes, out of order and duplicated
        commit = []
        for _ in range(rng.randint(1, 8)):
            k, sv = rng.choice(keys), rng.randint(1, 6)
            make = rng.choices([up, delete, reclassify, poison], weights=[4, 2, 1, 1])[0]
            if make is up:
                commit.append(up(k, sv, listing(rng.choice("ABC"))))
            else:
                commit.append(make(k) if make is reclassify else make(k, sv))
        env.land(*commit)
    env.drain(limit=rng.randint(1, 5))
    failed = failed_indexes(env)
    assert failed == {i for i, c in enumerate(env.landed) if is_poison(c)}  # exactly, any batching
    assert diff(expected_store(env.landed, failed), store.fingerprints(env.store)) == []
    merchant = [i for i, c in enumerate(env.landed) if c.op != "reclassify"]
    first = env.outcomes()
    env.offsets = {p: START for p in env.offsets}  # crash replay of the whole log
    env.drain()
    assert failed_indexes(env) == failed
    assert diff(expected_store(env.landed, failed), store.fingerprints(env.store)) == []
    # A merchant Change's status never changes on replay (internal reclassifies may improve).
    assert {i: env.outcomes()[i] for i in merchant} == {i: first[i] for i in merchant}
