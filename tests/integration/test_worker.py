import random
from datetime import UTC, datetime

import pytest
from support import delete, listing, product_in, reclassify, up

from catalog import events, landing, state, store, worker
from catalog.classify import UNCATEGORIZED, FakeClassifier
from catalog.events import EventLog
from catalog.keys import PARTITIONS
from catalog.landing import START
from catalog.replay import diff, expected_store
from catalog.status import fold

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
A, B = product_in(3), product_in(40)


class Env:
    def __init__(self, tmp_path, classifier=None):
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


def test_a_wrong_number_of_answers_counts_as_a_failure(env, monkeypatch):
    monkeypatch.setattr(env.classifier, "classify", lambda listings: [])
    env.land(up(A, 1))
    env.run()
    assert store.read(env.store, [("m_1", A)])[("m_1", A)].classification.needs_reclassify


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


@pytest.mark.parametrize("seed", range(3))
def test_replay_oracle_for_any_batching(env, seed):
    rng = random.Random(seed)
    keys = [product_in(p) for p in (3, 3, 17, 40)] + [product_in(3, prefix="alt")]
    for _ in range(6):  # several commits of mixed changes, out of order and duplicated
        commit = []
        for _ in range(rng.randint(1, 8)):
            k, sv = rng.choice(keys), rng.randint(1, 6)
            commit.append(
                rng.choice([up(k, sv, listing(rng.choice("ABC"))), delete(k, sv), reclassify(k)])
            )
        env.land(*commit)
    env.drain(limit=rng.randint(1, 5))
    assert diff(expected_store(env.landed), store.fingerprints(env.store)) == []
    merchant = [i for i, c in enumerate(env.landed) if c.op != "reclassify"]
    first = env.outcomes()
    env.offsets = {p: START for p in env.offsets}  # crash replay of the whole log
    env.drain()
    assert diff(expected_store(env.landed), store.fingerprints(env.store)) == []
    # A merchant Change's status never changes on replay (internal reclassifies may improve).
    assert {i: env.outcomes()[i] for i in merchant} == {i: first[i] for i in merchant}
