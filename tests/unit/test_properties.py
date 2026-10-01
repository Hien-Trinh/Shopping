"""The replay property (plan-v1.md, Testing rules): whatever the batching, duplicates, order
or crash replays, plan() lands the Listing Store exactly where the independent oracle says."""

from hypothesis import given, settings
from hypothesis import strategies as st
from support import TAX, classified, listing

from catalog.envelope import Change
from catalog.plan import Stored, plan
from catalog.replay import diff, expected_store
from catalog.status import fold

CONTENTS = [listing(title="red"), listing(title="red", price=2), listing(title="blue")]

changes = st.lists(
    st.one_of(
        st.builds(
            Change,
            st.sampled_from(["m_1", "m_2"]),
            st.sampled_from(["a", "b", "c"]),
            st.integers(1, 6),  # small range: plenty of ties and stale versions
            st.just("upsert"),
            st.sampled_from(CONTENTS),
        ),
        st.builds(
            Change,
            st.sampled_from(["m_1", "m_2"]),
            st.sampled_from(["a", "b", "c"]),
            st.integers(1, 6),
            st.just("delete"),
        ),
        st.builds(
            Change, st.just("m_1"), st.sampled_from(["a", "b"]), st.just(0), st.just("reclassify")
        ),
    ),
    max_size=40,
)
cuts = st.lists(st.integers(0, 40), max_size=8)


def batches(seq, cut_points):
    bounds = sorted({0, len(seq), *(c for c in cut_points if c < len(seq))})
    return [seq[a:b] for a, b in zip(bounds, bounds[1:], strict=False)]


def apply(store, batch):
    p = plan(batch, store, TAX)
    for w in p.writes:
        cls = w.classification or (None if w.listing is None else classified())
        store[w.key] = Stored(w.source_version, w.listing, cls)
    return list(p.outcomes)


def run(seq, cut_points):
    store: dict = {}
    outcomes = [o for batch in batches(seq, cut_points) for o in apply(store, batch)]
    return store, outcomes


def fingerprints(store):
    return {
        k: (
            s.source_version,
            Change(*k, s.source_version, "upsert", s.listing).content_hash,
            s.listing is None,
        )
        for k, s in store.items()
    }


@settings(max_examples=10_000)
@given(changes, cuts)
def test_store_matches_oracle_for_any_batching(seq, cut_points):
    store, _ = run(seq, cut_points)
    assert diff(expected_store(seq), fingerprints(store)) == []


@settings(max_examples=2_000)
@given(changes, cuts, cuts)
def test_outcomes_do_not_depend_on_batching(seq, cuts_a, cuts_b):
    assert run(seq, cuts_a)[1] == run(seq, cuts_b)[1]


@settings(max_examples=2_000)
@given(changes, cuts, st.integers(0, 40), st.integers(0, 40))
def test_crash_replay_changes_nothing(seq, cut_points, start, length):
    store, outcomes = run(seq, cut_points)
    before = dict(store)
    replayed = seq[start : start + length]
    replay_outcomes = apply(store, replayed)
    assert store == before

    def events(outs, offset=0):
        # Merchant Changes only: an internal reclassify legitimately re-runs on a replay
        # (skipped -> reclassified once its Listing exists): harmless, and it has no Submission.
        return [
            {"type": o, "submission_id": "s", "merchant_id": "m_1", "change_index": offset + i}
            for i, (o, c) in enumerate(zip(outs, seq[offset:], strict=False))
            if c.op != "reclassify"
        ]

    original = fold("s", events(outcomes))
    with_replay = fold("s", events(outcomes) + events(replay_outcomes, offset=start))
    assert with_replay == original
