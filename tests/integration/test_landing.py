from datetime import UTC, datetime

import pytest
from support import delete, listing, product_in, reclassify, up

from catalog import landing
from catalog.landing import MAX_SEQ, START

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
A, B = product_in(3), product_in(40)  # two partitions


@pytest.fixture
def log(tmp_path):
    path = str(tmp_path / "landing")
    landing.ensure(path)
    return path


def add(log, *changes, submission="s1"):
    return landing.append(log, [(submission, i, c) for i, c in enumerate(changes)], NOW)


def test_ensure_is_idempotent(log):
    landing.ensure(log)
    assert landing.read(log, {3: START}, limit=10).changes == []


def test_round_trip_in_landing_order(log):
    v1 = add(log, up(A, 1, listing(title="one")), delete(A, 2))
    v2 = add(log, reclassify(A), up(A, 3), submission="s2")
    batch = landing.read(log, {3: START}, limit=10)
    assert [x.change for x in batch.changes] == [
        up(A, 1, listing(title="one")),
        delete(A, 2),
        reclassify(A),
        up(A, 3),
    ]
    assert [x.position for x in batch.changes] == [(v1, 0), (v1, 1), (v2, 0), (v2, 1)]
    assert [(x.submission_id, x.change_index, x.partition) for x in batch.changes][2] == (
        "s2",
        0,
        3,
    )
    assert batch.offsets == {3: (v2, MAX_SEQ)}


def test_reads_only_owned_partitions_past_their_offsets(log):
    v = add(log, up(A, 1), up(B, 1), up(A, 2), up(B, 2))
    batch = landing.read(log, {40: (v, 1)}, limit=10)  # B's first row (seq 1) already processed
    assert [(x.change.merchant_product_id, x.change.source_version) for x in batch.changes] == [
        (B, 2)
    ]


def test_limit_advances_only_what_was_returned(log):
    v = add(log, up(A, 1), up(B, 1), up(A, 2), up(B, 2))
    first = landing.read(log, {3: START, 40: START}, limit=3)
    assert [x.position for x in first.changes] == [(v, 0), (v, 1), (v, 2)]
    assert first.offsets == {3: (v, 2), 40: (v, 1)}
    second = landing.read(log, first.offsets, limit=3)
    assert [x.position for x in second.changes] == [(v, 3)]
    assert second.offsets == {3: (v, MAX_SEQ), 40: (v, MAX_SEQ)}


def test_idle_stretch_does_not_stall(log):
    for i in range(6):
        add(log, up(B, i + 1))  # nothing for partition 3
    last = add(log, up(A, 1))
    offsets, seen = {3: START}, []
    for _ in range(3):
        batch = landing.read(log, offsets, limit=10, max_versions=3)
        seen += batch.changes
        offsets = batch.offsets
    assert [x.position for x in seen] == [(last, 0)]


def test_nothing_new(log):
    v = add(log, up(A, 1))
    assert landing.read(log, {3: (v, MAX_SEQ)}, limit=10) == landing.Batch([], {3: (v, MAX_SEQ)})
    assert landing.read(log, {}, limit=10) == landing.Batch([], {})
