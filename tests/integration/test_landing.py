import json
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from support import delete, listing, product_in, race, reclassify, up

from catalog import landing
from catalog.landing import START, Landed

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
A, B = product_in(3), product_in(40)  # two partitions


@pytest.fixture
def log(tmp_path):
    return landing.ensure(str(tmp_path / "landing"))


def add(log, *changes, submission="s1"):
    return landing.append(log, [(submission, i, c) for i, c in enumerate(changes)], NOW)


def test_ensure_never_touches_an_existing_table(log):
    add(log, up(A, 1))
    again = landing.ensure(log.table_uri)
    assert again.version() == log.version()
    assert landing.table_id(again) == landing.table_id(log)
    assert len(landing.read(again, {3: START}, limit=10).changes) == 1


def test_round_trip_in_landing_order(log):
    v1 = add(log, up(A, 1, listing(title="one")), delete(A, 2))
    v2 = add(log, reclassify(A), up(A, 3), submission="s2")
    batch = landing.read(log, {3: START}, limit=10)
    assert batch.changes == [
        Landed(up(A, 1, listing(title="one")), "s1", 0, 3, (v1, 0)),
        Landed(delete(A, 2), "s1", 1, 3, (v1, 1)),
        Landed(reclassify(A), "s2", 0, 3, (v2, 0)),
        Landed(up(A, 3), "s2", 1, 3, (v2, 1)),
    ]
    assert batch.offsets == {3: (v2 + 1, 0)}


def test_reads_only_owned_partitions_from_their_offsets(log):
    v = add(log, up(A, 1), up(B, 1), up(A, 2), up(B, 2))
    batch = landing.read(log, {40: (v, 2)}, limit=10)  # B's row at seq 1 is already processed
    assert [x.change for x in batch.changes] == [up(B, 2)]


def test_a_limit_cut_advances_every_partition_to_the_cut(log):
    v = add(log, up(A, 1), up(B, 1), up(A, 2), up(B, 2))
    first = landing.read(log, {3: START, 40: START}, limit=3)
    assert [x.position for x in first.changes] == [(v, 0), (v, 1), (v, 2)]
    assert first.offsets == {3: (v, 3), 40: (v, 3)}
    second = landing.read(log, first.offsets, limit=3)
    assert [x.position for x in second.changes] == [(v, 3)]
    assert second.offsets == {3: (v + 1, 0), 40: (v + 1, 0)}


def test_an_idle_partition_is_not_left_behind_by_a_busy_one(log):
    for sv in range(1, 6):
        add(log, up(A, sv), up(A, sv + 10))  # only partition 3 is busy
    offsets = {3: START, 40: START}
    for _ in range(4):
        offsets = landing.read(log, offsets, limit=3).offsets  # every read is cut by the limit
        assert offsets[40] == offsets[3]  # so partition 40 never pins the read window


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
    assert landing.read(log, {3: (v + 1, 0)}, limit=10) == landing.Batch([], {3: (v + 1, 0)})
    assert landing.read(log, {}, limit=10) == landing.Batch([], {})
    with pytest.raises(ValueError, match="limit"):
        landing.read(log, {3: START}, limit=0)


def test_a_handle_sees_other_writers(log):
    other = landing.ensure(log.table_uri)
    add(other, up(A, 1))
    assert len(landing.read(log, {3: START}, limit=10).changes) == 1


def test_empty_append_makes_no_commit(log):
    v = log.version()
    assert landing.append(log, [], NOW) == v
    assert DeltaTable(log.table_uri).version() == v


def test_naive_received_at_is_refused(log):
    with pytest.raises(ValueError, match="timezone-aware"):
        landing.append(log, [("s1", 0, up(A, 1))], datetime(2026, 9, 30, 12))


def test_null_keys_are_refused_by_the_table(log):
    row = {c: None for c in landing.SCHEMA.names} | {"partition": 3, "seq": 0}
    with pytest.raises(Exception, match="(?i)null|validation"):
        write_deltalake(log, pa.Table.from_pylist([row], schema=landing.SCHEMA), mode="append")


def test_rows_written_under_older_limits_still_read(log):
    # A later release may tighten Content (e.g. a shorter title); landed rows must still decode.
    v = add(log, up(A, 1))
    raw = landing.read(log, {3: START}, limit=1).changes[0]
    long_title = raw.change.listing.model_dump(mode="json") | {"title": "x" * 500}
    row = {
        "partition": 3,
        "seq": 0,
        "merchant_id": "m_1",
        "merchant_product_id": A,
        "source_version": 2,
        "op": "upsert",
        "listing": json.dumps(long_title),
        "submission_id": "s0",
        "change_index": 0,
        "received_at": NOW,
    }
    write_deltalake(log, pa.Table.from_pylist([row], schema=landing.SCHEMA), mode="append")
    (late,) = landing.read(log, {3: (v + 1, 0)}, limit=5).changes
    assert late.change.listing.title == "x" * 500


def test_landed_is_never_hashable(log):
    add(log, delete(A, 1))
    with pytest.raises(TypeError):
        hash(landing.read(log, {3: START}, limit=1).changes[0])


def test_concurrent_startup_creates_one_table(tmp_path):
    path = str(tmp_path / "landing")
    outcomes = race(_open_and_identify, (path,))
    assert {status for status, _ in outcomes} == {"ok"}, outcomes
    assert len({table for _, table in outcomes}) == 1
    assert DeltaTable(path).version() == 0


def _open_and_identify(path):
    return landing.table_id(landing.ensure(path))


def test_ensure_by_uri_locks_next_to_the_table(tmp_path, monkeypatch):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    log = landing.ensure(str(tmp_path / "landing"))
    again = landing.ensure(log.table_uri)  # file:///... form, as handles report it
    assert landing.table_id(again) == landing.table_id(log)
    assert list(cwd.iterdir()) == []  # no stray "file:" directory
    assert (tmp_path / "landing.lock").exists()
