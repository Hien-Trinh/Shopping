import json
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError
from support import delete, listing, product_in, race, reclassify, up

from catalog import landing
from catalog.landing import START, Landed

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
A, B = product_in(3), product_in(40)  # two partitions


@pytest.fixture
def log(tmp_path):
    return landing.ensure(str(tmp_path / "landing"))


def add(log, *changes, submission="s1"):
    return landing.append(log, [(submission, i, c, NOW) for i, c in enumerate(changes)])


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


def test_an_append_after_another_handles_delete_succeeds(log):
    """Retention's DELETE runs in another process; the API's handle predates it (step 5d)."""
    add(log, up(A, 1))
    landing.ensure(log.table_uri).delete("true")
    add(log, up(B, 1))
    assert [c.change.key for c in landing.read(log, {40: START}, limit=10).changes] == [("m_1", B)]


def test_a_delete_between_refresh_and_commit_is_retried(log, monkeypatch):
    add(log, up(A, 1))
    refresh, deletes = log.update_incremental, []

    def refresh_then_lose_the_race():
        refresh()
        if not deletes:  # once: the retry's refresh sees it
            deletes.append(landing.ensure(log.table_uri).delete("true"))

    monkeypatch.setattr(log, "update_incremental", refresh_then_lose_the_race)
    add(log, up(B, 1))
    assert deletes
    assert [c.change.key for c in landing.read(log, {40: START}, limit=10).changes] == [("m_1", B)]


def test_a_second_lost_race_fails_the_append(log, monkeypatch):
    """One retry, not a loop: the API answers 500 and the Merchant resends."""
    refresh, other = log.update_incremental, landing.ensure(log.table_uri)

    def refresh_then_always_lose():
        add(other, up(A, 1))  # a row the refresh sees, so the DELETE removes from its snapshot
        refresh()
        other.delete("true")

    monkeypatch.setattr(log, "update_incremental", refresh_then_always_lose)
    with pytest.raises(CommitFailedError):
        add(log, up(B, 1))


def test_empty_append_makes_no_commit(log):
    v = log.version()
    assert landing.append(log, []) == v
    assert DeltaTable(log.table_uri).version() == v


def test_naive_received_at_is_refused(log):
    with pytest.raises(ValueError, match="timezone-aware"):
        landing.append(log, [("s1", 0, up(A, 1), NOW), ("s1", 1, up(B, 1), datetime(2026, 9, 30))])


def test_each_row_keeps_its_own_received_at(log):
    later = datetime(2026, 9, 30, 12, 0, 1, tzinfo=UTC)
    landing.append(log, [("s1", 0, up(A, 1), NOW), ("s2", 0, up(B, 1), later)])
    # One file per partition, so the table's order isn't seq order.
    rows = log.to_pyarrow_table(columns=["seq", "received_at"]).sort_by("seq")
    assert rows["received_at"].to_pylist() == [NOW, later]


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


def test_retained_returns_one_partition_as_of_a_version_in_replay_order(log):
    later = NOW + timedelta(seconds=1)
    landing.append(log, [("s9", 0, up(A, 3), later), ("s9", 1, up(B, 3), later)])
    landing.append(log, [("s2", 0, up(A, 2), NOW), ("s1", 1, delete(A, 1), NOW)])
    pinned = landing.append(log, [("s1", 0, reclassify(A), NOW)])
    landing.append(log, [("s0", 0, up(A, 4), NOW)])  # after the pin
    log.optimize.compact()  # rewrites the files: commit versions are gone
    got = landing.retained(DeltaTable(log.table_uri, version=pinned), 3)
    assert [(x.submission_id, x.change_index, x.change, x.partition) for x in got] == [
        ("s1", 0, reclassify(A), 3),  # same received_at: submission, then change index
        ("s1", 1, delete(A, 1), 3),
        ("s2", 0, up(A, 2), 3),
        ("s9", 0, up(A, 3), 3),  # landed first, but received later
    ]
