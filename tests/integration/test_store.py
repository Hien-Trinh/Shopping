from dataclasses import replace
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from support import TAX, classified, listing, product_in

from catalog import store
from catalog.collapse import collapse
from catalog.envelope import content_hash
from catalog.plan import Classification, Stored, Write

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
K = ("m_1", product_in(5))
SHIRTS = classified("Apparel > Shirts")


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "store")
    store.ensure(path)
    store.ensure(path)  # idempotent
    return path


def write(sv, content=None, *, cls=SHIRTS, needs=False, changed=True, key=K):
    return Write(key, sv, content, cls if content else None, needs, changed)


def test_read_nothing(db):
    assert store.read(db, []) == {}
    assert store.read(db, [K]) == {}


def test_insert_and_read_back(db):
    before = DeltaTable(db).version()
    merged = store.merge(db, [write(5, listing(attributes={"brand": "x"}))], NOW)
    assert merged == store.Merged(1, before + 1)
    assert store.read(db, [K]) == {K: Stored(5, listing(attributes={"brand": "x"}), SHIRTS)}


def test_older_write_is_ignored(db):
    store.merge(db, [write(5, listing(title="new"))], NOW)
    assert store.merge(db, [write(4, listing(title="old"))], NOW).applied == 0
    assert store.read(db, [K])[K].listing.title == "new"


def test_tombstone(db):
    store.merge(db, [write(5, listing())], NOW)
    store.merge(db, [write(6)], NOW)
    assert store.read(db, [K]) == {K: Stored(6, None)}


def test_reclassify_only_write(db):
    store.merge(db, [write(5, listing())], NOW)
    jeans = classified("Apparel > Pants")
    assert store.merge(db, [write(5, listing(), cls=jeans, changed=False)], NOW).applied == 1
    assert store.read(db, [K])[K].classification == jeans
    # A reclassify planned against content that has since changed must not land.
    stale = write(5, listing(title="other"), cls=SHIRTS, changed=False)
    assert store.merge(db, [stale], NOW).applied == 0


def test_failed_classification_is_flagged(db):
    unsure = Classification("Uncategorized", 0.0, TAX)
    store.merge(db, [write(5, listing(), cls=unsure, needs=True)], NOW)
    assert store.read(db, [K])[K].needs_reclassify


def test_live_write_must_be_classified(db):
    with pytest.raises(ValueError, match="classify"):
        store.merge(db, [replace(write(5, listing()), classification=None)], NOW)


def test_empty_merge_makes_no_commit(db):
    v = DeltaTable(db).version()
    assert store.merge(db, [], NOW) == store.Merged(0, v)


def test_fingerprints_now_and_then(db):
    v1 = store.merge(db, [write(5, listing())], NOW).version
    store.merge(db, [write(6)], NOW)
    assert store.fingerprints(db) == {K: (6, content_hash(None), True)}
    assert store.fingerprints(db, version=v1) == {K: (5, content_hash(listing()), False)}


def test_compact_keeps_data(db):
    other = ("m_1", product_in(5, prefix="alt"))
    for sv in range(1, 4):
        store.merge(db, [write(sv, listing(price=sv)), write(sv, listing(), key=other)], NOW)
    before = store.fingerprints(db)
    store.compact(db, [5])
    assert store.fingerprints(db) == before


def test_change_feed_collapses_to_one_row_per_listing(db):
    v0 = DeltaTable(db).version()
    other = ("m_1", product_in(9))
    store.merge(db, [write(5, listing(title="a")), write(1, listing(), key=other)], NOW)
    store.merge(db, [write(6, listing(title="b"))], NOW)
    store.merge(db, [write(2, key=other)], NOW)
    feed = pa.table(DeltaTable(db).load_cdf(starting_version=v0 + 1).read_all()).to_pylist()
    rows = {
        (r["merchant_product_id"], r["op"], r["source_version"], r["title"]) for r in collapse(feed)
    }
    assert rows == {(K[1], "upsert", 6, "b"), (other[1], "delete", 2, None)}


def test_read_returns_only_requested_keys(db):
    neighbour = ("m_1", product_in(5, prefix="alt"))  # same partition as K
    store.merge(db, [write(5, listing()), write(1, listing(), key=neighbour)], NOW)
    assert set(store.read(db, [K])) == {K}


def _writer(path, part, n):
    key = ("m_1", product_in(part))
    return [
        store.merge(path, [write(sv, listing(price=sv), key=key)], NOW).version
        for sv in range(1, n + 1)
    ]


def test_concurrent_writers_each_get_their_own_commit_version(db):
    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(2) as pool:
        a, b = pool.map(_writer, [db, db], [11, 12], [15, 15])
    assert len(set(a) | set(b)) == 30  # no two MERGEs report the same commit
    assert {fp[0] for fp in store.fingerprints(db).values()} == {15}
