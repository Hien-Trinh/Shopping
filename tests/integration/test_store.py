import os
from dataclasses import replace
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from support import TAX, classified, listing, product_in, race

from catalog import store
from catalog.collapse import collapse
from catalog.envelope import content_hash
from catalog.plan import Classification, Stored, Write

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
K = ("m_1", product_in(5))
SHIRTS = classified("Apparel > Shirts")


@pytest.fixture
def db(tmp_path):
    return store.ensure(str(tmp_path / "store"))


def write(sv, content=None, *, cls=SHIRTS, needs=False, key=K):
    return Write(key, sv, content, cls if content else None, needs)


def files_in(db, part):
    return [
        a
        for a in pa.table(DeltaTable(db.table_uri).get_add_actions(flatten=True)).to_pylist()
        if a["partition.partition"] == part
    ]


def test_read_nothing(db):
    assert store.read(db, []) == {}
    assert store.read(db, [K]) == {}


def test_insert_and_read_back(db):
    before = db.version()
    merged = store.merge(db, [write(5, listing(attributes={"brand": "x"}))], NOW)
    assert merged == store.Merged(1, before + 1)
    assert store.read(db, [K]) == {K: Stored(5, listing(attributes={"brand": "x"}), SHIRTS)}


def test_ensure_never_touches_an_existing_table(db):
    store.merge(db, [write(5, listing())], NOW)
    assert store.read(store.ensure(db.table_uri), [K])[K].source_version == 5


def test_read_returns_only_requested_keys(db):
    neighbour = ("m_1", product_in(5, prefix="alt"))  # same partition as K
    other_merchant = ("m_2", K[1])  # same product ID, another Merchant
    store.merge(
        db,
        [
            write(5, listing()),
            write(1, listing(), key=neighbour),
            write(1, listing(), key=other_merchant),
        ],
        NOW,
    )
    assert set(store.read(db, [K])) == {K}


def test_older_write_is_ignored(db):
    store.merge(db, [write(5, listing(title="new"))], NOW)
    assert store.merge(db, [write(4, listing(title="old"))], NOW).applied == 0
    assert store.read(db, [K])[K].listing.title == "new"


def test_tombstone(db):
    store.merge(db, [write(5, listing())], NOW)
    store.merge(db, [write(6)], NOW)
    assert store.read(db, [K]) == {K: Stored(6, None)}


def test_reclassify_keeps_the_version(db):
    store.merge(db, [write(5, listing())], NOW)
    jeans = classified("Apparel > Pants")
    assert store.merge(db, [write(5, listing(), cls=jeans)], NOW).applied == 1
    assert store.read(db, [K])[K].classification == jeans
    # Same version but different content is not the same Listing state: never applied.
    assert store.merge(db, [write(5, listing(title="other"))], NOW).applied == 0


def test_the_success_path_does_not_ask_for_reclassification(db):
    planned = Write(K, 5, listing(), None, needs_classify=True)  # what plan() emits
    store.merge(db, [replace(planned, classification=SHIRTS)], NOW)  # the worker classified it
    assert store.read(db, [K])[K].classification.needs_reclassify is False


def test_provisional_classification_is_flagged(db):
    unsure = Classification("Uncategorized", 0.0, TAX, needs_reclassify=True)
    store.merge(db, [write(5, listing(), cls=unsure)], NOW)
    assert store.read(db, [K])[K].classification.needs_reclassify


def test_live_write_must_be_classified(db):
    with pytest.raises(ValueError, match="classify"):
        store.merge(db, [Write(K, 5, listing(), None, needs_classify=True)], NOW)


def test_empty_merge_makes_no_commit(db):
    v = db.version()
    assert store.merge(db, [], NOW) == store.Merged(0, v)


def test_naive_updated_at_is_refused(db):
    with pytest.raises(ValueError, match="timezone-aware"):
        store.merge(db, [write(5, listing())], datetime(2026, 9, 30, 12))


def test_null_keys_are_refused_by_the_table(db):
    row = {c: None for c in store.SCHEMA.names} | {"partition": 5}
    with pytest.raises(Exception, match="(?i)null|validation"):
        write_deltalake(db, pa.Table.from_pylist([row], schema=store.SCHEMA), mode="append")


def test_rows_written_under_older_limits_still_read(db):
    # A later release may tighten Content (e.g. a shorter title); stored rows must still decode.
    old = ("m_1", product_in(5, prefix="old"))
    row = store._row(write(6, listing(), key=old), NOW) | {"title": "x" * 500}
    write_deltalake(db, pa.Table.from_pylist([row], schema=store.SCHEMA), mode="append")
    assert store.read(db, [old])[old].listing.title == "x" * 500


def test_fingerprints_now_and_then(db):
    v1 = store.merge(db, [write(5, listing())], NOW).version
    store.merge(db, [write(6)], NOW)
    assert store.fingerprints(db) == {K: (6, content_hash(None), True)}
    assert store.fingerprints(db, version=v1) == {K: (5, content_hash(listing()), False)}


def test_fingerprints_at_an_old_version_survive_the_vacuum_of_its_files(db):
    # The oracles read the store at older versions; maintenance's vacuum (step 7f) removes those
    # versions' files, so fingerprints rebuild them from the change feed instead.
    k2 = ("m_1", product_in(5, prefix="other"))
    store.merge(db, [write(1, listing(title="first")), write(1, listing(), key=k2)], NOW)
    then = DeltaTable(db.table_uri, version=db.version())
    want = {
        (r["merchant_id"], r["merchant_product_id"]): store.fingerprint(r)
        for r in then.to_pyarrow_dataset().to_table().to_pylist()
    }  # read straight from the old version's files, while they exist
    k3 = ("m_1", product_in(5, prefix="third"))
    store.merge(db, [write(2, listing(title="second")), write(1, listing(), key=k3)], NOW)
    store.merge(db, [write(3)], NOW)  # a Tombstone, so K changed twice after version 1
    assert store.fingerprints(db, 1) == want
    db.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
    assert store.fingerprints(db, 1) == want
    with pytest.raises(Exception, match="not found|No such file"):
        DeltaTable(db.table_uri, version=1).to_pyarrow_dataset().to_table()  # the files are gone


def test_compact_merges_small_files_without_changing_data_or_feed(db):
    keys = [("m_1", product_in(5, prefix=f"c{i}")) for i in range(6)]
    for i, k in enumerate(keys):  # one new file per MERGE
        store.merge(db, [write(1, listing(price=i + 1), key=k)], NOW)
    assert len(files_in(db, 5)) == 6
    before, v = store.fingerprints(db), db.version()
    store.compact(db, [5])
    assert len(files_in(db, 5)) == 1
    assert store.fingerprints(db) == before
    assert pa.table(db.load_cdf(starting_version=v + 1).read_all()).num_rows == 0


def test_merge_rewrites_only_the_file_holding_the_listing(db):
    keys = [("m_1", product_in(5, prefix=f"c{i}")) for i in range(4)]
    for k in keys:
        store.merge(db, [write(1, listing(), key=k)], NOW)  # 4 files, one Listing each
    store.merge(db, [write(2, listing(price=9), key=keys[0])], NOW)
    ops = DeltaTable(db.table_uri).history(1)[0]["operationMetrics"]
    assert ops["num_target_files_removed"] == 1


def test_a_partition_is_written_as_small_files_so_a_merge_rewrites_one_of_them(db):
    # One MERGE lands 2,500 Listings of about 1.2 KB in partition 5: about 3 MB, so more than
    # one file at the 1 MiB target; a later update of one Listing then rewrites its file alone.
    keys = [("m_1", product_in(5, prefix=f"s{i}")) for i in range(2500)]
    text = {k: os.urandom(600).hex() for k in keys}  # 1.2 KB that does not compress away
    store.merge(db, [write(1, listing(description=text[k]), key=k) for k in keys], NOW)
    files = len(files_in(db, 5))
    assert files > 1
    store.merge(db, [write(2, listing(description=text[keys[7]], price=9), key=keys[7])], NOW)
    ops = DeltaTable(db.table_uri).history(1)[0]["operationMetrics"]
    assert ops["num_target_files_removed"] == 1
    assert len(files_in(db, 5)) == files


def test_ensure_creates_the_store_with_the_compaction_target_as_its_file_size(db):
    assert db.metadata().configuration["delta.targetFileSize"] == str(store.COMPACT_TARGET)


def test_a_handle_sees_other_writers(db):
    store.merge(store.ensure(db.table_uri), [write(5, listing())], NOW)
    assert store.read(db, [K])[K].source_version == 5


def test_a_stale_handle_merges_against_the_latest_state(db):
    store.read(db, [K])  # db's view is now from before the other writer
    store.merge(store.ensure(db.table_uri), [write(5, listing())], NOW)
    assert store.merge(db, [write(6, listing(price=6))], NOW).applied == 1
    assert store.fingerprints(db) == {K: (6, content_hash(listing(price=6)), False)}


def test_change_feed_collapses_to_one_row_per_listing(db):
    v0 = db.version()
    other = ("m_1", product_in(9))
    store.merge(db, [write(5, listing(title="a")), write(1, listing(), key=other)], NOW)
    store.merge(db, [write(6, listing(title="b"))], NOW)
    store.merge(db, [write(2, key=other)], NOW)
    feed = pa.table(db.load_cdf(starting_version=v0 + 1).read_all()).to_pylist()
    rows = {
        (r["merchant_product_id"], r["op"], r["source_version"], r["title"]) for r in collapse(feed)
    }
    assert rows == {(K[1], "upsert", 6, "b"), (other[1], "delete", 2, None)}


def _writer(path, part, n):
    dt, key = store.ensure(path), ("m_1", product_in(part))
    return [
        store.merge(dt, [write(sv, listing(price=sv), key=key)], NOW).version
        for sv in range(1, n + 1)
    ]


def test_concurrent_writers_each_get_their_own_commit_version(db):
    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(2) as pool:
        a, b = pool.map(_writer, [db.table_uri] * 2, [11, 12], [15, 15])
    assert len(set(a) | set(b)) == 30  # no two MERGEs report the same commit
    assert {fp[0] for fp in store.fingerprints(db).values()} == {15}


def test_concurrent_startup_creates_one_table(tmp_path):
    path = str(tmp_path / "store")
    outcomes = race(_open_and_identify, (path,))
    assert {status for status, _ in outcomes} == {"ok"}, outcomes
    assert len({table for _, table in outcomes}) == 1
    assert DeltaTable(path).version() == 0


def _open_and_identify(path):
    return store.ensure(path).metadata().id


def test_unstorable_names_the_bad_listing_only():
    from catalog.envelope import Content

    surrogate = Content.model_construct(**listing().model_dump() | {"title": "\ud800"})
    errors = store.unstorable([listing(), None, surrogate])
    assert errors[:2] == [None, None]  # a live Listing and a Tombstone store fine
    assert errors[2].startswith("UnicodeEncodeError")


def test_read_lists_only_the_partitions_it_needs(db, monkeypatch):
    store.merge(db, [write(5, listing()), write(1, listing(), key=("m_1", product_in(40)))], NOW)
    asked = []
    real = DeltaTable.to_pyarrow_dataset

    def spy(self, *args, **kwargs):
        asked.append(kwargs.get("file_pruning_predicate"))
        return real(self, *args, **kwargs)

    monkeypatch.setattr(DeltaTable, "to_pyarrow_dataset", spy)
    assert list(store.read(db, [K])) == [K]
    assert asked == ["partition IN (5)"]
