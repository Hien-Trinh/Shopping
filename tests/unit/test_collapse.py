from catalog.collapse import collapse


def feed(mpid, kind, version, sv=1, tombstone=False):
    return {
        "merchant_id": "m_1",
        "merchant_product_id": mpid,
        "source_version": sv,
        "is_tombstone": tombstone,
        "_change_type": kind,
        "_commit_version": version,
        "_commit_timestamp": 0,
    }


def summary(rows):
    return [(r["merchant_product_id"], r["source_version"], r["op"]) for r in rows]


def test_insert_is_an_upsert_without_feed_columns():
    (row,) = collapse([feed("a", "insert", 1)])
    assert row == {
        "merchant_id": "m_1",
        "merchant_product_id": "a",
        "source_version": 1,
        "is_tombstone": False,
        "op": "upsert",
    }


def test_update_keeps_postimage_only():
    rows = collapse([feed("a", "update_preimage", 2, sv=1), feed("a", "update_postimage", 2, sv=2)])
    assert summary(rows) == [("a", 2, "upsert")]


def test_latest_commit_wins_whatever_the_feed_order():
    rows = collapse([feed("a", "update_postimage", 5, sv=9), feed("a", "insert", 3, sv=1)])
    assert summary(rows) == [("a", 9, "upsert")]


def test_tombstone_and_physical_delete_export_as_delete():
    rows = collapse(
        [feed("b", "update_postimage", 2, sv=2, tombstone=True), feed("a", "delete", 2)]
    )
    assert summary(rows) == [("a", 1, "delete"), ("b", 2, "delete")]


def test_empty_feed():
    assert collapse([]) == []
