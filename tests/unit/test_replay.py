from support import delete, listing, reclassify, up

from catalog.envelope import content_hash
from catalog.replay import diff, expected_store, fingerprint, live, replay_exports

A, B = ("m_1", "a"), ("m_1", "b")
T1, T2 = listing(title="one"), listing(title="two")


def test_highest_source_version_wins_regardless_of_order():
    got = expected_store([up("a", 5, T2), up("a", 3, T1), up("a", 4, T1)])
    assert got == {A: (5, content_hash(T2), False)}


def test_tie_goes_to_earliest_landed():
    assert expected_store([up("a", 5, T1), up("a", 5, T2)])[A] == (5, content_hash(T1), False)


def test_delete_is_a_tombstone():
    assert expected_store([up("a", 1), delete("a", 2)])[A] == (2, content_hash(None), True)


def test_failed_and_reclassify_changes_do_not_count():
    landed = [up("a", 1, T1), up("a", 2, T2), reclassify("a"), up("b", 1)]
    assert expected_store(landed, failed={1, 3}) == {A: (1, content_hash(T1), False)}


def test_fingerprint_and_live():
    row = {"source_version": 3, "content_hash": "h", "is_tombstone": 0}
    assert fingerprint(row) == (3, "h", False)
    assert fingerprint(row | {"is_tombstone": 1}) == (3, "h", True)
    assert live({A: (1, "h", False), B: (2, "t", True)}) == {A: (1, "h", False)}


def test_replay_exports_applies_files_in_order():
    def row(mpid, sv, op):
        return {
            "merchant_id": "m_1",
            "merchant_product_id": mpid,
            "source_version": sv,
            "content_hash": f"h{sv}",
            "is_tombstone": op == "delete",
            "op": op,
        }

    files = [
        [row("a", 1, "upsert"), row("b", 1, "upsert")],
        [row("a", 2, "delete"), row("c", 3, "delete")],
        [row("b", 4, "upsert")],
    ]
    assert replay_exports(files) == {B: (4, "h4", False)}


def test_diff():
    assert diff({A: (1, "h", False)}, {A: (1, "h", False)}) == []
    assert diff({A: (1, "h", False)}, {A: (2, "h", False), B: (1, "x", True)}) == [
        "('m_1', 'a'): expected (1, 'h', False), got (2, 'h', False)",
        "('m_1', 'b'): expected None, got (1, 'x', True)",
    ]
