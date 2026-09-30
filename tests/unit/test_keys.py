import subprocess
import sys
from collections import Counter

import duckdb
import pytest
from hypothesis import given
from hypothesis import strategies as st

from catalog.keys import PARTITIONS, owned, owner, partition

merchant_ids = st.from_regex(r"m_[a-z0-9]{1,12}", fullmatch=True)
product_ids = st.text(max_size=128)
worker_counts = st.integers(1, PARTITIONS)


# Pinned forever: changing any of these silently re-routes every Listing (ADR-0001).
@pytest.mark.parametrize(
    ("merchant_id", "merchant_product_id", "expected"),
    [
        ("m_1", "SKU-123", 21),
        ("m_1", "sku-123", 63),  # case-sensitive
        ("m_42", "T-shirt/red/M", 1),  # '/' allowed in the product ID
        ("m_42", "漢字-é", 15),
        ("m_9", "", 5),
    ],
)
def test_partition_known_answers(merchant_id, merchant_product_id, expected):
    assert partition(merchant_id, merchant_product_id) == expected


def test_partition_is_stable_across_processes():
    code = "from catalog.keys import partition; print(partition('m_42', 'SKU-123'))"
    runs = {
        subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True
        ).stdout
        for _ in range(3)
    }
    assert runs == {f"{partition('m_42', 'SKU-123')}\n"}


def test_partition_rejects_ambiguous_merchant_id():
    with pytest.raises(ValueError, match="must not contain '/'"):
        partition("m_1/evil", "x")


@given(merchant_ids, product_ids)
def test_partition_in_range(merchant_id, merchant_product_id):
    assert 0 <= partition(merchant_id, merchant_product_id) < PARTITIONS


def test_partition_spreads_evenly():
    counts = Counter(partition("m_1", f"sku-{i}") for i in range(64_000))
    assert len(counts) == PARTITIONS
    assert min(counts.values()) > 850 and max(counts.values()) < 1150  # expected 1000


@given(st.lists(st.tuples(merchant_ids, product_ids), min_size=1, max_size=50))
def test_partition_matches_duckdb(keys):
    con = duckdb.connect()
    con.execute("create table k (m varchar, p varchar)")
    con.executemany("insert into k values (?, ?)", keys)
    sql = "select (('0x' || substr(sha256(m || '/' || p), 1, 16))::UBIGINT % 64)::INT from k"
    assert [row[0] for row in con.execute(sql).fetchall()] == [partition(m, p) for m, p in keys]


@given(worker_counts)
def test_owned_blocks_cover_every_partition_exactly_once(workers):
    blocks = [owned(i, workers) for i in range(workers)]
    assert [p for block in blocks for p in block] == list(range(PARTITIONS))


@given(worker_counts)
def test_owned_blocks_are_balanced(workers):
    sizes = {len(owned(i, workers)) for i in range(workers)}
    assert max(sizes) - min(sizes) <= 1


@given(st.integers(0, PARTITIONS - 1), worker_counts)
def test_owner_is_inverse_of_owned(p, workers):
    assert p in owned(owner(p, workers), workers)


def test_owner_non_divisor_worker_count():
    assert [len(owned(i, 3)) for i in range(3)] == [22, 21, 21]
    assert owner(21, 3) == 0 and owner(22, 3) == 1 and owner(63, 3) == 2


@pytest.mark.parametrize("workers", [0, PARTITIONS + 1])
def test_worker_count_bounds(workers):
    with pytest.raises(ValueError, match="workers must be"):
        owner(0, workers)
    with pytest.raises(ValueError, match="workers must be"):
        owned(0, workers)


@pytest.mark.parametrize("p", [-1, PARTITIONS])
def test_owner_partition_bounds(p):
    with pytest.raises(ValueError, match="partition must be"):
        owner(p, 4)


@pytest.mark.parametrize("index", [-1, 4])
def test_owned_index_bounds(index):
    with pytest.raises(ValueError, match="index must be"):
        owned(index, 4)
