"""Listing key -> partition -> owning worker. See ADR-0001."""

import hashlib

PARTITIONS = 64


def partition(merchant_id: str, merchant_product_id: str) -> int:
    """Stable partition of a Listing key; reproducible in DuckDB and Spark SQL via SHA-256.

    Never use the built-in hash(): it is randomized per process.
    """
    if "/" in merchant_id:
        raise ValueError(f"merchant_id must not contain '/': {merchant_id!r}")
    digest = hashlib.sha256(f"{merchant_id}/{merchant_product_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % PARTITIONS


def owner(partition: int, workers: int) -> int:
    """Index of the worker that owns `partition` when `workers` workers run."""
    _check_workers(workers)
    if not 0 <= partition < PARTITIONS:
        raise ValueError(f"partition must be 0..{PARTITIONS - 1}, got {partition}")
    return partition * workers // PARTITIONS


def owned(index: int, workers: int) -> range:
    """The contiguous block of partitions worker `index` owns; the inverse of owner()."""
    _check_workers(workers)
    if not 0 <= index < workers:
        raise ValueError(f"index must be 0..{workers - 1}, got {index}")
    return range(-(-index * PARTITIONS // workers), -(-(index + 1) * PARTITIONS // workers))


def _check_workers(workers: int) -> None:
    if not 1 <= workers <= PARTITIONS:
        raise ValueError(f"workers must be 1..{PARTITIONS}, got {workers}")
