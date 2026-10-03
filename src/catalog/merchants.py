"""The Merchant registry: SQLite, plus an admin CLI to create Merchants and rotate or revoke keys
(plan-v1 A16, docs/specs/step-4a.md).

A key is shown once and never stored: only its SHA-256 is, and `verify` confirms a match in
constant time. Every call opens its own short-lived connection, so the API's threads never share
one; WAL lets them read while the CLI writes. `verify` only reads: a wrong path raises instead of
creating an empty registry that refuses every key.
"""

import argparse
import contextlib
import hashlib
import hmac
import re
import secrets
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple

from catalog import entry, envelope

DB = Path("data/merchants.sqlite")


class Merchant(NamedTuple):
    merchant_id: str
    currency: str


class UnknownMerchant(LookupError):
    pass


@contextlib.contextmanager
def _connect(db: Path):
    db.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.closing(sqlite3.connect(db)) as c:
        if c.execute("PRAGMA journal_mode").fetchone() != ("wal",):  # switch once: it locks
            c.execute("PRAGMA journal_mode=WAL")
        c.execute(
            "CREATE TABLE IF NOT EXISTS merchants (merchant_id TEXT PRIMARY KEY,"
            " currency TEXT NOT NULL, key_hash TEXT NOT NULL UNIQUE, status TEXT NOT NULL)"
        )
        with c:  # one transaction: commit, or roll back on error
            yield c


def _hash(key: str) -> str:
    return hashlib.sha256(key.encode(errors="surrogatepass")).hexdigest()  # never raises


def create(db: Path, currency: str) -> tuple[str, str]:
    """Add an active Merchant; returns its id and its key, which nothing can recover later."""
    if not re.fullmatch(envelope.CURRENCY, currency):
        raise ValueError(f"currency must be 3 capital letters (ISO 4217), got {currency!r}")
    key = secrets.token_urlsafe(32)
    with _connect(db) as c:
        while True:
            merchant_id = "m_" + secrets.token_hex(8)  # [a-z0-9], never "/" (A7's separator)
            try:
                c.execute(
                    "INSERT INTO merchants VALUES (?, ?, ?, 'active')",
                    (merchant_id, currency, _hash(key)),
                )
                return merchant_id, key
            except sqlite3.IntegrityError:  # the id is taken: 1 in 2^64 per existing Merchant
                continue


def rotate(db: Path, merchant_id: str) -> str:
    """Replace the Merchant's key: the old one stops working at once. Status is unchanged."""
    key = secrets.token_urlsafe(32)
    _update(db, merchant_id, "key_hash = ?", _hash(key))
    return key


def revoke(db: Path, merchant_id: str) -> None:
    """Stop the Merchant's key working; its row, id and currency stay."""
    _update(db, merchant_id, "status = ?", "revoked")


def _update(db: Path, merchant_id: str, assignment: str, value: str) -> None:
    with _connect(db) as c:
        done = c.execute(
            f"UPDATE merchants SET {assignment} WHERE merchant_id = ?", (value, merchant_id)
        )
        if not done.rowcount:
            raise UnknownMerchant(f"no merchant {merchant_id!r}")


def verify(db: Path, key: str) -> Merchant | None:
    """The active Merchant this key belongs to, else None."""
    presented = _hash(key)
    with contextlib.closing(sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True)) as c:
        row = c.execute(
            "SELECT merchant_id, currency, key_hash, status FROM merchants WHERE key_hash = ?",
            (presented,),
        ).fetchone()
    if row and hmac.compare_digest(row[2], presented) and row[3] == "active":
        return Merchant(row[0], row[1])
    return None


def main(argv: Sequence[str] | None = None) -> int | None:
    args = argparse.ArgumentParser(prog="python -m catalog.merchants")
    args.add_argument("--db", type=Path, default=DB)
    commands = args.add_subparsers(dest="command", required=True)
    commands.add_parser("create").add_argument("--currency", required=True)
    commands.add_parser("rotate").add_argument("merchant_id")
    commands.add_parser("revoke").add_argument("merchant_id")
    a = args.parse_args(argv)
    try:
        if a.command == "create":
            merchant_id, key = create(a.db, a.currency)
        elif a.command == "rotate":
            merchant_id, key = a.merchant_id, rotate(a.db, a.merchant_id)
        else:
            revoke(a.db, a.merchant_id)
            print(f"revoked: {a.merchant_id}")
            return
    except (ValueError, UnknownMerchant) as e:
        args.error(str(e))  # exit 2, and no key was printed
    try:
        print(f"merchant_id: {merchant_id}\nkey: {key}", flush=True)
    except OSError:  # a closed pipe or a full disk: the key is lost, so say so and fail
        print(
            f"The key was not delivered: run rotate {merchant_id} for a new one.", file=sys.stderr
        )
        return 1
    print("Store the key now: it is not shown again.", file=sys.stderr)
    return None


if __name__ == "__main__":
    entry.exit_with(main)
