import contextlib
import hashlib
import io
import re
import sqlite3
import sys
import threading

import pytest

from catalog import merchants
from catalog.merchants import Merchant


@pytest.fixture
def db(tmp_path):
    return tmp_path / "data" / "merchants.sqlite"  # the directory doesn't exist yet


def rows(db):
    with contextlib.closing(sqlite3.connect(db)) as c:
        return c.execute("SELECT merchant_id, currency, key_hash, status FROM merchants").fetchall()


def refused(db, key) -> tuple[str, str | None]:
    """Why verify refuses `key`, and whose key it was, if anyone's."""
    with pytest.raises(merchants.Denied) as denied:
        merchants.verify(db, key)
    return denied.value.reason, denied.value.merchant_id


def test_create_then_verify_returns_the_merchant(db):
    merchant_id, key = merchants.create(db, "USD")
    assert re.fullmatch(r"m_[a-z0-9]+", merchant_id)
    assert re.fullmatch(r"[A-Za-z0-9_-]{43}", key)  # token_urlsafe(32)
    assert merchants.verify(db, key) == Merchant(merchant_id, "USD")


def test_only_the_keys_sha256_is_stored(db):
    merchant_id, key = merchants.create(db, "EUR")
    assert rows(db) == [(merchant_id, "EUR", hashlib.sha256(key.encode()).hexdigest(), "active")]
    assert key.encode() not in db.read_bytes()


def test_the_database_uses_wal(db):
    merchants.create(db, "USD")
    with contextlib.closing(sqlite3.connect(db)) as c:
        assert c.execute("PRAGMA journal_mode").fetchone() == ("wal",)


def test_two_merchants_get_distinct_ids_and_keys(db):
    (a, ka), (b, kb) = merchants.create(db, "USD"), merchants.create(db, "USD")
    assert a != b and ka != kb
    assert merchants.verify(db, ka).merchant_id == a
    assert merchants.verify(db, kb).merchant_id == b


@pytest.mark.parametrize("currency", ["usd", "US", "USDD", " USD", "", "U$D", "ÜSD"])
def test_create_refuses_a_bad_currency_before_writing(db, currency):
    with pytest.raises(ValueError, match="currency"):
        merchants.create(db, currency)
    assert not db.exists() or rows(db) == []


def test_create_regenerates_an_id_that_is_taken(db, monkeypatch):
    first, _ = merchants.create(db, "USD")
    ids = iter([first.removeprefix("m_"), "0123456789abcdef"])
    monkeypatch.setattr(merchants.secrets, "token_hex", lambda n: next(ids))
    second, key = merchants.create(db, "USD")
    assert second == "m_0123456789abcdef"
    assert merchants.verify(db, key) == Merchant(second, "USD")


def test_rotate_kills_the_old_key_and_issues_a_new_one(db):
    merchant_id, old = merchants.create(db, "USD")
    new = merchants.rotate(db, merchant_id)
    assert new != old
    assert refused(db, old) == ("unknown_key", None)
    assert merchants.verify(db, new) == Merchant(merchant_id, "USD")


def test_revoke_kills_the_key_and_keeps_the_row(db):
    merchant_id, key = merchants.create(db, "USD")
    merchants.revoke(db, merchant_id)
    assert refused(db, key) == ("revoked", merchant_id)
    assert rows(db) == [(merchant_id, "USD", hashlib.sha256(key.encode()).hexdigest(), "revoked")]


def test_rotate_does_not_revive_a_revoked_merchant(db):
    merchant_id, _ = merchants.create(db, "USD")
    merchants.revoke(db, merchant_id)
    assert refused(db, merchants.rotate(db, merchant_id)) == ("revoked", merchant_id)


@pytest.mark.parametrize("change", [merchants.rotate, merchants.revoke])
def test_an_unknown_merchant_is_an_error_and_writes_nothing(db, change):
    merchant_id, key = merchants.create(db, "USD")
    with pytest.raises(merchants.UnknownMerchant, match="m_nope"):
        change(db, "m_nope")
    assert merchants.verify(db, key) == Merchant(merchant_id, "USD")
    assert len(rows(db)) == 1


@pytest.mark.parametrize("key", ["", "x", "not a key", "\x00", "é" * 43, "\ud800"])
def test_verify_refuses_a_wrong_or_malformed_key_as_unknown(db, key):
    merchants.create(db, "USD")
    assert refused(db, key) == ("unknown_key", None)


def test_verify_of_a_missing_registry_raises_and_creates_nothing(db):
    with pytest.raises(sqlite3.OperationalError):  # a wrong path is loud, not a silent 401
        merchants.verify(db, "anything")
    assert not db.parent.exists()


def test_a_reader_is_not_blocked_by_an_open_write(db):
    merchant_id, key = merchants.create(db, "USD")
    writer = sqlite3.connect(db, timeout=0)
    writer.execute("BEGIN EXCLUSIVE")  # the CLI mid-commit: blocks readers unless WAL
    writer.execute("UPDATE merchants SET status = 'revoked'")
    found = []
    reader = threading.Thread(target=lambda: found.append(merchants.verify(db, key)))
    reader.start()
    reader.join(5)
    writer.rollback()
    writer.close()
    assert found == [Merchant(merchant_id, "USD")]  # WAL: it reads the last committed state


def test_cli_create_prints_the_id_and_the_key_once(db, capsys):
    assert merchants.main(["--db", str(db), "create", "--currency", "GBP"]) is None
    out = capsys.readouterr().out
    merchant_id = re.search(r"merchant_id: (m_[a-z0-9]+)", out)[1]
    key = re.search(r"key: (\S+)", out)[1]
    assert out.count(key) == 1
    assert merchants.verify(db, key) == Merchant(merchant_id, "GBP")


def test_cli_rotate_prints_a_working_new_key(db, capsys):
    merchant_id, old = merchants.create(db, "USD")
    merchants.main(["--db", str(db), "rotate", merchant_id])
    new = re.search(r"key: (\S+)", capsys.readouterr().out)[1]
    assert refused(db, old) == ("unknown_key", None)
    assert merchants.verify(db, new) == Merchant(merchant_id, "USD")


def test_cli_revoke(db, capsys):
    merchant_id, key = merchants.create(db, "USD")
    merchants.main(["--db", str(db), "revoke", merchant_id])
    assert merchant_id in capsys.readouterr().out
    assert refused(db, key) == ("revoked", merchant_id)


def test_cli_reports_a_key_it_could_not_deliver(db, capsys, monkeypatch):
    class Closed(io.StringIO):  # a closed pipe: the write buffers, the flush fails
        def flush(self):
            raise BrokenPipeError

    monkeypatch.setattr(sys, "stdout", Closed())
    assert merchants.main(["--db", str(db), "create", "--currency", "USD"]) == 1
    merchant_id = rows(db)[0][0]
    assert f"rotate {merchant_id}" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("argv", "error"),
    [
        (["create", "--currency", "usd"], "currency"),
        (["rotate", "m_nope"], "m_nope"),
        (["revoke", "m_nope"], "m_nope"),
        ([], "required"),
    ],
)
def test_cli_errors_exit_2_without_printing_a_key(db, capsys, argv, error):
    with pytest.raises(SystemExit) as stopped:
        merchants.main(["--db", str(db), *argv])
    assert stopped.value.code == 2
    out, err = capsys.readouterr()
    assert "key:" not in out
    assert error in err
