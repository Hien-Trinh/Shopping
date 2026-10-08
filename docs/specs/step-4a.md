# Step 4a: merchant registry and admin CLI (mini PRD)

Status: approved Oct 3: the scope split (4a ships `verify`, 4b wires it), `revoke` in 4a, the hard-cutover rotate and the test points are confirmed. Plan row: [plan-v1.md, PR steps, 4a](../plan-v1.md). Design: [registry row](../design-commerce-ingestion-pipeline.md) and A16 in [plan-v1.md](../plan-v1.md). Terms follow [GLOSSARY.md](../../GLOSSARY.md).

## Problem

Nothing stores Merchants yet. The Ingestion API (4b) will authenticate every request against an API key and needs the Merchant's `merchant_id` and `currency` to stamp and validate a Change (`envelope.check_batch` already takes both). A16 settled the shape; this step builds it:

- A SQLite registry: `merchant_id`, `currency`, `key_hash`, `status`.
- An admin CLI to create a Merchant and rotate its key.
- A `verify(key)` the API will call per request to authenticate.

Security is the point of this step, so its rules are not simplified: the plaintext key is shown once and never stored, only its SHA-256 is kept, and the compare is constant-time.

## Solution

1. `src/catalog/merchants.py`: a thin SQLite shell with `create`, `rotate`, `revoke` and `verify`, plus schema/connection setup (WAL).
2. A CLI (`python -m catalog.merchants`) with `create`, `rotate` and `revoke` subcommands, exiting through `entry.exit_with` like the worker and supervisor.
3. No change to any existing module. 4b wires `verify` into the API's auth; this step ships and tests `verify` on its own.

## User stories

1. As an admin, I create a Merchant with a currency and get a one-time API key printed, so the Merchant can start submitting. The key is shown once and is unrecoverable afterwards.
2. As an admin, I rotate a Merchant's key and get a new one-time key; the old key stops working the instant the rotate commits.
3. As an admin, I revoke a Merchant so its key stops working, while its `merchant_id`, `currency` and data stay, so I can cut off access without deleting anything.
4. As a security-conscious operator, I want only the SHA-256 of a key in the database, never the key itself, so a leaked `merchants.sqlite` cannot be used to submit.
5. As the Ingestion API, I verify a presented key in one indexed lookup and get back `(merchant_id, currency)` or nothing, so I can authenticate a request. An empty key, an unknown key, or a revoked Merchant's key all return nothing.
6. As an operator, I want the registry safe for the API to read while the CLI writes, so an admin action never corrupts it or blocks ingestion.

## Failure scenarios

Each becomes a test or is named out of scope.

| Scenario | Expected |
|---|---|
| `create` with a valid currency | A row is written; one API key is printed; `verify` of that key returns `(merchant_id, currency)` |
| `create` with a bad currency (not `^[A-Z]{3}$`) | Rejected before any write; nonzero exit; nothing printed as a key |
| `create` twice | Two distinct `merchant_id`s (`m_[a-z0-9]+`), each with its own key |
| `rotate` an existing Merchant | A new key prints; the old key no longer verifies; the new key does |
| `rotate` / `revoke` an unknown `merchant_id` | Clear error, nonzero exit, no partial write |
| `revoke` an active Merchant | Its key stops verifying; the row and its `currency` remain; status is `revoked` |
| `verify` of an empty or malformed key | Returns nothing; never raises |
| `verify` of a wrong key | Returns nothing |
| `verify` of a revoked Merchant's correct key | Returns nothing |
| First use, no DB file yet | `create`, `rotate` and `revoke` create the file and `merchants` table (WAL). `verify` only reads: it raises rather than creating an empty registry at a wrong path (from the review) |
| CLI writes while a reader reads | WAL lets the read proceed; no corruption |
| Stored `key_hash` inspected | It equals `sha256(key)` hex and is never the plaintext key |

## Implementation decisions

1. **One table, created on connect.** `_connect(path)` opens SQLite, sets `PRAGMA journal_mode=WAL`, and runs `CREATE TABLE IF NOT EXISTS merchants(merchant_id TEXT PRIMARY KEY, currency TEXT NOT NULL, key_hash TEXT NOT NULL UNIQUE, status TEXT NOT NULL)`. No migrations in v1. Default path `data/merchants.sqlite` (design doc data layout), overridable with `--db`.
2. **Short-lived connection per call.** Every public function opens its own connection, acts, closes. This sidesteps SQLite's cross-thread rules (FastAPI runs sync deps in a threadpool) and is cheap under WAL. Ceiling: a connection per `verify` call; cache a per-process handle in 4b only if a stress measurement says so.
3. **Identity and keys, all stdlib.**
   - `merchant_id = "m_" + secrets.token_hex(8)` — hex is a subset of `[a-z0-9]`, satisfies `m_[a-z0-9]+`, and never contains `/` (keeps A7's partition separator safe). On the astronomically rare PRIMARY KEY collision, regenerate.
   - API key = `secrets.token_urlsafe(32)`, printed once to stdout, never stored or logged.
   - `key_hash = hashlib.sha256(key.encode()).hexdigest()`.
4. **Verify by indexed hash, confirmed constant-time.** `verify` computes `sha256(presented)` and does `SELECT merchant_id, currency, status FROM merchants WHERE key_hash = ?`. A match is confirmed with `hmac.compare_digest(row_hash, computed_hash)`, and a `revoked` row returns nothing. The exact-hash lookup avoids scanning every Merchant; `compare_digest` keeps the final equality constant-time (A16).
5. **Rotate is a hard cutover.** One `key_hash` per Merchant; rotate overwrites it, so the old key dies immediately. No second active key, no grace window.
6. **Revoke keeps the row.** `status` goes to `revoked`; `merchant_id`, `currency` and `key_hash` stay. `verify` gates on status. (`status` is `active` or `revoked`.)
7. **CLI shape mirrors the worker.** `argparse` with `create`/`rotate`/`revoke` subcommands, a `main(argv)` entry, `--db` default `data/merchants.sqlite`, launched under `entry.exit_with`. An unknown Merchant or bad currency exits nonzero (argparse's `error` → exit 2) so a scripted admin notices.

## Testing decisions

- **Location:** `tests/integration/test_merchants.py`, against a real SQLite file in `tmp_path`, no mocks (matches the landing/store/state convention). Fast enough to stay in `make check`.
- **Registry roundtrips:** create → `verify` returns `(merchant_id, currency)`; rotate invalidates the old key and validates the new; revoke invalidates; wrong/empty key → nothing; unknown-Merchant rotate/revoke raise.
- **Security asserts:** the printed key never appears in the DB; the stored `key_hash` equals `sha256(key)` hex; two Merchants get distinct ids and keys.
- **Shape, not value:** the id and key are opaque; assert `m_[a-z0-9]+` and key length/charset and a create→verify roundtrip, never a fixed random value. No injected RNG.
- **CLI as a black box:** drive `main(argv)`, capture stdout for the one-time key and exit codes for the error paths, following `test_worker.py`'s CLI tests.
- **Coverage:** `merchants.py` is a shell module under the 90% package gate, not the 100% pure set.

## Out of scope

- API auth enforcement and the 413/503 guards (4b); the IDOR check on `GET /submissions/{id}` (4d).
- A `list` / inspect command — read the table with the `sqlite3` CLI or DuckDB.
- Multiple active keys, key history, rotation grace periods.
- Per-merchant rate limits, a merchant read API, multi-market/multi-currency Merchants (design non-goals).
- Deleting a Merchant.

## Size

About 70 lines of production code and roughly 150 of tests, so the PR stays well under 300.
