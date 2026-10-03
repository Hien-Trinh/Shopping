"""Ingestion API: POST /listings:batch (design doc, lifecycle steps 1-2; docs/specs/step-4b.md).

Accepted Changes land in one Landing log commit per request; only then do their events go out, and
only then the 202, so no `accepted` event ever names a Change that didn't land.
"""

import argparse
import contextlib
import json
import shutil
import sqlite3
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from catalog import entry, envelope, landing, merchants, state
from catalog.events import EventLog
from catalog.keys import partition

MAX_BODY = 32 * 2**20  # bytes; uvicorn and FastAPI set no limit (plan-v1 B8)
MIN_FREE = 5 * 2**30  # bytes of free disk below which every write gets 503 (plan-v1 A13)
UNAUTHORIZED = "missing or invalid API key"  # the same for every reason: it tells a caller nothing


def create_app(
    data: Path,
    db: Path,
    *,
    max_body: int = MAX_BODY,
    min_free: int = MIN_FREE,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    # The query every request runs, so a missing or unreadable registry fails here, at startup.
    with contextlib.suppress(merchants.Denied):
        merchants.verify(db, "")
    log = landing.ensure(str(data / "landing_log"))
    events = EventLog(data / "events", "api", clock)
    app = FastAPI(openapi_url=None)  # no /docs: nothing here is a FastAPI model

    def refuse(status: int, reason: str, detail: str, merchant_id: str | None = None):
        """The refusal, and a `refused` event to count it by: never the key, its hash, the body."""
        event = {"type": "refused", "status": status, "reason": reason}
        with contextlib.suppress(OSError):  # best effort: a full disk never turns a 4xx into a 500
            events.emit([event | ({"merchant_id": merchant_id} if merchant_id else {})])
        challenge = {"WWW-Authenticate": "Bearer"} if status == 401 else None
        return JSONResponse({"detail": detail}, status, challenge)

    @app.post("/listings:batch")
    async def submit(request: Request):
        received = datetime.fromtimestamp(clock(), UTC)
        if shutil.disk_usage(data).free < min_free:  # first: a low disk refuses everything
            return refuse(503, "low_disk", "the server is low on disk; retry later")
        scheme, _, key = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer":
            return refuse(401, "no_key", UNAUTHORIZED)
        try:
            merchant = merchants.verify(db, key.strip())
        except merchants.Denied as e:
            return refuse(401, e.reason, UNAUTHORIZED, e.merchant_id)
        too_large = f"the body is over {max_body} bytes"
        length = request.headers.get("content-length")  # uvicorn has checked it's a number
        if length is not None and int(length) > max_body:
            return refuse(413, "too_large", too_large, merchant.merchant_id)
        body = bytearray()
        async for chunk in request.stream():  # counted: a chunked body declares no length
            body += chunk
            if len(body) > max_body:
                return refuse(413, "too_large", too_large, merchant.merchant_id)
        try:
            raw = json.loads(body)
        except ValueError, RecursionError:  # deep nesting raises RecursionError, not ValueError
            return refuse(400, "bad_json", "the body is not JSON", merchant.merchant_id)
        try:
            checked = envelope.check_batch(
                raw,
                merchant_id=merchant.merchant_id,
                currency=merchant.currency,
                now_ms=int(received.timestamp() * 1000),
            )
        except envelope.BadBatch as e:
            return refuse(400, "bad_batch", str(e), merchant.merchant_id)
        submission = str(uuid.uuid7())  # before the append: its events fall in its hour or later
        landing.append(log, [(submission, i, c) for i, c in checked.accepted], received)
        events.emit(submission_events(submission, merchant.merchant_id, checked))
        rejected = [{"index": i, "errors": list(errors)} for i, errors in checked.rejected]
        reply = {"submission_id": submission, "accepted": len(checked.accepted)}
        return JSONResponse(reply | {"rejected": rejected}, 202)

    return app


def submission_events(submission: str, merchant_id: str, checked: envelope.Checked) -> list[dict]:
    """An `accepted` event per landed Change and a `rejected` one per invalid Change."""
    ids = {"submission_id": submission, "merchant_id": merchant_id}
    return [
        *(
            ids
            | {"type": "accepted", "change_index": i}
            | {"merchant_product_id": c.merchant_product_id, "partition": partition(*c.key)}
            for i, c in checked.accepted
        ),
        *(
            ids | {"type": "rejected", "change_index": i, "errors": list(errors)}
            for i, errors in checked.rejected
        ),
    ]


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m catalog.api")
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--db", type=Path, default=merchants.DB)
    args.add_argument("--port", type=int, default=8000)
    a = args.parse_args(argv)
    try:
        app = create_app(a.data, a.db)
    except sqlite3.Error as e:
        args.error(f"no usable merchant registry at {a.db} ({e}): run catalog.merchants create")
    # Never wider than localhost: there is no TLS and no rate limit yet (plan-v1, section C).
    uvicorn.run(app, host="127.0.0.1", port=a.port)


if __name__ == "__main__":
    entry.exit_with(main)
