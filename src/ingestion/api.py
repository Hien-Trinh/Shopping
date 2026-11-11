"""Ingestion API: POST /listings:batch and GET /submissions/{id} (docs/specs/step-4b.md, 4d.md).

Every Change goes through the group commit (docs/specs/step-4c.md, step-a1.md): the accepted ones
land in the Landing log, then all of them in the Submission log. Only then do their events go out,
and only then the 202, so no `accepted` event or Submission log row names a Change that didn't
land.
"""

import argparse
import asyncio
import contextlib
import json
import os
import shutil
import signal
import sqlite3
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import uvicorn
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from ingestion import entry, envelope, landing, merchants, state, status, tables
from ingestion import events as event_files
from ingestion.events import EventLog
from ingestion.keys import partition
from ingestion.tables import uuid7_at  # its home: the job needs it without FastAPI

MAX_BODY = 32 * 2**20  # bytes; uvicorn and FastAPI set no limit (plan-v1 B8)
MIN_FREE = 5 * 2**30  # bytes of free disk below which every write gets 503 (plan-v1 A13)
UNAUTHORIZED = "missing or invalid API key"  # the same for every reason: it tells a caller nothing
NOT_FOUND = "no such submission"  # the same for an unknown id and another Merchant's


def create_app(
    data: Path,
    db: Path,
    *,
    max_body: int = MAX_BODY,
    min_free: int = MIN_FREE,
    clock: Callable[[], float] = time.time,
    window: float = landing.WINDOW,
) -> FastAPI:
    # The query every request runs, so a missing or unreadable registry fails here, at startup.
    with contextlib.suppress(merchants.Denied):
        merchants.verify(db, "")
    # Opened, never created: two replicas starting on an empty store would both create them.
    # The init step does (tables.init), and a missing one fails here, before uvicorn binds.
    log = DeltaTable(str(data / "landing_log"))
    submission_log = DeltaTable(str(data / "submission_log"))
    events = EventLog(data / "events", "api", clock)
    # Its own file: the commit runs in a thread, beside the event loop's emits on `events`.
    retries = EventLog(data / "events", "api", clock)

    def commit(records: list[tables.Record]) -> int:
        """The Landing log's commit first, so no Submission log row names a Change that didn't
        land. Both are looked up at each commit, not bound here, so a test can make either fail."""
        accepted = [(s, i, change, at) for s, i, _, change, at in records if change is not None]
        version = landing.append(log, accepted, retries)
        tables.record(submission_log, records, retries)
        return version

    appender = landing.Appender(commit, window)

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI):
        # uvicorn drains the requests in flight before this resumes, so they all get their commit.
        task = asyncio.create_task(appender.run())
        yield
        task.cancel()

    app = FastAPI(openapi_url=None, lifespan=lifespan)  # no /docs: nothing here is a model

    def refuse(status: int, reason: str, detail: str, merchant_id: str | None = None):
        """The refusal, and a `refused` event to count it by: never the key, its hash, the body."""
        event = {"type": "refused", "status": status, "reason": reason}
        with contextlib.suppress(OSError):  # best effort: a full disk never turns a 4xx into a 500
            events.emit([event | ({"merchant_id": merchant_id} if merchant_id else {})])
        challenge = {"WWW-Authenticate": "Bearer"} if status == 401 else None
        return JSONResponse({"detail": detail}, status, challenge)

    def authenticate(request: Request) -> merchants.Merchant | JSONResponse:
        """The Merchant the request's Bearer key belongs to, or the 401 to send instead."""
        scheme, _, key = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer":
            return refuse(401, "no_key", UNAUTHORIZED)
        try:
            return merchants.verify(db, key.strip())
        except merchants.Denied as e:
            return refuse(401, e.reason, UNAUTHORIZED, e.merchant_id)

    @app.post("/listings:batch")
    async def submit(request: Request):
        received = datetime.fromtimestamp(clock(), UTC)
        if shutil.disk_usage(data).free < min_free:  # first: a low disk refuses everything
            return refuse(503, "low_disk", "the server is low on disk; retry later")
        merchant = authenticate(request)
        if isinstance(merchant, JSONResponse):
            return merchant
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
        except (ValueError, RecursionError):  # deep nesting raises RecursionError, not ValueError
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
        # Before the append, from the clock its events use: they fall in its hour or later (A12).
        submission = str(uuid7_at(int(received.timestamp() * 1000)))
        # Rejected Changes too: the Submission log records every Change before the 202. The
        # commit is shared with the other requests in the window.
        mid = merchant.merchant_id
        records = [(submission, i, mid, c, received) for i, c in checked.accepted]
        records += [(submission, i, mid, None, received) for i, _ in checked.rejected]
        await appender.submit(records)
        events.emit(submission_events(submission, mid, checked))
        rejected = [{"index": i, "errors": list(errors)} for i, errors in checked.rejected]
        reply = {"submission_id": submission, "accepted": len(checked.accepted)}
        return JSONResponse(reply | {"rejected": rejected}, 202)

    @app.get("/submissions/{submission_id}")
    async def lookup(request: Request, submission_id: str):
        merchant = authenticate(request)  # no disk guard: a read lands nothing
        if isinstance(merchant, JSONResponse):
            return merchant

        def not_found(reason: str) -> JSONResponse:
            return refuse(404, reason, NOT_FOUND, merchant.merchant_id)

        try:
            parsed = uuid.UUID(submission_id)
            if parsed.version != 7:
                raise ValueError
            # Only the hours from the id's own (A12).
            since = datetime.fromtimestamp(0, UTC) + timedelta(milliseconds=parsed.int >> 80)
        except (ValueError, OverflowError):  # a 48-bit timestamp reaches past the year 9999
            return not_found("unknown_submission")
        # Its events are pruned (step 5d): a forged old id would otherwise scan every hour kept.
        if since < datetime.fromtimestamp(clock(), UTC) - event_files.RETENTION:
            return not_found("expired_submission")
        submission = str(parsed)  # canonical, so case and braces don't matter
        # In a thread, so a long scan doesn't stall the POSTs; emits stay on this one.
        found = await run_in_threadpool(
            lambda: status.fold(submission, event_files.read(data / "events", since, submission))
        )
        if found is None:
            return not_found("unknown_submission")
        if found.merchant_id != merchant.merchant_id:
            return not_found("not_owner")
        changes = [{"index": i, "outcome": o} for i, o in found.outcomes.items()]
        reply = {"submission_id": submission, "done": found.done, "counts": dict(found.counts)}
        return JSONResponse(reply | {"changes": changes})

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
            ids | {"type": status.Outcome.REJECTED, "change_index": i, "errors": list(errors)}
            for i, errors in checked.rejected
        ),
    ]


class _Server(uvicorn.Server):
    """Logs api_start with the port it bound once it listens: with --port 0 the OS picks a free
    one, so a caller never chooses a port another process could take before the API binds it."""

    def __init__(self, config: uvicorn.Config, log: EventLog):
        super().__init__(config)
        self.log = log

    async def startup(self, sockets=None) -> None:
        await super().startup(sockets)
        port = self.servers[0].sockets[0].getsockname()[1]
        with contextlib.suppress(OSError):  # best effort: a full disk at boot still serves
            self.log.emit([{"type": "api_start", "pid": os.getpid(), "port": port}])


def main(argv: Sequence[str] | None = None) -> None:
    args = argparse.ArgumentParser(prog="python -m ingestion.api")
    args.add_argument("--data", type=Path, default=state.DATA)
    args.add_argument("--db", type=Path, default=merchants.DB)
    args.add_argument("--port", type=int, default=8000)
    args.add_argument("--min-free", type=int, default=MIN_FREE, help="bytes; 0 turns the guard off")
    a = args.parse_args(argv)
    supervisor = entry.supervisor_pid(args)  # first: create_app writes to disk
    try:
        app = create_app(a.data, a.db, min_free=a.min_free)
    except sqlite3.Error as e:
        args.error(f"no usable merchant registry at {a.db} ({e}): run ingestion.merchants create")
    except TableNotFoundError as e:
        args.error(f"no tables in {a.data} ({e}): run python -m ingestion.tables --data {a.data}")
    # Never wider than localhost: there is no TLS and no rate limit yet (plan-v1, section C).
    log = EventLog(a.data / "events", "api")
    server = _Server(uvicorn.Config(app, host="127.0.0.1", port=a.port), log)
    # So a kill -9ed supervisor leaves no API holding the port: uvicorn finishes the requests in
    # flight and stops (step 4e).
    stop = SimpleNamespace(set=lambda: setattr(server, "should_exit", True), reason=None)
    entry.watch_supervisor(supervisor, a.data / "events", "api", stop=stop)
    # uvicorn handles these while it serves, then restores these handlers and re-raises the
    # signal: with Python's defaults the process would die by it before api_stop is logged.
    # So the first one stops the server (that re-raise, or a signal before uvicorn's handlers
    # are in), and a second one dies by it, as before: a hung API still stops on a repeat.
    heard = []

    def on_signal(sig, _):
        if heard:
            signal.signal(sig, signal.SIG_DFL)
            signal.raise_signal(sig)
        heard.append(sig)
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)
    with event_files.stopping(log, stop):  # api_stop
        server.run()


if __name__ == "__main__":
    entry.exit_with(main)
