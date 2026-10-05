"""Load generator (docs/specs/step-7a.md): posts generated Changes to a running API from several
processes, then prints one JSON summary line. The Merchant's key comes from CATALOG_API_KEY and
is never printed.

    python -m catalog.load --rate 50 --changes 30000                    # steady
    python -m catalog.load --changes 10000 --batch 10000                # bulk
    python -m catalog.load --changes 1000000 --keys 1000000 --order sequential --batch 1000
"""

import argparse
import json
import math
import multiprocessing
import os
import queue
import random
import signal
import sys
import time
import urllib.error
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from http.client import HTTPException
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from catalog import entry, envelope

LABELS = Path(__file__).parents[2] / "eval" / "labels.jsonl"
MAX_CHANGES = 10_000_000  # source_version runs ahead of the clock by up to this many ms (A14)
DESCRIPTION = 500  # characters: a 10k batch stays about 8 MB, under the API's 32 MB
TIMEOUT = 30  # seconds per request; not retried, since a retry is a duplicate (7b's scenario)
AVAILABILITY = ("in_stock", "out_of_stock", "preorder")


def texts(path: Path = LABELS) -> list[tuple[str, str]]:
    """(title, description) of the labeled set's real Listings."""
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return [(r["title"], r["description"][:DESCRIPTION]) for r in rows]


def change(i: int, *, seed, start_ms: int, keys: int, order: str, texts, currency: str,
           deletes: float = 0) -> dict:  # fmt: skip
    """Change `i` of a run: the same `(seed, i, start_ms)` always gives the same Change, a delete
    with chance `deletes`."""
    rng = random.Random(f"{seed}:{i}")  # a str seed is hashed with SHA-512: stable across runs
    n = i % keys if order == "sequential" else rng.randrange(keys)
    title, description = texts[i % len(texts)]
    listing = {"title": title, "description": description, "currency": currency}
    listing |= {"price_micros": rng.randint(1, 10**9), "availability": rng.choice(AVAILABILITY)}
    # A later Change of a key is newer. A later run is newer only once it starts after the
    # earlier run's last version, start_ms + changes ms: overlap and its Changes go stale.
    source_version = start_ms + i
    if rng.random() < deletes:  # drawn last, so the other draws match a run without deletes
        return {"op": "delete", "merchant_product_id": f"p{n}", "source_version": source_version}
    return {"op": "upsert", "merchant_product_id": f"p{n}", "source_version": source_version,
            "listing": listing}  # fmt: skip


def batches(j: int, processes: int, *, changes: int, batch: int, **options) -> Iterable[dict]:
    """Process `j`'s request bodies: batches j, j + processes, j + 2 * processes, ..."""
    for b in range(j, -(-changes // batch), processes):
        span = range(b * batch, min((b + 1) * batch, changes))
        yield {"changes": [change(i, **options) for i in span]}


def send(bodies: Iterable[dict], post: Callable[[dict], tuple[int, dict]], *, rate: float,
         clock=time.monotonic, sleep=time.sleep, stop=lambda: False) -> dict:  # fmt: skip
    """Post each body when due: `t0 + changes sent before it / rate`, at once if late (open
    loop, so a slow API shows as latency, not a lower rate). Counts every answer; raises none.
    A 401 ends the loop: every later request would get one too.

    ponytail: a process blocks on each POST, so a slow API caps the rate at processes / latency
    (4 processes at 156 ms: 25/s, step-7a.md); the summary's late_s shows it. Async sends if
    one Mac's processes can't keep up."""
    tally = {"sent": 0, "accepted": 0, "errors": 0, "late": 0.0, "latencies": []}
    statuses, t0 = Counter(), clock()
    for body in bodies:
        if stop():
            break
        if rate:
            due = t0 + tally["sent"] / rate
            if (wait := due - clock()) > 0:
                sleep(wait)
            tally["late"] = max(tally["late"], clock() - due)
        began = clock()
        try:
            status, reply = post(body)
        # Refused, reset or timed out (URLError and TimeoutError are OSErrors), a cut-off
        # response (HTTPException) or a body that isn't JSON (ValueError).
        except OSError, HTTPException, ValueError:
            tally["errors"] += 1
            status, reply = None, {}
        tally["latencies"].append(clock() - began)
        tally["sent"] += len(body["changes"])
        tally["accepted"] += reply.get("accepted", 0) if status == 202 else 0
        if status is not None:
            statuses[status] += 1
        if status == 401:
            break
    return tally | {"statuses": dict(statuses)}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):  # urllib would re-send the key to wherever it points
        return None


_urlopen = build_opener(_NoRedirect).open


def http(url: str, key: str):
    def post(body: dict) -> tuple[int, dict]:
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        request = Request(f"{url}/listings:batch", json.dumps(body).encode(), headers)
        try:
            with _urlopen(request, timeout=TIMEOUT) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:  # a status, not a lost connection
            return e.code, {}

    return post


def _process(j: int, options: dict, stop: Callable[[], bool]) -> dict:
    o = dict(options)
    url, rate, processes = o.pop("url"), o.pop("rate"), o.pop("processes")
    bodies = batches(j, processes, texts=texts(), **o)
    post = http(url, os.environ["CATALOG_API_KEY"])
    return send(bodies, post, rate=rate / processes, stop=stop)


def _child(j: int, options: dict, stopping, results) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the parent stops us through `stopping`
    results.put((j, _process(j, options, stopping.is_set)))  # a raise exits 1, traceback shown


def collect(procs, results, stopping, poll: float = 0.2) -> tuple[dict[int, dict], list[int]]:
    """Each process's tally as it arrives, and the processes that ended without one: killed,
    or raised. A dead child is noticed, never waited on forever. Ctrl-C sets `stopping`, so
    each process ends after its request in flight."""
    tallies, pending = {}, set(range(len(procs)))
    while pending:
        try:
            j, tally = results.get(timeout=poll)
            tallies[j] = tally
            pending.discard(j)
        except queue.Empty:
            # A child puts its tally before it exits, so an empty queue means none is coming.
            pending -= {j for j in pending if procs[j].exitcode is not None and results.empty()}
        except KeyboardInterrupt:
            stopping.set()
    return tallies, sorted(set(range(len(procs))) - tallies.keys())


def summary(tallies: Sequence[dict], elapsed: float, start_ms: int, failed=()) -> dict:
    statuses, latencies = Counter(), sorted(x for t in tallies for x in t["latencies"])
    for t in tallies:
        statuses.update(t["statuses"])
    sent = sum(t["sent"] for t in tallies)

    def ms(q):  # nearest rank, in milliseconds
        return round(1000 * latencies[max(math.ceil(q * len(latencies)) - 1, 0)], 1)

    return {
        "start_ms": start_ms,
        "processes": len(tallies) + len(failed),
        "failed_processes": list(failed),
        "sent": sent,
        "accepted": sum(t["accepted"] for t in tallies),
        "statuses": {str(s): n for s, n in sorted(statuses.items())},
        "errors": sum(t["errors"] for t in tallies),
        "changes_per_s": round(sent / elapsed, 1) if elapsed else None,
        "latency_ms": {"p50": ms(0.5), "p99": ms(0.99), "max": ms(1)} if latencies else None,
        "late_s": round(max((t["late"] for t in tallies), default=0.0), 3),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = argparse.ArgumentParser(prog="python -m catalog.load")
    args.add_argument("--url", default="http://127.0.0.1:8000")
    args.add_argument("--rate", type=float, default=50, help="Changes/s in all; 0: no pacing")
    args.add_argument("--changes", type=int, default=3000)
    args.add_argument("--batch", type=int, default=1)
    args.add_argument("--keys", type=int, default=10_000)
    args.add_argument("--order", choices=("random", "sequential"), default="random")
    # 16: a batch-1 POST waits about 156 ms for the group commit, so 4 processes top out at 25/s
    args.add_argument("--processes", type=int, default=16)
    args.add_argument("--seed", type=int, default=0)
    args.add_argument("--currency", default="USD", help="the Merchant's currency")
    args.add_argument("--deletes", type=float, default=0, help="the fraction of deletes")
    args.add_argument("--start-ms", type=int, help="the first source_version; default now")
    a = args.parse_args(argv)
    if not os.environ.get("CATALOG_API_KEY"):
        args.error("CATALOG_API_KEY is not set: export the Merchant's key first")
    if not 1 <= a.changes <= MAX_CHANGES:
        args.error(f"--changes must be 1 to {MAX_CHANGES}")
    if not 1 <= a.batch <= envelope.MAX_BATCH:
        args.error(f"--batch must be 1 to {envelope.MAX_BATCH}")
    if a.keys < 1 or a.processes < 1 or a.rate < 0:
        args.error("--keys and --processes must be at least 1, and --rate at least 0")
    if not 0 <= a.deletes <= 1:
        args.error("--deletes must be 0 to 1")
    if urlsplit(a.url).scheme not in ("http", "https"):  # urllib would also take file: or ftp:
        args.error("--url must be http:// or https://")
    a.processes = min(a.processes, -(-a.changes // a.batch))  # an idle one's rate share is lost
    start_ms = a.start_ms if a.start_ms is not None else time.time_ns() // 1_000_000
    options = vars(a) | {"start_ms": start_ms}
    ctx = multiprocessing.get_context("spawn")
    stopping, results, began = ctx.Event(), ctx.Queue(), time.monotonic()
    procs = [ctx.Process(target=_child, args=(j, options, stopping, results))
             for j in range(a.processes)]  # fmt: skip
    for p in procs:
        p.start()
    tallies, failed = collect(procs, results, stopping)
    for p in procs:
        p.join()
    for j in failed:
        print(
            f"load: process {j} ended without a tally (exit {procs[j].exitcode})", file=sys.stderr
        )
    out = summary(list(tallies.values()), time.monotonic() - began, start_ms, failed)
    print(json.dumps(out))
    if stopping.is_set():
        return 130
    return 0 if not failed and out["accepted"] == out["sent"] else 1  # every Change landed


if __name__ == "__main__":
    entry.exit_with(main)
