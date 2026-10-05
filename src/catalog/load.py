"""Load generator (docs/specs/step-7a.md): posts generated Changes to a running API from several
processes, then prints one JSON summary line. The Merchant's key comes from CATALOG_API_KEY and
is never printed.

    python -m catalog.load --rate 50 --changes 30000                    # steady
    python -m catalog.load --changes 10000 --batch 10000                # bulk
    python -m catalog.load --changes 1000000 --keys 1000000 --order sequential --batch 1000
"""

import argparse
import json
import multiprocessing
import os
import random
import signal
import sys
import time
import urllib.error
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
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


def change(i: int, *, seed, start_ms: int, keys: int, order: str, texts, currency: str) -> dict:
    """Change `i` of a run: the same `(seed, i, start_ms)` always gives the same Change."""
    rng = random.Random(f"{seed}:{i}")  # a str seed is hashed with SHA-512: stable across runs
    n = i % keys if order == "sequential" else rng.randrange(keys)
    title, description = texts[i % len(texts)]
    listing = {"title": title, "description": description, "currency": currency}
    listing |= {"price_micros": rng.randint(1, 10**9), "availability": rng.choice(AVAILABILITY)}
    source_version = start_ms + i  # a later Change of a key is newer, and so is a later run
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
    A 401 ends the loop: every later request would get one too."""
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
        except OSError:  # refused, reset or timed out: URLError and TimeoutError are OSErrors
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


def _init(stopping):
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the parent stops us through `stopping`
    global _stopping
    _stopping = stopping


def _process(j: int, options: dict) -> dict:
    o = dict(options)
    url, rate, processes = o.pop("url"), o.pop("rate"), o.pop("processes")
    bodies = batches(j, processes, texts=texts(), **o)
    post = http(url, os.environ["CATALOG_API_KEY"])
    return send(bodies, post, rate=rate / processes, stop=_stopping.is_set)


def summary(tallies: Sequence[dict], elapsed: float, start_ms: int) -> dict:
    statuses, latencies = Counter(), sorted(x for t in tallies for x in t["latencies"])
    for t in tallies:
        statuses.update(t["statuses"])
    sent = sum(t["sent"] for t in tallies)

    def ms(q):  # nearest rank, in milliseconds
        return round(1000 * latencies[min(int(q * len(latencies)), len(latencies) - 1)], 1)

    return {
        "start_ms": start_ms,
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
    a = args.parse_args(argv)
    if not os.environ.get("CATALOG_API_KEY"):
        args.error("CATALOG_API_KEY is not set: export the Merchant's key first")
    if not 1 <= a.changes <= MAX_CHANGES:
        args.error(f"--changes must be 1 to {MAX_CHANGES}")
    if not 1 <= a.batch <= envelope.MAX_BATCH:
        args.error(f"--batch must be 1 to {envelope.MAX_BATCH}")
    if a.keys < 1 or a.processes < 1 or a.rate < 0:
        args.error("--keys and --processes must be at least 1, and --rate at least 0")
    start_ms = time.time_ns() // 1_000_000
    options = vars(a) | {"start_ms": start_ms}
    ctx = multiprocessing.get_context("spawn")
    stopping, began = ctx.Event(), time.monotonic()
    tallies, code = [], 0
    with ctx.Pool(a.processes, _init, (stopping,)) as pool:
        running = [pool.apply_async(_process, (j, options)) for j in range(a.processes)]
        for j, result in enumerate(running):
            while True:
                try:
                    tallies.append(result.get(timeout=0.2))
                    break
                except multiprocessing.TimeoutError:
                    continue
                except KeyboardInterrupt:  # each process ends after its request in flight
                    stopping.set()
                    code = 130
                except Exception as e:
                    print(f"load: process {j} failed: {e!r}", file=sys.stderr)
                    code = code or 1
                    break
    out = summary(tallies, time.monotonic() - began, start_ms)
    print(json.dumps(out))
    if not code and ("401" in out["statuses"] or "202" not in out["statuses"]):
        code = 1
    return code


if __name__ == "__main__":
    entry.exit_with(main)
