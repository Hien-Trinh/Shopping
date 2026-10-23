"""Time the student's encoder: total Listings/s for `--procs` processes encoding at once, per
CHUNK and ONNX-threads setting. A spec quotes this command next to its numbers.

    make bench ARGS="--chunk 256,16 --threads 0,2 --procs 4,1"

Each round waits until the 1-minute load average is under `--max-load` (other work on the Mac
moved one setting from 10 s to 32 s), then runs every setting, reversing their order every other
round so drift hits all of them alike. Prints one JSON line per run, then the median per setting.
"""

import argparse
import itertools
import json
import multiprocessing as mp
import os
import statistics
import time
from pathlib import Path

ROWS = 2000  # training rows skipped before the first slice (each process takes its own slice)


def _work(chunk, threads, offset, n, models, barrier, out):
    from catalog import classify, student

    student.CHUNK = chunk
    if threads:  # 0: fastembed's default; forced over whatever the caller passes
        load = classify.fastembed
        classify.fastembed = lambda *a, **k: load(*a, **{**k, "threads": threads})
    embed, _ = student.embedder(models)
    rows = student.load_training()[offset : offset + n]
    texts = [student._text(r["title"], r["description"]) for r in rows]
    student.embed_chunks(embed, texts[:64])  # the ONNX session's warm-up
    barrier.wait()
    start = time.perf_counter()
    student.embed_chunks(embed, texts)
    out.put(time.perf_counter() - start)


def _run(chunk, threads, procs, n, models) -> float:
    barrier, out = mp.Barrier(procs), mp.Queue()
    ps = [
        mp.Process(target=_work, args=(chunk, threads, ROWS + i * n, n, models, barrier, out))
        for i in range(procs)
    ]
    for p in ps:
        p.start()
    secs = [out.get() for _ in ps]
    for p in ps:
        p.join()
    return procs * n / max(secs)


def _ints(text: str) -> list[int]:
    return [int(x) for x in text.split(",")]


def main() -> None:
    args = argparse.ArgumentParser(prog="scripts/bench-encode.py")
    args.add_argument("--chunk", type=_ints, default=[16])
    args.add_argument("--threads", type=_ints, default=[0], help="0: fastembed's default")
    args.add_argument("--procs", type=_ints, default=[4, 1])
    args.add_argument("--rounds", type=int, default=3)
    args.add_argument("--rows", type=int, default=1000, help="Listings per process")
    args.add_argument("--max-load", type=float, default=8.0)
    args.add_argument("--models", type=Path, default=Path("models"))
    a = args.parse_args()
    settings = list(itertools.product(a.chunk, a.threads, a.procs))
    got = {s: [] for s in settings}
    for rnd in range(a.rounds):
        while (load := os.getloadavg()[0]) >= a.max_load:
            print(f"waiting: load {load:.1f} >= {a.max_load}", flush=True)
            time.sleep(30)
        for s in settings if rnd % 2 == 0 else settings[::-1]:
            rate = _run(*s, a.rows, a.models)
            got[s].append(rate)
            line = {"round": rnd, "chunk": s[0], "threads": s[1], "procs": s[2]}
            print(json.dumps(line | {"per_s": round(rate), "load": round(load, 1)}), flush=True)
    print("chunk threads procs  median Listings/s (total)")
    for (chunk, threads, procs), rates in got.items():
        print(f"{chunk:>5} {threads or 'dflt':>7} {procs:>5}  {statistics.median(rates):.0f}")


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
