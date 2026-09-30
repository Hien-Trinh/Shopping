"""Phase 0 spikes (throwaway; results in spikes/NOTES.md).

uv run --with deltalake --with pyarrow python spikes/delta_spikes.py [name ...]
"""

import multiprocessing as mp
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

SCHEMA = pa.schema(
    [("partition", pa.int32()), ("key", pa.string()), ("sv", pa.int64()), ("title", pa.string())]
)
CDF = {"delta.enableChangeDataFeed": "true"}


def rows(parts, n, sv=1, prefix="k"):
    ps = [p for p in parts for _ in range(n)]
    return pa.table(
        {
            "partition": pa.array(ps, pa.int32()),
            "key": [f"{prefix}{p}_{i}" for p in parts for i in range(n)],
            "sv": pa.array([sv] * len(ps), pa.int64()),
            "title": ["x" * 200] * len(ps),
        },
        schema=SCHEMA,
    )


def merge(path, part, tbl):
    return (
        DeltaTable(path)
        .merge(
            tbl,
            predicate=f"t.partition = {part} AND t.partition = s.partition AND t.key = s.key",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all(predicate="s.sv > t.sv")
        .when_not_matched_insert_all()
        .execute()
    )


def fresh(path, tbl, **kw):
    shutil.rmtree(path, ignore_errors=True)
    write_deltalake(path, tbl, partition_by=["partition"], configuration=CDF, **kw)


# --- B1: try to reproduce the MERGE hang -------------------------------------------------
HANG_CHILD = """
import sys; sys.path.insert(0, {here!r})
from delta_spikes import fresh, merge, rows
fresh({path!r}, rows(range(8), 100))
merge({path!r}, 0, rows([0], 3, sv=5))
"""


def b1_hang(tmp):
    path, hangs, runs = str(tmp / "b1"), 0, 40
    for _ in range(runs):
        code = HANG_CHILD.format(here=str(Path(__file__).parent), path=path)
        try:
            subprocess.run([sys.executable, "-c", code], timeout=30, check=True)
        except subprocess.TimeoutExpired:
            hangs += 1
    return f"{hangs}/{runs} fresh-table + first-MERGE runs hung (30 s timeout)"


# --- kill -9 mid-MERGE leaves the table consistent (watchdog premise) --------------------
def _merge_forever(path):
    merge(path, 0, rows([0], 5000, sv=int(time.time() * 1000)))


def kill_mid_merge(tmp):
    path = str(tmp / "kill")
    fresh(path, rows(range(4), 5000))
    bad, killed_mid = 0, 0
    for _ in range(30):
        before = DeltaTable(path).version()
        p = mp.Process(target=_merge_forever, args=(path,))
        p.start()
        time.sleep(random.uniform(0, 0.4))
        if p.is_alive():
            killed_mid += 1
            p.kill()
        p.join()
        dt = DeltaTable(path)
        n = dt.to_pyarrow_table().num_rows
        if dt.version() not in (before, before + 1) or n != 20000:
            bad += 1
    merge(path, 0, rows([0], 10, sv=2**40))  # table still writable afterwards
    return f"{killed_mid}/30 killed mid-MERGE; {bad} left the table inconsistent; next MERGE ok"


# --- B2: OPTIMIZE concurrent with MERGE and with appends ---------------------------------
def _merger(path, part, n, q):
    errors = 0
    for i in range(n):
        for _ in range(10):
            try:
                merge(path, part, rows([part], 5, sv=10 + i))
                break
            except Exception:
                errors += 1
    q.put(("merge", part, errors))


def _compactor(path, parts, n, q):
    errors, last = 0, ""
    for _ in range(n):
        for _ in range(10):
            try:
                DeltaTable(path).optimize.compact(
                    partition_filters=[("partition", "in", [str(p) for p in parts])]
                )
                break
            except Exception as e:
                errors += 1
                last = type(e).__name__
    q.put(("compact", tuple(parts), errors, last))


def _appender(path, n, q):
    errors = 0
    for i in range(n):
        for _ in range(10):
            try:
                write_deltalake(path, rows(range(64), 1, sv=i, prefix=f"a{i}_"), mode="append")
                break
            except Exception:
                errors += 1
    q.put(("append", errors))


def b2_optimize(tmp):
    path, q = str(tmp / "b2"), mp.Queue()
    fresh(path, rows(range(8), 200))
    ps = [mp.Process(target=_merger, args=(path, p, 30, q)) for p in range(4)]
    ps += [mp.Process(target=_compactor, args=(path, [4, 5, 6, 7], 10, q))]  # other partitions
    ps += [mp.Process(target=_compactor, args=(path, [0, 1], 10, q))]  # partitions being MERGEd
    [p.start() for p in ps]
    [p.join() for p in ps]
    merges = [q.get() for _ in ps]

    log = str(tmp / "b2log")
    fresh(log, rows(range(64), 1))
    ps = [mp.Process(target=_appender, args=(log, 40, q)) for _ in range(2)]
    ps += [mp.Process(target=_compactor, args=(log, list(range(64)), 5, q))]
    [p.start() for p in ps]
    [p.join() for p in ps]
    appends = [q.get() for _ in ps]
    store = DeltaTable(path).to_pyarrow_table().to_pylist()
    keys = [r["key"] for r in store]
    stale = [r for r in store if r["partition"] < 4 and r["key"].split("_")[1] in {str(i) for i in range(5)} and r["sv"] != 39]
    landed = DeltaTable(log).to_pyarrow_table().num_rows
    return (f"merge vs compact retries: {merges}\nappend vs compact retries: {appends}\n"
            f"store: {len(keys)} rows, {len(keys) - len(set(keys))} duplicate keys, {len(stale)} lost updates; "
            f"log: {landed} rows (expect {64 + 2 * 40 * 64})")


# --- B3: per-partition change-feed reads of an append-only log ---------------------------
def b3_cdf(tmp):
    path = str(tmp / "b3")
    fresh(path, rows(range(64), 1, prefix="init"))
    start = DeltaTable(path).version() + 1
    for i in range(100):
        write_deltalake(path, rows(range(64), 5, sv=i, prefix=f"c{i}_"), mode="append")
    dt = DeltaTable(path)
    t = time.perf_counter()
    full = pa.table(dt.load_cdf(starting_version=start).read_all())
    t_full = time.perf_counter() - t
    t = time.perf_counter()
    try:
        part = pa.table(
            dt.load_cdf(starting_version=start, predicate="partition IN (0, 1, 2, 3)").read_all()
        )
        pred = f"predicate: {part.num_rows} rows in {time.perf_counter() - t:.3f}s"
    except TypeError as e:
        pred = f"predicate unsupported: {e}"
    dt.optimize.compact()
    after = pa.table(DeltaTable(path).load_cdf(starting_version=start).read_all())
    return (
        f"full: {full.num_rows} rows in {t_full:.3f}s; {pred} (expect 2000)\n"
        f"after OPTIMIZE, change feed still {after.num_rows} rows (no duplicates if == {full.num_rows})"
    )


# --- group-commit sizing: append latency by batch size ----------------------------------
def append_latency(tmp):
    out = []
    for size in (1, 10, 100, 1000):
        path = str(tmp / f"app{size}")
        fresh(path, rows([0], 1))
        n = 30
        t = time.perf_counter()
        for i in range(n):
            write_deltalake(path, rows(range(64), max(1, size // 64), sv=i, prefix=f"b{i}_"),
                            mode="append")
        ms = (time.perf_counter() - t) / n * 1000
        out.append(f"{size:>5} rows/commit: {ms:6.1f} ms/commit")
    return "\n".join(out)


# --- MERGE latency into a 1M-row store, before and after compaction ---------------------
def merge_latency(tmp):
    path = str(tmp / "big")
    fresh(path, rows(range(64), 15_625))  # 1M rows
    def timed(k):
        t = time.perf_counter()
        for i in range(k):
            merge(path, 7, rows([7], 1000, sv=100 + i))
        return (time.perf_counter() - t) / k * 1000
    first = timed(5)
    timed(45)
    before = timed(5)
    DeltaTable(path).optimize.compact(partition_filters=[("partition", "=", "7")])
    after = timed(5)
    return (f"1,000-row MERGE into 1M-row store: {first:.0f} ms fresh, "
            f"{before:.0f} ms after 50 MERGEs, {after:.0f} ms after compacting")


SPIKES = {f.__name__: f for f in (b1_hang, kill_mid_merge, b2_optimize, b3_cdf,
                                   append_latency, merge_latency)}

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.default_int_handler)
    import deltalake

    print("deltalake", deltalake.__version__, "python", sys.version.split()[0])
    for name in sys.argv[1:] or SPIKES:
        with tempfile.TemporaryDirectory() as d:
            t = time.perf_counter()
            result = SPIKES[name](Path(d))
            print(f"\n## {name} ({time.perf_counter() - t:.0f}s)\n{result}", flush=True)
