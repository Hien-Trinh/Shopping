"""Chaos runner (docs/specs/step-7b.md): a scratch system under a steady load with one fault in
the middle, then the three oracles (design doc, "Three oracles"). Prints one JSON line; exits 0
only if every oracle holds.

    python -m catalog.chaos kill-worker --seconds 60 --rate 20
"""

import argparse
import contextlib
import json
import math
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path

from deltalake import DeltaTable

from catalog import entry, events, export, landing, merchants, snapshots, state, store
from catalog.envelope import Change, Content
from catalog.keys import PARTITIONS
from catalog.plan import Outcome
from catalog.replay import diff, expected_store, live, replay_exports

PROCFILE = Path(__file__).parents[2] / "Procfile"
OUTCOMES = {o.value for o in Outcome} | {"failed"}
STARTS = ("worker_start", "export_start", "maintenance_start", "snapshots_start", "backfill_start")
KEYS = 1000  # small, so concurrent loads and deletes keep hitting the same Listings
SHOWN = 20  # differing keys per oracle in the summary


def procfile(text: str, port: int, *, workers=4, classifier="fake", min_free=0) -> str:
    """The repo's Procfile for a scratch system: its API on `port`, the passes fast, the
    `fake` classifier (CI never calls the paid Jev API). Raises if a line it rewrites changed."""

    def sub(pattern, replace, expected):
        nonlocal text
        text, n = re.subn(pattern, replace, text, flags=re.M)
        if n != expected:
            raise ValueError(f"the Procfile changed: {pattern} matched {n} lines, not {expected}")

    sub(r"^api: python -m catalog\.api$", rf"\g<0> --port {port} --min-free {min_free}", 1)
    sub(r"^export: python -m catalog\.export$", r"\g<0> --interval 0.2", 1)
    sub(r"^maintenance: python -m catalog\.maintenance$", r"\g<0> --interval 0.2", 1)
    # Every second at least, so consecutive snapshots never share a second's name.
    sub(r"^snapshots: python -m catalog\.snapshots$", r"\g<0> --every 1", 1)
    sub(r"^((?:worker-\d|backfill): .*)--classifier jev$", rf"\1--classifier {classifier}", 5)
    sub(r" --workers 4 ", f" --workers {workers} ", 4)
    sub(rf"^worker-[{workers}-9]: .*\n", "", 4 - workers)  # a rescale down drops the last ones
    if "jev" in text:
        raise ValueError("a Procfile line the rewrite missed would call the paid API")
    return text


def oracles(data: Path, state_dir: Path) -> dict[str, list[str]]:
    """The three oracles over a stopped system: one line per differing key (empty: it holds)."""
    log = landing.ensure(str(data / "landing_log"))
    landed = _landed(log)
    failed = {(e["submission_id"], e["change_index"]) for e in events.read(data / "events")
              if e["type"] == "failed"}  # fmt: skip
    skip = {i for i, x in enumerate(landed) if (x.submission_id, x.change_index) in failed}
    listings = store.ensure(str(data / "listing_store"))
    found = {"store": diff(expected_store([x.change for x in landed], skip),
                           store.fingerprints(listings))}  # fmt: skip
    watermark = state.load_watermark(state_dir, listings.metadata().id)
    files = [_rows(p) for p in export.files(data / "export")]
    at = live(store.fingerprints(listings, watermark)) if watermark >= 0 else {}
    found["export"] = diff(at, replay_exports(files))
    found["snapshot"] = [
        f"{path.name}: {line}"
        for path in snapshots.existing(data / "snapshots")
        for line in diff(
            store.fingerprints(listings, snapshots.pinned(path)),
            store.fingerprints(DeltaTable(str(path))),
        )  # fmt: skip
    ]
    return found


def _landed(log) -> list[landing.Landed]:
    """The whole Landing log in landing order."""
    return landing.read(log, dict.fromkeys(range(PARTITIONS), landing.START), 2**62,
                        max_versions=2**62).changes  # fmt: skip


def _rows(path: Path) -> list[dict]:
    import pyarrow.parquet as pq  # only the export oracle needs it

    return pq.read_table(path).to_pylist()


def pending(data: Path) -> int:
    """Landed Changes with no Outcome event yet."""
    done = {(e["submission_id"], e["change_index"]) for e in events.read(data / "events")
            if e["type"] in OUTCOMES}  # fmt: skip
    landed = _landed(landing.ensure(str(data / "landing_log")))
    return sum((x.submission_id, x.change_index) not in done for x in landed)


def flagged(data: Path) -> int:
    """Live Listings still waiting for the Backfill to reclassify them."""
    rows = store.ensure(str(data / "listing_store")).to_pyarrow_dataset().to_table(
        columns=["needs_reclassify", "is_tombstone"]).to_pylist()  # fmt: skip
    return sum(r["needs_reclassify"] and not r["is_tombstone"] for r in rows)


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # another user's process took the pid: not ours, but alive
        return True
    return True


class Failed(Exception):
    """The run can't go on: the summary says why, and it exits 1."""


class System:
    """The repo's Procfile under one supervisor in `root`, the API on a free port, one Merchant."""

    def __init__(self, root: Path):
        self.root, self.data = root, root / "data"
        self.port, self.supervisor, self.restarts, self.loads = _free_port(), None, 0, []
        (root / "data").mkdir(parents=True, exist_ok=True)
        _, self.key = merchants.create(root / merchants.DB, "USD")
        self.url = f"http://127.0.0.1:{self.port}"

    def start(self, **options) -> None:
        (self.root / "Procfile").write_text(procfile(PROCFILE.read_text(), self.port, **options))
        argv = [sys.executable, "-m", "catalog.supervisor"]
        # Its own session: a Ctrl-C reaches only the runner, which stops it in order. Its logs
        # go to stderr, so stdout holds only the summary.
        self.supervisor = subprocess.Popen(argv, cwd=self.root, start_new_session=True,
                                           stdout=2)  # fmt: skip
        wait(self.serving, 60, "the API never served")

    def restart(self, **options) -> None:
        """Stop, as an operator does, and start with another Procfile (rescale, a flag)."""
        self.stop()
        self.start(**options)
        self.restarts += 1

    def stop(self) -> None:
        self.supervisor.send_signal(signal.SIGTERM)
        try:
            code = self.supervisor.wait(timeout=60)
        except subprocess.TimeoutExpired:
            raise Failed("the supervisor took over 60 s to stop") from None
        if code != 0:
            raise Failed(f"the supervisor stopped with exit {code}")

    def kill(self) -> None:
        """kill -9 the supervisor, reap it (an unreaped pid looks alive, 3e), and wait for what
        it ran to stop by itself, so their locks and the port are free."""
        old = [pid for pid in self.pids() if alive(pid)]  # a long-dead one's pid may be reused
        self.supervisor.kill()
        self.supervisor.wait()
        wait(lambda: not any(map(alive, old)), 10, "processes outlived their supervisor")
        self.start()
        self.restarts += 1

    def pids(self) -> list[int]:
        """Every process the supervisors ran, the API included."""
        found = events.read(self.data / "events")
        api = subprocess.run(["pgrep", "-f", f"catalog.api --port {self.port} "],
                             capture_output=True).stdout.split()  # fmt: skip
        return [e["pid"] for e in found if e["type"] in STARTS] + [int(p) for p in api]

    def workers(self) -> list[int]:
        found = events.read(self.data / "events")
        return [e["pid"] for e in found if e["type"] == "worker_start" and alive(e["pid"])]

    def serving(self) -> bool:
        self.check()
        with contextlib.suppress(OSError), socket.create_connection(("127.0.0.1", self.port), 1):
            return True
        return False

    def check(self) -> None:
        if self.supervisor.poll() is not None:
            raise Failed(f"the supervisor exited with {self.supervisor.returncode}")

    def load(self, *flags) -> subprocess.Popen:
        """catalog.load in the background; its key goes through the environment, never argv."""
        argv = [sys.executable, "-m", "catalog.load", "--url", self.url, "--keys", str(KEYS),
                "--processes", "8", *map(str, flags)]  # fmt: skip
        env = os.environ | {"CATALOG_API_KEY": self.key}
        self.loads.append(subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, text=True))
        return self.loads[-1]

    def poison(self, rng: random.Random, n: int = 10) -> None:
        """Changes whose content fails storage, beside the API's appends: no post can carry one."""
        bad = Content.model_construct(title="poison", price_micros="free", currency="USD",
                                      availability="in_stock")  # fmt: skip
        submission, now = f"chaos-{uuid.uuid4().hex}", datetime.now(UTC)
        newest = time.time_ns() // 1_000_000 + 10**7  # past every load's, so it would be written
        merchant = merchants.verify(self.root / merchants.DB, self.key).merchant_id
        changes = [Change(merchant, f"p{rng.randrange(KEYS)}", newest + i, "upsert", bad)
                   for i in range(n)]  # fmt: skip
        log = landing.ensure(str(self.data / "landing_log"))
        landing.append(log, [(submission, i, c, now) for i, c in enumerate(changes)])

    def close(self) -> None:
        """Kill whatever is left, so a failed run leaks no process holding the port or locks."""
        for load in self.loads:
            load.kill()  # a no-op once it exited
            load.wait()
        if self.supervisor and self.supervisor.poll() is None:
            with contextlib.suppress(Failed, subprocess.TimeoutExpired):
                self.stop()
        if self.supervisor:
            self.supervisor.kill()
            self.supervisor.wait()
        for pid in filter(ours, self.pids()):
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)


def ours(pid: int) -> bool:
    """Whether `pid` still runs a catalog process: a long-dead one's pid may be reused."""
    command = subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True)
    return b"catalog." in command.stdout


def wait(condition: Callable[[], object], timeout: float, why: str | Callable[[], str]) -> None:
    """Until `condition` holds; past `timeout` seconds, Failed with `why` (called if callable)."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise Failed(why() if callable(why) else why)
        # ponytail: each settle poll rescans the events and the whole Landing log; fine at a
        # scenario's size, keep a cursor if runs grow to the 1M load.
        time.sleep(0.5)


def _free_port() -> int:
    # ponytail: another program could take it before the API binds; then readiness times out.
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _windowed(**options):
    """A fault on for the middle third of the run, through a restart either side."""

    def fault(system, rng, at):
        at(1 / 3)
        system.restart(**options)
        at(2 / 3)
        system.restart()

    return fault


def _duplicates(system, rng, at):
    """The main load again, at once: same seed and versions, as a retrying client sends."""
    system.extra.append(system.load(*system.flags, "--start-ms", system.start_ms))


def _out_of_order(system, rng, at):
    """The main load again, at once, its versions half the run's Changes older: each key gets
    older and newer versions in random arrival order, deletes among them."""
    earlier = system.start_ms - system.changes // 2
    system.extra.append(system.load(*system.flags, "--start-ms", earlier))


def _bulk(system, rng, at):
    at(1 / 2)
    system.extra.append(system.load("--changes", 10_000, "--batch", 10_000, "--rate", 0,
                                    "--seed", rng.randrange(2**31), "--deletes", 0.1))  # fmt: skip


def _poison(system, rng, at):
    at(1 / 2)
    system.poison(rng)


def _kill_worker(system, rng, at):
    for k in (1, 2, 3):
        at(k / 4)
        workers = system.workers()
        if not workers:
            raise Failed("no worker running to kill")
        with contextlib.suppress(ProcessLookupError):
            os.kill(rng.choice(sorted(workers)), signal.SIGKILL)


def _kill_supervisor(system, rng, at):
    at(1 / 2)
    system.kill()


def _rescale(system, rng, at):
    at(1 / 2)
    system.restart(workers=3)


SCENARIOS = {
    "steady": lambda system, rng, at: None,
    "bulk": _bulk,
    "out-of-order": _out_of_order,
    "duplicates": _duplicates,
    "poison": _poison,
    "classifier-outage": _windowed(classifier="down"),
    "kill-worker": _kill_worker,
    "kill-supervisor": _kill_supervisor,
    "rescale": _rescale,
    "disk-full": _windowed(min_free=2**62),
}

# What each fault must leave behind, so a fault that silently did nothing fails the run.
EFFECTS = {
    "steady": ("nothing", lambda r: True),
    "bulk": ("10,000 more accepted", lambda r: r["loads"][1]["accepted"] == 10_000),
    "out-of-order": ("stale Outcomes", lambda r: r["outcomes"].get("stale", 0) > 0),
    "duplicates": ("already_applied Outcomes", lambda r: r["outcomes"].get("already_applied")),
    "poison": ("10 failed Outcomes", lambda r: r["outcomes"].get("failed", 0) >= 10),
    "classifier-outage": ("classify_failed events", lambda r: r["events"]["classify_failed"]),
    "kill-worker": ("3 process exits", lambda r: r["events"]["process_exit"] >= 3),
    "kill-supervisor": ("a second supervisor", lambda r: r["events"]["supervisor_start"] == 2),
    "rescale": ("a 3-worker start", lambda r: r["rescaled"]),
    "disk-full": ("503s", lambda r: any("503" in load["statuses"] for load in r["loads"])),
}


def run(system: System, scenario: str, *, seconds: float, rate: float, seed: int,
        settle: float) -> dict:  # fmt: skip
    """The scenario on a started system; the summary, with each oracle's differing keys."""
    rng, system.changes = random.Random(f"{seed}:{scenario}"), max(1, round(rate * seconds))
    system.start_ms, system.extra = time.time_ns() // 1_000_000, []
    system.flags = ["--rate", rate, "--changes", system.changes, "--seed", seed, "--deletes", 0.1]
    began = time.monotonic()
    main = system.load(*system.flags, "--start-ms", system.start_ms)

    def at(fraction):  # until that fraction of the load time has passed
        due = began + fraction * seconds
        wait(lambda: time.monotonic() >= due, seconds, "the load time ran out")

    SCENARIOS[scenario](system, rng, at)
    loads = []
    for p in (main, *system.extra):
        try:
            out = p.communicate(timeout=seconds + settle)[0]
        except subprocess.TimeoutExpired:
            raise Failed(f"a load ran {seconds + settle:.0f} s past its start") from None
        try:
            loads.append(json.loads(out))
        except ValueError:
            raise Failed(f"a load printed no summary (exit {p.returncode})") from None
    if not loads[0]["accepted"]:
        raise Failed(f"nothing landed: {loads[0]['statuses']}, {loads[0]['errors']} errors")
    system.check()
    out = {"settle_s": settle_down(system, scenario, settle)}
    system.stop()
    found = events.read(system.data / "events")
    kinds = Counter(e["type"] for e in found)
    checks = oracles(system.data, system.root / "state")
    seen = {
        "landed": len(_landed(landing.ensure(str(system.data / "landing_log")))),
        "export_files": len(export.files(system.data / "export")),
        "snapshots": len(snapshots.existing(system.data / "snapshots")),
    }
    rescaled = any(e["type"] == "worker_start" and e["workers"] == 3 for e in found)
    effect, happened = EFFECTS[scenario]
    record = {"loads": loads, "outcomes": {k: kinds[k] for k in sorted(OUTCOMES & kinds.keys())},
              "events": kinds, "rescaled": rescaled}  # fmt: skip
    problems = [f"no {k}" for k, n in seen.items() if not n]  # an oracle that checked nothing
    problems += [] if happened(record) else [f"the fault left no {effect}"]
    return out | {
        "loads": loads,
        "outcomes": record["outcomes"],
        "process_exits": kinds["process_exit"],
        "supervisor_restarts": system.restarts,
        "checked": seen,
        "oracles": {
            k: {"differ": len(v), "first": v[:SHOWN]} if v else "ok" for k, v in checks.items()
        },  # fmt: skip
        "problems": problems,
        "ok": not any(checks.values()) and not problems,
    }


def settle_down(system, scenario: str, budget: float) -> float:
    """Wait, `budget` seconds in all, until every landed Change has an Outcome, Change Export
    has caught up and (after an outage) the Backfill has reclassified every flagged Listing.
    Returns the seconds it took."""
    began = time.monotonic()

    def left():
        return began + budget - time.monotonic()

    wait(lambda: system.check() or not pending(system.data), left(),
         lambda: f"{pending(system.data)} Changes pending after {budget:.0f} s")  # fmt: skip
    listings = store.ensure(str(system.data / "listing_store"))
    table = listings.metadata().id
    wait(lambda: state.load_watermark(system.root / "state", table) >= listings.version(), left(),
         "Change Export never reached the Listing Store's head")  # fmt: skip
    if scenario == "classifier-outage":
        wait(lambda: not flagged(system.data), left(),
             lambda: f"{flagged(system.data)} Listings still flagged")  # fmt: skip
    return round(time.monotonic() - began, 1)


def main(argv: Sequence[str] | None = None) -> int:
    args = argparse.ArgumentParser(prog="python -m catalog.chaos")
    args.add_argument("scenario", choices=SCENARIOS)
    args.add_argument("--seconds", type=float, default=60, help="load time")
    args.add_argument("--rate", type=float, default=20, help="Changes/s")
    args.add_argument("--seed", type=int, default=0)
    args.add_argument("--dir", type=Path, help="an empty or new folder; default a temp one")
    args.add_argument("--settle", type=float, default=120, help="seconds to wait for Outcomes")
    a = args.parse_args(argv)
    if not all(math.isfinite(x) and x > 0 for x in (a.seconds, a.rate, a.settle)):
        args.error("--seconds, --rate and --settle must be above 0")
    if a.dir and a.dir.exists() and any(a.dir.iterdir()):
        args.error(f"--dir {a.dir} is not empty: the store oracle needs an empty system (A4)")
    root = a.dir or Path(tempfile.mkdtemp(prefix=f"chaos-{a.scenario}-"))
    out, code, system = {"scenario": a.scenario, "seed": a.seed, "dir": str(root)}, 1, None
    try:
        system = System(root)
        system.start()
        out |= run(system, a.scenario, seconds=a.seconds, rate=a.rate, seed=a.seed,
                   settle=a.settle)  # fmt: skip
        code = 0 if out["ok"] else 1
    except Failed as e:
        out["error"] = str(e)
    except KeyboardInterrupt:
        out["error"], code = "interrupted", 130
    finally:
        if system:
            system.close()
    if code == 0 and not a.dir:
        shutil.rmtree(root, ignore_errors=True)
    print(json.dumps(out))
    return code


if __name__ == "__main__":
    entry.exit_with(main)
