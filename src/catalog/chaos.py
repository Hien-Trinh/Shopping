"""Chaos runner (docs/specs/step-7b.md): a scratch system under a steady load with one fault in
the middle, then the three oracles (design doc, "Three oracles"). Prints one JSON line; exits 0
only if every oracle holds.

    python -m catalog.chaos kill-worker --seconds 60 --rate 20
"""

import argparse
import contextlib
import json
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
        self.port, self.supervisor, self.restarts = _free_port(), None, 0
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
        self.wait(self.serving, 60, "the API never served")

    def restart(self, **options) -> None:
        """Stop, as an operator does, and start with another Procfile (rescale, a flag)."""
        self.stop()
        self.start(**options)
        self.restarts += 1

    def stop(self) -> None:
        self.supervisor.send_signal(signal.SIGTERM)
        if (code := self.supervisor.wait(timeout=60)) != 0:
            raise Failed(f"the supervisor stopped with exit {code}")

    def kill(self) -> None:
        """kill -9 the supervisor, reap it (an unreaped pid looks alive, 3e), and wait for what
        it ran to stop by itself, so their locks and the port are free."""
        old = self.pids()
        self.supervisor.kill()
        self.supervisor.wait()
        self.wait(lambda: not any(map(alive, old)), 10, "processes outlived their supervisor")
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

    def wait(self, condition: Callable[[], object], timeout: float, why: str) -> None:
        deadline = time.monotonic() + timeout
        while not condition():
            if time.monotonic() > deadline:
                raise Failed(why)
            time.sleep(0.2)

    def load(self, *flags) -> subprocess.Popen:
        """catalog.load in the background; its key goes through the environment, never argv."""
        argv = [sys.executable, "-m", "catalog.load", "--url", self.url, "--keys", str(KEYS),
                "--processes", "8", *map(str, flags)]  # fmt: skip
        env = os.environ | {"CATALOG_API_KEY": self.key}
        return subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, text=True)

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
        if self.supervisor and self.supervisor.poll() is None:
            with contextlib.suppress(Failed, subprocess.TimeoutExpired):
                self.stop()
        if self.supervisor:
            self.supervisor.kill()
            self.supervisor.wait()
        for pid in self.pids():
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)


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


def _second_load(earlier):
    """Another load beside the main one, same seed: `earlier` ms before it (0: duplicates)."""

    def fault(system, rng, at):
        system.extra.append(system.load(*system.flags, "--start-ms", system.start_ms - earlier))

    return fault


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
    "out-of-order": None,  # set in run: half the run's versions earlier
    "duplicates": _second_load(0),
    "poison": _poison,
    "classifier-outage": _windowed(classifier="down"),
    "kill-worker": _kill_worker,
    "kill-supervisor": _kill_supervisor,
    "rescale": _rescale,
    "disk-full": _windowed(min_free=2**62),
}


def run(system: System, scenario: str, *, seconds: float, rate: float, seed: int,
        settle: float) -> dict:  # fmt: skip
    """The scenario on a started system; the summary, with each oracle's differing keys."""
    rng, changes = random.Random(f"{seed}:{scenario}"), max(1, round(rate * seconds))
    system.start_ms, system.extra = time.time_ns() // 1_000_000, []
    system.flags = ["--rate", rate, "--changes", changes, "--seed", seed, "--deletes", 0.1]
    fault = SCENARIOS[scenario] or _second_load(changes // 2)  # out-of-order
    began = time.monotonic()
    main = system.load(*system.flags, "--start-ms", system.start_ms)

    def at(fraction):  # until that fraction of the load time has passed
        due = began + fraction * seconds
        system.wait(lambda: time.monotonic() >= due, seconds, "the load time ran out")

    fault(system, rng, at)
    loads = [json.loads(p.communicate()[0] or "null") for p in (main, *system.extra)]
    system.check()
    settled = time.monotonic()
    system.wait(lambda: system.check() or not pending(system.data), settle, "Changes pending")
    listings = store.ensure(str(system.data / "listing_store"))
    table = listings.metadata().id
    system.wait(lambda: state.load_watermark(system.root / "state", table) >= listings.version(),
                settle, "Change Export never reached the Listing Store's head")  # fmt: skip
    if scenario == "classifier-outage":
        system.wait(lambda: not flagged(system.data), settle, "Listings still flagged")
    out = {"settle_s": round(time.monotonic() - settled, 1)}
    system.stop()
    found = events.read(system.data / "events")
    outcomes = Counter(e["type"] for e in found if e["type"] in OUTCOMES)
    restarts = sum(e["type"] == "process_exit" for e in found)
    checks = oracles(system.data, system.root / "state")
    return out | {
        "loads": loads,
        "outcomes": dict(sorted(outcomes.items())),
        "process_exits": restarts,
        "supervisor_restarts": system.restarts,
        "oracles": {k: v[:SHOWN] or "ok" for k, v in checks.items()},
        "ok": not any(checks.values()),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = argparse.ArgumentParser(prog="python -m catalog.chaos")
    args.add_argument("scenario", choices=SCENARIOS)
    args.add_argument("--seconds", type=float, default=60, help="load time")
    args.add_argument("--rate", type=float, default=20, help="Changes/s")
    args.add_argument("--seed", type=int, default=0)
    args.add_argument("--dir", type=Path, help="an empty or new folder; default a temp one")
    args.add_argument("--settle", type=float, default=120, help="seconds to wait for Outcomes")
    a = args.parse_args(argv)
    if a.seconds <= 0 or a.rate <= 0 or a.settle <= 0:
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
