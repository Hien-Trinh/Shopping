"""The shipped system end to end (docs/specs/step-4e.md): the supervisor runs the repo's Procfile,
a Merchant posts over a real socket, and the workers' Outcomes come back over HTTP."""

import contextlib
import itertools
import json
import os
import re
import signal
import socket
import subprocess
import sys
from pathlib import Path

import httpx2
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable
from test_supervisor import alive, wait_for

from catalog import chaos, events, export, landing, load, snapshots, state, store
from catalog.keys import PARTITIONS, partition
from catalog.landing import START
from catalog.plan import Outcome
from catalog.replay import diff, expected_store, live, replay_exports

PROCFILE = Path(__file__).parents[2] / "Procfile"
SUPERVISOR = [sys.executable, "-m", "catalog.supervisor"]
OUTCOMES = {o.value for o in Outcome} | {"failed"}


def free_port() -> int:
    # ponytail: another program could take it before the API binds; then readiness times out.
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def api_pids(port: int) -> list[int]:
    """The API serving `port`: it logs no start event, so find it by its command line."""
    found = subprocess.run(["pgrep", "-f", f"catalog.api --port {port}"], capture_output=True)
    return [int(pid) for pid in found.stdout.split()]


def bindable(port: int) -> bool:
    """Whether a new API could bind `port`: SO_REUSEADDR, as uvicorn sets, so closed
    connections lingering in TIME_WAIT don't count as taking it."""
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
        return True


def worker_pids(tmp: Path, start="worker_start") -> list[int]:
    found = events.read(tmp / "data" / "events")
    return [e["pid"] for e in found if e["type"] == start]


def export_pids(tmp: Path) -> list[int]:
    return worker_pids(tmp, "export_start")


def maintenance_pids(tmp: Path) -> list[int]:
    return worker_pids(tmp, "maintenance_start")


def snapshot_pids(tmp: Path) -> list[int]:
    return worker_pids(tmp, "snapshots_start")


def backfill_pids(tmp: Path) -> list[int]:
    return worker_pids(tmp, "backfill_start")


def serving(port: int) -> bool:
    with contextlib.suppress(httpx2.TransportError):
        return httpx2.get(f"http://127.0.0.1:{port}/submissions/x").status_code == 401
    return False


class System:
    """The repo's Procfile under one supervisor in `tmp`, the API on `port`, one Merchant."""

    def __init__(self, tmp: Path, port: int):
        self.tmp, self.port, self.supervisors = tmp, port, []
        # The fake classifier, --min-free 0 and fast passes, as the chaos runner's systems.
        text = chaos.procfile(PROCFILE.read_text(), port)
        (tmp / "Procfile").write_text(text)
        create = [sys.executable, "-m", "catalog.merchants", "create", "--currency", "USD"]
        out = subprocess.run(create, cwd=tmp, capture_output=True, text=True, check=True).stdout
        self.merchant_id, self.key = re.findall(r"^\w+: (\S+)$", out, flags=re.M)
        self.http = httpx2.Client(base_url=f"http://127.0.0.1:{port}", timeout=30,
                                 headers={"Authorization": f"Bearer {self.key}"})  # fmt: skip

    def start(self) -> subprocess.Popen:
        self.supervisors.append(subprocess.Popen(SUPERVISOR, cwd=self.tmp))
        wait_for(lambda: serving(self.port))
        return self.supervisors[-1]

    def post(self, *changes) -> str:
        r = self.http.post("/listings:batch", json={"changes": list(changes)})
        assert r.status_code == 202, r.text
        return r.json()["submission_id"]

    def outcomes(self, submission: str) -> dict[int, str]:
        """Once every Change has an Outcome."""

        def done():  # fmt: skip
            r = self.http.get(f"/submissions/{submission}")
            assert r.status_code == 200, r.text
            return r.json()["done"] and r.json()

        return {c["index"]: c["outcome"] for c in wait_for(done)["changes"]}

    def close(self):
        """Kill whatever is left, so a failed assert leaks no process holding the port or locks."""
        self.http.close()
        for proc in self.supervisors:
            proc.kill()
            proc.wait()
        others = [*export_pids(self.tmp), *snapshot_pids(self.tmp), *maintenance_pids(self.tmp)]
        others += backfill_pids(self.tmp)
        for pid in [*worker_pids(self.tmp), *others, *api_pids(self.port)]:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)


@pytest.fixture
def system(tmp_path):
    made = System(tmp_path, free_port())
    yield made
    made.close()


def one_per_partition(merchant_id: str) -> list[str]:
    """A Merchant product ID in each partition."""
    found = {}
    for i in itertools.count():
        found.setdefault(partition(merchant_id, f"sku-{i}"), f"sku-{i}")
        if len(found) == PARTITIONS:
            return list(found.values())


def upsert(mpid, sv=2, title="Red shirt"):
    listing = {"title": title, "price_micros": 1_000_000, "currency": "USD"}
    listing |= {"availability": "in_stock"}
    return {"op": "upsert", "merchant_product_id": mpid, "source_version": sv, "listing": listing}


def test_a_merchants_batches_travel_http_landing_log_worker_listing_store(system):
    supervisor = system.start()
    skus = one_per_partition(system.merchant_id)  # so all 4 workers have work
    invalid = upsert("bad", title="")
    first = system.post(*map(upsert, skus), invalid)
    # Sent without waiting for the first: Landing log order alone fixes these Outcomes.
    deleted = {"op": "delete", "merchant_product_id": skus[0], "source_version": 3}
    second = system.post(deleted, upsert(skus[1]), upsert(skus[2], sv=1))

    assert system.outcomes(first) == {i: "written" for i in range(PARTITIONS)} | {
        PARTITIONS: "rejected"
    }
    assert system.outcomes(second) == {0: "written", 1: "already_applied", 2: "stale"}
    data = system.tmp / "data"
    log = landing.ensure(str(data / "landing_log"))
    landed = landing.read(log, dict.fromkeys(range(PARTITIONS), START), 10_000).changes
    assert len(landed) == PARTITIONS + 3
    listings = store.ensure(str(data / "listing_store"))
    assert diff(expected_store([x.change for x in landed]), store.fingerprints(listings)) == []
    rows = listings.to_pyarrow_dataset().to_table().to_pylist()
    assert {r["primary_category"] for r in rows if not r["is_tombstone"]} == {"Fake > R"}
    assert [r["merchant_product_id"] for r in rows if r["is_tombstone"]] == [skus[0]]

    # Change Export (step 5a): replaying its files gives the live Listing Store.
    head = store.ensure(str(data / "listing_store")).version()
    table = listings.metadata().id
    wait_for(lambda: state.load_watermark(system.tmp / "state", table) == head)
    files = [pq.read_table(p).to_pylist() for p in export.files(data / "export")]
    assert diff(live(store.fingerprints(listings, head)), replay_exports(files)) == []

    # Catalog Snapshots (step 5c): each equals the Listing Store at its pinned version.
    wait_for(lambda: [snapshots.pinned(p) for p in snapshots.existing(data / "snapshots")][-1:]
             == [head])  # fmt: skip
    for path in snapshots.existing(data / "snapshots"):
        copy = store.fingerprints(DeltaTable(str(path)))
        assert diff(store.fingerprints(listings, snapshots.pinned(path)), copy) == []

    pids = [*worker_pids(system.tmp), *export_pids(system.tmp), *snapshot_pids(system.tmp)]
    pids += [*maintenance_pids(system.tmp), *backfill_pids(system.tmp), *api_pids(system.port)]
    assert len(pids) == 9  # 4 workers, Change Export, Snapshots, maintenance, Backfill, the API
    assert not bindable(system.port)  # so the check below can fail
    supervisor.send_signal(signal.SIGTERM)
    assert supervisor.wait(timeout=30) == 0
    assert not any(map(alive, pids))
    assert bindable(system.port)


def test_killing_the_supervisor_stops_its_api_so_a_new_one_serves_on_its_port(system):
    first = system.start()
    wait_for(lambda: len(worker_pids(system.tmp)) == 4)
    [api], workers = api_pids(system.port), worker_pids(system.tmp)
    first.kill()
    first.wait()  # reaped, as by the shell that started it: until then its pid looks alive
    wait_for(lambda: not alive(api), timeout=5)
    wait_for(lambda: not any(map(alive, workers)), timeout=10)  # 3e: their locks are free
    wait_for(lambda: not any(map(alive, export_pids(system.tmp))), timeout=10)  # and its lock
    wait_for(lambda: not any(map(alive, maintenance_pids(system.tmp))), timeout=10)
    wait_for(lambda: not any(map(alive, snapshot_pids(system.tmp))), timeout=10)
    wait_for(lambda: not any(map(alive, backfill_pids(system.tmp))), timeout=10)
    second = system.start()  # its API binds the same port: no restart loop
    assert api_pids(system.port) not in ([], [api])
    second.send_signal(signal.SIGTERM)
    assert second.wait(timeout=30) == 0


def test_the_load_generator_posts_through_the_real_system(system, monkeypatch, capsys):
    system.start()
    monkeypatch.setenv("CATALOG_API_KEY", system.key)
    url = f"http://127.0.0.1:{system.port}"
    argv = ["--url", url, "--changes", "200", "--batch", "10", "--keys", "500", "--rate", "0"]
    assert load.main([*argv, "--processes", "2", "--seed", "3"]) == 0
    out = capsys.readouterr()
    summary = json.loads(out.out)
    assert summary["statuses"] == {"202": 20}
    assert (summary["sent"], summary["accepted"], summary["errors"]) == (200, 200, 0)
    assert system.key not in out.out + out.err

    # Every Change gets an Outcome: one per (submission, index) among the workers' events.
    def outcomes():
        found = events.read(system.tmp / "data" / "events")
        done = {(e["submission_id"], e["change_index"]) for e in found if e["type"] in OUTCOMES}
        return len(done) == 200

    wait_for(outcomes, timeout=60)


def test_the_load_generator_needs_a_key(monkeypatch, capsys):
    monkeypatch.delenv("CATALOG_API_KEY", raising=False)
    with pytest.raises(SystemExit) as stopped:
        load.main(["--changes", "1"])
    assert stopped.value.code == 2
    assert "CATALOG_API_KEY" in capsys.readouterr().err
