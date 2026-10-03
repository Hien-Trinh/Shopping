import contextlib
import os
import random
import signal
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import pytest
from support import delete, listing, product_in, reclassify, up
from test_supervisor import alive, wait_for
from test_worker import A, Env, failed_indexes, is_poison, poison

from catalog import events, landing, state, store, worker
from catalog.keys import PARTITIONS
from catalog.replay import diff, expected_store

DRIVER = Path(__file__).with_name("chaos_driver.py")
SUPERVISOR = [sys.executable, "-m", "catalog.supervisor"]
pytestmark = pytest.mark.filterwarnings("ignore::UserWarning")  # pydantic, on poison content


def test_a_watched_worker_stops_when_its_supervisor_dies_and_exits_hard_if_stuck():
    stop, seen, checks = threading.Event(), [], iter([True, True, False])
    worker.watch(
        lambda: next(checks), stop, deadline=1.5,
        sleep=lambda s: seen.append((s, stop.is_set())), exit=seen.append,
    )  # fmt: skip
    # Checked each second while it lived. Once it's gone, stop is set so the tick in progress
    # can finish; a worker still running a deadline later is stuck in a native call: exit 1.
    assert seen == [(1, False), (1, False), (1.5, True), 1]


def starts(tmp):
    """The pid of every worker run so far, from its worker_start event."""
    return [e["pid"] for e in events.read(tmp / "events") if e["type"] == "worker_start"]


@pytest.fixture
def spawn(tmp_path):
    """Popen. Whatever a test leaves running is killed when it ends, its workers included."""
    procs = []
    yield lambda argv, **kwargs: procs.append(subprocess.Popen(argv, **kwargs)) or procs[-1]
    for proc in procs:
        proc.kill()
        proc.wait()
    for pid in starts(tmp_path):  # a killed supervisor's, if they outlived it
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


def driver(tmp, *wrap):
    """Worker 0 of 4 (partitions 0-15) on tmp's directories, run by chaos_driver.py."""
    args = ["--index", "0", "--workers", "4", "--data", str(tmp), "--state", str(tmp / "state")]
    return [sys.executable, DRIVER, *wrap, "--", *args]


def procfile(tmp, workers):
    lines = (f"worker-{i}: python -m catalog.worker --index {i} --workers {workers}"
             " --data . --state state\n" for i in range(workers))  # fmt: skip
    (tmp / "Procfile").write_text("".join(lines))


def test_killing_the_supervisor_stops_its_workers_so_a_new_one_can_start_them(tmp_path, spawn):
    procfile(tmp_path, 2)
    first = spawn(SUPERVISOR, cwd=tmp_path)
    wait_for(lambda: len(starts(tmp_path)) == 2)
    pids = starts(tmp_path)
    first.kill()
    first.wait()  # reaped, as by the shell that started it: until then its pid looks alive
    wait_for(lambda: not any(map(alive, pids)), timeout=10)
    with state.claim(tmp_path / "state", range(PARTITIONS)):  # their locks are free
        pass
    second = spawn(SUPERVISOR, cwd=tmp_path)
    wait_for(lambda: len(starts(tmp_path)) == 4)  # both started again: no PartitionTaken
    second.send_signal(signal.SIGTERM)
    assert second.wait(timeout=30) == 0


def test_a_worker_stuck_when_its_supervisor_dies_still_exits(tmp_path, spawn):
    gone = subprocess.Popen(["true"])
    gone.wait()  # a supervisor that died: its pid names no process now
    env = os.environ | {"CATALOG_SUPERVISOR": str(gone.pid)}
    stuck = spawn(driver(tmp_path, "landing.ensure", "hang", "1"), env=env)  # after its claim
    assert stuck.wait(timeout=10) == 1  # the watch's deadline, 1 s here
    with state.claim(tmp_path / "state", range(16)):  # its locks are free
        pass


def test_a_supervisor_started_while_old_workers_hold_partitions_stops_loudly(tmp_path):
    procfile(tmp_path, 1)
    with state.claim(tmp_path / "state", [0]):  # a worker of a killed supervisor, still exiting
        run = subprocess.run(SUPERVISOR, cwd=tmp_path, capture_output=True, text=True, timeout=60)
    assert run.returncode == 3 and "PartitionTaken" in run.stderr


def caught_up(env, partitions=range(16)):
    """Every partition's offset is past the Landing log's last commit: all of it is processed."""
    table = landing.table_id(env.landing)
    offsets = state.load_offsets(env.state, partitions, table)
    return min(v for v, _ in offsets.values()) > env.landing.version()


def drain(env, spawn):
    """Restart the worker; stop it once it has started (so SIGTERM is handled) and processed
    the whole Landing log."""
    runs = len(starts(env.tmp))
    proc = spawn(driver(env.tmp))
    wait_for(lambda: len(starts(env.tmp)) > runs and caught_up(env))
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=30) == 0


def reported(env):
    """How many Outcomes each Change got: more than one means its batch was replayed."""
    counts = Counter(e["change_index"] for e in events.read(env.events_root) if "change_index" in e)
    return [counts[i] for i in range(len(env.landed))]


def assert_the_oracle_holds(env):
    failed = failed_indexes(env)
    assert failed == {i for i, c in enumerate(env.landed) if is_poison(c)}
    fingerprints = store.fingerprints(env.store)
    assert diff(expected_store(env.landed, failed), fingerprints) == []
    assert env.store.to_pyarrow_dataset().count_rows() == len(fingerprints)  # no Listing twice
    assert set(env.outcomes()) == set(range(len(env.landed)))  # none pending


X = product_in(0)  # A is in partition 3
WRITTEN = {0: "written", 1: "written", 2: "stale", 3: "written", 4: "failed"}
REPLAYED = {0: "stale", 1: "already_applied", 2: "stale", 3: "already_applied", 4: "failed"}


@pytest.mark.parametrize(
    ("kill", "outcomes", "reports"),
    [
        (["store.read", "after", "1"], WRITTEN, [1] * 5),  # nothing written: all redone
        (["store.merge", "after", "1"], REPLAYED, [1] * 5),  # committed, but no events yet
        (["state.save_offsets", "before", "1"], WRITTEN, [2] * 5),  # the best Outcome wins
        # Between offset files (after the startup beat and p00's): only partition 3 replays.
        (["state.save", "after", "2"], WRITTEN, [1, 2, 2, 1, 2]),
    ],
)
def test_a_worker_killed_at_any_step_of_a_batch_loses_and_duplicates_nothing(
    tmp_path, spawn, kill, outcomes, reports
):
    env = Env(tmp_path)
    env.land(up(X, 1), up(A, 2), up(A, 1), delete(X, 2), poison(A, 3))
    assert spawn(driver(tmp_path, *kill)).wait(timeout=30) == -signal.SIGKILL
    drain(env, spawn)
    assert_the_oracle_holds(env)
    assert (env.outcomes(), reported(env)) == (outcomes, reports)


def test_a_worker_killed_while_compacting_leaves_the_store_readable(tmp_path, spawn):
    env = Env(tmp_path)
    for i in range(8):  # a small file per partition per batch: something to compact
        env.land(*(up(product_in(p, prefix=f"k{i}"), 1) for p in range(16)))
        env.run()
    env.land(up(A, 1))
    killed = spawn(driver(tmp_path, "store.compact", "during", "1"))  # after that one batch
    assert killed.wait(timeout=30) == -signal.SIGKILL
    assert_the_oracle_holds(env)
    store.compact(env.store, range(16))  # the next compaction succeeds
    assert_the_oracle_holds(env)


KEYS = [X, A, product_in(9), product_in(3, prefix="alt")]


def random_change(rng):
    k, sv = rng.choice(KEYS), rng.randint(1, 6)
    make = rng.choices([up, delete, reclassify, poison], weights=[4, 2, 1, 1])[0]
    if make is up:
        return up(k, sv, listing(rng.choice("ABC")))
    return make(k) if make is reclassify else make(k, sv)


@pytest.mark.parametrize("kills", [3, pytest.param(40, marks=pytest.mark.slow)])
def test_kills_at_random_moments_lose_and_duplicate_nothing(tmp_path, spawn, kills):
    env, rng = Env(tmp_path), random.Random(kills)
    for run in range(1, kills + 1):
        for _ in range(rng.randint(1, 3)):
            env.land(*(random_change(rng) for _ in range(rng.randint(1, 8))))
        proc = spawn(driver(tmp_path))
        wait_for(lambda n=run: len(starts(tmp_path)) == n)
        time.sleep(rng.uniform(0, 0.1))  # in its first batch, mostly
        proc.kill()
        proc.wait()
    drain(env, spawn)
    assert_the_oracle_holds(env)


def test_rescaling_from_4_workers_to_3_resumes_every_partition_from_its_offset(tmp_path, spawn):
    env = Env(tmp_path)
    keys = [product_in(p) for p in range(0, PARTITIONS, 3)]  # every worker's, under 4 and 3
    for workers, sv in ((4, 1), (3, 3)):  # the rescale: stop, edit the Procfile, start again
        env.land(*(up(k, sv) for k in keys))
        procfile(tmp_path, workers)
        supervisor = spawn(SUPERVISOR, cwd=tmp_path)
        wait_for(lambda: caught_up(env, range(PARTITIONS)))  # so its workers have started
        if workers == 4:
            env.land(*(up(k, 2) for k in keys))  # stopped mid-stream: the 3 finish it
        supervisor.send_signal(signal.SIGTERM)
        assert supervisor.wait(timeout=30) == 0
    assert_the_oracle_holds(env)
    assert reported(env) == [1] * len(env.landed)  # nothing replayed, so nothing reread
