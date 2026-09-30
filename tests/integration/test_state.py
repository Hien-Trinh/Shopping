import os
import signal
import subprocess
import sys
import time

import pytest

from catalog import state
from catalog.landing import START


def test_load_default_and_round_trip(tmp_path):
    path = tmp_path / "x" / "w.json"
    assert state.load(path, {"v": 0}) == {"v": 0}
    state.save(path, {"v": 1})
    assert state.load(path, None) == {"v": 1}


def test_crash_mid_save_keeps_the_old_value(tmp_path, monkeypatch):
    path = tmp_path / "w.json"
    state.save(path, {"v": 1})

    def crash(*_):
        raise OSError("power cut")

    monkeypatch.setattr(os, "replace", crash)
    with pytest.raises(OSError):
        state.save(path, {"v": 2})
    assert state.load(path, None) == {"v": 1}


def test_offsets(tmp_path):
    assert state.load_offsets(tmp_path, [0, 1]) == {0: START, 1: START}
    state.save_offsets(tmp_path, {1: (7, 3)})
    assert state.load_offsets(tmp_path, [0, 1]) == {0: START, 1: (7, 3)}


def test_claim_is_exclusive_and_released(tmp_path):
    with state.claim(tmp_path, [1, 2]):
        with pytest.raises(state.PartitionTaken, match="partition 2"), state.claim(tmp_path, [2]):
            pass
        with state.claim(tmp_path, [3]):  # others are free
            pass
    with state.claim(tmp_path, [1, 2]):  # released on exit
        pass


HOLDER = """
import sys, time
from pathlib import Path
from catalog import state
with state.claim(Path(sys.argv[1]), [7]):
    print("held", flush=True)
    time.sleep(60)
"""


def test_lock_blocks_other_processes_and_dies_with_its_holder(tmp_path):
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(tmp_path)], stdout=subprocess.PIPE, text=True
    )
    try:
        assert holder.stdout.readline() == "held\n"
        with pytest.raises(state.PartitionTaken), state.claim(tmp_path, [7]):
            pass
    finally:
        holder.send_signal(signal.SIGKILL)  # the watchdog's kill -9
        holder.wait()
    with state.claim(tmp_path, [7]):
        pass


def test_heartbeat(tmp_path):
    assert state.heartbeat_age(tmp_path, "w0", time.time()) is None
    state.beat(tmp_path, "w0", 100.0)
    assert state.heartbeat_age(tmp_path, "w0", 160.0) == 60.0
