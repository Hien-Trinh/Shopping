import os
import sys

import pytest

from catalog import entry, state


@pytest.fixture
def exits(monkeypatch):
    codes = []
    monkeypatch.setattr(os, "_exit", codes.append)  # what the process would have exited with
    return codes


def test_exit_with_uses_mains_return_value(exits):
    entry.exit_with(lambda: 3)
    entry.exit_with(lambda: None)
    assert exits == [3, 0]


def test_exit_with_maps_an_exception_to_its_code_else_1(exits, capsys):
    def taken():
        raise state.PartitionTaken("partition 0")

    def bug():
        raise KeyError("x")

    for main in (taken, bug):
        entry.exit_with(main, {state.PartitionTaken: 3})
    assert exits == [3, 1]
    assert capsys.readouterr().err.count("Traceback") == 2


def test_exit_with_keeps_argparses_code_and_survives_closed_streams(exits, monkeypatch):
    def bad_flag():
        raise SystemExit(2)

    monkeypatch.setattr(sys, "stdout", None)  # e.g. started with stdout closed
    entry.exit_with(bad_flag)
    assert exits == [2]
