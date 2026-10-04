"""Backfill (docs/specs/step-6c.md): op=reclassify for flagged and outdated Listings."""

import shutil
import threading
from datetime import UTC, datetime, timedelta

import pytest
from support import delete, listing, product_in, up
from test_supervisor import REPO, wait_for
from test_worker import NOW, Env

from catalog import backfill, events, landing, state, store
from catalog.classify import UNCATEGORIZED, FakeClassifier
from catalog.events import EventLog
from catalog.keys import PARTITIONS
from catalog.plan import Classification, Write
from catalog.supervisor import parse_procfile

pytestmark = pytest.mark.filterwarnings("ignore::UserWarning")
A, B, C, D = (product_in(p) for p in (3, 20, 40, 60))
T0 = datetime(2026, 9, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def roomy_disk(monkeypatch):
    """A tick refuses below api.MIN_FREE (5 GiB) of real free disk, so these tests would fail on a
    nearly full machine. Report plenty instead: under the 2**62 of the low-disk test."""
    real = shutil.disk_usage
    monkeypatch.setattr(shutil, "disk_usage", lambda path: real(path)._replace(free=2**61))


def put(dt, mpid, *, version="v2", flagged=False, at=T0, sv=5, tombstone=False):
    """One Listing Store row, last updated `at`."""
    cls = None if tombstone else Classification("Apparel", 0.9, version, flagged)
    content = None if tombstone else listing()
    store.merge(dt, [Write(("m_1", mpid), sv, content, cls, False)], at)


@pytest.fixture
def tables(tmp_path):
    landing_dt = landing.ensure(str(tmp_path / "landing_log"))
    store_dt = store.ensure(str(tmp_path / "listing_store"))
    return landing_dt, store_dt, tmp_path / "state", EventLog(tmp_path / "events", "backfill")


def tick(tables, version="v2", **kw):
    landing_dt, store_dt, state_dir, log = tables
    return backfill.tick(landing_dt, store_dt, state_dir, log, version, NOW, **kw)


def caught_up(tables):
    """Every worker offset past the Landing log's head."""
    landing_dt, _, state_dir, _ = tables
    landing_dt.update_incremental()
    head = {p: (landing_dt.version() + 1, 0) for p in range(PARTITIONS)}
    state.save_offsets(state_dir, head, landing.table_id(landing_dt))


def landed(landing_dt):
    landing_dt.update_incremental()
    return landing_dt.to_pyarrow_table().sort_by("seq").to_pylist()


# Test point 1: pending


def test_pending_picks_flagged_and_outdated_live_rows_oldest_first(tables):
    _, dt, *_ = tables
    put(dt, A, flagged=True, at=T0 + timedelta(hours=2))
    put(dt, B, version="v1", at=T0 + timedelta(hours=1), sv=7)
    put(dt, C)  # current and unflagged
    put(dt, D, version="v1", tombstone=True)
    p = backfill.pending(dt, "v2", limit=10)
    assert p.rows == [(("m_1", B), 7), (("m_1", A), 5)]
    assert (p.flagged, p.outdated) == (1, 1)


def test_pending_caps_rows_but_counts_everything(tables):
    _, dt, *_ = tables
    for i, mpid in enumerate((A, B, C)):
        put(dt, mpid, version="v1", at=T0 + timedelta(minutes=i))
    put(dt, D, version="v1", flagged=True, at=T0 + timedelta(hours=1))
    p = backfill.pending(dt, "v2", limit=2)
    assert p.rows == [(("m_1", A), 5), (("m_1", B), 5)]
    assert (p.flagged, p.outdated) == (1, 3)


def test_pending_breaks_updated_at_ties_by_key(tables):
    _, dt, *_ = tables
    mpids = sorted((A, B, C, D))
    store.merge(  # one MERGE: one updated_at
        dt,
        [Write(("m_1", m), 5, listing(), Classification("X", 0.9, "v1"), False) for m in mpids],
        T0,
    )
    assert [k[1] for k, _ in backfill.pending(dt, "v2", limit=2).rows] == mpids[:2]


def test_pending_on_an_empty_store(tables):
    _, dt, *_ = tables
    assert backfill.pending(dt, "v2", limit=10) == backfill.Pending([], 0, 0)


# Test point 2: tick


def test_tick_appends_one_commit_of_reclassify_changes(tables):
    landing_dt, dt, state_dir, log = tables
    put(dt, A, flagged=True, sv=9)
    put(dt, B, version="v1", at=T0 + timedelta(hours=1))
    store_version = dt.version()
    version = tick(tables)
    assert version == landing_dt.version()
    rows = landed(landing_dt)
    assert [(r["merchant_product_id"], r["source_version"], r["op"]) for r in rows] == [
        (A, 9, "reclassify"),
        (B, 5, "reclassify"),
    ]
    assert [r["change_index"] for r in rows] == [0, 1]
    assert rows[0]["listing"] is None
    assert rows[0]["submission_id"].startswith("backfill-")
    assert len({r["submission_id"] for r in rows}) == 1
    assert state.load_backfill(state_dir, landing.table_id(landing_dt)) == version
    [e] = [e for e in events.read(log.root) if e["type"] == "backfill"]
    assert (e["appended"], e["flagged"], e["outdated"], e["version"]) == (2, 1, 1, "v2")
    assert e["landing_version"] == version
    assert e["submission_id"] == rows[0]["submission_id"]  # joins the workers' reclassified events
    assert dt.version() == store_version  # only the workers write the Listing Store (ADR-0001)


def test_tick_waits_until_every_worker_passed_its_last_commit(tables):
    landing_dt, dt, state_dir, _ = tables
    put(dt, A, flagged=True)
    first = tick(tables)
    assert tick(tables) is None  # no offsets saved yet: nothing processed
    table = landing.table_id(landing_dt)
    caught_up(tables)
    state.save_offsets(state_dir, {17: (first, 0)}, table)  # one partition still at it
    assert tick(tables) is None
    assert landing_dt.version() == first
    caught_up(tables)
    assert tick(tables) == first + 1  # A is still flagged: the worker hasn't fixed it


def test_tick_with_nothing_pending_appends_and_saves_nothing(tables):
    landing_dt, dt, state_dir, log = tables
    put(dt, A)
    assert tick(tables) is None
    assert landed(landing_dt) == []
    assert state.load_backfill(state_dir, landing.table_id(landing_dt)) == -1
    assert [e for e in events.read(log.root) if e["type"] == "backfill"] == []


def test_tick_on_low_disk_appends_nothing_and_says_why(tables):
    landing_dt, dt, _, log = tables
    put(dt, A, flagged=True)
    assert tick(tables, min_free=2**62) is None
    assert landed(landing_dt) == []
    skipped = [e for e in events.read(log.root) if e["type"] == "backfill_skipped"]
    assert [e["reason"] for e in skipped] == ["low_disk"]


def test_tick_refuses_state_from_another_landing_log(tables):
    _, dt, state_dir, _ = tables
    put(dt, A, flagged=True)
    state.save_backfill(state_dir, 1, "another-table")
    with pytest.raises(state.OffsetsMismatch):
        tick(tables)


# Test point 3: through the worker


def reclassified(env):
    return [e for e in events.read(env.events_root) if e["type"] == "reclassified"]


def backfill_tick(env, version):
    log = EventLog(env.events_root, "backfill", clock=lambda: NOW.timestamp())
    return backfill.tick(env.landing, env.store, env.state, log, version, NOW)


def test_a_taxonomy_bump_reclassifies_every_live_listing(tmp_path):
    env = Env(tmp_path)
    env.land(up(A, 1), up(B, 1), up(C, 1), up(D, 1), delete(D, 2))
    env.drain()
    env.classifier = FakeClassifier(taxonomy_version="fake-2")
    assert backfill_tick(env, "fake-2") is not None
    env.drain()
    rows = store.read(env.store, {("m_1", k) for k in (A, B, C, D)})
    live = {k[1]: s.classification for k, s in rows.items() if s.listing is not None}
    assert set(live) == {A, B, C}
    assert {c.taxonomy_version for c in live.values()} == {"fake-2"}
    assert rows[("m_1", D)].listing is None  # the Tombstone stays one
    assert sorted(e["merchant_product_id"] for e in reclassified(env)) == sorted([A, B, C])
    assert backfill_tick(env, "fake-2") is None  # nothing left


def test_listings_flagged_by_an_outage_are_fixed_once_the_classifier_is_back(tmp_path):
    env = Env(tmp_path, FakeClassifier(fail=True))
    env.land(up(A, 1), up(B, 1))
    env.drain()
    flagged = store.read(env.store, {("m_1", A), ("m_1", B)})
    assert {s.classification.category for s in flagged.values()} == {UNCATEGORIZED}
    env.classifier = FakeClassifier()
    assert backfill_tick(env, "fake-1") is not None
    env.drain()
    fixed = store.read(env.store, {("m_1", A), ("m_1", B)})
    assert {s.classification.category for s in fixed.values()} == {"Fake > R"}
    assert not any(s.classification.needs_reclassify for s in fixed.values())
    assert backfill_tick(env, "fake-1") is None


# Test point 4: the process


def test_a_second_backfill_on_one_state_directory_exits(tmp_path):
    with state.claim_backfill(tmp_path / "state"), pytest.raises(state.PartitionTaken):
        backfill.run(tmp_path / "data", tmp_path / "state", "fake-1", stop=None)


def test_the_procfile_runs_backfill_with_the_workers_classifier():
    procs = parse_procfile((REPO / "Procfile").read_text())

    def kind(argv):
        return argv[argv.index("--classifier") + 1] if "--classifier" in argv else "fake"

    assert procs["backfill"][1:3] == ["-m", "catalog.backfill"]
    assert "--classifier" in procs["backfill"]
    workers = {kind(argv) for n, argv in procs.items() if n.startswith(state.WORKER)}
    assert workers == {kind(procs["backfill"])}


def test_run_ticks_until_stopped(tmp_path):
    data, stop = tmp_path / "data", threading.Event()
    put(store.ensure(str(data / "listing_store")), A, flagged=True)
    args = (data, tmp_path / "state", "v2")
    t = threading.Thread(target=backfill.run, args=args, kwargs={"stop": stop, "interval": 0.01})
    t.start()
    try:
        wait_for(lambda: {"backfill_start", "backfill"} <= types(data))
    finally:
        stop.set()
        t.join(timeout=10)
    assert not t.is_alive()


def types(data):
    return {e["type"] for e in events.read(data / "events")}


@pytest.mark.parametrize("flag", [["--limit", "0"], ["--interval", "0"], ["--interval", "nan"]])
def test_main_refuses_a_flag_that_would_spin(flag, monkeypatch):
    monkeypatch.setattr(backfill, "run", lambda *a, **kw: pytest.fail("ran with a bad flag"))
    with pytest.raises(SystemExit) as e:
        backfill.main(["--classifier", "fake", *flag])
    assert e.value.code == 2
