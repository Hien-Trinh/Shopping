"""Change Export (docs/specs/step-5a.md): change feed since the watermark → export files."""

import shutil
import threading
from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest
from support import classified, listing, product_in
from test_supervisor import wait_for

from catalog import events, export, state, store
from catalog.events import EventLog
from catalog.plan import Write
from catalog.replay import diff, live, replay_exports

NOW = datetime(2026, 10, 3, 12, tzinfo=UTC)
A, B, C = (("m_1", product_in(p)) for p in (5, 9, 40))


def write(key, sv, content=None, cls=None):
    return Write(key, sv, content, (cls or classified()) if content else None, False)


class Env:
    def __init__(self, tmp):
        self.tmp = tmp
        self.dt = store.ensure(str(tmp / "data" / "listing_store"))
        self.out, self.state = tmp / "data" / "export", tmp / "state"
        self.events = EventLog(tmp / "data" / "events", "export")

    def merge(self, *writes):
        return store.merge(self.dt, list(writes), NOW).version

    def tick(self, max_versions=1000):
        return export.tick(self.dt, self.out, self.state, self.events, max_versions=max_versions)

    def watermark(self):
        return state.load_watermark(self.state, self.dt.metadata().id)

    def names(self):
        return [p.name for p in export.files(self.out)]

    def oracle(self):
        """Replaying the files equals the live Listing Store at the watermark."""
        version = self.watermark()
        files = [pq.read_table(p).to_pylist() for p in export.files(self.out)]
        return diff(live(store.fingerprints(self.dt, version)), replay_exports(files))


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def name(v1, v2):
    return f"{v1:012}-{v2:012}.parquet"


def test_upserts_deletes_and_reclassifies_replay_to_the_live_store(env):
    env.merge(write(A, 1, listing(title="a")), write(B, 1, listing()))
    env.merge(write(A, 2, listing(title="b")), write(C, 1, listing()))
    env.merge(write(B, 2))  # a Tombstone
    env.merge(write(C, 1, listing(), classified("Home > Mugs")))  # a reclassify
    head = env.dt.version()
    assert env.tick() is False
    assert env.names() == [name(0, head)]
    assert env.watermark() == head
    assert env.oracle() == []
    rows = pq.read_table(export.files(env.out)[0]).to_pylist()
    got = {r["merchant_product_id"]: (r["source_version"], r["op"]) for r in rows}
    assert got == {A[1]: (2, "upsert"), B[1]: (2, "delete"), C[1]: (1, "upsert")}
    assert next(r for r in rows if r["merchant_product_id"] == C[1])["primary_category"] == (
        "Home > Mugs"
    )
    (event,) = [e for e in events.read(env.tmp / "data" / "events") if e["type"] == "export"]
    assert (event["from"], event["to"], event["rows"], event["deletes"]) == (0, head, 3, 1)


def test_an_idle_store_writes_nothing(env):
    env.tick()
    before = (env.names(), env.watermark())
    assert env.tick() is False
    assert (env.names(), env.watermark()) == before


def test_compaction_alone_writes_no_file_but_moves_the_watermark(env):
    env.merge(write(A, 1, listing()))
    env.merge(write(("m_1", product_in(5, prefix="alt")), 1, listing()))  # a 2nd file in A's
    env.tick()
    before, v = env.names(), env.dt.version()
    store.compact(env.dt, [5])
    assert env.dt.version() == v + 1
    assert env.tick() is False
    assert env.names() == before
    assert env.watermark() == env.dt.version()
    assert env.oracle() == []


def test_a_far_behind_export_splits_into_capped_ranges_in_version_order(env):
    for sv in range(1, 13):  # versions 1-12, all one Listing: replaying out of order shows
        env.merge(write(A, sv, listing(title=f"t{sv}")))
    assert [env.tick(max_versions=4) for _ in range(4)] == [True, True, True, False]
    assert env.names() == [name(0, 3), name(4, 7), name(8, 11), name(12, 12)]
    assert env.oracle() == []


def test_a_crash_before_the_watermark_adopts_the_file_rather_than_overlap_it(env):
    env.merge(write(A, 1, listing()))
    env.tick()
    first = env.dt.version()
    state.save_watermark(env.state, -1, env.dt.metadata().id)  # as if the crash came first
    env.merge(write(A, 2, listing(title="b")), write(B, 1, listing()))
    env.tick()
    assert env.names() == [name(0, first), name(first + 1, env.dt.version())]
    assert env.oracle() == []


def test_a_torn_write_leaves_a_temp_file_the_files_skip(env):
    env.merge(write(A, 1, listing()))
    env.out.mkdir(parents=True)
    (env.out / f".{name(0, 1)}.0123.tmp").write_bytes(b"PAR1 torn")
    env.tick()
    assert env.names() == [name(0, 1)]
    assert env.oracle() == []


def test_a_watermark_from_a_recreated_store_is_refused(env):
    env.merge(write(A, 1, listing()))
    env.tick()
    shutil.rmtree(env.tmp / "data" / "listing_store")
    env.dt = store.ensure(str(env.tmp / "data" / "listing_store"))
    with pytest.raises(state.OffsetsMismatch, match="export watermark"):
        env.tick()


def test_a_torn_watermark_is_corrupt_state(env):
    env.state.mkdir()
    (env.state / "export_watermark.json").write_text('{"table": ')
    with pytest.raises(state.CorruptState):
        env.tick()


def test_run_exports_until_stopped(env):
    env.merge(write(A, 1, listing()))
    stop = threading.Event()
    runner = threading.Thread(
        target=export.run, args=(env.tmp / "data", env.state), kwargs={"stop": stop}
    )
    runner.start()
    wait_for(lambda: env.names() == [name(0, 1)])
    stop.set()
    runner.join(timeout=10)
    assert not runner.is_alive()  # the minute's wait ends at once


def test_a_second_exporter_is_refused_before_reading_anything(env):
    env.merge(write(A, 1, listing()))
    with state.claim_export(env.state), pytest.raises(state.PartitionTaken, match="Change Export"):
        export.run(env.tmp / "data", env.state, stop=threading.Event())
    assert env.names() == []
