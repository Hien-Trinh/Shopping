import signal
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime

import pytest
from support import listing, product_in, up

from catalog import classify, landing, store, taxonomy

NOW = datetime(2026, 10, 4, tzinfo=UTC)

pytestmark = pytest.mark.model


@pytest.fixture(scope="module")
def real():
    return classify.EmbeddingClassifier(taxonomy.load(), classify.fastembed(classify.MODELS))


def test_a_t_shirt_is_apparel(real):
    ((category, confidence),) = real.classify([listing("Men's cotton crew-neck t-shirt")])
    assert category.startswith("Apparel & Accessories") and 0.0 <= confidence <= 1.0


def test_other_threads_run_while_it_embeds(real):
    """The supervisor watch must run while a worker embeds (step 3e): the GIL is released."""
    done, gaps = threading.Event(), [0.0]

    def tick():  # the longest this thread waits between two of its own steps
        last = time.monotonic()
        while not done.is_set():
            now = time.monotonic()
            gaps[0], last = max(gaps[0], now - last), now

    thread = threading.Thread(target=tick)
    thread.start()
    started = time.monotonic()
    try:
        real.embed([f"Blue denim jacket, size {i}, " * 20 for i in range(64)])
    finally:
        done.set()
        thread.join()
    took = time.monotonic() - started  # one native call: embed runs a single ONNX batch
    assert gaps[0] < took / 4, (took, gaps[0])  # holding the GIL, the gap would be the call


def test_a_bulk_batch_overruns_the_budget_by_at_most_about_a_chunk(real):
    """Bounds relative to this machine: CI's runner is several times slower than a Mac."""
    text = "Cotton t-shirt " + "soft cotton crew neck " * 25  # a 500-character description
    real.embed([text] * 16)  # warm
    started = time.monotonic()
    real.embed([text] * 16)
    sixteen = time.monotonic() - started  # one chunk of 16 here
    started = time.monotonic()
    answers = real.classify([listing(text[:150], description=text) for _ in range(400)])
    took = time.monotonic() - started
    assert answers[0] is not None and answers[-1] is None  # some answered, the rest left over
    assert took < real.budget + 2 * sixteen, (took, sixteen)  # a 64 chunk alone takes 4x sixteen


def test_the_worker_cli_classifies_with_the_real_model(tmp_path):
    data, state_dir = tmp_path / "data", tmp_path / "state"
    mpid = product_in(0)
    landing.append(
        landing.ensure(str(data / "landing_log")),
        [("s1", 0, up(mpid, 1, listing("Men's cotton t-shirt")), NOW)],
    )
    args = ["--index", "0", "--workers", "4", "--data", str(data), "--state", str(state_dir)]
    proc = subprocess.Popen(
        [sys.executable, "-m", "catalog.worker", *args, "--classifier", "embedding"]
    )
    try:
        deadline = time.monotonic() + 120
        while not (rows := store.read(store.ensure(str(data / "listing_store")), [("m_1", mpid)])):
            assert proc.poll() is None and time.monotonic() < deadline
            time.sleep(0.2)
    finally:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=30)
    cls = rows[("m_1", mpid)].classification
    assert cls.category.startswith("Apparel & Accessories")
    assert cls.taxonomy_version == "shopify-2026-08+bge-small-en-v1.5"
