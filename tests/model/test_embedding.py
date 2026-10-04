import threading
import time

import pytest
from support import listing

from catalog import classify, taxonomy

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
    assert took > 0.3 and gaps[0] < 0.1, (took, gaps[0])
