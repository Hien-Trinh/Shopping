"""Small builders shared by the unit tests."""

from catalog.envelope import Change, Content
from catalog.keys import partition
from catalog.plan import Classification, Stored

TAX = "shopify-2026-09"


def listing(title="Red shirt", price=1_000_000, description="", attributes=None) -> Content:
    return Content(
        title=title,
        description=description,
        price_micros=price,
        currency="USD",
        availability="in_stock",
        attributes=attributes or {},
    )


def up(mpid, sv, content=None, merchant="m_1") -> Change:
    return Change(merchant, mpid, sv, "upsert", content or listing())


def delete(mpid, sv, merchant="m_1") -> Change:
    return Change(merchant, mpid, sv, "delete")


def reclassify(mpid, merchant="m_1") -> Change:
    return Change(merchant, mpid, 0, "reclassify")


def classified(category="Apparel > Shirts", taxonomy=TAX, needs_reclassify=False) -> Classification:
    return Classification(category, 0.9, taxonomy, needs_reclassify)


def stored(sv, content=None, *, tombstone=False, cls=None) -> Stored:
    content = None if tombstone else (content or listing())
    cls = None if tombstone else (cls or classified())
    return Stored(sv, content, cls)


def product_in(target: int, prefix="sku") -> str:
    """A merchant product ID whose Listing key ("m_1", id) lands in partition `target`."""
    i = 0
    while partition("m_1", f"{prefix}-{i}") != target:
        i += 1
    return f"{prefix}-{i}"


def race(target, args, n=6):
    """Run target(*args) in n processes released at the same instant; their (status, result)s."""
    import multiprocessing

    ctx = multiprocessing.get_context("spawn")
    barrier, results = ctx.Barrier(n), ctx.Queue()
    procs = [ctx.Process(target=_released, args=(barrier, results, target, args)) for _ in range(n)]
    for p in procs:
        p.start()
    out = [results.get(timeout=120) for _ in procs]
    for p in procs:
        p.join()
    return out


def _released(barrier, results, target, args):
    barrier.wait()
    try:
        results.put(("ok", target(*args)))
    except Exception as e:  # report, don't hang the parent
        results.put(("error", repr(e)))
