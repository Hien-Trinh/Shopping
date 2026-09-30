"""Small builders shared by the unit tests."""

from catalog.envelope import Change, Content
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


def classified(category="Apparel > Shirts", taxonomy=TAX) -> Classification:
    return Classification(category, 0.9, taxonomy)


def stored(sv, content=None, *, tombstone=False, cls=None, needs_reclassify=False) -> Stored:
    content = None if tombstone else (content or listing())
    cls = None if tombstone else (cls or classified())
    return Stored(sv, content, cls, needs_reclassify)
