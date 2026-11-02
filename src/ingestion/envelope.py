"""Ingestion API envelope: validate a Merchant's batch into Changes. Pure.

Invalid Changes are rejected individually; the rest of the batch is still accepted.
"""

import hashlib
import json
from dataclasses import dataclass
from functools import cached_property
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

MAX_BATCH = 10_000
INT64_MAX = 2**63 - 1
# A far-future source version would freeze a Listing forever (plan-v1.md A14).
FUTURE_SLACK_MS = 24 * 60 * 60 * 1000

Key = tuple[str, str]  # (merchant_id, merchant_product_id)
Op = Literal["upsert", "delete", "reclassify"]  # reclassify is internal only


def _printable_trimmed(v: str) -> str:
    if not v.isprintable() or v != v.strip():
        raise ValueError("must be printable, with no leading or trailing whitespace")
    return v


ProductId = Annotated[str, Field(min_length=1, max_length=128), AfterValidator(_printable_trimmed)]
AttributeName = Annotated[str, Field(min_length=1, max_length=100)]
AttributeValue = Annotated[str, Field(max_length=1000)]


class _Strict(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


CURRENCY = r"^[A-Z]{3}$"  # a Merchant's currency too (merchants.create)


class Content(_Strict):
    """A Listing's complete merchant-owned state. An upsert replaces all of it (A2)."""

    title: str = Field(min_length=1, max_length=150)
    description: str = Field(default="", max_length=5000)
    price_micros: int = Field(gt=0, le=INT64_MAX)
    currency: str = Field(pattern=CURRENCY)
    availability: Literal["in_stock", "out_of_stock", "preorder"]
    attributes: dict[AttributeName, AttributeValue] = Field(default_factory=dict, max_length=100)


class _Item(_Strict):
    merchant_product_id: ProductId
    source_version: int = Field(gt=0, le=INT64_MAX)


class _Upsert(_Item):
    op: Literal["upsert"]
    listing: Content


class _Delete(_Item):
    op: Literal["delete"]


_ITEM: TypeAdapter[_Upsert | _Delete] = TypeAdapter(
    Annotated[_Upsert | _Delete, Field(discriminator="op")]
)


def content_hash(listing: Content | None) -> str:
    """SHA-256 of the canonical JSON of a Listing's content; None is a Tombstone."""
    doc = None if listing is None else listing.model_dump(mode="json")
    canonical = json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class Change:
    merchant_id: str
    merchant_product_id: str
    source_version: int
    op: Op
    listing: Content | None = None

    # Content holds a dict, so hashing would fail only for upserts; fail for every Change instead.
    __hash__ = None

    @property
    def key(self) -> Key:
        return (self.merchant_id, self.merchant_product_id)

    @cached_property
    def content_hash(self) -> str:
        return content_hash(self.listing)


class InvalidChange(ValueError):
    def __init__(self, errors: tuple[str, ...]):
        super().__init__("; ".join(errors))
        self.errors = errors


class BadBatch(ValueError):
    """The request as a whole is malformed; nothing in it is accepted."""


@dataclass(frozen=True)
class Checked:
    accepted: list[tuple[int, Change]]
    rejected: list[tuple[int, tuple[str, ...]]]


def check_batch(body: object, *, merchant_id: str, currency: str, now_ms: int) -> Checked:
    if not isinstance(body, dict) or body.keys() != {"changes"}:
        raise BadBatch('body must be {"changes": [...]}')
    items = body["changes"]
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_BATCH:
        raise BadBatch(f"changes must be a list of 1 to {MAX_BATCH} items")
    accepted, rejected = [], []
    for i, raw in enumerate(items):
        try:
            accepted.append(
                (i, check_change(raw, merchant_id=merchant_id, currency=currency, now_ms=now_ms))
            )
        except InvalidChange as e:
            rejected.append((i, e.errors))
    return Checked(accepted, rejected)


def check_change(raw: object, *, merchant_id: str, currency: str, now_ms: int) -> Change:
    try:
        item = _ITEM.validate_python(raw)
    except ValidationError as e:
        raise InvalidChange(tuple(_describe(err) for err in e.errors())) from None
    errors = []
    if item.source_version > now_ms + FUTURE_SLACK_MS:
        errors.append("source_version: must not be more than 24 hours in the future")
    listing = item.listing if isinstance(item, _Upsert) else None
    if listing is not None and listing.currency != currency:
        errors.append(f"listing.currency: must be {currency}, the merchant's currency")
    if errors:
        raise InvalidChange(tuple(errors))
    return Change(merchant_id, item.merchant_product_id, item.source_version, item.op, listing)


def _describe(err) -> str:
    loc = err["loc"][1:]  # drop the union tag ("upsert"/"delete"); an untagged loc is empty
    return f"{'.'.join(map(str, loc)) or 'change'}: {err['msg']}"
