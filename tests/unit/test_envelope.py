import copy
import hashlib

import pytest

from catalog.envelope import (
    FUTURE_SLACK_MS,
    INT64_MAX,
    MAX_BATCH,
    BadBatch,
    Change,
    Content,
    InvalidChange,
    check_batch,
    check_change,
    content_hash,
)

NOW = 1_790_000_000_000
MERCHANT = {"merchant_id": "m_1", "currency": "USD", "now_ms": NOW}
DROP = object()


def upsert(**changes):
    """A valid upsert item; `changes` maps dotted paths to new values (DROP removes the field)."""
    item = {
        "op": "upsert",
        "merchant_product_id": "SKU-1",
        "source_version": 5,
        "listing": {
            "title": "Red shirt",
            "price_micros": 19_990_000,
            "currency": "USD",
            "availability": "in_stock",
        },
    }
    for path, value in changes.items():
        *parents, leaf = path.split("__")
        target = item
        for p in parents:
            target = target[p]
        if value is DROP:
            del target[leaf]
        else:
            target[leaf] = value
    return item


def delete(**over):
    return {"op": "delete", "merchant_product_id": "SKU-1", "source_version": 6, **over}


def errors_for(raw) -> str:
    with pytest.raises(InvalidChange) as e:
        check_change(raw, **MERCHANT)
    return " | ".join(e.value.errors)


def test_valid_upsert():
    c = check_change(upsert(), **MERCHANT)
    assert c == Change("m_1", "SKU-1", 5, "upsert", c.listing)
    assert c.key == ("m_1", "SKU-1")
    assert c.listing.description == "" and c.listing.attributes == {}


def test_valid_delete():
    assert check_change(delete(), **MERCHANT) == Change("m_1", "SKU-1", 6, "delete", None)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("merchant_product_id", "x" * 128),
        ("merchant_product_id", "Shirt/Red/M"),
        ("merchant_product_id", "漢字 é"),
        ("source_version", NOW + FUTURE_SLACK_MS),
        ("listing__title", "x" * 150),
        ("listing__description", "x" * 5000),
        ("listing__price_micros", INT64_MAX),
        ("listing__availability", "preorder"),
        ("listing__attributes", {f"k{i}": "v" for i in range(100)}),
        ("listing__attributes", {"k" * 100: "v" * 1000}),
    ],
)
def test_boundaries_accepted(path, value):
    check_change(upsert(**{path: value}), **MERCHANT)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("merchant_product_id", "", "merchant_product_id: String should have at least 1"),
        ("merchant_product_id", "x" * 129, "at most 128"),
        ("merchant_product_id", " SKU", "printable"),
        ("merchant_product_id", "SKU\t1", "printable"),
        ("merchant_product_id", "SKU\n", "printable"),
        ("merchant_product_id", 7, "valid string"),
        ("source_version", 0, "greater than 0"),
        ("source_version", "5", "valid integer"),
        ("source_version", 5.0, "valid integer"),
        ("source_version", True, "valid integer"),
        ("source_version", INT64_MAX + 1, "less than or equal"),
        ("source_version", NOW + FUTURE_SLACK_MS + 1, "24 hours in the future"),
        ("op", "patch", "does not match"),
        ("op", DROP, "Unable to extract tag"),
        ("colour", "red", "colour: Extra inputs"),
        ("listing", DROP, "listing: Field required"),
        ("listing__title", "", "listing.title: String should have at least 1"),
        ("listing__title", "x" * 151, "at most 150"),
        ("listing__description", "x" * 5001, "at most 5000"),
        ("listing__price_micros", 0, "greater than 0"),
        ("listing__price_micros", INT64_MAX + 1, "less than or equal"),
        ("listing__currency", "usd", "should match pattern"),
        ("listing__currency", "USDX", "should match pattern"),
        ("listing__currency", "EUR", "must be USD, the merchant's currency"),
        ("listing__availability", "sold_out", "listing.availability"),
        ("listing__attributes", {f"k{i}": "v" for i in range(101)}, "at most 100"),
        ("listing__attributes", {"": "v"}, "at least 1"),
        ("listing__attributes", {"k" * 101: "v"}, "at most 100"),
        ("listing__attributes", {"k": "v" * 1001}, "at most 1000"),
        ("listing__attributes", {"k": 1}, "valid string"),
        ("listing__size", "M", "listing.size: Extra inputs"),
    ],
)
def test_rejected(path, value, message):
    assert message in errors_for(upsert(**{path: value}))


def test_errors_name_the_field_without_the_op_tag():
    assert errors_for(upsert(listing__title="")).startswith("listing.title: String should")
    assert errors_for(delete(source_version=0)).startswith("source_version: Input should")
    assert errors_for(upsert(merchant_product_id=" x")) == (
        "merchant_product_id: Value error, must be printable, with no leading or trailing"
        " whitespace"
    )
    assert errors_for(upsert(source_version=NOW + FUTURE_SLACK_MS + 1)) == (
        "source_version: must not be more than 24 hours in the future"
    )
    assert str(InvalidChange(("a: x", "b: y"))) == "a: x; b: y"


@pytest.mark.parametrize("raw", [None, "SKU-1", [], 5])
def test_rejects_non_objects(raw):
    assert errors_for(raw).startswith("change:")


def test_delete_must_not_carry_a_listing():
    assert "listing: Extra inputs" in errors_for(delete(listing=upsert()["listing"]))


def test_reports_every_business_rule_at_once():
    raw = upsert(source_version=NOW + FUTURE_SLACK_MS + 1, listing__currency="EUR")
    with pytest.raises(InvalidChange) as e:
        check_change(raw, **MERCHANT)
    assert len(e.value.errors) == 2
    assert "24 hours" in str(e.value) and "EUR" not in e.value.errors[0]


@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        {},
        {"changes": []},
        {"changes": "x"},
        {"changes": [upsert()], "dry_run": True},
        {"changes": [upsert()] * (MAX_BATCH + 1)},
    ],
)
def test_bad_batches(body):
    with pytest.raises(BadBatch) as e:
        check_batch(body, **MERCHANT)
    assert str(e.value) in {
        'body must be {"changes": [...]}',
        f"changes must be a list of 1 to {MAX_BATCH} items",
    }


def test_single_change_batch_keeps_the_merchant():
    checked = check_batch({"changes": [delete()]}, merchant_id="m_9", currency="USD", now_ms=NOW)
    assert checked.accepted == [(0, Change("m_9", "SKU-1", 6, "delete"))]


def test_partial_batch():
    checked = check_batch({"changes": [upsert(), upsert(source_version=0), delete()]}, **MERCHANT)
    assert [i for i, _ in checked.accepted] == [0, 2]
    assert [(i, "greater than 0" in errs[0]) for i, errs in checked.rejected] == [(1, True)]


def test_max_batch_accepted():
    checked = check_batch({"changes": [upsert()] * MAX_BATCH}, **MERCHANT)
    assert len(checked.accepted) == MAX_BATCH and not checked.rejected


def test_input_is_not_mutated():
    raw = upsert()
    before = copy.deepcopy(raw)
    check_change(raw, **MERCHANT)
    assert raw == before


def listing(**over) -> Content:
    return Content(
        **{
            "title": "t",
            "price_micros": 1,
            "currency": "USD",
            "availability": "in_stock",
            **over,
        }
    )


def test_content_hash():
    base = listing(attributes={"a": "1", "b": "2"})
    assert content_hash(base) == content_hash(listing(attributes={"b": "2", "a": "1"}))
    assert content_hash(base) != content_hash(listing(attributes={"a": "1", "b": "3"}))
    assert content_hash(base) != content_hash(listing(price_micros=2))
    assert content_hash(None) != content_hash(base)
    assert Change("m_1", "x", 1, "upsert", base).content_hash == content_hash(base)


def test_changes_are_never_hashable():
    # Content holds a dict, so a hashable Change would fail only for upserts; fail always.
    with pytest.raises(TypeError):
        hash(Change("m_1", "x", 1, "delete"))


def test_content_hash_bytes_are_pinned():
    # Stored in every Listing Store row: a format change would make every stored hash disagree.
    c = listing(title="Áo đỏ", currency="VND", price_micros=1, attributes={"màu": "đỏ", "b": "2"})
    canonical = (
        '{"attributes":{"b":"2","màu":"đỏ"},"availability":"in_stock","currency":"VND",'
        '"description":"","price_micros":1,"title":"Áo đỏ"}'
    )
    assert content_hash(c) == hashlib.sha256(canonical.encode()).hexdigest()
    assert content_hash(None) == hashlib.sha256(b"null").hexdigest()
