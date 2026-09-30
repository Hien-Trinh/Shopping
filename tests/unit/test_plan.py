from support import TAX, classified, delete, listing, reclassify, stored, up

from catalog.plan import Outcome, Stored, Write, plan

K = ("m_1", "a")
W, AA, ST, CF = Outcome.WRITTEN, Outcome.ALREADY_APPLIED, Outcome.STALE, Outcome.CONFLICT


def run(changes, store=None, taxonomy=TAX):
    return plan(changes, store or {}, taxonomy)


def only_write(p) -> Write:
    assert len(p.writes) == 1
    return p.writes[0]


def test_empty_batch():
    p = run([])
    assert p.outcomes == () and p.writes == ()


def test_new_listing_is_written_and_classified():
    p = run([up("a", 5)])
    assert p.outcomes == (W,)
    assert only_write(p) == Write(K, 5, listing(), None, needs_classify=True, content_changed=True)


def test_rules_against_stored_row():
    store = {K: stored(5, listing(title="old"))}
    assert run([up("a", 4)], store).outcomes == (ST,)
    assert run([up("a", 5, listing(title="old"))], store).outcomes == (AA,)
    assert run([up("a", 5, listing(title="new"))], store).outcomes == (CF,)
    assert run([up("a", 6)], store).outcomes == (W,)
    for p in (run([up("a", 4)], store), run([up("a", 5, listing(title="new"))], store)):
        assert p.writes == ()


def test_batch_is_applied_in_landing_order():
    changes = [
        up("a", 3),
        up("a", 5, listing(title="v5")),
        up("a", 4),
        up("a", 5, listing(title="v5")),
        up("a", 5, listing(title="other")),
    ]
    p = run(changes)
    assert p.outcomes == (W, W, ST, AA, CF)
    w = only_write(p)
    assert (w.source_version, w.listing.title) == (5, "v5")


def test_delete_of_unknown_listing_leaves_a_tombstone():
    p = run([delete("a", 7)])
    assert p.outcomes == (W,)
    assert only_write(p) == Write(K, 7, None, None, needs_classify=False, content_changed=True)
    assert run([up("a", 6)], {K: stored(7, tombstone=True)}).outcomes == (ST,)


def test_tombstone_same_version():
    store = {K: stored(7, tombstone=True)}
    assert run([delete("a", 7)], store).outcomes == (AA,)
    assert run([up("a", 7)], store).outcomes == (CF,)


def test_delete_of_live_listing():
    w = only_write(run([delete("a", 6)], {K: stored(5)}))
    assert (w.listing, w.classification, w.needs_classify) == (None, None, False)


def test_price_or_stock_change_keeps_classification():
    cls = classified("Apparel > Shirts")
    w = only_write(run([up("a", 6, listing(price=2_000_000))], {K: stored(5, cls=cls)}))
    assert (w.needs_classify, w.classification) == (False, cls)


def test_reclassification_triggers():
    base = stored(5, listing(title="t", description="d", attributes={"brand": "x"}))
    cases = {
        "title": up("a", 6, listing(title="T", description="d", attributes={"brand": "x"})),
        "description": up("a", 6, listing(title="t", description="D", attributes={"brand": "x"})),
        "attributes": up("a", 6, listing(title="t", description="d", attributes={"brand": "y"})),
    }
    for name, change in cases.items():
        w = only_write(run([change], {K: base}))
        assert (w.needs_classify, w.classification) == (True, None), name


def test_reclassifies_when_row_needs_it():
    same = listing()
    for base in [
        stored(5, tombstone=True),  # re-created after a delete
        stored(5, same, needs_reclassify=True),  # classifier timed out earlier
        stored(5, same, cls=classified(taxonomy="shopify-2025-01")),  # taxonomy changed
    ]:
        assert only_write(run([up("a", 6, same)], {K: base})).needs_classify


def test_row_without_classification_is_classified():
    w = only_write(run([up("a", 6)], {K: Stored(5, listing(), None)}))
    assert w.needs_classify


def test_reclassify_live_listing():
    p = run([reclassify("a")], {K: stored(5)})
    assert p.outcomes == (Outcome.RECLASSIFIED,)
    assert only_write(p) == Write(K, 5, listing(), None, needs_classify=True, content_changed=False)


def test_reclassify_missing_or_deleted_listing_is_skipped():
    assert run([reclassify("a")]).outcomes == (Outcome.SKIPPED,)
    p = run([reclassify("a")], {K: stored(5, tombstone=True)})
    assert (p.outcomes, p.writes) == ((Outcome.SKIPPED,), ())
    p = run([delete("a", 6), reclassify("a")], {K: stored(5)})
    assert p.outcomes == (W, Outcome.SKIPPED)


def test_reclassify_mixed_with_merchant_changes():
    w = only_write(run([reclassify("a"), delete("a", 6)], {K: stored(5)}))
    assert (w.listing, w.needs_classify, w.content_changed) == (None, False, True)
    w = only_write(run([up("a", 6, listing(price=9)), reclassify("a")], {K: stored(5)}))
    assert (w.source_version, w.needs_classify, w.content_changed) == (6, True, True)


def test_writes_in_first_touch_order():
    p = run([up("b", 1), reclassify("c"), up("a", 1), up("b", 2)], {("m_1", "c"): stored(1)})
    assert [w.key[1] for w in p.writes] == ["b", "c", "a"]
