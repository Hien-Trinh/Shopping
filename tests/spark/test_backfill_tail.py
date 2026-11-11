"""The Backfill as each micro-batch's tail, through the real ingest query (docs/specs/step-a5.md).

A batch runs only when a Change lands, so each run below lands one first.
"""

from pathlib import Path

from support import delete, listing, up
from test_classify_udf import answers
from test_ingest import cached, commit, crash_around, land, outcomes, submission

from ingestion import classify_udf, ingest
from ingestion.taxonomy import UNCATEGORIZED

AT = 1_700_003_600.0  # the injected clock: an hour after every submission()
ROUND_HOUR = "2023-11-14T23"
OLD = classify_udf.Spec("fake", "fake-0")  # fake's answers, stamped with an older version
FAKES = ("Fake > R", 0.9, "fake-1", False)
FLAGGED = (UNCATEGORIZED, 0.0, "fake-1", True)


def run(spark, data: Path, classifier=classify_udf.FAKE) -> None:
    query = ingest.start(
        spark, str(data), trigger={"availableNow": True}, classifier=classifier, clock=lambda: AT
    )
    query.awaitTermination()
    assert not cached(spark)


def rounds(spark, data: Path) -> list[list[tuple]]:
    """Each Backfill round's Outcomes, as (hour, change_index, outcome)."""
    found: dict[str, list[tuple]] = {}
    for hour, sid, index, outcome in outcomes(spark, data):
        if sid.startswith("backfill-"):
            found.setdefault(sid, []).append((hour, index, outcome))
    return [found[sid] for sid in sorted(found)]


def updated_at(spark, data: Path) -> dict:
    rows = spark.read.format("delta").load(f"{data}/listing_store").collect()
    return {r.merchant_product_id: r.updated_at for r in rows}


def test_listings_an_outage_flagged_are_reclassified_once_the_classifier_is_back(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("b", 1), up("a", 1)]))
    run(spark, tmp_path, classify_udf.DOWN)
    assert answers(spark, tmp_path) == {"a": FLAGGED, "b": FLAGGED}
    assert rounds(spark, tmp_path) == []  # nothing was pending before this batch
    land(tmp_path, commit(1, [up("other", 1)]))
    run(spark, tmp_path)
    assert answers(spark, tmp_path) == {"a": FAKES, "b": FAKES, "other": FAKES}
    assert rounds(spark, tmp_path) == [
        [(ROUND_HOUR, 0, "reclassified"), (ROUND_HOUR, 1, "reclassified")]
    ]


def test_a_version_bump_moves_live_listings_and_picks_no_tombstone_or_current_row(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("old", 1), up("dead", 1)]))
    run(spark, tmp_path, OLD)
    land(tmp_path, commit(1, [delete("dead", 2)]))
    run(spark, tmp_path, OLD)
    assert rounds(spark, tmp_path) == []  # every row is on the classifier's version
    land(tmp_path, commit(2, [up("new", 1)]))
    run(spark, tmp_path)
    assert answers(spark, tmp_path) == {
        "old": FAKES,
        "new": FAKES,
        "dead": (None, None, None, False),
    }
    assert rounds(spark, tmp_path) == [[(ROUND_HOUR, 0, "reclassified")]]  # "old" alone
    before = updated_at(spark, tmp_path)
    land(tmp_path, commit(3, [up("later", 1)]))
    run(spark, tmp_path)
    assert len(rounds(spark, tmp_path)) == 1  # the Tombstone and the current rows: never picked
    after = updated_at(spark, tmp_path)
    assert {k: after[k] for k in before} == before


def test_a_round_takes_the_oldest_up_to_the_limit_and_breaks_a_tie_by_key(
    spark, tmp_path, monkeypatch
):
    monkeypatch.setattr(ingest, "LIMIT", 2)
    land(tmp_path, ["init"], commit(0, [up("c", 1)]))
    run(spark, tmp_path, OLD)
    land(tmp_path, commit(1, [up("b", 1), up("a", 1)]))  # one MERGE: the same updated_at
    run(spark, tmp_path, OLD)
    land(tmp_path, commit(2, [up("z", 1)]))
    run(spark, tmp_path)
    versions = {k: v[2] for k, v in answers(spark, tmp_path).items()}
    assert versions == {"c": "fake-1", "a": "fake-1", "b": "fake-0", "z": "fake-1"}
    land(tmp_path, commit(3, [up("y", 1)]))
    run(spark, tmp_path)
    assert answers(spark, tmp_path)["b"] == FAKES
    # Sorted: both ids hold the injected clock's millisecond, so their order is random.
    assert sorted(len(r) for r in rounds(spark, tmp_path)) == [1, 2]


def test_a_pick_the_classifier_still_fails_keeps_its_answer_flagged_at_the_back_of_the_queue(
    spark, tmp_path
):
    land(tmp_path, ["init"], commit(0, [up("a", 1)]))
    run(spark, tmp_path, OLD)
    before = updated_at(spark, tmp_path)["a"]
    land(tmp_path, commit(1, [up("x", 1)]))
    run(spark, tmp_path, classify_udf.DOWN)  # at fake-1: "a" is outdated, and every call fails
    assert answers(spark, tmp_path) == {"a": ("Fake > R", 0.9, "fake-0", True), "x": FLAGGED}
    assert updated_at(spark, tmp_path)["a"] > before
    assert rounds(spark, tmp_path) == [[(ROUND_HOUR, 0, "reclassified")]]  # v1's Outcome


def test_a_merchant_change_for_a_picked_key_comes_first_in_the_same_batch(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("t", 1), up("d", 1)]))
    run(spark, tmp_path, classify_udf.DOWN)
    land(tmp_path, commit(1, [up("t", 2, listing(title="Blue hat")), delete("d", 2)]))
    run(spark, tmp_path)
    assert answers(spark, tmp_path) == {
        "t": ("Fake > B", 0.9, "fake-1", False),  # the new title, classified
        "d": (None, None, None, False),
    }
    rows = spark.read.format("delta").load(f"{tmp_path}/listing_store").collect()
    assert {r.merchant_product_id: r.source_version for r in rows} == {"t": 2, "d": 2}
    merchant = [o[-1] for o in outcomes(spark, tmp_path) if o[1] == submission(1)]
    assert merchant == ["written", "written"]
    # "d" sorts first: its pick meets the Tombstone its delete left.
    assert rounds(spark, tmp_path) == [
        [(ROUND_HOUR, 0, "skipped"), (ROUND_HOUR, 1, "reclassified")]
    ]


def test_a_crash_after_the_merge_loses_that_rounds_outcomes_and_the_replay_picks_the_next(
    spark, tmp_path, monkeypatch
):
    monkeypatch.setattr(ingest, "LIMIT", 1)
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1)]))
    run(spark, tmp_path, classify_udf.DOWN)
    land(tmp_path, commit(1, [up("x", 1)]))
    crash_around(spark, tmp_path, "merge", after=True)  # its round, "a", landed in the store
    assert answers(spark, tmp_path) == {"a": FAKES, "b": FLAGGED, "x": FAKES}
    assert rounds(spark, tmp_path) == []
    run(spark, tmp_path)  # the replay: "a" is no longer pending, so its round is "b"
    assert answers(spark, tmp_path) == {"a": FAKES, "b": FAKES, "x": FAKES}
    assert rounds(spark, tmp_path) == [[(ROUND_HOUR, 0, "reclassified")]]  # "a" has no Outcome
    assert [o[1:] for o in outcomes(spark, tmp_path) if not o[1].startswith("backfill-")] == [
        (submission(0), 0, "written"),
        (submission(0), 1, "written"),
        (submission(1), 0, "already_applied"),  # rule 3; one Outcome row per Merchant Change
    ]


def test_a_batch_with_no_merchant_change_still_runs_its_round(spark, tmp_path):
    land(tmp_path, ["init"], commit(0, [up("b", 1), up("a", 1)]))
    run(spark, tmp_path, classify_udf.DOWN)
    # A retention DELETE's rows start a batch that holds no Change: the round is all it plans.
    assert land(tmp_path, ["delete", "received_at < '2100-01-01T00:00:00+00:00'"]) == [2]
    run(spark, tmp_path)
    assert answers(spark, tmp_path) == {"a": FAKES, "b": FAKES}
    assert rounds(spark, tmp_path) == [
        [(ROUND_HOUR, 0, "reclassified"), (ROUND_HOUR, 1, "reclassified")]
    ]


def test_a_plan_recomputed_after_its_merge_keeps_its_picks(spark, tmp_path, monkeypatch):
    land(tmp_path, ["init"], commit(0, [up("a", 1), up("b", 1)]))
    run(spark, tmp_path, classify_udf.DOWN)
    merge = ingest.merge

    def merge_then_drop_the_persist(path, writes, planned):
        merge(path, writes, planned)
        spark.catalog.clearCache()

    monkeypatch.setattr(ingest, "merge", merge_then_drop_the_persist)
    land(tmp_path, commit(1, [up("x", 1)]))
    run(spark, tmp_path)
    assert rounds(spark, tmp_path) == [
        [(ROUND_HOUR, 0, "reclassified"), (ROUND_HOUR, 1, "reclassified")]
    ]
    assert [o[-1] for o in outcomes(spark, tmp_path) if o[1] == submission(1)] == ["written"]
