from catalog.status import SubmissionStatus, fold

S = "0199a000-0000-7000-8000-000000000001"


def ev(kind, i, submission=S):
    return {"type": kind, "submission_id": submission, "merchant_id": "m_1", "change_index": i}


def test_unknown_submission():
    assert fold(S, [ev("accepted", 0, submission="other")]) is None


def test_pending_until_every_change_has_an_outcome():
    s = fold(S, [ev("accepted", 0), ev("accepted", 1), ev("written", 0)])
    assert s == SubmissionStatus(S, "m_1", {0: "written", 1: "pending"})
    assert not s.done and s.counts == {"written": 1, "pending": 1}


def test_done_with_mixed_outcomes_including_api_rejections():
    s = fold(
        S,
        [
            ev("rejected", 1),
            ev("accepted", 0),
            ev("accepted", 2),
            ev("stale", 0),
            ev("conflict", 2),
        ],
    )
    assert s.outcomes == {0: "stale", 1: "rejected", 2: "conflict"}
    assert s.done


def test_replays_never_make_an_outcome_worse():
    events = [ev("accepted", 0), ev("written", 0), ev("stale", 0), ev("already_applied", 0)]
    assert fold(S, events).outcomes == {0: "written"}


def test_retry_after_failure_wins():
    assert fold(S, [ev("accepted", 0), ev("failed", 0), ev("written", 0)]).outcomes == {
        0: "written"
    }


def test_outcome_before_accepted_event_is_kept():
    assert fold(S, [ev("written", 0), ev("accepted", 0)]).outcomes == {0: "written"}


def test_other_event_types_are_ignored():
    assert fold(S, [ev("accepted", 0), ev("exported", 0)]).outcomes == {0: "pending"}


def test_internal_reclassify_outcomes():
    assert fold(S, [ev("skipped", 0), ev("reclassified", 0)]).outcomes == {0: "reclassified"}
