import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from itertools import count

import pytest
from fastapi.testclient import TestClient
from support import delete, up

from catalog import api, events, landing, merchants
from catalog.events import EventLog
from catalog.keys import PARTITIONS
from catalog.landing import START
from catalog.status import fold


def change(mpid, sv=1, **listing):
    """An upsert as a Merchant sends it; the defaults match support.listing()."""
    content = {"title": "Red shirt", "price_micros": 1_000_000, "currency": "USD"}
    content |= {"availability": "in_stock"} | listing
    return {"op": "upsert", "merchant_product_id": mpid, "source_version": sv, "listing": content}


NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
DAY_MS = 24 * 60 * 60 * 1000


class Api:
    def __init__(self, tmp_path, currency="USD", **limits):
        self.data = tmp_path / "data"
        self.db = self.data / "merchants.sqlite"
        self.merchant_id, self.key = merchants.create(self.db, currency)
        options = {"min_free": 0, "clock": NOW.timestamp, "window": 0.01} | limits
        app = api.create_app(self.data, self.db, **options)
        self.client = TestClient(app, raise_server_exceptions=False)
        self.client.__enter__()  # runs the lifespan, which starts the appender
        self.log = landing.ensure(str(self.data / "landing_log"))

    def post(self, body=None, key=None, **kwargs):
        headers = {"Authorization": f"Bearer {key or self.key}"} | kwargs.pop("headers", {})
        return self.client.post("/listings:batch", json=body, headers=headers, **kwargs)

    def get(self, submission_id, key=None):
        headers = {"Authorization": f"Bearer {key or self.key}"}
        return self.client.get(f"/submissions/{submission_id}", headers=headers)

    def landed(self):
        return landing.read(self.log, {p: START for p in range(PARTITIONS)}, limit=10_000).changes

    def events(self, kind=None):
        found = events.read(self.data / "events")
        return [e for e in found if kind is None or e["type"] == kind]


@pytest.fixture
def make_api(tmp_path):
    """Builds an Api, and stops its client (and so its appender) after the test."""
    made = []

    def make(**options):
        made.append(Api(tmp_path, **options))
        return made[-1]

    yield make
    for a in made:
        a.client.__exit__(None, None, None)


@pytest.fixture
def client(make_api):
    return make_api()


def test_a_valid_batch_lands_in_one_commit_and_gets_202(client):
    before = client.log.version()
    delete_c = {"op": "delete", "merchant_product_id": "c", "source_version": 2}
    r = client.post({"changes": [change("a"), change("b", 5), delete_c]})
    assert r.status_code == 202
    submission = r.json()["submission_id"]
    assert uuid.UUID(submission).version == 7
    assert r.json() == {"submission_id": submission, "accepted": 3, "rejected": []}
    client.log.update_incremental()
    assert client.log.version() == before + 1
    mid = client.merchant_id
    landed = client.landed()
    assert [(x.change, x.submission_id, x.change_index) for x in landed] == [
        (up("a", 1, merchant=mid), submission, 0),
        (up("b", 5, merchant=mid), submission, 1),
        (delete("c", 2, merchant=mid), submission, 2),
    ]
    accepted = client.events("accepted")
    assert [(e["change_index"], e["merchant_product_id"], e["partition"]) for e in accepted] == [
        (x.change_index, x.change.merchant_product_id, x.partition) for x in landed
    ]
    assert {e["submission_id"] for e in accepted} == {submission}
    assert {e["merchant_id"] for e in accepted} == {mid}
    assert fold(submission, client.events()).outcomes == {0: "pending", 1: "pending", 2: "pending"}


def post_together(client, *mpids):
    """One request per Merchant product ID, all sent at once from threads."""
    with ThreadPoolExecutor(len(mpids)) as pool:
        return list(pool.map(lambda mpid: client.post({"changes": [change(mpid)]}), mpids))


def test_concurrent_requests_share_one_commit_and_keep_their_own_submissions(make_api):
    ticks = count(NOW.timestamp(), 0.001)  # every request gets its own received_at
    client = make_api(window=1, clock=lambda: next(ticks))
    before = client.log.version()
    replies = post_together(client, "a", "b", "c")
    assert [r.status_code for r in replies] == [202, 202, 202]
    client.log.update_incremental()
    assert client.log.version() == before + 1
    submissions = [r.json()["submission_id"] for r in replies]
    landed = client.landed()
    assert sorted((x.submission_id, x.change.merchant_product_id) for x in landed) == sorted(
        zip(submissions, "abc", strict=True)
    )
    stamps = client.log.to_pyarrow_table(columns=["received_at"])["received_at"].to_pylist()
    assert len(set(stamps)) == 3
    for s in submissions:
        assert fold(s, client.events()).outcomes == {0: "pending"}


def test_invalid_changes_are_rejected_one_by_one_and_the_rest_land(client):
    spoofed = change("b") | {"merchant_id": "m_someone_else"}
    r = client.post({"changes": [spoofed, change("a"), change("c", currency="EUR")]})
    assert r.status_code == 202
    body = r.json()
    assert body["accepted"] == 1
    assert [x["index"] for x in body["rejected"]] == [0, 2]
    assert "merchant_id" in body["rejected"][0]["errors"][0]
    assert "currency" in body["rejected"][1]["errors"][0]
    [landed] = client.landed()
    assert (landed.change.key, landed.change_index) == ((client.merchant_id, "a"), 1)
    rejected = client.events("rejected")
    assert [(e["change_index"], e["errors"]) for e in rejected] == [
        (x["index"], x["errors"]) for x in body["rejected"]
    ]
    assert fold(body["submission_id"], client.events()).outcomes == {
        0: "rejected", 1: "pending", 2: "rejected"
    }  # fmt: skip


def test_a_batch_with_nothing_valid_still_gets_a_submission_but_no_commit(client):
    before = client.log.version()
    r = client.post({"changes": [change("a", 0), change("b", price_micros=0)]})
    assert r.status_code == 202
    assert r.json()["accepted"] == 0
    client.log.update_incremental()
    assert client.log.version() == before
    assert fold(r.json()["submission_id"], client.events()).outcomes == {
        0: "rejected", 1: "rejected"
    }  # fmt: skip


UNREAD = b"{not json"  # refused before the body is read, or this would be a 400


def refusal(r):
    return r.status_code, r.json(), r.headers.get("www-authenticate")


def refused(client):
    return [(e["status"], e["reason"], e.get("merchant_id")) for e in client.events("refused")]


@pytest.mark.parametrize(
    ("authorization", "reason"),
    [
        (None, "no_key"),
        ("Basic abc", "no_key"),
        ("Bearer", "unknown_key"),
        ("Bearer x", "unknown_key"),
    ],
)
def test_a_missing_or_unknown_key_gets_401_before_the_body_is_read(client, authorization, reason):
    headers = {} if authorization is None else {"Authorization": authorization}
    r = client.client.post("/listings:batch", content=UNREAD, headers=headers)
    assert refusal(r) == (401, {"detail": "missing or invalid API key"}, "Bearer")
    assert refused(client) == [(401, reason, None)]
    assert client.landed() == []


def test_a_revoked_key_gets_the_same_401_and_names_its_merchant_in_the_event(client):
    merchants.revoke(client.db, client.merchant_id)
    r = client.post(content=UNREAD)
    assert refusal(r) == (401, {"detail": "missing or invalid API key"}, "Bearer")
    assert refused(client) == [(401, "revoked", client.merchant_id)]


def test_the_scheme_is_case_insensitive_and_no_event_holds_the_key(client):
    r = client.post({"changes": [change("a")]}, headers={"Authorization": f"bearer  {client.key}"})
    assert r.status_code == 202  # any case, any spacing
    merchants.revoke(client.db, client.merchant_id)
    assert client.post({"changes": [change("a")]}).status_code == 401
    logged = b"".join(f.read_bytes() for f in (client.data / "events").rglob("*.jsonl"))
    assert logged and client.key.encode() not in logged
    assert hashlib.sha256(client.key.encode()).hexdigest().encode() not in logged


def test_low_disk_refuses_every_write_before_auth_or_the_body(make_api):
    client = make_api(min_free=2**62)
    unauthenticated = client.client.post("/listings:batch", content=UNREAD)
    assert unauthenticated.status_code == 503
    assert client.post({"changes": [change("a")]}).status_code == 503
    assert refused(client) == [(503, "low_disk", None)] * 2
    assert client.landed() == []


BATCH = json.dumps({"changes": [change("a")]}).encode()


def test_a_declared_length_over_the_cap_gets_413_before_the_body_is_read(make_api):
    client = make_api(max_body=100)
    r = client.post(content=UNREAD, headers={"Content-Length": "101"})
    assert r.status_code == 413
    assert refused(client) == [(413, "too_large", client.merchant_id)]


def test_a_streamed_body_is_counted_and_refused_once_it_passes_the_cap(make_api):
    client = make_api(max_body=len(BATCH) - 1)
    r = client.post(content=iter([BATCH[:10], BATCH[10:]]))
    assert "content-length" not in r.request.headers
    assert r.status_code == 413
    assert refused(client) == [(413, "too_large", client.merchant_id)]
    assert client.landed() == []


def test_a_body_exactly_at_the_cap_is_accepted(make_api):
    client = make_api(max_body=len(BATCH))
    assert client.post(content=BATCH).status_code == 202
    assert client.post(content=iter([BATCH[:10], BATCH[10:]])).status_code == 202


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (UNREAD, "bad_json"),
        (b"", "bad_json"),
        (b"\xff\xfe\x00", "bad_json"),  # not decodable
        (b"[" * 200_000 + b"]" * 200_000, "bad_json"),  # json.loads raises RecursionError
        (json.dumps({"changes": []}).encode(), "bad_batch"),
        (json.dumps([change("a")]).encode(), "bad_batch"),
        (json.dumps({"changes": [change("a")], "more": 1}).encode(), "bad_batch"),
    ],
    ids=["not-json", "empty", "undecodable", "deep", "no-changes", "a-list", "extra-key"],
)
def test_a_malformed_request_gets_400_and_nothing_lands(client, body, reason):
    r = client.post(content=body)
    assert r.status_code == 400
    assert refused(client) == [(400, reason, client.merchant_id)]
    assert client.landed() == []


def fail(*_, **__):
    raise OSError(28, "No space left on device")


def test_a_failed_append_is_a_500_with_no_events(client, monkeypatch):
    monkeypatch.setattr(landing, "append", fail)
    assert client.post({"changes": [change("a"), change("b", 0)]}).status_code == 500
    assert client.events() == []  # never an accepted event for a Change that didn't land


def test_commit_retries_are_logged_to_a_file_of_their_own(client, tmp_path, monkeypatch):
    real = landing.append  # PR #80 review: the commit runs in a thread, beside the request's emits

    def racing(dt, entries, events=None):
        events.emit([{"type": "append_retry", "rows": len(entries), "ms": 1}])
        return real(dt, entries, events)

    monkeypatch.setattr(landing, "append", racing)
    assert client.post({"changes": [change("a")]}).status_code == 202
    files = {kind: {f for f in tmp_path.rglob("*.jsonl") if kind in f.read_text()}
             for kind in ("append_retry", "accepted")}  # fmt: skip
    assert files["append_retry"] and files["accepted"]
    assert not files["append_retry"] & files["accepted"]


def test_a_failed_shared_commit_is_a_500_for_every_request_in_it(make_api, monkeypatch):
    client = make_api(window=1)
    monkeypatch.setattr(landing, "append", fail)
    assert [r.status_code for r in post_together(client, "a", "b")] == [500, 500]
    assert client.events() == []


def test_failed_events_after_the_commit_are_a_500_so_the_merchant_retries(client, monkeypatch):
    monkeypatch.setattr(EventLog, "emit", fail)
    assert client.post({"changes": [change("a")]}).status_code == 500
    assert len(client.landed()) == 1  # the retry is a duplicate, which replays safely (A3)


def test_a_refusal_goes_out_even_if_its_event_cant_be_written(client, monkeypatch):
    monkeypatch.setattr(EventLog, "emit", fail)
    assert client.post({"changes": [change("a")]}, key="nope").status_code == 401


def test_a_registry_removed_while_running_is_a_500(client):
    for f in client.data.glob("merchants.sqlite*"):
        f.unlink()
    assert client.post({"changes": [change("a")]}).status_code == 500
    assert client.landed() == []


def test_the_cli_refuses_to_start_without_a_registry(tmp_path, capsys):
    db = tmp_path / "nowhere" / "merchants.sqlite"
    with pytest.raises(SystemExit) as stopped:
        api.main(["--data", str(tmp_path / "data"), "--db", str(db)])
    assert stopped.value.code == 2  # a wrong path is loud, not a 401 for every Merchant
    assert str(db) in capsys.readouterr().err
    assert not (tmp_path / "data").exists()


def test_the_cli_serves_on_localhost_only(tmp_path, monkeypatch):
    db = tmp_path / "data" / "merchants.sqlite"
    merchants.create(db, "USD")
    served = []
    monkeypatch.setattr(api.uvicorn.Server, "run", lambda server: served.append(server.config))
    api.main(["--data", str(tmp_path / "data"), "--db", str(db), "--port", "8123"])
    [config] = served
    assert (config.host, config.port) == ("127.0.0.1", 8123)


def test_an_error_that_stops_the_server_is_in_the_events(tmp_path, monkeypatch):  # PR #79 review
    db = tmp_path / "data" / "merchants.sqlite"
    merchants.create(db, "USD")

    def crash(server):
        raise OSError(48, "Address already in use")

    monkeypatch.setattr(api.uvicorn.Server, "run", crash)
    with pytest.raises(OSError):
        api.main(["--data", str(tmp_path / "data"), "--db", str(db)])
    [stop] = [e for e in events.read(tmp_path / "data" / "events") if e["type"] == "api_stop"]
    assert stop["error"] == "OSError(48, 'Address already in use')"


def test_the_cli_takes_the_disk_guard_from_min_free(tmp_path, monkeypatch):
    db = tmp_path / "data" / "merchants.sqlite"
    _, key = merchants.create(db, "USD")
    served = []
    monkeypatch.setattr(api.uvicorn.Server, "run", lambda server: served.append(server.config))
    api.main(["--data", str(tmp_path / "data"), "--db", str(db), "--min-free", str(2**62)])
    with TestClient(served[0].app) as client:
        r = client.post("/listings:batch", json={"changes": [change("a")]},
                        headers={"Authorization": f"Bearer {key}"})  # fmt: skip
    assert r.status_code == 503


@pytest.mark.parametrize("pid", ["abc", "0", "-1", " 7", str(2**31)])
def test_a_malformed_supervisor_pid_is_refused_as_a_bad_flag(tmp_path, monkeypatch, capsys, pid):
    db = tmp_path / "data" / "merchants.sqlite"
    merchants.create(db, "USD")
    monkeypatch.setenv("CATALOG_SUPERVISOR", pid)  # 0 or -1 would make the watch signal a group
    monkeypatch.setattr(api.uvicorn.Server, "run", lambda _: pytest.fail("served unwatched"))
    with pytest.raises(SystemExit) as stopped:
        api.main(["--data", str(tmp_path / "data"), "--db", str(db)])
    assert stopped.value.code == 2
    assert "CATALOG_SUPERVISOR" in capsys.readouterr().err


def test_changes_are_checked_against_the_merchants_own_currency(make_api):
    client = make_api(currency="EUR")
    r = client.post({"changes": [change("a", currency="EUR"), change("b", currency="USD")]})
    assert (r.json()["accepted"], [x["index"] for x in r.json()["rejected"]]) == (1, [1])


def test_the_request_time_stamps_the_landing_and_bounds_source_versions(client):
    now_ms = int(NOW.timestamp() * 1000)
    r = client.post({"changes": [change("a", now_ms + DAY_MS), change("b", now_ms + DAY_MS + 1)]})
    assert [x["index"] for x in r.json()["rejected"]] == [1]  # more than 24 h ahead (A14)
    client.log.update_incremental()
    assert client.log.to_pyarrow_table(columns=["received_at"])["received_at"].to_pylist() == [NOW]
    assert {e["ts"] for e in client.events()} == {now_ms}


# GET /submissions/{id} (step 4d)

NOT_FOUND = {"detail": "no such submission"}


LATER = NOW.timestamp() + 60


def report(client, submission, *outcomes, at=LATER):
    """Outcome events as a worker writes them, as (change_index, outcome) pairs."""
    worker = EventLog(client.data / "events", "worker", lambda: at)
    ids = {"submission_id": submission, "merchant_id": client.merchant_id}
    worker.emit(ids | {"change_index": i, "type": o} for i, o in outcomes)


def uuid7_at(ms):
    return str(api.uuid7_at(ms))


def test_the_submission_id_carries_the_api_clock(client):
    r = client.post({"changes": [change("a")]})
    assert uuid.UUID(r.json()["submission_id"]).int >> 80 == int(NOW.timestamp() * 1000)


def test_status_moves_from_pending_to_done_and_keeps_each_changes_best_outcome(client):
    r = client.post({"changes": [change("a"), change("b"), change("c", 0)]})
    submission = r.json()["submission_id"]
    got = client.get(submission)
    assert got.status_code == 200
    assert got.json() == {
        "submission_id": submission,
        "done": False,
        "counts": {"pending": 2, "rejected": 1},
        "changes": [
            {"index": 0, "outcome": "pending"},
            {"index": 1, "outcome": "pending"},
            {"index": 2, "outcome": "rejected"},
        ],
    }
    report(client, submission, (0, "written"))
    assert client.get(submission).json()["done"] is False
    report(client, submission, (0, "already_applied"), (1, "stale"))  # a crash replay of 0
    body = client.get(submission).json()
    assert body["done"] is True
    assert [c["outcome"] for c in body["changes"]] == ["written", "stale", "rejected"]
    assert body["counts"] == {"written": 1, "stale": 1, "rejected": 1}


def test_a_submission_with_nothing_valid_is_done_and_rejected(client):
    submission = client.post({"changes": [change("a", 0)]}).json()["submission_id"]
    body = client.get(submission).json()
    assert (body["done"], body["counts"]) == (True, {"rejected": 1})


def test_another_merchants_submission_gets_the_same_404_as_an_unknown_one(client):
    _, other_key = merchants.create(client.db, "USD")
    theirs = client.post({"changes": [change("a")]}, key=other_key).json()["submission_id"]
    now_ms = int(NOW.timestamp() * 1000)
    foreign = client.get(theirs)
    unknown = client.get(uuid7_at(now_ms))
    future = client.get(uuid7_at(now_ms + DAY_MS))
    assert foreign.status_code == unknown.status_code == future.status_code == 404
    assert foreign.content == unknown.content == future.content
    assert unknown.json() == NOT_FOUND
    mine = client.merchant_id
    assert refused(client) == [
        (404, "not_owner", mine),
        (404, "unknown_submission", mine),
        (404, "unknown_submission", mine),
    ]
    assert client.get(theirs, key=other_key).status_code == 200


@pytest.mark.parametrize(
    "raw",
    [
        "nope",
        str(uuid.uuid4()),
        "00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-7fff-bfff-ffffffffffff",
    ],
)
def test_an_id_that_isnt_a_uuid7_gets_404_without_reading_events(client, monkeypatch, raw):
    monkeypatch.setattr(events, "read", fail)
    r = client.get(raw)
    assert (r.status_code, r.json()) == (404, NOT_FOUND)
    monkeypatch.undo()  # refused() reads events itself
    assert refused(client) == [(404, "unknown_submission", client.merchant_id)]


def test_an_id_dated_before_the_events_horizon_gets_404_without_reading_events(client, monkeypatch):
    """Its events are pruned, so a forged old id can't make one lookup scan every hour kept."""
    horizon_ms = int((NOW - timedelta(days=3)).timestamp() * 1000)  # plan-v1 A13
    monkeypatch.setattr(events, "read", fail)
    r = client.get(uuid7_at(horizon_ms - 1))
    assert (r.status_code, r.json()) == (404, NOT_FOUND)
    monkeypatch.undo()  # refused() reads events itself
    assert refused(client) == [(404, "expired_submission", client.merchant_id)]
    inside = uuid7_at(horizon_ms)
    report(client, inside, (0, "written"), at=horizon_ms / 1000)
    assert client.get(inside).status_code == 200


def test_the_id_is_normalized_before_the_lookup(client):
    submission = client.post({"changes": [change("a")]}).json()["submission_id"]
    for form in (submission.upper(), "{" + submission + "}"):
        r = client.get(form)
        assert r.status_code == 200
        assert r.json()["submission_id"] == submission


@pytest.mark.parametrize("key", [None, "nope"])
def test_a_missing_or_unknown_key_gets_the_posts_401(client, key):
    headers = {} if key is None else {"Authorization": f"Bearer {key}"}
    r = client.client.get(f"/submissions/{uuid7_at(0)}", headers=headers)
    assert refusal(r) == (401, {"detail": "missing or invalid API key"}, "Bearer")
    assert refused(client) == [(401, "no_key" if key is None else "unknown_key", None)]


def test_a_revoked_merchant_gets_401_and_a_rotated_key_still_reads(client):
    submission = client.post({"changes": [change("a")]}).json()["submission_id"]
    new_key = merchants.rotate(client.db, client.merchant_id)
    assert client.get(submission).status_code == 401  # the old key
    assert client.get(submission, key=new_key).status_code == 200
    merchants.revoke(client.db, client.merchant_id)
    assert client.get(submission, key=new_key).status_code == 401
    assert refused(client) == [(401, "unknown_key", None), (401, "revoked", client.merchant_id)]


def test_low_disk_doesnt_refuse_a_read(make_api):
    client = make_api(min_free=2**62)
    submission = uuid7_at(int(NOW.timestamp() * 1000))
    report(client, submission, (0, "written"), at=NOW.timestamp())
    assert client.get(submission).status_code == 200


def test_the_lookup_starts_at_the_hour_in_the_id(client):
    submission = client.post({"changes": [change("a")]}).json()["submission_id"]
    report(client, submission, (0, "written"), at=NOW.timestamp() - 1)  # 11:59:59, an hour early
    assert client.get(submission).json()["changes"] == [{"index": 0, "outcome": "pending"}]


def test_a_failed_read_is_a_500(client, monkeypatch):
    submission = client.post({"changes": [change("a")]}).json()["submission_id"]
    monkeypatch.setattr(events, "read", fail)
    assert client.get(submission).status_code == 500


def test_a_404_goes_out_even_if_its_event_cant_be_written(client, monkeypatch):
    monkeypatch.setattr(EventLog, "emit", fail)
    assert client.get(uuid7_at(0)).status_code == 404
