import hashlib
import json
import uuid

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


class Api:
    def __init__(self, tmp_path, **limits):
        self.data = tmp_path / "data"
        self.db = self.data / "merchants.sqlite"
        self.merchant_id, self.key = merchants.create(self.db, "USD")
        app = api.create_app(self.data, self.db, **{"min_free": 0} | limits)
        self.client = TestClient(app, raise_server_exceptions=False)
        self.log = landing.ensure(str(self.data / "landing_log"))

    def post(self, body=None, key=None, **kwargs):
        headers = {"Authorization": f"Bearer {key or self.key}"} | kwargs.pop("headers", {})
        return self.client.post("/listings:batch", json=body, headers=headers, **kwargs)

    def landed(self):
        return landing.read(self.log, {p: START for p in range(PARTITIONS)}, limit=10_000).changes

    def events(self, kind=None):
        found = events.read(self.data / "events")
        return [e for e in found if kind is None or e["type"] == kind]


@pytest.fixture
def client(tmp_path):
    return Api(tmp_path)


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
    r = client.post({"changes": [change("a")]}, headers={"Authorization": f"bearer {client.key}"})
    assert r.status_code == 202
    merchants.revoke(client.db, client.merchant_id)
    assert client.post({"changes": [change("a")]}).status_code == 401
    logged = b"".join(f.read_bytes() for f in (client.data / "events").rglob("*.jsonl"))
    assert logged and client.key.encode() not in logged
    assert hashlib.sha256(client.key.encode()).hexdigest().encode() not in logged


def test_low_disk_refuses_every_write_before_auth_or_the_body(tmp_path):
    client = Api(tmp_path, min_free=2**62)
    unauthenticated = client.client.post("/listings:batch", content=UNREAD)
    assert unauthenticated.status_code == 503
    assert client.post({"changes": [change("a")]}).status_code == 503
    assert refused(client) == [(503, "low_disk", None)] * 2
    assert client.landed() == []


BATCH = json.dumps({"changes": [change("a")]}).encode()


def test_a_declared_length_over_the_cap_gets_413_before_the_body_is_read(tmp_path):
    client = Api(tmp_path, max_body=100)
    r = client.post(content=UNREAD, headers={"Content-Length": "101"})
    assert r.status_code == 413
    assert refused(client) == [(413, "too_large", client.merchant_id)]


def test_a_streamed_body_is_counted_and_refused_once_it_passes_the_cap(tmp_path):
    client = Api(tmp_path, max_body=len(BATCH) - 1)
    r = client.post(content=iter([BATCH[:10], BATCH[10:]]))
    assert "content-length" not in r.request.headers
    assert r.status_code == 413
    assert refused(client) == [(413, "too_large", client.merchant_id)]
    assert client.landed() == []


def test_a_body_exactly_at_the_cap_is_accepted(tmp_path):
    client = Api(tmp_path, max_body=len(BATCH))
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
    monkeypatch.setattr(api.uvicorn, "run", lambda app, **options: served.append(options))
    api.main(["--data", str(tmp_path / "data"), "--db", str(db), "--port", "8123"])
    [options] = served
    assert (options["host"], options["port"]) == ("127.0.0.1", 8123)
