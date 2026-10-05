"""The load generator's HTTP and its exit codes, against a stub API (docs/specs/step-7a.md)."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from catalog import load

KEY = "k_stub"


@pytest.fixture
def stub():
    """An API that answers each POST with `stub.reply(body) -> (status, json, headers)`."""
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append((self.path, self.headers["Authorization"]))
            status, reply, headers = server.reply(body)
            data = json.dumps(reply).encode()
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.reply = lambda body: (202, {"accepted": len(body["changes"])}, {})
    server.seen, server.url = seen, f"http://127.0.0.1:{server.server_address[1]}"
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()


def test_http_sends_the_key_counts_statuses_and_follows_no_redirect(stub):
    post = load.http(stub.url, KEY)
    assert post({"changes": [{}]}) == (202, {"accepted": 1})
    stub.reply = lambda _: (400, {"detail": "bad"}, {})
    assert post({"changes": [{}]}) == (400, {})
    stub.reply = lambda _: (307, {}, {"Location": f"{stub.url}/elsewhere"})
    assert post({"changes": [{}]}) == (307, {})
    assert stub.seen == [("/listings:batch", f"Bearer {KEY}")] * 3  # never /elsewhere


def run(stub, monkeypatch, capsys, *argv):
    monkeypatch.setenv("CATALOG_API_KEY", KEY)
    code = load.main(["--url", stub.url, "--rate", "0", "--changes", "6", *argv])
    out = capsys.readouterr()
    assert KEY not in out.out + out.err
    return code, json.loads(out.out)


def test_a_run_that_lands_every_change_exits_0(stub, monkeypatch, capsys):
    code, summary = run(stub, monkeypatch, capsys)
    assert (code, summary["sent"], summary["accepted"]) == (0, 6, 6)
    assert summary["processes"] == 6  # 16 by default, but only 6 batches: no idle share


def test_a_401_exits_1(stub, monkeypatch, capsys):
    stub.reply = lambda _: (401, {"detail": "no"}, {})
    code, summary = run(stub, monkeypatch, capsys, "--processes", "2")
    assert (code, summary["statuses"]) == (1, {"401": 2})


def test_changes_the_api_rejects_exit_1(stub, monkeypatch, capsys):
    stub.reply = lambda _: (202, {"accepted": 0}, {})  # e.g. another currency than the Merchant's
    code, summary = run(stub, monkeypatch, capsys, "--processes", "2")
    assert (code, summary["accepted"]) == (1, 0)


def test_a_url_that_isnt_http_is_refused(monkeypatch, capsys):
    monkeypatch.setenv("CATALOG_API_KEY", KEY)
    with pytest.raises(SystemExit) as stopped:
        load.main(["--url", "file:///etc/passwd"])
    assert stopped.value.code == 2
    assert "--url" in capsys.readouterr().err
