"""scripts/free-edition.py as a subprocess, against a stub of the Databricks APIs
(docs/specs/step-a3.md)."""

import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "free-edition.py"
SHA = "abc123"
WHEEL = "ingestion-0.1.0-py3-none-any.whl"
WHEEL_PATH = f"/Volumes/workspace/default/shopping/wheels/{SHA}/{WHEEL}"
VOLUMES = "/api/2.1/unity-catalog/volumes"
UPLOAD = f"/api/2.0/fs/files{WHEEL_PATH}?overwrite=true"
SUBMIT = "/api/2.2/jobs/runs/submit"
GET = "/api/2.2/jobs/runs/get?run_id=7"
OUTPUT = "/api/2.2/jobs/runs/get-output?run_id=8"  # the task's id: the parent's returns 400

# What the real replies hold that a public log must not.
LEAKS = {
    "start_time": 1_700_000_000_000,
    "end_time": 1_700_000_100_000,
    "run_duration": 100_000,
    "execution_duration": 90_000,
    "run_page_url": "https://workspace.example/run/7",
    "creator_user_name": "someone@example.com",
}


def run_reply(code, message=""):
    details = {"code": code, "type": "CLIENT_ERROR", "message": message}
    status = {"state": "TERMINATED", "termination_details": details}
    return 200, {"status": status, "tasks": [{"run_id": 8, **LEAKS}], **LEAKS}


class Stub:
    """Replies by method and path, recording every request."""

    def __init__(self, replies):
        self.requests = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                stub.requests.append((self.command, self.path, self.headers["Authorization"], body))
                status, reply = replies.get(
                    (self.command, self.path), (404, {"error_code": "NOPE"})
                )
                payload = json.dumps(reply).encode() if reply else b""
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_POST = do_PUT = answer

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def body(self, method, path):
        return next(r[3] for r in self.requests if r[:2] == (method, path))

    def called(self):
        return [r[:2] for r in self.requests]


def replies(**overrides):
    base = {
        ("POST", VOLUMES): (200, {"full_name": "workspace.default.shopping"}),
        ("PUT", UPLOAD): (204, None),
        ("POST", SUBMIT): (200, {"run_id": 7}),
        ("GET", GET): run_reply("SUCCESS"),
        ("GET", OUTPUT): (200, {"logs": "noise\nPASS one\nPASS two\ntook 12 s\n", **LEAKS}),
    }
    return base | {key: value for key, value in overrides.items()}


@pytest.fixture
def script(tmp_path):
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / WHEEL).write_bytes(b"wheel bytes")
    stubs = []

    def start(table):
        stub = Stub(table)
        stubs.append(stub)
        env = {
            "FREE_EDITION_HOST": f"http://127.0.0.1:{stub.server.server_port}/",
            "FREE_EDITION_TOKEN": "the-token",
            "GITHUB_SHA": SHA,
        }
        args = [sys.executable, str(SCRIPT)]
        done = subprocess.run(args, env=env, cwd=tmp_path, capture_output=True, text=True)
        return stub, done

    yield start
    for stub in stubs:
        stub.server.shutdown()


def test_a_passing_run_prints_only_its_check_lines_and_code_and_exits_0(script):
    stub, done = script(replies())
    assert done.returncode == 0, done.stderr
    assert done.stdout.splitlines() == ["PASS one", "PASS two", "run: SUCCESS"]
    assert stub.called() == [
        ("POST", VOLUMES),
        ("PUT", UPLOAD),
        ("POST", SUBMIT),
        ("GET", GET),
        ("GET", OUTPUT),
    ]
    assert {r[2] for r in stub.requests} == {"Bearer the-token"}
    assert stub.body("PUT", UPLOAD) == b"wheel bytes"
    assert json.loads(stub.body("POST", VOLUMES)) == {
        "catalog_name": "workspace",
        "schema_name": "default",
        "name": "shopping",
        "volume_type": "MANAGED",
    }
    submit = json.loads(stub.body("POST", SUBMIT))
    assert submit["timeout_seconds"] == 1800
    assert submit["environments"] == [
        {
            "environment_key": "wheel",
            "spec": {"environment_version": "5", "dependencies": [WHEEL_PATH]},
        }
    ]
    task = submit["tasks"][0]
    assert task["environment_key"] == "wheel"
    assert task["python_wheel_task"] == {
        "package_name": "ingestion",
        "entry_point": "ingestion-smoke",
        "parameters": ["--data", "/Volumes/workspace/default/shopping/smoke"],
    }


def test_a_failed_run_prints_its_code_message_and_error_with_the_email_replaced_and_exits_1(script):
    table = replies()
    table["GET", GET] = run_reply("RUN_EXECUTION_ERROR", "Task failed for someone@example.com")
    table["GET", OUTPUT] = (
        200,
        {
            "logs": "PASS one\nFAIL two: ValueError: x\n",
            "error": "RuntimeError: 1 of 2 checks failed",
        },
    )
    _, done = script(table)
    assert done.returncode == 1
    assert done.stdout.splitlines() == [
        "PASS one",
        "FAIL two: ValueError: x",
        "run: RUN_EXECUTION_ERROR",
        "Task failed for <email>",
        "RuntimeError: 1 of 2 checks failed",
    ]


def test_a_volume_that_exists_carries_on(script):
    table = replies()
    table["POST", VOLUMES] = (400, {"error_code": "RESOURCE_ALREADY_EXISTS", "message": "exists"})
    _, done = script(table)
    assert done.returncode == 0, done.stdout + done.stderr


@pytest.mark.parametrize(
    ("failing", "status", "code", "before"),
    [
        (("POST", VOLUMES), 403, "PERMISSION_DENIED", 1),
        (("PUT", UPLOAD), 500, "INTERNAL_ERROR", 2),
        (("POST", SUBMIT), 400, "INVALID_PARAMETER_VALUE", 3),
    ],
)
def test_any_other_failed_call_stops_with_its_status_and_exits_1(
    script, failing, status, code, before
):
    table = replies()
    table[failing] = (status, {"error_code": code, "message": "for someone@example.com"})
    stub, done = script(table)
    assert done.returncode == 1
    assert len(stub.requests) == before  # nothing after the failed call
    assert str(status) in done.stdout
    assert code in done.stdout
    assert "someone@example.com" not in done.stdout + done.stderr
