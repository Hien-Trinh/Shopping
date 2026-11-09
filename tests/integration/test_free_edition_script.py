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
ROOT = "/Volumes/workspace/default/shopping"
WHEEL_PATH = f"{ROOT}/wheels/{SHA}/{WHEEL}"
VOLUMES = "/api/2.1/unity-catalog/volumes"
UPLOAD = f"/api/2.0/fs/files{WHEEL_PATH}?overwrite=true"
SUBMIT = "/api/2.2/jobs/runs/submit"
GET = "/api/2.2/jobs/runs/get?run_id=7"
OUTPUT = "/api/2.2/jobs/runs/get-output?run_id=8"  # the task's id: the parent's returns 400
# The student's files as the model job's cache holds them, and train/ (step-a4.md, decision 9).
ENCODER = "models/models--Hien-Trinh--listing-student-ft/snapshots/rev/onnx/model.onnx"
INDEX = "models/student/index-rev-fp32-hash.npz"
TRAIN = "train/shopify.jsonl.gz"
LOCAL = {
    ENCODER: b"encoder",
    INDEX: b"index",
    TRAIN: b"train",
    "models/models--Qdrant--bge-small-en-v1.5-onnx-Q/model.onnx": b"bge-small",  # not the student's
    "models/student/vectors-rev.npy": b"vectors",  # nor the index
}
PINS = "# compiled\nfastembed==0.9.0\n    # via\n    #   -c runtime-18.txt\n\nnumpy==2.1.3\n"

DROP = object()  # the stub closes the connection instead of replying
# An email across the 1500-character cut, so a cut before the scrub would strand its first part.
LONG_ERROR = "x" * 1473 + " someone@example.com"
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
                missing = (404, {"error_code": "NOPE"})
                answers = replies.get((self.command, self.path), missing)
                answer = answers.pop(0) if isinstance(answers, list) else answers
                if answer is DROP:
                    self.close_connection = True
                    return
                status, reply = answer
                payload = json.dumps(reply).encode() if reply else b""
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                if self.command != "HEAD":  # a HEAD reply has no body
                    self.wfile.write(payload)

            do_GET = do_POST = do_PUT = do_HEAD = answer

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def body(self, method, path):
        return next(r[3] for r in self.requests if r[:2] == (method, path))

    def called(self):
        return [r[:2] for r in self.requests]


def files(method, local):
    """A Files API call on the volume's copy of `local`."""
    return method, f"/api/2.0/fs/files{ROOT}/{local}" + (
        "?overwrite=true" if method == "PUT" else ""
    )


def replies(**overrides):
    base = {
        ("POST", VOLUMES): (200, {"full_name": "workspace.default.shopping"}),
        ("PUT", UPLOAD): (204, None),
        **{files("PUT", local): (204, None) for local in (ENCODER, INDEX, TRAIN)},
        ("POST", SUBMIT): (200, {"run_id": 7}),
        ("GET", GET): run_reply("SUCCESS"),
        ("GET", OUTPUT): (200, {"logs": "noise\nPASS one\nPASS two\ntook 12 s\n", **LEAKS}),
    }
    return base | {key: value for key, value in overrides.items()}


@pytest.fixture
def script(tmp_path):
    (tmp_path / "dist").mkdir()
    (tmp_path / "dist" / WHEEL).write_bytes(b"wheel bytes")
    for local, data in LOCAL.items():
        (tmp_path / local).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / local).write_bytes(data)
    (tmp_path / "requirements").mkdir()
    (tmp_path / "requirements" / "databricks.txt").write_text(PINS)
    stubs = []

    def start(table):
        stub = Stub(table)
        stubs.append(stub)
        env = {
            "FREE_EDITION_HOST": f"http://127.0.0.1:{stub.server.server_port}/",
            "FREE_EDITION_TOKEN": "the-token",
            "GITHUB_SHA": SHA,
            "FREE_EDITION_POLL_SECONDS": "0",
        }
        args = [sys.executable, str(SCRIPT)]
        kwargs = {"capture_output": True, "text": True, "timeout": 30}  # the script polls forever
        done = subprocess.run(args, env=env, cwd=tmp_path, **kwargs)
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
        files("HEAD", ENCODER),
        files("PUT", ENCODER),
        files("HEAD", INDEX),
        files("PUT", INDEX),
        files("PUT", TRAIN),
        ("POST", SUBMIT),
        ("GET", GET),
        ("GET", OUTPUT),
    ]
    assert {r[2] for r in stub.requests} == {"Bearer the-token"}
    assert stub.body("PUT", UPLOAD) == b"wheel bytes"
    uploaded = [stub.body(*files("PUT", local)) for local in (ENCODER, INDEX, TRAIN)]
    assert uploaded == [b"encoder", b"index", b"train"]
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
            "spec": {
                "environment_version": "5",
                "dependencies": [WHEEL_PATH, "fastembed==0.9.0", "numpy==2.1.3"],
            },
        }
    ]
    task = submit["tasks"][0]
    assert task["environment_key"] == "wheel"
    assert task["python_wheel_task"] == {
        "package_name": "ingestion",
        "entry_point": "ingestion-smoke",
        "parameters": ["--data", f"{ROOT}/smoke", "--models", f"{ROOT}/models"],
    }


def test_a_failed_run_prints_its_code_message_and_error_with_the_email_replaced_and_exits_1(script):
    table = replies()
    table["GET", GET] = run_reply("RUN_EXECUTION_ERROR", "Task failed for someone@example.com")
    table["GET", OUTPUT] = (
        200,
        {
            "logs": f"PASS one\nFAIL two: ValueError: {LONG_ERROR}\n",
            "error": "RuntimeError: 1 of 2 checks failed",
        },
    )
    _, done = script(table)
    assert done.returncode == 1
    assert "someone" not in done.stdout
    assert done.stdout.splitlines() == [
        "PASS one",
        f"FAIL two: ValueError: {'x' * 1473} <email>"[:1500],
        "run: RUN_EXECUTION_ERROR",
        "Task failed for <email>",
        "RuntimeError: 1 of 2 checks failed",
    ]


def test_polling_goes_on_through_a_running_state_a_dropped_connection_and_a_5xx(script):
    table = replies()
    running = (200, {"status": {"state": "RUNNING"}, **LEAKS})
    busy = (503, {"error_code": "TEMPORARILY_UNAVAILABLE"})
    table["GET", GET] = [running, DROP, busy, run_reply("SUCCESS")]
    stub, done = script(table)
    assert done.returncode == 0, done.stdout + done.stderr
    assert done.stdout.splitlines() == ["PASS one", "PASS two", "run: SUCCESS"]
    assert stub.called().count(("GET", GET)) == 4


@pytest.mark.parametrize("output", [(200, {}), (200, {"logs": None}), (200, {"logs": "noise\n"})])
def test_success_with_no_check_lines_exits_1(script, output):
    table = replies()
    table["GET", OUTPUT] = output
    _, done = script(table)
    assert done.returncode == 1
    assert done.stdout.splitlines() == ["run: SUCCESS"]


def test_success_with_a_fail_line_exits_1_since_a_retry_can_hide_a_failure(script):
    table = replies()
    table["GET", OUTPUT] = (200, {"logs": "PASS one\nFAIL two: ValueError: x\n"})
    _, done = script(table)
    assert done.returncode == 1
    assert done.stdout.splitlines() == ["PASS one", "FAIL two: ValueError: x", "run: SUCCESS"]


@pytest.mark.parametrize(
    ("failure", "first_line"),
    [
        ((500, {"error_code": "INTERNAL_ERROR"}), f"GET {OUTPUT}: 500 "),
        (DROP, "Remote end closed"),
    ],
)
def test_a_failed_get_output_still_prints_the_runs_code_and_message(script, failure, first_line):
    table = replies()
    table["GET", GET] = run_reply("LIBRARY_INSTALLATION_ERROR", "the install failed")
    table["GET", OUTPUT] = failure
    _, done = script(table)
    assert done.returncode == 1
    lines = done.stdout.splitlines()
    assert lines[0].startswith(first_line)
    assert lines[1:] == ["run: LIBRARY_INSTALLATION_ERROR", "the install failed"]


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
        (files("PUT", INDEX), 500, "INTERNAL_ERROR", 6),
        (files("PUT", TRAIN), 500, "INTERNAL_ERROR", 7),
        (("POST", SUBMIT), 400, "INVALID_PARAMETER_VALUE", 8),
        (("GET", GET), 403, "PERMISSION_DENIED", 9),
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
    assert done.stdout.startswith(f"{failing[0]} {failing[1]}: {status} ")  # names the call
    assert code in done.stdout
    assert "someone@example.com" not in done.stdout + done.stderr


def test_a_student_file_the_volume_holds_stays_and_train_uploads_again(script):
    table = replies()
    table[files("HEAD", ENCODER)] = (200, None)
    table[files("HEAD", INDEX)] = (200, None)
    stub, done = script(table)
    assert done.returncode == 0, done.stdout + done.stderr
    assert [call for call in stub.called() if call[0] == "PUT"] == [
        ("PUT", UPLOAD),
        files("PUT", TRAIN),
    ]


def test_a_failed_head_other_than_404_stops_the_run(script):
    table = replies()
    table[files("HEAD", INDEX)] = (403, None)
    stub, done = script(table)
    assert done.returncode == 1
    assert len(stub.requests) == 5  # nothing after the failed call
    assert done.stdout.startswith(f"HEAD {files('HEAD', INDEX)[1]}: 403")
