# ruff: noqa
"""On the runner: one probe run on Free Edition. Prints statuses and PASS or FAIL lines, and no
timing, no run URL and no identity, since these logs are public."""

import glob
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

HOST = os.environ["FREE_EDITION_HOST"].rstrip("/")
TOKEN = os.environ["FREE_EDITION_TOKEN"]
SHA = os.environ["GITHUB_SHA"][:12]
ATTEMPT = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
HERE = os.path.dirname(os.path.abspath(__file__))


def scrub(text: str) -> str:
    return re.sub(r"[\w.+-]+@[\w.-]+", "<email>", str(text))


def say(text: str) -> None:
    print(scrub(text), flush=True)


def api(method: str, path: str, body=None, raw=None) -> tuple[int, bytes]:
    data = raw if raw is not None else (None if body is None else json.dumps(body).encode())
    headers = {"Authorization": f"Bearer {TOKEN}"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(HOST + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=120) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def error_of(payload: bytes) -> str:
    try:
        parsed = json.loads(payload)
    except ValueError:
        return payload[:200].decode(errors="replace")
    return f"{parsed.get('error_code')}: {str(parsed.get('message'))[:200]}"


def main() -> int:
    status, _ = api("GET", "/api/2.0/preview/scim/v2/Me")
    say(f"token against the SCIM API: {status}")

    status, payload = api("GET", "/api/2.1/unity-catalog/catalogs")
    catalogs = [c["name"] for c in json.loads(payload).get("catalogs", [])] if status == 200 else []
    say(f"catalogs: {status} {catalogs if status == 200 else error_of(payload)}")
    catalog = "workspace" if "workspace" in catalogs else next(iter(catalogs), "workspace")
    status, payload = api("GET", f"/api/2.1/unity-catalog/schemas?catalog_name={catalog}")
    schemas = [s["name"] for s in json.loads(payload).get("schemas", [])] if status == 200 else []
    say(f"schemas in {catalog}: {status} {schemas if status == 200 else error_of(payload)}")
    schema = "default"

    volume = {
        "catalog_name": catalog,
        "schema_name": schema,
        "name": "shopping",
        "volume_type": "MANAGED",
    }
    for attempt in ("create", "create again"):
        status, payload = api("POST", "/api/2.1/unity-catalog/volumes", body=volume)
        say(f"volume {attempt}: {status} {'' if status == 200 else error_of(payload)}")

    root = f"/Volumes/{catalog}/{schema}/shopping/probe/{SHA}-{ATTEMPT}"
    wheel = glob.glob("dist/*.whl")[0]
    uploads = {
        os.path.join(HERE, "probe.py"): f"{root}/probe.py",
        "tests/support.py": f"{root}/support.py",
        wheel: f"{root}/{os.path.basename(wheel)}",
    }
    for i, (local, remote) in enumerate(uploads.items()):
        with open(local, "rb") as f:
            status, payload = api("PUT", f"/api/2.0/fs/files{remote}?overwrite=true", raw=f.read())
        say(
            f"upload {os.path.basename(local)}{' (no folder made first)' if i == 0 else ''}: {status} {'' if status < 300 else error_of(payload)}"
        )
        if status >= 300:
            return 1

    submit = {
        "run_name": "shopping-probe",
        "idempotency_token": f"probe-{SHA}-{ATTEMPT}",
        "timeout_seconds": 1800,
        "tasks": [
            {
                "task_key": "probe",
                "environment_key": "wheel",
                "spark_python_task": {"python_file": f"{root}/probe.py", "parameters": [root]},
            }
        ],
        "environments": [
            {
                "environment_key": "wheel",
                "spec": {
                    "environment_version": os.environ.get("ENVIRONMENT", "5"),
                    "dependencies": [uploads[wheel]],
                },
            }
        ],
    }
    status, payload = api("POST", "/api/2.2/jobs/runs/submit", body=submit)
    say(f"runs/submit: {status} {'' if status == 200 else error_of(payload)}")
    if status != 200:
        return 1
    run_id = json.loads(payload)["run_id"]

    run = {}
    deadline = time.monotonic() + 2100
    while time.monotonic() < deadline:
        time.sleep(20)
        status, payload = api("GET", f"/api/2.2/jobs/runs/get?run_id={run_id}")
        if status != 200:
            say(f"runs/get: {status} {error_of(payload)}")
            return 1
        run = json.loads(payload)
        if run.get("status", {}).get("state") == "TERMINATED":
            break
    details = run.get("status", {}).get("termination_details", {})
    say(
        f"state: {run.get('status', {}).get('state')} code {details.get('code')} type {details.get('type')}"
    )
    say(f"message: {str(details.get('message'))[:1500]}")

    tasks = run.get("tasks", [])
    task_id = tasks[0]["run_id"] if tasks else run_id
    status, payload = api("GET", f"/api/2.2/jobs/runs/get-output?run_id={task_id}")
    say(f"get-output by task id: {status}")
    logged = 0
    if status == 200:
        output = json.loads(payload)
        logs = output.get("logs") or ""
        logged = sum(line.startswith(("PASS ", "FAIL ")) for line in logs.splitlines())
        say(
            f"logs: {len(logs) > 0}, PASS or FAIL lines in them {logged}, truncated {output.get('logs_truncated')}"
        )
        say(f"error: {str(output.get('error'))[:1500]}")
        say(f"error_trace tail: {str(output.get('error_trace'))[-1500:]}")
    status, _ = api("GET", f"/api/2.2/jobs/runs/get-output?run_id={run_id}")
    say(f"get-output by the parent's id: {status}")

    status, payload = api("GET", f"/api/2.0/fs/files{root}/results.txt")
    say(f"results.txt: {status}")
    if status == 200:
        print("--- results", flush=True)
        for line in payload.decode().splitlines():
            say(line)
    return 0 if details.get("code") == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
