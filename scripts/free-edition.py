#!/usr/bin/env python3
"""One Free Edition run of the wheel in dist/, for the free-edition workflow
(docs/specs/step-a3.md). Standard library only, so it needs no env.

These logs are public, so it prints only the checks' PASS and FAIL lines and the run's code, and on
a failure the run's message and error with any email replaced. It exits 0 only on SUCCESS with
check lines.
"""

import glob
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

CATALOG, SCHEMA, VOLUME = "workspace", "default", "shopping"
ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
ENVIRONMENT = "5"  # Runtime 18's libraries, and persist needs 5 or later (decision 6)
TIMEOUT_SECONDS = 1800
POLL_SECONDS = float(os.environ.get("FREE_EDITION_POLL_SECONDS", 15))  # tests poll at once
LINE = 1500  # each printed line's limit, cut after scrub so it can't strand part of an email


class ApiError(Exception):
    def __init__(self, status: int, text: str):
        super().__init__(text)
        self.status = status


def api(method: str, path: str, body: dict | None = None, data: bytes | None = None) -> dict:
    headers = {"Authorization": f"Bearer {os.environ['FREE_EDITION_TOKEN']}"}
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    url = os.environ["FREE_EDITION_HOST"].rstrip("/") + path
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            reply = response.read()
    except urllib.error.HTTPError as e:
        reply = e.read().decode(errors="replace")
        raise ApiError(e.code, f"{method} {path}: {e.code} {reply}") from None
    return json.loads(reply) if reply else {}


def scrub(text: str) -> str:
    """Databricks' errors quote the identity running the job, which is an email."""
    return re.sub(r"[\w.+-]+@[\w.-]+", "<email>", text)


def ensure_volume() -> None:
    volume = {
        "catalog_name": CATALOG,
        "schema_name": SCHEMA,
        "name": VOLUME,
        "volume_type": "MANAGED",
    }
    try:
        api("POST", "/api/2.1/unity-catalog/volumes", volume)
    except ApiError as e:
        if "RESOURCE_ALREADY_EXISTS" not in str(e):
            raise


def upload(wheel: str) -> str:
    # By commit: a job may not pick up a changed package at the same path and version.
    path = f"{ROOT}/wheels/{os.environ['GITHUB_SHA']}/{os.path.basename(wheel)}"
    with open(wheel, "rb") as f:
        api("PUT", f"/api/2.0/fs/files{path}?overwrite=true", data=f.read())
    return path


def submit(wheel_path: str) -> int:
    task = {
        "package_name": "ingestion",
        "entry_point": "ingestion-smoke",
        "parameters": ["--data", f"{ROOT}/smoke"],
    }
    spec = {"environment_version": ENVIRONMENT, "dependencies": [wheel_path]}
    body = {
        "run_name": "free-edition",
        "timeout_seconds": TIMEOUT_SECONDS,
        "tasks": [{"task_key": "smoke", "environment_key": "wheel", "python_wheel_task": task}],
        "environments": [{"environment_key": "wheel", "spec": spec}],
    }
    return api("POST", "/api/2.2/jobs/runs/submit", body)["run_id"]


def wait(run_id: int) -> dict:
    """A failed poll isn't the run's failure: stopping would leave it writing to --data for the next
    dispatch to wipe. A lasting one ends at the job's timeout."""
    while True:
        try:
            run = api("GET", f"/api/2.2/jobs/runs/get?run_id={run_id}")
            if run["status"]["state"] == "TERMINATED":
                return run
        except OSError:
            pass
        except ApiError as e:
            if e.status < 500:
                raise
        time.sleep(POLL_SECONDS)


def task_output(run: dict) -> dict:
    """By the task's run id, since the parent's returns 400. Failing, it leaves report the code."""
    try:
        return api("GET", f"/api/2.2/jobs/runs/get-output?run_id={run['tasks'][0]['run_id']}")
    except (ApiError, OSError) as e:
        print(scrub(str(e))[:LINE])
        return {}


def report(run: dict, output: dict) -> int:
    logs = (output.get("logs") or "").splitlines()
    checks = [line for line in logs if line.startswith(("PASS ", "FAIL "))]
    for line in checks:
        print(scrub(line)[:LINE])
    details = run["status"].get("termination_details", {})
    code = details.get("code")
    print(f"run: {code}")
    if code == "SUCCESS" and checks:
        return 0
    for text in (details.get("message"), output.get("error")):
        if text:
            print(scrub(text)[:LINE])
    return 1


def main() -> int:
    (wheel,) = glob.glob("dist/*.whl")
    try:
        ensure_volume()
        run = wait(submit(upload(wheel)))
    except ApiError as e:
        print(scrub(str(e))[:LINE])
        return 1
    return report(run, task_output(run))


if __name__ == "__main__":
    sys.exit(main())
