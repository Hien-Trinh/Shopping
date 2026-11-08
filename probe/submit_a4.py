# ruff: noqa
"""On the runner: one probe run of A4 on Free Edition (never merged). Uploads the student's files
to a volume, submits the probe wheel task with the compiled pins, and prints only PASS, FAIL and
INFO lines, the run's code and, on a failure, its scrubbed message: no time, run URL or identity,
since these logs are public."""

import glob
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

ROOT = "/Volumes/workspace/default/shopping/probe-a4"
REV = "1970ef96152e2390c0ae7e08d2ad1db7fd25fb91"
SNAPSHOT = f"models/models--Hien-Trinh--listing-student-ft/snapshots/{REV}"


class ApiError(Exception):
    def __init__(self, status, text):
        super().__init__(text)
        self.status = status


def scrub(text: str) -> str:
    return re.sub(r"[\w.+-]+@[\w.-]+", "<email>", str(text))


def say(text: str) -> None:
    print(scrub(text)[:1500], flush=True)


def api(method, path, body=None, data=None):
    headers = {"Authorization": f"Bearer {os.environ['FREE_EDITION_TOKEN']}"}
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    url = os.environ["FREE_EDITION_HOST"].rstrip("/") + path
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=600) as response:
            reply = response.read()
    except urllib.error.HTTPError as e:
        raise ApiError(e.code, f"{method} {path}: {e.code} {e.read().decode(errors='replace')[:300]}") from None
    return json.loads(reply) if reply else {}


def exists(path: str) -> bool:
    try:
        api("HEAD", f"/api/2.0/fs/files{path}")
        return True
    except ApiError as e:
        if e.status == 404:
            return False
        raise


def put(local: str, remote: str) -> None:
    with open(local, "rb") as f:
        api("PUT", f"/api/2.0/fs/files{remote}?overwrite=true", data=f.read())


def main() -> int:
    try:
        api("POST", "/api/2.1/unity-catalog/volumes", {
            "catalog_name": "workspace", "schema_name": "default", "name": "shopping",
            "volume_type": "MANAGED",
        })
    except ApiError as e:
        if "RESOURCE_ALREADY_EXISTS" not in str(e):
            raise
    (index,) = glob.glob("models/student/index-*.npz")
    files = [f"{SNAPSHOT}/{n}" for n in ("config.json", "tokenizer.json", "tokenizer_config.json", "onnx/model.onnx")]
    for local in [*files, index]:
        remote = f"{ROOT}/{local}"
        found = exists(remote)
        if not found:
            put(local, remote)
        say(f"INFO {'kept' if found else 'uploaded'} {local}: HEAD said {'200' if found else '404'}")
    (wheel,) = glob.glob("dist/*.whl")
    wheel_path = f"{ROOT}/wheels/{os.environ['GITHUB_SHA']}/{os.path.basename(wheel)}"
    put(wheel, wheel_path)
    pins = [
        line.strip()
        for line in open("requirements/databricks.txt")
        if line.strip() and not line.startswith(("#", " "))
    ]
    say(f"INFO {len(pins)} pins from requirements/databricks.txt")
    version = open("version.txt").read().strip()
    task = {
        "package_name": "ingestion",
        "entry_point": "probe-a4",
        "parameters": ["--models", f"{ROOT}/models", "--index", f"{ROOT}/{index}", "--version", version],
    }
    body = {
        "run_name": "probe-a4",
        "timeout_seconds": 1800,
        "tasks": [{"task_key": "probe", "environment_key": "wheel", "python_wheel_task": task}],
        "environments": [{
            "environment_key": "wheel",
            "spec": {"environment_version": "5", "dependencies": [wheel_path, *pins]},
        }],
    }
    run_id = api("POST", "/api/2.2/jobs/runs/submit", body)["run_id"]
    while True:
        time.sleep(15)
        try:
            run = api("GET", f"/api/2.2/jobs/runs/get?run_id={run_id}")
        except (ApiError, OSError) as e:
            say(f"INFO poll failed: {e}")
            continue
        if run["status"]["state"] == "TERMINATED":
            break
    details = run["status"].get("termination_details", {})
    try:
        output = api("GET", f"/api/2.2/jobs/runs/get-output?run_id={run['tasks'][0]['run_id']}")
    except ApiError as e:
        say(f"get-output: {e}")
        output = {}
    for line in (output.get("logs") or "").splitlines():
        if line.startswith(("PASS ", "FAIL ", "INFO ")):
            say(line)
    say(f"run: {details.get('code')}")
    if details.get("code") != "SUCCESS":
        say(f"message: {details.get('message')}")
        say(f"error: {output.get('error')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
