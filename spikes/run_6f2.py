"""Step 6f.2's hand run (docs/specs/step-6f.md): the shipped Procfile under the supervisor, with
Jev, on 20 labeled Listings. Costs under a cent. Needs TYPESAFE_API_KEY and the embedding model.

    uv run python spikes/run_6f2.py [--models PATH]
"""

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx2
from deltalake import DeltaTable

from catalog import events

REPO = Path(__file__).parents[1]
PORT = 8765


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--models", type=Path, default=REPO / "models")
    a = args.parse_args()
    if not os.environ.get("TYPESAFE_API_KEY"):
        sys.exit("export TYPESAFE_API_KEY first")
    if not a.models.is_dir():
        sys.exit(
            f"no models in {a.models}: pass --models or run python -m catalog.classify --download"
        )
    run = Path(tempfile.mkdtemp(prefix="run6f2-"))
    text = (REPO / "Procfile").read_text()
    (run / "Procfile").write_text(re.sub(r"^api: .*$", rf"\g<0> --port {PORT}", text, flags=re.M))
    (run / "models").symlink_to(a.models.resolve())
    out = subprocess.run(
        [sys.executable, "-m", "catalog.merchants", "create", "--currency", "USD"],
        cwd=run,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    _, key = re.findall(r"^\w+: (\S+)$", out, flags=re.M)
    labels = [
        json.loads(line) for line in (REPO / "eval/labels.jsonl").read_text().splitlines()[:20]
    ]
    supervisor = subprocess.Popen([sys.executable, "-m", "catalog.supervisor"], cwd=run)
    try:
        http = httpx2.Client(
            base_url=f"http://127.0.0.1:{PORT}",
            timeout=30,
            headers={"Authorization": f"Bearer {key}"},
        )
        until(lambda: ok(http), 60, "the API to start")
        changes = [
            {
                "op": "upsert",
                "merchant_product_id": x["id"],
                "source_version": 1,
                "listing": {
                    "title": x["title"],
                    "description": x["description"],
                    "price_micros": 1_000_000,
                    "currency": "USD",
                    "availability": "in_stock",
                },
            }
            for x in labels
        ]
        r = http.post("/listings:batch", json={"changes": changes})
        r.raise_for_status()
        sid = r.json()["submission_id"]
        until(lambda: http.get(f"/submissions/{sid}").json()["done"], 120, "every Outcome")
        rows = DeltaTable(str(run / "data/listing_store")).to_pyarrow_table().to_pylist()
        got = {r["merchant_product_id"]: r for r in rows}
        right = 0
        for x in labels:
            row = got[x["id"]]
            hit = row["primary_category"] == x["category"]
            right += hit
            print(
                f"{'✓' if hit else '·'} {row['classify_confidence']:.2f} "
                f"{row['primary_category']}  (label: {x['category']})"
            )
        log = events.read(run / "data/events")
        usd = sum(e.get("usd", 0) for e in log if e["type"] == "batch")
        failed = [e for e in log if e["type"] in ("classify_failed", "process_exit")]
        print(f"\n{right}/20 exact, ${usd:.6f} spent, version {rows[0]['taxonomy_version']}")
        print(f"{len(failed)} classify_failed or process_exit events", *failed, sep="\n")
    finally:
        supervisor.send_signal(signal.SIGTERM)
        supervisor.wait(30)
        shutil.rmtree(run)


def ok(http):
    try:
        http.get("/submissions/x")
        return True
    except httpx2.TransportError:
        return False


def until(check, seconds, what):
    deadline = time.monotonic() + seconds
    while not check():
        if time.monotonic() > deadline:
            sys.exit(f"timed out waiting for {what}")
        time.sleep(0.5)


if __name__ == "__main__":
    main()
