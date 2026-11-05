#!/usr/bin/env python3
"""Shows a test going red: runs it with one string swapped in a file, then restores the file.

    scripts/red.py FILE OLD NEW TEST...     from the repo root; TEST is pytest's arguments
    scripts/red.py src/ingestion/ingest.py 'MERGE_ATTEMPTS = 3' 'MERGE_ATTEMPTS = 2' \
        tests/spark/test_ingest.py -k every_attempt

Exits 0 if the test failed, 1 if it passed (the mutation survives), and 2 if neither happened: OLD
isn't in FILE exactly once, or pytest couldn't run the test. It prints where each failure happened,
so a red in the wrong place shows. The swap is in place, so nothing else may use this checkout
meanwhile.
"""

import re
import shlex
import subprocess
import sys
from pathlib import Path


def main(path: str, old: str, new: str, *test: str) -> int:
    file = Path(path)
    original = file.read_text()
    if original.count(old) != 1:
        print(f"OLD must appear once in {path}, and it appears {original.count(old)} times")
        return 2
    args = [*test, "-q", "--show-capture=no", "--tb=short"]
    if any(a.startswith("tests/spark") for a in test):
        command = ["make", "spark", f"TEST={shlex.join(args)}"]
    else:
        command = ["uv", "run", "pytest", "-p", "no:xdist", "--no-cov", *args]
    file.write_text(original.replace(old, new))
    try:
        done = subprocess.run(command, capture_output=True, text=True)
    finally:
        file.write_text(original)
    lines = done.stdout.splitlines()
    print("\n".join(line[:300] for line in lines if re.match(r"\S+\.py:\d+: |E  ", line))[:6000])
    summary = next((s for s in reversed(lines) if re.search(r"\d+ \w+.* in [\d.]+s", s)), "")
    print(summary or done.stderr[-2000:])
    if re.search(r"\d+ errors?\b", summary) or not re.search(r"\d+ (failed|passed)", summary):
        print("no verdict: pytest didn't run the test")
        return 2
    if re.search(r"\d+ failed", summary):
        print("red")
        return 0
    print("green: the mutation survives")
    return 1


if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.exit(__doc__)
    sys.exit(main(*sys.argv[1:]))
