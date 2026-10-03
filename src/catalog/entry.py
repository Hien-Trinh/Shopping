"""How a standalone entry point exits: flush, then os._exit (plan-v1, Phase 3 carried rules).

Arrow can hang at process exit after a Delta scan (spikes/NOTES.md, "Exit hang"), so no entry
point leaves through normal interpreter shutdown.
"""

import contextlib
import os
import sys
import traceback
from collections.abc import Callable, Mapping


def exit_with(main: Callable[[], int | None], codes: Mapping[type, int] | None = None) -> None:
    """Run `main` and exit with its return value: argparse's code for a bad flag, the `codes`
    entry for an exception, else 1."""
    try:
        code = main() or 0
    except SystemExit as e:  # argparse: --help or a bad flag
        code = e.code or 0
    except BaseException as e:
        traceback.print_exc()
        code = (codes or {}).get(type(e), 1)
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):  # closed, or a broken pipe: exit anyway
            stream.flush()
    os._exit(code)
