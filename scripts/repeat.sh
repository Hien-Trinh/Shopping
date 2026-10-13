#!/bin/bash
# Reruns a test to reproduce a flake (make repeat). Runs it N times in each of J parallel
# loops and stops every loop at the first failure, keeping that run's log and pytest temp dir.
#   scripts/repeat.sh N J <pytest args...>    e.g. scripts/repeat.sh 25 4 tests/x.py::test_y
# Each run gets its own --basetemp: pytest keeps only its 3 newest temp dirs, so with parallel
# loops the failing run's dir would otherwise be deleted by a later run.
set -u
[ $# -ge 3 ] || { echo "usage: $0 N J <pytest args...>" >&2; exit 2; }
n=$1 j=$2
shift 2
tmp=${TMPDIR:-/tmp}
root=$(mktemp -d "${tmp%/}/repeat.XXXXXX")

loop() {
  local l=$1 i run
  shift
  for ((i = 1; i <= n; i++)); do
    [ -e "$root/failed" ] && return
    run=$root/loop$l-run$i
    if uv run pytest "$@" -p no:xdist --no-cov -q --basetemp="$run" >"$run.log" 2>&1; then
      echo pass >>"$root/counts"
      rm -rf "$run" "$run.log"
    elif [ ! -e "$root/stopped" ] && mkdir "$root/failed" 2>/dev/null; then  # mkdir is atomic: the first failure wins
      echo fail >>"$root/counts"
      echo "$run" >"$root/failed/run"
      pkill -INT -f -- "--basetemp=$root/"  # stop the other loops' runs; pytest tears down on SIGINT
      return
    else
      return  # interrupted (or a second failure) after the first one
    fi
  done
}

# Job control: without it bash starts background loops with SIGINT ignored, and pytest
# could not be interrupted. Each loop then has its own process group, so Ctrl-C is relayed.
set -m
trap 'touch "$root/stopped"; pkill -INT -f -- "--basetemp=$root/"' INT TERM
echo "repeat: $n runs x $j loops of: $*  (runs in $root)"
for ((l = 1; l <= j; l++)); do loop "$l" "$@" & done
wait
wait  # again: a trapped Ctrl-C returns from the first wait early

passed=$(grep -c pass "$root/counts" 2>/dev/null)
failed=$(grep -c fail "$root/counts" 2>/dev/null)
echo "repeat: ${passed:-0} passed, ${failed:-0} failed"
if [ -e "$root/failed" ]; then
  run=$(cat "$root/failed/run")
  echo "failing run log: $run.log"
  echo "failing run basetemp: $run"
  exit 1
fi
[ -e "$root/stopped" ] && { echo "repeat: interrupted, runs kept in $root"; exit 130; }
rm -rf "$root"
