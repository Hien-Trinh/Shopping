#!/bin/bash
# Checks the Mac before a stress run (docs/runbook.md, Stress runs). Exits 1 with one line
# naming the first failed check; prints what it checked otherwise.
#   scripts/stress-preflight.sh [volume]      default /Volumes/T9
#   MAX_SWAP_GB=8 to change the swap ceiling.
set -u
vol=${1:-/Volumes/T9}
max_swap_gb=${MAX_SWAP_GB:-8}
guard_gib=5   # src/catalog/api.py MIN_FREE: the API refuses writes (low_disk) under this
min_free_gib=$((guard_gib + 2))

fail() { echo "preflight FAILED: $*" >&2; exit 1; }

# 1. A real mount, not a folder on the internal volume (an unmounted /Volumes/T9 still exists).
[ -d "$vol" ] || fail "$vol does not exist"
mnt=$(df -P "$vol" | awk 'NR==2 {print $6}')
dev=$(df -P "$vol" | awk 'NR==2 {print $1}')
case $mnt in
  /|/System/Volumes/Data) fail "$vol is not mounted (df reports $dev on $mnt, the internal volume)" ;;
esac

# 2. A USB drive's link is 10 Gb/s or more (the T9 once came up at 480 Mb/s, USB 2.0).
link="not USB"
if diskutil info "$mnt" | grep -q 'Protocol: *USB'; then
  # APFS volumes sit on a synthesized disk; system_profiler lists the physical store's disk.
  store=$(diskutil info "$dev" | awk -F': *' '/APFS Physical Store/ {print $2}')
  whole=$(echo "${store:-${dev#/dev/}}" | sed -E 's/s[0-9]+$//')
  # The last "Speed:" line before the drive's "BSD Name: diskN" is its own.
  link=$(system_profiler SPUSBDataType 2>/dev/null |
    awk -v d="$whole" '/Speed:/ {s=$0} $1=="BSD" && $3==d {sub(/.*Speed: */, "", s); print s; exit}')
  [ -n "$link" ] || fail "$mnt is USB but system_profiler shows no link speed for $whole"
  echo "$link" | grep -Eq '([1-9][0-9]+|[0-9]{3,}) Gb/s' || fail "$mnt's USB link is '$link', under 10 Gb/s: move it to a 10 Gb/s port"
fi

# 3. Spotlight is not indexing it.
mdutil -s "$mnt" 2>&1 | grep -q 'Indexing disabled' || fail "Spotlight indexes $mnt: run sudo mdutil -i off $mnt"

# 4. Swap is under the ceiling (the desktop alone held 10 GB of it on Oct 9).
swap=$(sysctl -n vm.swapusage | sed -E 's/.*used = ([0-9.]+)([MG]).*/\1 \2/')
swap_gb=$(echo "$swap" | awk '{printf "%.1f", ($2 == "G" ? $1 : $1 / 1024)}')
[ -n "$swap_gb" ] || fail "cannot read swap from sysctl vm.swapusage"
awk -v s="$swap_gb" -v m="$max_swap_gb" 'BEGIN {exit !(s < m)}' ||
  fail "swap used is ${swap_gb} GB, not under ${max_swap_gb} GB: quit apps or reboot"

# 5. Free space above the API's guard plus 2 GiB, on the internal volume (swap lives there) and on $mnt.
for v in /System/Volumes/Data "$mnt"; do
  free_gib=$(df -Pk "$v" | awk 'NR==2 {printf "%.1f", $4 / 1048576}')
  awk -v f="$free_gib" -v m="$min_free_gib" 'BEGIN {exit !(f >= m)}' ||
    fail "$v has ${free_gib} GiB free, under ${min_free_gib} GiB (the API refuses writes under ${guard_gib} GiB)"
done

echo "preflight ok: $mnt mounted ($dev), link $link, Spotlight off, swap ${swap_gb} GB < ${max_swap_gb} GB, free internal $(df -Pk /System/Volumes/Data | awk 'NR==2 {printf "%.1f", $4 / 1048576}') GiB and $mnt ${free_gib} GiB >= ${min_free_gib} GiB"
