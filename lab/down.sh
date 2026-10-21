#!/usr/bin/env bash
# Remove everything lab/up.sh created: the three member instances and the lab network.
set -uo pipefail
LAB=${LAB:-pgl}
for m in "$LAB-1" "$LAB-2" "$LAB-3"; do incus delete --force "$m" 2>/dev/null && echo "deleted $m"; done
incus network delete "${LAB}br0" 2>/dev/null && echo "deleted ${LAB}br0"
exit 0
