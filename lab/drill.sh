#!/usr/bin/env bash
# The live failure drills (scripts/drill.sh) against the lab from lab/up.sh, refereed by
# `pagurus audit`. Exit status 0 means every drill met its expectation and the audit saw
# 0 violations (never two members running at once).
#
#   lab/drill.sh                       move graceful restart crash pause stall
#   lab/drill.sh crash pause           just those
set -euo pipefail
LAB=${LAB:-pgl}
LAB_SUBNET=${LAB_SUBNET:-10.190.7}
export MEMBERS="$LAB-1=$LAB_SUBNET.11 $LAB-2=$LAB_SUBNET.12 $LAB-3=$LAB_SUBNET.13"
export ETCD_ENDPOINTS="$LAB_SUBNET.11:2379,$LAB_SUBNET.12:2379,$LAB_SUBNET.13:2379"
export ON=incus SUDO= REMOTE_CLI=1 PAGURUS_CONFIG=/etc/pagurus/whoami.json
export SVC=whoami HEALTH_PATH=/health HEALTH_PORT=80
[ $# -gt 0 ] || set -- move graceful restart crash pause stall
exec "$(dirname "$0")/../scripts/drill.sh" "$@"
