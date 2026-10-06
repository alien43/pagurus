#!/usr/bin/env bash
# Create (idempotently) one Incus project per member, each pinned to that
# member's single-member cluster group (host-<member>). The pin is what makes
# "fence member M" mean "stop project <prefix>-M"; the agent refuses to run
# without it.
#
#   provision.sh <project_prefix> <member>...      e.g. provision.sh sgl-whoami asus dell rpi5
set -euo pipefail
prefix=${1:?project prefix}; shift
[ $# -gt 0 ] || { echo "members required" >&2; exit 64; }

for m in "$@"; do
  p="$prefix-$m"
  group="host-$m"
  members=$(incus query "/1.0/cluster/groups/$group" | jq -c '.members')
  if [ "$members" != "[\"$m\"]" ]; then
    echo "cluster group $group has members $members, want [\"$m\"]" >&2
    exit 1
  fi
  if incus project show "$p" >/dev/null 2>&1; then
    echo "[ok]  project $p exists"
  else
    incus project create "$p" \
      -c features.images=true -c features.profiles=true \
      -c features.storage.volumes=true -c features.storage.buckets=true \
      -c restricted.cluster.groups="$group"
    incus project set "$p" user.singleton.managed=true
    echo "[new] project $p -> $group"
  fi
  got=$(incus project get "$p" restricted.cluster.groups)
  [ "$got" = "$group" ] || { echo "project $p: restricted.cluster.groups=$got, want $group" >&2; exit 1; }
done
