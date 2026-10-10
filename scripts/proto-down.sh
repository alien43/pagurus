#!/usr/bin/env bash
# Remove every trace of the prototype: stop the transient units (the agents
# release the office and stop the workload on the way out), delete the
# instances, images and projects, the tmpfs files, and the etcd keys.
#
#   proto-down.sh            stop agents, keep projects (fast re-up)
#   proto-down.sh --purge    also delete projects and etcd state
set -euo pipefail
svc=whoami
unit=singleton-$svc
prefix=sgl-$svc
declare -A ssh=([asus]=10.0.1.10 [dell]=10.0.1.7 [rpi5]="")
on() { local m=$1; shift; if [ -z "${ssh[$m]}" ]; then bash -c "$*"; else ssh -o BatchMode=yes "${ssh[$m]}" "$*"; fi; }

for m in asus dell rpi5; do
  echo "== $m"
  on "$m" "sudo systemctl stop $unit $unit-px 2>/dev/null; sudo systemctl reset-failed $unit $unit-px 2>/dev/null; true"
done

# Nothing may be left running in any of the projects.
for m in asus dell rpi5; do
  p=$prefix-$m
  incus project show "$p" >/dev/null 2>&1 || continue
  for i in $(incus list --project "$p" -c n --format csv); do
    incus stop --project "$p" --force "$i" 2>/dev/null || true
  done
done

[ "${1:-}" = "--purge" ] || { echo "agents stopped (projects kept; --purge to remove)"; exit 0; }

for m in asus dell rpi5; do
  p=$prefix-$m
  if incus project show "$p" >/dev/null 2>&1; then
    for i in $(incus list --project "$p" -c n --format csv); do incus delete --project "$p" --force "$i"; done
    for f in $(incus image list --project "$p" -c f --format csv); do incus image delete --project "$p" "$f"; done
    for v in $(incus storage volume list zfs-pool --project "$p" type=custom -c n --format csv 2>/dev/null); do
      incus storage volume delete zfs-pool "$v" --project "$p"; done
    incus project delete "$p" && echo "deleted project $p"
  fi
  on "$m" "sudo rm -rf /run/singleton-proto /var/lib/singleton-proto"
done
eps=10.0.1.3:2379,10.0.1.6:2379,10.0.1.8:2379
etcdctl --endpoints=$eps del --prefix "/singleton/v1/$svc" >/dev/null   # the pre-conch schema
etcdctl --endpoints=$eps del --prefix "/pagurus/v2/$svc" >/dev/null
etcdctl --endpoints=$eps del --prefix "/conch/v1/elect/pagurus/v2/$svc/" >/dev/null
echo "purged"
