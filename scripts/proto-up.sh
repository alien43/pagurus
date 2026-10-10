#!/usr/bin/env bash
# Start the experimental prototype on the live cluster: build, copy to each
# host's tmpfs (/run/singleton-proto — gone at reboot), and start the agent as
# a TRANSIENT systemd unit. Nothing persistent is written to any host; no
# NixOS module, no unit file on disk. scripts/proto-down.sh undoes all of it.
#
#   proto-up.sh [--drill-proxy] [--only <member>[,<member>...]]
#
# --only (re)starts the agent on just those members — transient units vanish
# when stopped, so this is how a drill brings a stopped agent back.
#
# --drill-proxy also runs blackhole-proxy on each host and points the agent at
# it, so drills can cut one agent off etcd (SIGUSR1) without touching any
# firewall.
#
# The watchdog is systemd's per-service watchdog (WatchdogSec) WITHOUT a
# FailureAction: a wedged or unfit agent is killed and restarted, the host is
# never rebooted. To arm reboots later: -p FailureAction=reboot-force.
set -euo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
svc=whoami
unit=singleton-$svc
remote_dir=/run/singleton-proto
cfg="$here/examples/landos/whoami.json"
compose="$here/examples/landos/whoami-compose.yaml"
drill=0
only="asus dell rpi5"
while [ $# -gt 0 ]; do
  case $1 in
    --drill-proxy) drill=1 ;;
    --only) only=${2//,/ }; shift ;;
    *) echo "unknown argument $1" >&2; exit 64 ;;
  esac
  shift
done

# member -> ssh target ("" = local), arch, PATH for incus/incus-compose
declare -A ssh=([asus]=10.0.1.10 [dell]=10.0.1.7 [rpi5]="")
declare -A arch=([asus]=amd64 [dell]=amd64 [rpi5]=arm64)
# Binaries live on tmpfs where it is executable; rpi5's /run is noexec, so
# there they go to /var/lib/singleton-proto (proto-down.sh --purge removes it).
declare -A bin=([asus]=$remote_dir [dell]=$remote_dir [rpi5]=/var/lib/singleton-proto)
declare -A path=(
  [asus]=/run/current-system/sw/bin
  [dell]=/run/current-system/sw/bin
  [rpi5]=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/andrey/.local/bin
)
etcd_eps=$(jq -r '.etcd.endpoints | join(",")' "$cfg")

on() { # on <member> <cmd...>
  local m=$1; shift
  if [ -z "${ssh[$m]}" ]; then bash -c "$*"; else ssh -o BatchMode=yes "${ssh[$m]}" "$*"; fi
}
push() { # push <member> <src> <dst>
  if [ -z "${ssh[$1]}" ]; then sudo install -D -m 0755 "$2" "$3"
  else scp -q "$2" "${ssh[$1]}:/tmp/$(basename "$3").$$" && ssh "${ssh[$1]}" "sudo install -D -m 0755 /tmp/$(basename "$3").$$ $3 && rm /tmp/$(basename "$3").$$"; fi
}

ver="proto-$(git -C "$here" rev-parse --short HEAD 2>/dev/null || echo dev)$(git -C "$here" diff --quiet -- . 2>/dev/null || echo -dirty)"
build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT
for a in amd64 arm64; do
  (cd "$here" && CGO_ENABLED=0 GOOS=linux GOARCH=$a go build -trimpath -ldflags "-s -w -X main.version=$ver" -o "$build/singleton-$a" ./cmd/pagurus)
  (cd "$here" && CGO_ENABLED=0 GOOS=linux GOARCH=$a go build -trimpath -ldflags "-s -w" -o "$build/blackhole-proxy-$a" ./cmd/blackhole-proxy)
done

# Per-member groups and pinned projects (idempotent; the groups exist on this cluster).
"$build/singleton-$(go env GOARCH)" provision -c "$cfg" --yes

for m in $only; do
  echo "== $m"
  if on "$m" "systemctl is-active --quiet $unit"; then
    echo "   $unit already active — run proto-down.sh first"; exit 1
  fi
  push "$m" "$build/singleton-${arch[$m]}" "${bin[$m]}/singleton"
  push "$m" "$build/blackhole-proxy-${arch[$m]}" "${bin[$m]}/blackhole-proxy"
  push "$m" "$cfg" "$remote_dir/$svc/singleton.json"
  push "$m" "$compose" "$remote_dir/$svc/compose.yaml"
  on "$m" "sudo chmod 0755 $remote_dir $remote_dir/$svc && sudo chmod 0644 $remote_dir/$svc/singleton.json $remote_dir/$svc/compose.yaml"

  eps=$etcd_eps
  if [ $drill = 1 ]; then
    specs=""; i=1; eps=""
    for e in ${etcd_eps//,/ }; do
      specs+=" 127.0.0.1:2379$i=$e"; eps+="${eps:+,}127.0.0.1:2379$i"; i=$((i+1))
    done
    # a failed transient unit lingers until reset; clear it before re-creating
    on "$m" "systemctl is-active --quiet $unit-px" || on "$m" "sudo systemctl reset-failed $unit-px 2>/dev/null; sudo systemctl stop $unit-px 2>/dev/null; sudo systemd-run --quiet --unit=$unit-px --description='singleton drill etcd proxy' -p Restart=always ${bin[$m]}/blackhole-proxy$specs"
  fi
  on "$m" "env PATH=${path[$m]} ${bin[$m]}/singleton validate -c $remote_dir/$svc/singleton.json"
  on "$m" "sudo systemctl reset-failed $unit 2>/dev/null; true"
  on "$m" "sudo systemd-run --quiet --unit=$unit --description='singleton prototype ($svc)' \
    -p Type=notify -p NotifyAccess=main -p WatchdogSec=30s \
    -p Restart=always -p RestartSec=3s -p StartLimitIntervalSec=0 \
    -p TimeoutStopSec=60s -p KillMode=mixed \
    --setenv=PATH=${path[$m]} \
    ${bin[$m]}/singleton run -c $remote_dir/$svc/singleton.json --self $m --etcd $eps"
  echo "   started $unit ($ver)"
done
