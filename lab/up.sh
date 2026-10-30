#!/usr/bin/env bash
# A throwaway three-member pagurus lab on one Incus host.
#
# Creates three system containers (nested Incus inside each), forms an Incus cluster and an
# etcd cluster across them, provisions the whoami example and starts one pagurus agent per
# member. Nothing outside the lab's own network and instances is touched; lab/down.sh removes
# it all.
#
#   lab/up.sh                       build pagurus + conch from this checkout, then bring the lab up
#   PAGURUS_BIN=… CONCH_BIN=… lab/up.sh   use prebuilt binaries instead (linux, the host's arch)
#
# Settings (environment):
#   LAB=pgl                 instance/network name prefix
#   LAB_SUBNET=10.190.7     the lab bridge's /24 (members at .11 .12 .13); pick one free on your host
#   WORK_SUBNET=10.191.7    the /24 of the workload bridge inside each member
#   IMAGE=images:ubuntu/24.04
#   ETCD_VERSION=v3.6.12  INCUS_COMPOSE_VERSION=v1.3.5
#
# The members are privileged, AppArmor-unconfined containers with nesting: a lab shortcut so the
# nested Incus can run its own containers (confined, its cowsql socket gets refused once it does). Run it on a disposable host (or the CI job in .github/workflows/lab.yml),
# not next to anything you care about.
set -euo pipefail
cd "$(dirname "$0")/.."

LAB=${LAB:-pgl}
LAB_SUBNET=${LAB_SUBNET:-10.190.7}
WORK_SUBNET=${WORK_SUBNET:-10.191.7}
IMAGE=${IMAGE:-images:ubuntu/24.04}
ETCD_VERSION=${ETCD_VERSION:-v3.6.12}
INCUS_COMPOSE_VERSION=${INCUS_COMPOSE_VERSION:-v1.3.5}
net=${LAB}br0
members=("$LAB-1" "$LAB-2" "$LAB-3")
declare -A ip=([$LAB-1]=$LAB_SUBNET.11 [$LAB-2]=$LAB_SUBNET.12 [$LAB-3]=$LAB_SUBNET.13)
eps="${ip[$LAB-1]}:2379,${ip[$LAB-2]}:2379,${ip[$LAB-3]}:2379"
arch=$(uname -m); case $arch in x86_64) goarch=amd64 ;; aarch64) goarch=arm64 ;; *) echo "unsupported arch $arch" >&2; exit 1 ;; esac
t0=$SECONDS
log() { printf '%s  [%3ss] %s\n' "$(date +%T)" "$((SECONDS - t0))" "$*"; }
on() { local m=$1; shift; incus exec "$m" -- bash -c "$*"; }
retry() { local n=$1; shift; for _ in $(seq "$n"); do "$@" && return 0; sleep 2; done; "$@"; }

for m in "${members[@]}"; do
  incus info "$m" >/dev/null 2>&1 && { echo "$m already exists: run lab/down.sh first" >&2; exit 1; }
done

bin=$(mktemp -d)
trap 'rm -rf "$bin"' EXIT
if [ -n "${PAGURUS_BIN:-}" ]; then cp "$PAGURUS_BIN" "$bin/pagurus"; else
  log "building pagurus"; CGO_ENABLED=0 go build -o "$bin/pagurus" ./cmd/pagurus; fi
if [ -n "${PAGURUS_TERM_BIN:-}" ]; then cp "$PAGURUS_TERM_BIN" "$bin/pagurus-term"; else
  CGO_ENABLED=0 go build -o "$bin/pagurus-term" ./cmd/pagurus-term; fi
if [ -n "${CONCH_BIN:-}" ]; then cp "$CONCH_BIN" "$bin/conch"; else
  log "building conch (the version pinned in go.mod)"; CGO_ENABLED=0 go build -o "$bin/conch" github.com/alien43/conch/cmd/conch; fi

log "network $net ($LAB_SUBNET.0/24)"
incus network create "$net" ipv4.address="$LAB_SUBNET.1/24" ipv4.nat=true ipv6.address=none >/dev/null

for m in "${members[@]}"; do
  log "member $m (${ip[$m]})"
  incus init "$IMAGE" "$m" --network "$net" \
    -c security.nesting=true -c security.privileged=true \
    -c security.syscalls.intercept.mknod=true -c security.syscalls.intercept.setxattr=true \
    -c raw.lxc=lxc.apparmor.profile=unconfined >/dev/null
  incus config device set "$m" eth0 ipv4.address="${ip[$m]}" >/dev/null
  incus start "$m"
done

setup_member() {
  local m=$1
  retry 30 on "$m" "getent hosts pkgs.zabbly.com >/dev/null"
  on "$m" "export DEBIAN_FRONTEND=noninteractive
    apt-get -qq update && apt-get -qq install -y curl jq ca-certificates >/dev/null
    mkdir -p /etc/apt/keyrings && curl -fsSL https://pkgs.zabbly.com/key.asc -o /etc/apt/keyrings/zabbly.asc
    . /etc/os-release
    printf 'Enabled: yes\nTypes: deb\nURIs: https://pkgs.zabbly.com/incus/stable\nSuites: %s\nComponents: main\nArchitectures: $goarch\nSigned-By: /etc/apt/keyrings/zabbly.asc\n' \"\$VERSION_CODENAME\" >/etc/apt/sources.list.d/zabbly-incus-stable.sources
    apt-get -qq update && apt-get -qq install -y incus >/dev/null
    curl -fsSL https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION/etcd-$ETCD_VERSION-linux-$goarch.tar.gz | tar -xz -C /usr/local/bin --strip-components=1 etcd-$ETCD_VERSION-linux-$goarch/etcd etcd-$ETCD_VERSION-linux-$goarch/etcdctl
    v=${INCUS_COMPOSE_VERSION#v}
    curl -fsSL https://github.com/lxc/incus-compose/releases/download/$INCUS_COMPOSE_VERSION/incus-compose_\${v}_linux_$goarch.tar.gz | tar -xz -C /usr/local/bin incus-compose"
  incus file push "$bin/pagurus-term" "$bin/pagurus" "$bin/conch" "$m/usr/local/bin/" --mode 0755
}
for m in "${members[@]}"; do log "installing on $m (incus, etcd $ETCD_VERSION, incus-compose $INCUS_COMPOSE_VERSION, pagurus, conch)"; setup_member "$m" & done
wait

log "etcd cluster"
initial=$(for m in "${members[@]}"; do printf '%s=http://%s:2380,' "$m" "${ip[$m]}"; done); initial=${initial%,}
for m in "${members[@]}"; do
  on "$m" "cat >/etc/systemd/system/etcd.service <<EOF
[Unit]
Description=etcd (pagurus lab)
After=network-online.target
[Service]
Type=notify
ExecStart=/usr/local/bin/etcd --name $m --data-dir /var/lib/etcd \\
  --listen-peer-urls http://${ip[$m]}:2380 --initial-advertise-peer-urls http://${ip[$m]}:2380 \\
  --listen-client-urls http://${ip[$m]}:2379,http://127.0.0.1:2379 --advertise-client-urls http://${ip[$m]}:2379 \\
  --initial-cluster $initial --initial-cluster-state new --initial-cluster-token $LAB
Restart=always
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload && systemctl enable --now --no-block etcd"
done
retry 30 on "${members[0]}" "etcdctl --endpoints=$eps endpoint health >/dev/null 2>&1"

log "Incus cluster"
first=${members[0]}
on "$first" "incus admin init --preseed" <<EOF
config:
  core.https_address: ${ip[$first]}:8443
storage_pools:
- name: default
  driver: dir
networks:
- name: incusbr0
  type: bridge
  config:
    ipv4.address: $WORK_SUBNET.1/24
    ipv4.nat: "true"
    ipv6.address: none
profiles:
- name: default
  devices:
    root: {path: /, pool: default, type: disk}
    eth0: {name: eth0, network: incusbr0, type: nic}
cluster:
  server_name: $first
  enabled: true
EOF
for m in "${members[@]:1}"; do
  token=$(on "$first" "incus cluster add $m --quiet")
  on "$m" "incus admin init --preseed" <<EOF
cluster:
  enabled: true
  server_name: $m
  server_address: ${ip[$m]}:8443
  cluster_token: $token
  member_config:
  - entity: storage-pool
    name: default
    key: source
    value: ""
EOF
done
on "$first" "incus cluster list -f csv -c n,s"

log "pagurus: whoami on $LAB-1..3"
cfg=$(jq --argjson m "$(printf '%s\n' "${members[@]}" | jq -R . | jq -s .)" \
  --argjson e "$(tr ',' '\n' <<<"$eps" | jq -R . | jq -s .)" \
  '.members = $m | .prefer = $m | .etcd.endpoints = $e' examples/whoami/pagurus.json)
for m in "${members[@]}"; do
  on "$m" "mkdir -p /etc/pagurus/whoami"
  incus file push examples/whoami/compose.yaml "$m/etc/pagurus/whoami/compose.yaml"
  printf '%s\n' "$cfg" >"$bin/whoami.json"; incus file push "$bin/whoami.json" "$m/etc/pagurus/whoami.json"
  on "$m" "cat >/etc/systemd/system/pagurus@.service <<'EOF'
[Unit]
Description=pagurus: %i on at most one member
After=incus.service network-online.target etcd.service
StartLimitIntervalSec=0
[Service]
Type=notify
NotifyAccess=main
WatchdogSec=30s
Restart=always
RestartSec=3s
SuccessExitStatus=78
RestartPreventExitStatus=78
TimeoutStopSec=60s
KillMode=mixed
Environment=PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
ExecStart=/usr/local/bin/pagurus run -c /etc/pagurus/%i.json --self $m
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload"
done
on "$first" "pagurus provision -c /etc/pagurus/whoami.json --yes"
for m in "${members[@]}"; do on "$m" "pagurus validate -c /etc/pagurus/whoami.json && systemctl enable --now pagurus@whoami"; done

log "waiting for whoami to be served"
serving() {
  local l addr
  l=$(on "$first" "etcdctl --endpoints=$eps get /pagurus/v2/whoami/leader --print-value-only" 2>/dev/null | jq -r '.member // empty') || return 1
  addr=$(on "$first" "etcdctl --endpoints=$eps get /pagurus/v2/whoami/endpoint --print-value-only" 2>/dev/null | jq -r '.addresses | to_entries[0].value // empty') || return 1
  [ -n "$l" ] && [ -n "$addr" ] && on "$l" "curl -fsS -m 3 http://$addr/health" >/dev/null 2>&1 && echo "$l $addr"
}
for _ in $(seq 150); do s=$(serving) && break; sleep 2; done
[ -n "${s:-}" ] || { log "whoami not served after 5 min"; on "$first" "pagurus status -c /etc/pagurus/whoami.json" || true; exit 1; }
log "up: whoami served by ${s% *} at ${s#* } — lab/drill.sh next, lab/down.sh to remove"
