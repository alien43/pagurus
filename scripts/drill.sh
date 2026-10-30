#!/usr/bin/env bash
# Live failure drills against the production agents (pagurus@<svc> on every
# member), refereed by `pagurus audit`. Each drill injects a fault on whichever
# member currently leads and waits for the expected outcome. Run from a
# workstation that reaches every member over ssh and etcd directly; pagurus
# itself (move, audit) runs on $CTL, a member with the binary and config.
#
#   drill.sh [move|graceful|restart|crash|pause|stall|partition]...   (default: all but stall and partition, in that order)
#   SVC=<service> HEALTH_PATH=<path> HEALTH_PORT=<port> CTL=<member> select the service, its probe path and port, the control member.
#
# Where the cluster is (required):
#   MEMBERS="m1=addr1 m2=addr2 m3=addr3"   member names (as in the pagurus config) and the address each is reached at
#   ETCD_ENDPOINTS=h1:2379,h2:2379,h3:2379  the etcd cluster
# How to reach it (optional):
#   ON=ssh (default) runs member commands over `ssh <addr>`; ON=incus runs them with `incus exec <member>`
#     (the lab: members are instances on this host)
#   SUDO=sudo (default); SUDO= when member commands already run as root
#   REMOTE_CLI=1 runs `incus` and `etcdctl` on CTL instead of here (no Incus client or etcdctl on this host)
#   PAGURUS_CONFIG=<path on CTL>; default: the first *.json on the unit's command line (`systemctl cat`)
#   HEALTH_TCP=1 probes with a TCP connect to HEALTH_PORT instead of HTTP (a workload with no HTTP: mosquitto 1883).
#
# Exit status is non-zero if any drill misses its expectation or the audit
# ever saw two members running at once.
#
# partition (attended, never in the default set): cuts PART_MEMBER's HOST off
# etcd (TCP 2379) and the Incus cluster (TCP 8443) with an nft table in a
# transient unit that removes itself after CUT seconds. Port 22 and container
# traffic are never matched. The member self-fences by cgroup.kill, which on
# Incus 7.5 leaks the instance volume's mount refcount in its incusd
# (an Incus bug); the drill then moves the service
# back to it and reports whether it can host. HOLD=<members> keeps those
# members' agents stopped for the drill (e.g. a member already poisoned).
set -uo pipefail
svc=${SVC:-whoami}
health_path=${HEALTH_PATH:-/health} # an HTTP path the workload answers 200 on
health_port=${HEALTH_PORT:-80}       # the port it answers on inside the instance (gokapi: 53842)
health_tcp=${HEALTH_TCP:-}
unit=pagurus@$svc
: "${MEMBERS:?MEMBERS=\"m1=addr1 m2=addr2 ...\" is required}" "${ETCD_ENDPOINTS:?ETCD_ENDPOINTS=h1:2379,... is required}"
declare -A addr=()
for kv in $MEMBERS; do addr[${kv%%=*}]=${kv#*=}; done
read -ra member_names <<<"$(for kv in $MEMBERS; do printf '%s ' "${kv%%=*}"; done)"
CTL=${CTL:-${member_names[0]}}
eps=$ETCD_ENDPOINTS
S=${SUDO-sudo}; S=${S:+$S }
if [ "${ON:-ssh}" = incus ]; then
  on() { local m=$1; shift; command incus exec "$m" -- sh -c "$*"; }
else
  on() { local m=$1; shift; ssh -o BatchMode=yes "${addr[$m]}" "$*"; }
fi
if [ -n "${REMOTE_CLI:-}" ]; then
  incus() { on "$CTL" "incus $(printf '%q ' "$@")"; }
  etcdctl() { on "$CTL" "etcdctl $(printf '%q ' "$@")"; }
fi
log() { printf '%s  %s\n' "$(date +%T)" "$*"; }
fail=0
miss() { log "MISS: $*"; fail=1; }

PART_MEMBER=${PART_MEMBER:-${member_names[1]}}
# The stop hook incusd runs after the kill retries the cluster database for
# ~45-100 s; the leak needs the cut to outlast that (60 s did not, 2026-10-14).
CUT=${CUT:-180}
etcd_ips=$(tr ',' '\n' <<<"$eps" | sed -E 's#^[a-z]+://##; s#:[0-9]+$##' | paste -sd,)
declare -A incus_ip=()
for m in "${!addr[@]}"; do incus_ip[$m]=${addr[$m]#*@}; done
HOLD=${HOLD:-}
read -ra hold <<<"${HOLD//,/ }"

cfg=${PAGURUS_CONFIG:-$(on "$CTL" "systemctl cat $unit" | grep -oE '(^|[ =])/[^ ]*\.json' | tr -d ' =' | head -1)}
[ -n "$cfg" ] || { echo "no $unit config on $CTL" >&2; exit 2; }
conf=$(on "$CTL" "cat $cfg")
mapfile -t members < <(jq -r '.members[]' <<<"$conf")
prefix=$(jq -r '.project_prefix' <<<"$conf")
kprefix=$(jq -r '.etcd_prefix // "/pagurus/v2/\(.service)"' <<<"$conf")
P() { on "$CTL" "pagurus $*"; }

leader() { etcdctl --endpoints=$eps get "$kprefix/leader" --print-value-only 2>/dev/null | jq -r '.member // empty'; }
epoch() { etcdctl --endpoints=$eps get "$kprefix/leader" --print-value-only 2>/dev/null | jq -r '.epoch // empty'; }
running_on() {
  local out=()
  for m in "${members[@]}"; do
    [ "$(incus list --project "$prefix-$m" -c s --format csv 2>/dev/null | grep -c RUNNING)" -gt 0 ] && out+=("$m")
  done
  echo "${out[*]:-}"
}
# wait_for <timeout_s> <cmd...>: poll until cmd succeeds; prints seconds taken
wait_for() {
  local t=$1; shift; local s=$SECONDS
  while (( SECONDS - s < t )); do "$@" && { echo $((SECONDS - s)); return 0; }; sleep 1; done
  return 1
}
endpoint_ip() { etcdctl --endpoints=$eps get "$kprefix/endpoint" --print-value-only 2>/dev/null | jq -r '.addresses | to_entries[0].value // empty'; }
# probe M IP: the workload at IP answers, asked from M (the instance sits on
# M's own incusbr0)
probe() {
  if [ -n "$health_tcp" ]; then on "$1" "timeout 3 bash -c '</dev/tcp/$2/$health_port'" >/dev/null 2>&1
  else on "$1" "curl -fsS -m 3 http://$2:$health_port$health_path" >/dev/null 2>&1; fi
}
# healthy_on M: M leads, only M runs, and the workload answers on M
healthy_on() {
  local ip; ip=$(endpoint_ip)
  [ "$(leader)" = "$1" ] && [ "$(running_on)" = "$1" ] && [ -n "$ip" ] && probe "$1" "$ip"
}
some_other_healthy() { local l; l=$(leader); [ -n "$l" ] && [ "$l" != "$1" ] && healthy_on "$l"; }
settled() { local l; l=$(leader); [ -n "$l" ] && healthy_on "$l"; }
settle() { wait_for 180 settled >/dev/null || miss "cluster did not settle (leader=$(leader) running=$(running_on))"; }
not_running_on() { ! running_on | grep -qw "$1"; }
mainpid() { on "$1" "systemctl show -p MainPID --value $unit"; }
restart_agent() { on "$1" "${S}systemctl start $unit" || miss "could not restart the agent on $1"; }
# reboot_armed M: M's unit has a reboot FailureAction (rebootOnWatchdog). There ANY failure of the agent --
# SIGKILL, crash, OOM kill, missed watchdog -- force-reboots the host (systemd runs FailureAction on the
# failed result before Restart= applies). That is the fences_by_reboot contract
# peers rely on, so restart/crash/pause there are host reboots: refused unless ALLOW_REBOOT=1.
reboot_armed() { on "$1" "systemctl show -p FailureAction --value $unit" | grep -q '^reboot'; }
refuse_armed() {
  reboot_armed "$1" || return 1
  [ "${ALLOW_REBOOT:-}" = 1 ] && { log "   ALLOW_REBOOT=1: this REBOOTS $1 (all its pagurus services fail over)"; return 1; }
  miss "$2 on $1 refused: $1 is reboot-armed, so this reboots the host (ALLOW_REBOOT=1 to do it anyway)"
}

audit_log=$(mktemp /tmp/pagurus-audit.XXXX)
on "$CTL" "pagurus audit -c $cfg --every 500ms" >"$audit_log" 2>&1 </dev/null &
audit_pid=$!
# SIGINT ends the remote audit with its summary line.
stop_audit() { on "$CTL" "pkill -INT -f 'pagurus audit -c $cfg'" 2>/dev/null; wait $audit_pid 2>/dev/null; }
# a held agent must not stay stopped if the drill dies
unhold() { for p in "${hold[@]}"; do on "$p" "${S}systemctl start $unit"; done; }
trap 'stop_audit; unhold' EXIT
sleep 3

drill_move() {
  log "== move: round-trip through every member"
  local order=("${members[@]:1}" "${members[0]}")
  for to in "${order[@]}"; do
    t=$( { time -p P move -c "$cfg" --to "$to" >/dev/null; } 2>&1 | awk '/real/{print $2}')
    # move returns once the target holds the office and started; its endpoint record and the
    # first good probe can trail that by a moment
    if w=$(wait_for 60 healthy_on "$to"); then log "   -> $to in ${t}s (+${w}s to healthy)"; else miss "move to $to"; fi
  done
}

drill_graceful() {
  local l; l=$(leader); log "== graceful: systemctl stop the agent on $l"
  on "$l" "${S}systemctl stop $unit"
  [ -z "$(running_on | grep -w "$l")" ] || miss "$l still running after its agent stopped"
  if t=$(wait_for 120 some_other_healthy "$l"); then log "   $(leader) serving ${t}s after the stop"; else miss "no handover"; fi
  restart_agent "$l"
}

# No lease resume: a killed agent's term ends. systemd restarts the agent,
# whose startup fence stops the orphaned copy; the office comes back once the
# dead term's lease has expired. Never two. On a reboot-armed member the kill
# reboots the host instead (refuse_armed).
drill_restart() {
  local l pid e; l=$(leader); refuse_armed "$l" restart && return
  e=$(epoch); pid=$(mainpid "$l"); log "== restart: SIGKILL the agent on $l (pid $pid, epoch $e); systemd restarts it within RestartSec"
  on "$l" "${S}kill -KILL $pid"
  new_term() { local n; n=$(epoch); [ -n "$n" ] && [ "$n" != "$e" ] && settled; }
  if t=$(wait_for 120 new_term); then log "   $(leader) serving a new term (epoch $(epoch)) ${t}s after the kill"; else miss "restart (leader=$(leader) epoch=$(epoch) running=$(running_on))"; fi
}

# On a member armed with FailureAction=reboot-force this REBOOTS the host, as
# any agent failure there does (seen on a production member, and with a bare SIGKILL).
drill_crash() {
  local l; l=$(leader); refuse_armed "$l" crash && return
  log "== crash: SIGKILL the agent on $l and keep it down (workload left running)"
  on "$l" "${S}systemctl kill -s KILL --kill-whom=main $unit; ${S}systemctl stop $unit"
  if t=$(wait_for 120 some_other_healthy "$l"); then log "   $(leader) serving ${t}s after the crash (old copy fenced via API)"; else miss "no failover"; fi
  restart_agent "$l"
}

# The frozen agent stops petting; systemd's watchdog kills the unit (conch
# with it, unfenced). The restarted agent's startup fence stops the orphaned
# copy, and a new term starts once the dead lease expires.
drill_pause() {
  local l pid e; l=$(leader); refuse_armed "$l" pause && return
  e=$(epoch); pid=$(mainpid "$l"); log "== pause: SIGSTOP the agent on $l (epoch $e) for up to 90s"
  on "$l" "${S}kill -STOP $pid"
  new_term() { local n; n=$(epoch); [ -n "$n" ] && [ "$n" != "$e" ] && settled; }
  if t=$(wait_for 90 new_term); then log "   $(leader) serving a new term (epoch $(epoch)) ${t}s into the pause"; else miss "no new term during pause (leader=$(leader) epoch=$(epoch))"; fi
  on "$l" "${S}kill -CONT $pid 2>/dev/null || true"
  sleep 10
  [ "$(running_on)" = "$(leader)" ] || miss "after SIGCONT: running on $(running_on), leader $(leader)"
}

# The leader's incusd freezes for STALL seconds (SIGSTOP; the Incus cluster
# database stalled the whole API for 90 s on 2026-10-21..23). The agent must
# hold: same term, no restart in place, no watchdog starve. Run from a member
# other than CTL, whose incus the drill itself needs: the service moves off
# CTL first. Not in the default set: on a reboot-armed member a regression
# reboots the host, so it is refused there unless ALLOW_REBOOT=1.
drill_stall() {
  local l e ipid since t0 out
  l=$(leader)
  if [ "$l" = "$CTL" ]; then
    for p in "${members[@]}"; do [ "$p" != "$CTL" ] && { l=$p; break; }; done
    P move -c "$cfg" --to "$l" >/dev/null 2>&1
    wait_for 120 healthy_on "$l" >/dev/null || { miss "stall: could not move the service to $l first"; return; }
  fi
  refuse_armed "$l" stall && return
  e=$(epoch); ipid=$(on "$l" "systemctl show -p MainPID --value incus")
  [ -n "$ipid" ] && [ "$ipid" != 0 ] || { miss "stall: no incusd on $l"; return; }
  since=$(on "$l" "date '+%F %T'")
  log "== stall: SIGSTOP incusd on $l (pid $ipid, epoch $e) for ${STALL:-90}s"
  trap 'on "$l" "${S}kill -CONT $ipid"; stop_audit; unhold' EXIT
  on "$l" "${S}kill -STOP $ipid"; t0=$SECONDS
  sleep "${STALL:-90}"
  on "$l" "${S}kill -CONT $ipid"
  trap 'stop_audit; unhold' EXIT
  log "   incusd on $l resumed after $((SECONDS - t0))s"
  wait_for 120 healthy_on "$l" >/dev/null || miss "stall: $l not healthy after the stall (leader=$(leader) running=$(running_on))"
  [ "$(epoch)" = "$e" ] || miss "stall: the term changed (epoch $e -> $(epoch))"
  out=$(agent_log "$l" "$since")
  grep -m1 'backend API unavailable' <<<"$out" | sed 's/^/   /' || miss "stall: $l's agent never saw the API unavailable"
  grep -m1 'backend API answers again' <<<"$out" | sed 's/^/   /'
  for bad in 'restarting workload in place' 'watchdog-starve' 'heartbeat withheld' 'NOT FIT'; do
    grep -q "$bad" <<<"$out" && miss "stall: $l logged \"$bad\": $(grep -m1 "$bad" <<<"$out")"
  done
  log "   $l kept its term (epoch $e): no restart, no starve"
}

online_in_incus() { incus cluster list --format csv -c ns 2>/dev/null | grep -qx "$1,ONLINE"; }
agent_log() { on "$1" "journalctl -u $unit --since '$2' --no-pager -q -o cat"; }

drill_partition() {
  local m=$PART_MEMBER peers=() p t0 since t ips rules nft
  for p in "${!incus_ip[@]}"; do [ "$p" = "$m" ] || peers+=("${incus_ip[$p]}"); done
  ips="$etcd_ips,$(IFS=,; echo "${peers[*]}")"
  log "== partition: cut $m's host from etcd + Incus (TCP 2379/8443 <-> $ips) for ${CUT}s"
  for p in "${hold[@]}"; do log "   holding $p out: systemctl stop $unit"; on "$p" "${S}systemctl stop $unit"; done
  t=$( { time -p P move -c "$cfg" --to "$m" >/dev/null; } 2>&1 | awk '/real/{print $2}')
  healthy_on "$m" || { miss "partition: could not move the service to $m first"; return; }
  log "   $m serving (move ${t}s)"
  rules="table inet drillcut {
  chain in  { type filter hook input  priority -10; ip saddr { $ips } tcp sport { 2379, 8443 } drop; ip saddr { $ips } tcp dport { 2379, 8443 } drop; }
  chain out { type filter hook output priority -10; ip daddr { $ips } tcp dport { 2379, 8443 } drop; ip daddr { $ips } tcp sport { 2379, 8443 } drop; }
}"
  on "$m" "${S}tee /run/pagurus-drillcut.nft" <<<"$rules" >/dev/null || { miss "partition: could not stage the rules on $m"; return; }
  # NixOS keeps nft in the system profile, Raspberry Pi OS in /usr/sbin
  nft=$(on "$m" 'PATH=/run/current-system/sw/bin:/usr/sbin:/sbin:$PATH command -v nft')
  [ -n "$nft" ] || { miss "partition: no nft on $m"; return; }
  since=$(on "$m" "date '+%F %T'")
  on "$m" "${S}systemd-run --unit=pagurus-drill-cut --collect -p RuntimeMaxSec=$((CUT + 30)) -p 'ExecStopPost=-$nft delete table inet drillcut' /bin/sh -c '$nft -f /run/pagurus-drillcut.nft && sleep $CUT; $nft delete table inet drillcut'" >/dev/null 2>&1 \
    || { miss "partition: could not start the cut on $m"; return; }
  t0=$SECONDS
  sleep 1
  on "$m" "${S}nft list table inet drillcut" >/dev/null 2>&1 || { miss "partition: the drillcut table is not on $m (cut did not apply)"; return; }
  # While m is offline Incus keeps its last state (RUNNING: the stop hook never
  # ran), so the failover gap is judged on the new leader alone; the audit
  # still referees overlap from the cgroup ground truth.
  serving_elsewhere() {
    local l ip; l=$(leader); ip=$(endpoint_ip)
    [ -n "$l" ] && [ "$l" != "$m" ] && [ -n "$ip" ] \
      && [ "$(incus list --project "$prefix-$l" -c s --format csv 2>/dev/null | grep -c RUNNING)" -gt 0 ] \
      && probe "$l" "$ip"
  }
  if t=$(wait_for 240 serving_elsewhere); then log "   $(leader) serving ${t}s after the cut (failover gap)"; else miss "partition: no failover within 240s (leader=$(leader) running=$(running_on))"; fi
  wait_for $((CUT + 90)) online_in_incus "$m" >/dev/null || miss "partition: $m not ONLINE again"
  on "$m" "${S}nft list table inet drillcut" >/dev/null 2>&1 && miss "partition: drillcut table still on $m"
  log "   cut lifted, $m ONLINE $((SECONDS - t0))s after the cut"
  agent_log "$m" "$since" | grep -m1 'kind=cgroup-kill' | sed 's/^/   /' || log "   (no cgroup-kill fence step on $m)"
  agent_log "$m" "$since" | grep -q 'fence-self confirmed' || miss "partition: $m's self-fence not confirmed"
  on "$m" "journalctl -u incus --since '$since' --no-pager -q" | grep -m1 'The stop hook failed to load' | sed 's/^.*level=/   incusd: level=/'

  log "   move back to $m"
  since=$(on "$m" "date '+%F %T'"); t0=$SECONDS
  on "$CTL" "timeout 60 pagurus move -c $cfg --to $m" >/dev/null 2>&1
  if healthy_on "$m"; then
    log "   $m hosts the service again ($((SECONDS - t0))s): no leak"
  else
    start_failed() { agent_log "$m" "$since" | grep -q 'start failed'; }
    wait_for 90 start_failed >/dev/null
    agent_log "$m" "$since" | grep -m1 'start failed' | grep -q 'In use' && log "   $m: start failed: In use (leaked mount)" \
      || agent_log "$m" "$since" | grep -m1 'start failed' | sed 's/^/   /'
    if t=$(wait_for 300 some_other_healthy "$m"); then
      log "   $(leader) serving again, $((SECONDS - t0))s after the move to $m (the move's outage)"
    else
      miss "partition: nobody healthy 300s after the failed move to $m"
    fi
    agent_log "$m" "$since" | grep -m1 'releasing the office' | sed 's/^/   /'
    agent_log "$m" "$since" | grep -m1 'backing off after giving up' | sed 's/^/   /'
    log "   $m unable to host (incusd leak: clear with an incus restart on $m, runbook §6)"
  fi
  for p in "${hold[@]}"; do log "   restarting $unit on $p"; restart_agent "$p"; done
}

drills=("$@"); [ ${#drills[@]} -gt 0 ] || drills=(move graceful restart crash pause)
for d in "${drills[@]}"; do settle; "drill_$d"; done
settle
stop_audit
trap - EXIT
unhold
log "== audit timeline"; sed 's/^/   /' "$audit_log"
grep -q "^audit: .* 0 violations" "$audit_log" && ! grep -q VIOLATION "$audit_log" \
  || { log "AUDIT FAILED: two members ran at once (or the audit did not finish)"; fail=1; }
exit $fail
