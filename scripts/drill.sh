#!/usr/bin/env bash
# Live failure drills against the production agents (pagurus@<svc> on every
# member), refereed by `pagurus audit`. Each drill injects a fault on whichever
# member currently leads and waits for the expected outcome. Run from a
# workstation that reaches every member over ssh and etcd directly; pagurus
# itself (move, audit) runs on $CTL, a member with the binary and config.
#
#   drill.sh [move|graceful|restart|crash|pause|partition]...   (default: all but partition, in that order)
#   SVC=<service> HEALTH_PATH=<path> HEALTH_PORT=<port> CTL=<member> select the service, its probe path and port, the control member.
#
# Exit status is non-zero if any drill misses its expectation or the audit
# ever saw two members running at once.
#
# partition (attended, never in the default set): cuts PART_MEMBER's HOST off
# etcd (TCP 2379) and the Incus cluster (TCP 8443) with an nft table in a
# transient unit that removes itself after CUT seconds. Port 22 and container
# traffic are never matched. The member self-fences by cgroup.kill, which on
# Incus 7.5 leaks the instance volume's mount refcount in its incusd
# (tasks/pagurus-fenced-member-recovery.md); the drill then moves the service
# back to it and reports whether it can host. HOLD=<members> keeps those
# members' agents stopped for the drill (e.g. a member already poisoned).
set -uo pipefail
svc=${SVC:-whoami}
health_path=${HEALTH_PATH:-/health} # an HTTP path the workload answers 200 on
health_port=${HEALTH_PORT:-80}       # the port it answers on inside the instance (gokapi: 53842)
unit=pagurus@$svc
declare -A ssh=([asus]=10.0.1.10 [dell]=10.0.1.7 [rpi5]=10.0.1.4)
CTL=${CTL:-asus}
eps=10.0.1.3:2379,10.0.1.6:2379,10.0.1.8:2379
on() { local m=$1; shift; ssh -o BatchMode=yes "${ssh[$m]}" "$*"; }
log() { printf '%s  %s\n' "$(date +%T)" "$*"; }
fail=0
miss() { log "MISS: $*"; fail=1; }

PART_MEMBER=${PART_MEMBER:-dell}
# The stop hook incusd runs after the kill retries the cluster database for
# ~45-100 s; the leak needs the cut to outlast that (60 s did not, 2026-10-14).
CUT=${CUT:-180}
etcd_ips=10.0.1.3,10.0.1.6,10.0.1.8
declare -A incus_ip=([asus]=10.0.1.10 [dell]=10.0.1.7 [rpi5]=10.0.1.4)
HOLD=${HOLD:-}
read -ra hold <<<"${HOLD//,/ }"

cfg=$(on "$CTL" "systemctl cat $unit" | grep -o '/nix/store/[^ ]*\.json' | head -1)
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
# healthy_on M: M leads, only M runs, and the workload answers on M (curl from
# M: the instance sits on M's own incusbr0)
healthy_on() {
  local ip; ip=$(endpoint_ip)
  [ "$(leader)" = "$1" ] && [ "$(running_on)" = "$1" ] && [ -n "$ip" ] && on "$1" "curl -fsS -m 3 http://$ip:$health_port$health_path" >/dev/null 2>&1
}
some_other_healthy() { local l; l=$(leader); [ -n "$l" ] && [ "$l" != "$1" ] && healthy_on "$l"; }
settled() { local l; l=$(leader); [ -n "$l" ] && healthy_on "$l"; }
settle() { wait_for 180 settled >/dev/null || miss "cluster did not settle (leader=$(leader) running=$(running_on))"; }
not_running_on() { ! running_on | grep -qw "$1"; }
mainpid() { on "$1" "systemctl show -p MainPID --value $unit"; }
restart_agent() { on "$1" "sudo systemctl start $unit" || miss "could not restart the agent on $1"; }
# reboot_armed M: M's unit has a reboot FailureAction (rebootOnWatchdog). There ANY failure of the agent --
# SIGKILL, crash, OOM kill, missed watchdog -- force-reboots the host (systemd runs FailureAction on the
# failed result before Restart= applies; dell 2026-10-18 08:00:52). That is the fences_by_reboot contract
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
unhold() { for p in "${hold[@]}"; do on "$p" "sudo systemctl start $unit"; done; }
trap 'stop_audit; unhold' EXIT
sleep 3

drill_move() {
  log "== move: round-trip through every member"
  local order=("${members[@]:1}" "${members[0]}")
  for to in "${order[@]}"; do
    t=$( { time -p P move -c "$cfg" --to "$to" >/dev/null; } 2>&1 | awk '/real/{print $2}')
    if healthy_on "$to"; then log "   -> $to in ${t}s"; else miss "move to $to"; fi
  done
}

drill_graceful() {
  local l; l=$(leader); log "== graceful: systemctl stop the agent on $l"
  on "$l" "sudo systemctl stop $unit"
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
  on "$l" "sudo kill -KILL $pid"
  new_term() { local n; n=$(epoch); [ -n "$n" ] && [ "$n" != "$e" ] && settled; }
  if t=$(wait_for 120 new_term); then log "   $(leader) serving a new term (epoch $(epoch)) ${t}s after the kill"; else miss "restart (leader=$(leader) epoch=$(epoch) running=$(running_on))"; fi
}

# On a member armed with FailureAction=reboot-force this REBOOTS the host, as
# any agent failure there does (dell 2026-10-12, and a bare SIGKILL 2026-10-18).
drill_crash() {
  local l; l=$(leader); refuse_armed "$l" crash && return
  log "== crash: SIGKILL the agent on $l and keep it down (workload left running)"
  on "$l" "sudo systemctl kill -s KILL --kill-whom=main $unit; sudo systemctl stop $unit"
  if t=$(wait_for 120 some_other_healthy "$l"); then log "   $(leader) serving ${t}s after the crash (old copy fenced via API)"; else miss "no failover"; fi
  restart_agent "$l"
}

# The frozen agent stops petting; systemd's watchdog kills the unit (conch
# with it, unfenced). The restarted agent's startup fence stops the orphaned
# copy, and a new term starts once the dead lease expires.
drill_pause() {
  local l pid e; l=$(leader); refuse_armed "$l" pause && return
  e=$(epoch); pid=$(mainpid "$l"); log "== pause: SIGSTOP the agent on $l (epoch $e) for up to 90s"
  on "$l" "sudo kill -STOP $pid"
  new_term() { local n; n=$(epoch); [ -n "$n" ] && [ "$n" != "$e" ] && settled; }
  if t=$(wait_for 90 new_term); then log "   $(leader) serving a new term (epoch $(epoch)) ${t}s into the pause"; else miss "no new term during pause (leader=$(leader) epoch=$(epoch))"; fi
  on "$l" "sudo kill -CONT $pid 2>/dev/null || true"
  sleep 10
  [ "$(running_on)" = "$(leader)" ] || miss "after SIGCONT: running on $(running_on), leader $(leader)"
}

online_in_incus() { incus cluster list --format csv -c ns 2>/dev/null | grep -qx "$1,ONLINE"; }
agent_log() { on "$1" "journalctl -u $unit --since '$2' --no-pager -q -o cat"; }

drill_partition() {
  local m=$PART_MEMBER peers=() p t0 since t ips rules
  for p in "${!incus_ip[@]}"; do [ "$p" = "$m" ] || peers+=("${incus_ip[$p]}"); done
  ips="$etcd_ips,$(IFS=,; echo "${peers[*]}")"
  log "== partition: cut $m's host from etcd + Incus (TCP 2379/8443 <-> $ips) for ${CUT}s"
  for p in "${hold[@]}"; do log "   holding $p out: systemctl stop $unit"; on "$p" "sudo systemctl stop $unit"; done
  t=$( { time -p P move -c "$cfg" --to "$m" >/dev/null; } 2>&1 | awk '/real/{print $2}')
  healthy_on "$m" || { miss "partition: could not move the service to $m first"; return; }
  log "   $m serving (move ${t}s)"
  rules="table inet drillcut {
  chain in  { type filter hook input  priority -10; ip saddr { $ips } tcp sport { 2379, 8443 } drop; ip saddr { $ips } tcp dport { 2379, 8443 } drop; }
  chain out { type filter hook output priority -10; ip daddr { $ips } tcp dport { 2379, 8443 } drop; ip daddr { $ips } tcp sport { 2379, 8443 } drop; }
}"
  on "$m" "sudo tee /run/pagurus-drillcut.nft" <<<"$rules" >/dev/null || { miss "partition: could not stage the rules on $m"; return; }
  since=$(on "$m" "date '+%F %T'")
  on "$m" "sudo systemd-run --unit=pagurus-drill-cut --collect -p RuntimeMaxSec=$((CUT + 30)) -p 'ExecStopPost=-/run/current-system/sw/bin/nft delete table inet drillcut' /run/current-system/sw/bin/bash -c 'PATH=/run/current-system/sw/bin; nft -f /run/pagurus-drillcut.nft && sleep $CUT; nft delete table inet drillcut'" >/dev/null 2>&1 \
    || { miss "partition: could not start the cut on $m"; return; }
  t0=$SECONDS
  sleep 1
  on "$m" "sudo nft list table inet drillcut" >/dev/null 2>&1 || { miss "partition: the drillcut table is not on $m (cut did not apply)"; return; }
  # While m is offline Incus keeps its last state (RUNNING: the stop hook never
  # ran), so the failover gap is judged on the new leader alone; the audit
  # still referees overlap from the cgroup ground truth.
  serving_elsewhere() {
    local l ip; l=$(leader); ip=$(endpoint_ip)
    [ -n "$l" ] && [ "$l" != "$m" ] && [ -n "$ip" ] \
      && [ "$(incus list --project "$prefix-$l" -c s --format csv 2>/dev/null | grep -c RUNNING)" -gt 0 ] \
      && on "$l" "curl -fsS -m 3 http://$ip:$health_port$health_path" >/dev/null 2>&1
  }
  if t=$(wait_for 240 serving_elsewhere); then log "   $(leader) serving ${t}s after the cut (failover gap)"; else miss "partition: no failover within 240s (leader=$(leader) running=$(running_on))"; fi
  wait_for $((CUT + 90)) online_in_incus "$m" >/dev/null || miss "partition: $m not ONLINE again"
  on "$m" "sudo nft list table inet drillcut" >/dev/null 2>&1 && miss "partition: drillcut table still on $m"
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
