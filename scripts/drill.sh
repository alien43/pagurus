#!/usr/bin/env bash
# Live failure drills against the running prototype (scripts/proto-up.sh
# --drill-proxy), refereed by `singleton audit`. Each drill injects a fault
# on whichever member currently leads and waits for the expected outcome.
# Run from rpi5.
#
#   drill.sh [move|graceful|restart|crash|pause|partition]...   (default: all, in that order)
#
# Exit status is non-zero if any drill misses its expectation or the audit
# ever saw two members running at once.
set -uo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
svc=whoami
unit=singleton-$svc
S=/var/lib/singleton-proto/singleton   # rpi5's copy (its /run is noexec)
cfg=/run/singleton-proto/$svc/singleton.json
eps=10.0.1.3:2379,10.0.1.6:2379,10.0.1.8:2379
declare -A ssh=([asus]=10.0.1.10 [dell]=10.0.1.7 [rpi5]="")
on() { local m=$1; shift; if [ -z "${ssh[$m]}" ]; then bash -c "$*"; else ssh -o BatchMode=yes "${ssh[$m]}" "$*"; fi; }
log() { printf '%s  %s\n' "$(date +%T)" "$*"; }
fail=0
miss() { log "MISS: $*"; fail=1; }

leader() { etcdctl --endpoints=$eps get /pagurus/v2/$svc/leader --print-value-only 2>/dev/null | jq -r '.member // empty'; }
running_on() {
  local out=()
  for m in asus dell rpi5; do
    [ "$(incus list --project sgl-$svc-$m -c s --format csv 2>/dev/null | grep -c RUNNING)" -gt 0 ] && out+=("$m")
  done
  echo "${out[*]:-}"
}
# wait_for <timeout_s> <cmd...>: poll until cmd succeeds; prints seconds taken
wait_for() {
  local t=$1; shift; local s=$SECONDS
  while (( SECONDS - s < t )); do "$@" && { echo $((SECONDS - s)); return 0; }; sleep 1; done
  return 1
}
endpoint_ip() { etcdctl --endpoints=$eps get /pagurus/v2/$svc/endpoint --print-value-only 2>/dev/null | jq -r '.addresses["whoami-1"] // empty'; }
# healthy_on M: M leads, only M runs, and whoami answers on M (curl from M: the
# instance sits on M's own incusbr0)
healthy_on() {
  local ip; ip=$(endpoint_ip)
  [ "$(leader)" = "$1" ] && [ "$(running_on)" = "$1" ] && [ -n "$ip" ] && on "$1" "curl -fsS -m 3 http://$ip:80/health" >/dev/null 2>&1
}
some_other_healthy() { local l; l=$(leader); [ -n "$l" ] && [ "$l" != "$1" ] && healthy_on "$l"; }
settled() { local l; l=$(leader); [ -n "$l" ] && healthy_on "$l"; }
settle() { wait_for 180 settled >/dev/null || miss "cluster did not settle (leader=$(leader) running=$(running_on))"; }
not_running_on() { ! running_on | grep -qw "$1"; }
mainpid() { on "$1" "systemctl show -p MainPID --value $unit"; }

audit_log=$(mktemp /tmp/singleton-audit.XXXX)
sudo -n true
$S audit -c "$cfg" --every 500ms >"$audit_log" 2>&1 &
audit_pid=$!
trap 'kill -INT $audit_pid 2>/dev/null' EXIT
sleep 2

drill_move() {
  log "== move: round-trip through every member"
  for to in dell rpi5 asus; do
    t=$( { time -p $S move -c "$cfg" --to $to >/dev/null; } 2>&1 | awk '/real/{print $2}')
    if healthy_on $to; then log "   -> $to in ${t}s"; else miss "move to $to"; fi
  done
}

drill_graceful() {
  local l; l=$(leader); log "== graceful: systemctl stop the agent on $l"
  on $l "sudo systemctl stop $unit"
  [ -z "$(running_on | grep -w $l)" ] || miss "$l still running after its agent stopped"
  if t=$(wait_for 120 some_other_healthy $l); then log "   $(leader) serving ${t}s after the stop"; else miss "no handover"; fi
  restart_agent $l
}
restart_agent() { "$here/scripts/proto-up.sh" --drill-proxy --only "$1" >/dev/null || miss "could not restart the agent on $1"; }

# conch has no lease resume (Andrey's Q3 decision): a killed agent's term ends.
# systemd restarts the agent, whose startup fence stops the orphaned copy; the
# office comes back once the dead term's lease has expired. Never two.
drill_restart() {
  local l pid; l=$(leader); pid=$(mainpid $l); log "== restart: SIGKILL the agent on $l (pid $pid); systemd restarts it within RestartSec"
  on $l "sudo kill -KILL $pid"
  if t=$(wait_for 120 settled); then log "   $(leader) serving ${t}s after the kill"; else miss "restart (leader=$(leader) running=$(running_on))"; fi
}

drill_crash() {
  local l pid; l=$(leader); pid=$(mainpid $l); log "== crash: SIGKILL the agent on $l and keep it down (workload left running)"
  on $l "sudo systemctl kill -s KILL --kill-whom=main $unit; sudo systemctl stop $unit"
  if t=$(wait_for 120 some_other_healthy $l); then log "   $(leader) serving ${t}s after the crash (old copy fenced via API)"; else miss "no failover"; fi
  restart_agent $l
}

drill_pause() {
  local l pid; l=$(leader); pid=$(mainpid $l); log "== pause: SIGSTOP the agent on $l for 45s"
  on $l "sudo kill -STOP $pid"
  if t=$(wait_for 90 some_other_healthy $l); then log "   $(leader) serving ${t}s into the pause"; else miss "no failover during pause"; fi
  on $l "sudo kill -CONT $pid 2>/dev/null || true"
  sleep 10
  [ "$(running_on)" = "$(leader)" ] || miss "after SIGCONT: running on $(running_on), leader $(leader)"
}

drill_partition() {
  local l; l=$(leader); log "== partition: cut the agent on $l off etcd (drill proxy)"
  on $l "sudo systemctl kill -s USR1 --kill-whom=main $unit-px"
  local s=$SECONDS
  if wait_for 30 not_running_on $l >/dev/null; then
    log "   $l self-fenced after $((SECONDS - s))s"
  else miss "$l did not self-fence"; fi
  if t=$(wait_for 120 some_other_healthy $l); then log "   $(leader) serving $((SECONDS - s))s after the partition"; else miss "no failover"; fi
  on $l "sudo systemctl kill -s USR2 --kill-whom=main $unit-px"
  sleep 15
  [ "$(running_on)" = "$(leader)" ] || miss "after healing: running on $(running_on), leader $(leader)"
}

drills=("$@"); [ ${#drills[@]} -gt 0 ] || drills=(move graceful restart crash pause partition)
for d in "${drills[@]}"; do settle; "drill_$d"; done
settle
kill -INT $audit_pid; wait $audit_pid; audit_rc=$?
trap - EXIT
log "== audit timeline"; sed 's/^/   /' "$audit_log"
[ $audit_rc = 0 ] || { log "AUDIT FAILED: two members ran at once"; fail=1; }
exit $fail
