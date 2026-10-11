#!/usr/bin/env bash
# Live failure drills against the production agents (pagurus@<svc> on every
# member), refereed by `pagurus audit`. Each drill injects a fault on whichever
# member currently leads and waits for the expected outcome. Run from a
# workstation that reaches every member over ssh and etcd directly; pagurus
# itself (move, audit) runs on $CTL, a member with the binary and config.
#
#   drill.sh [move|graceful|restart|crash|pause]...   (default: all, in that order)
#
# Exit status is non-zero if any drill misses its expectation or the audit
# ever saw two members running at once. Partition drills need an etcd proxy in
# front of an agent; production has none, so they live in the sim.
set -uo pipefail
svc=${SVC:-whoami}
unit=pagurus@$svc
declare -A ssh=([asus]=10.0.1.10 [dell]=10.0.1.7 [rpi5]=10.0.1.4)
CTL=${CTL:-asus}
eps=10.0.1.3:2379,10.0.1.6:2379,10.0.1.8:2379
on() { local m=$1; shift; ssh -o BatchMode=yes "${ssh[$m]}" "$*"; }
log() { printf '%s  %s\n' "$(date +%T)" "$*"; }
fail=0
miss() { log "MISS: $*"; fail=1; }

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
  [ "$(leader)" = "$1" ] && [ "$(running_on)" = "$1" ] && [ -n "$ip" ] && on "$1" "curl -fsS -m 3 http://$ip:80/health" >/dev/null 2>&1
}
some_other_healthy() { local l; l=$(leader); [ -n "$l" ] && [ "$l" != "$1" ] && healthy_on "$l"; }
settled() { local l; l=$(leader); [ -n "$l" ] && healthy_on "$l"; }
settle() { wait_for 180 settled >/dev/null || miss "cluster did not settle (leader=$(leader) running=$(running_on))"; }
not_running_on() { ! running_on | grep -qw "$1"; }
mainpid() { on "$1" "systemctl show -p MainPID --value $unit"; }
restart_agent() { on "$1" "sudo systemctl start $unit" || miss "could not restart the agent on $1"; }

audit_log=$(mktemp /tmp/pagurus-audit.XXXX)
on "$CTL" "pagurus audit -c $cfg --every 500ms" >"$audit_log" 2>&1 </dev/null &
audit_pid=$!
# SIGINT ends the remote audit with its summary line.
stop_audit() { on "$CTL" "pkill -INT -f 'pagurus audit -c $cfg'" 2>/dev/null; wait $audit_pid 2>/dev/null; }
trap stop_audit EXIT
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
# dead term's lease has expired. Never two.
drill_restart() {
  local l pid e; l=$(leader); e=$(epoch); pid=$(mainpid "$l"); log "== restart: SIGKILL the agent on $l (pid $pid, epoch $e); systemd restarts it within RestartSec"
  on "$l" "sudo kill -KILL $pid"
  new_term() { local n; n=$(epoch); [ -n "$n" ] && [ "$n" != "$e" ] && settled; }
  if t=$(wait_for 120 new_term); then log "   $(leader) serving a new term (epoch $(epoch)) ${t}s after the kill"; else miss "restart (leader=$(leader) epoch=$(epoch) running=$(running_on))"; fi
}

drill_crash() {
  local l; l=$(leader); log "== crash: SIGKILL the agent on $l and keep it down (workload left running)"
  on "$l" "sudo systemctl kill -s KILL --kill-whom=main $unit; sudo systemctl stop $unit"
  if t=$(wait_for 120 some_other_healthy "$l"); then log "   $(leader) serving ${t}s after the crash (old copy fenced via API)"; else miss "no failover"; fi
  restart_agent "$l"
}

# The frozen agent stops petting; systemd's watchdog kills the unit (conch
# with it, unfenced). The restarted agent's startup fence stops the orphaned
# copy, and a new term starts once the dead lease expires.
drill_pause() {
  local l pid e; l=$(leader); e=$(epoch); pid=$(mainpid "$l"); log "== pause: SIGSTOP the agent on $l (epoch $e) for up to 90s"
  on "$l" "sudo kill -STOP $pid"
  new_term() { local n; n=$(epoch); [ -n "$n" ] && [ "$n" != "$e" ] && settled; }
  if t=$(wait_for 90 new_term); then log "   $(leader) serving a new term (epoch $(epoch)) ${t}s into the pause"; else miss "no new term during pause (leader=$(leader) epoch=$(epoch))"; fi
  on "$l" "sudo kill -CONT $pid 2>/dev/null || true"
  sleep 10
  [ "$(running_on)" = "$(leader)" ] || miss "after SIGCONT: running on $(running_on), leader $(leader)"
}

drills=("$@"); [ ${#drills[@]} -gt 0 ] || drills=(move graceful restart crash pause)
for d in "${drills[@]}"; do settle; "drill_$d"; done
settle
stop_audit
trap - EXIT
log "== audit timeline"; sed 's/^/   /' "$audit_log"
grep -q "^audit: .* 0 violations" "$audit_log" && ! grep -q VIOLATION "$audit_log" \
  || { log "AUDIT FAILED: two members ran at once (or the audit did not finish)"; fail=1; }
exit $fail
