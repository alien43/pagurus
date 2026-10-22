# Fencing and the watchdog

## Fences

How a member's copy is stopped is configured, not hard-wired. `fences.self` is an ordered
chain the member runs on itself before its lease can expire. It stops at the first fence whose
**confirm** says nothing runs (`internal/fence`):

```jsonc
"fences": {
  "self": [
    {"kind": "incus-stop",  "budget": "3s"},                    // incus stop --force; confirmed from cgroupfs
    {"kind": "cgroup-kill", "budget": "2s"},                    // cgroup.kill on lxc.payload.<project>_*; no incusd, no quorum
    {"kind": "exec", "budget": "2s",                            // anything else: a power fence, a site script
     "command": ["/etc/pagurus/plug-off", "{member}"],          // {member} and {project} are expanded per argv element, no shell
     "confirm": ["/etc/pagurus/plug-is-off", "{member}"],       // optional; without it, the command's exit 0 confirms
     "for": ["container"]}                                      // optional: target types it applies to
  ]
}
```

- **Absent:** `incus-stop` (`self_fence.stop_budget`) then `cgroup-kill`
  (`self_fence.kill_budget`), exactly the prototype's behaviour. Set either `self_fence` or
  `fences.self`, not both.
- **Empty (`"self": []`), or no fence applies** (the container fences skip VMs: a VM's qemu
  has no `lxc.payload` cgroup):
  - the member has **no verified self-fence**, and advertises it (`Caps.NoVerifiedSelfFence`);
  - peers then refuse to trust it under `fence.unverified=proceed` and block as under `block`,
    until it returns or an operator runs `fence-ack`;
  - when it must fence itself, it cannot confirm, so it goes unfit and the watchdog is the
    backstop.
- **Timing:** the chain's total budget replaces `stop_budget + kill_budget` in every timing
  rule.

`pagurus fence-check -c cfg --member <m>` runs only the *confirm* of each fence and prints
the chain. It is read-only, so safe on production. Run it **on** that member, because the
container fences read the local cgroupfs.

### VMs

An Incus API stop **needs cluster quorum** for VMs exactly as for containers. It opens
cluster-DB transactions before it reaches the qemu driver (Incus v7.5.1
`internal/server/instance/instance_utils.go:335`, `internal/server/operations/linux.go:20`).
And a VM's qemu has **no `lxc.payload` cgroup**: it runs in `incus.service`'s. So for VMs:

```jsonc
"fences": {"self": [{"kind": "incus-stop", "budget": "4s"}, {"kind": "qemu-kill", "budget": "3s"}]}
```

The whole self chain runs inside conch's fence budget (its total + 1s), and conch requires loss
detection (TTL/2 at the default TTL) + that budget + 1s < TTL. At TTL 20s the chain must total
under 8s; a longer chain needs a longer `lease.ttl`. `pagurus validate` checks this.

`incus-stop` and `cgroup-kill` skip VMs, because they confirm from cgroupfs, which cannot see
qemu. `qemu-kill` is the Incus-free VM self-fence:
- it reads `/run/incus/<project>_<name>/qemu.pid` and opens a pidfd;
- it accepts the process only if its cmdline is qemu (`qemu-system`/`qemu-kvm`) and names that
  pidfile, as Incus checks its own PID (`driver_qemu.go:6517`);
- it SIGKILLs through the pidfd, so a reused PID is never hit, and confirms when no such
  process is left.

Measured on node1 (Alpine VM): confirmed in 117 ms, and Incus then reports the VM `STOPPED`
and can start it again. Without `qemu-kill` a VM member has no verified self-fence, which is
allowed and advertised (§ Fences). `boot.autostart=false` keeps a VM down across a host reboot
just as for a container: `instanceShouldAutoStart` (`cmd/incusd/instances.go:276`) ignores the
instance type.

### Writing a fence

1. Implement `fence.Fence`: `Kind`, `Budget`, `Applies(target)`, `Run`, `Confirm`.
   - `Run` must be idempotent.
   - `Confirm` must not change anything.
   - Both must honour the context, because the chain gives the pair no more than `Budget`.
2. Add it to `fence.BuildSelf`.
3. Test it with `internal/fence/fencetest` (no real Incus, cgroupfs or host):
   - `FakeBin`: scripted binaries that record their argv;
   - `FakeCgroupFS`: a cgroup2 tree whose `cgroup.kill` empties the cgroup, or doesn't;
   - `FakeProc`: a real process with a pidfile;
   - then pass `fencetest.Contract`, which checks: it confirms after a run; confirm is
     read-only; it is idempotent; it honours its budget; errors name the kind.

### Peer fences

`fences.peer` is what a new leader does to every other member that may still run the
workload. It is retried each fencing round, and the **first fence that does not skip
decides**:

```jsonc
"fences": {
  "peer": [
    {"kind": "incus-api-stop", "budget": "20s"},       // stop via the cluster API, confirm from the cluster's view
    {"kind": "exec", "budget": "20s",                  // a power fence: smart plug, IPMI, PDU
     "command": ["/etc/pagurus/plug-off", "{member}"],
     "confirm": ["/etc/pagurus/plug-is-off", "{member}"]},
    {"kind": "watchdog-wait"}                          // trust an armed reboot watchdog after its timeout + fence.reboot_margin
  ]
}
```

| Outcome | Meaning |
| :--- | :--- |
| **confirmed, verified** | observed stopped (API stop, a confirmed power-off): the member's record is set to `stopped` |
| **confirmed, assumed** | `watchdog-wait` ran out: treated as fenced, but the `may-run` record is kept, so every later leader deals with it again |
| **wait** | stop the chain and retry later. Examples: the cluster still *sees* the instance running (no later fence may override that), a watchdog is still counting, or project pinning is broken (`BLOCKED`) |
| **skip** | this fence can't act. The member is unreachable, the fence doesn't apply (`exec` and `watchdog-wait` only act on a member recorded `may-run`), or the power fence didn't confirm. Try the next fence |

If every fence skips:
- a member recorded `stopped` needs nothing;
- otherwise `fence.unverified` decides, as before (`block`, or `proceed` after
  `unverified_wait`, never for a member advertising no verified self-fence).

The default chain, `incus-api-stop` (`fence.api_timeout`) then `watchdog-wait`, is the
prototype's behaviour exactly. An `exec` peer fence runs again every fencing round until it
confirms, so the command must be idempotent.

`pagurus fence-run -c cfg --member <m> --fence <i> --i-know` runs one **self** fence for real,
then its confirm. It is meant for drills on scratch projects.

## Watchdog

| `watchdog.mode` | What a missed pet does | Advertised as reboot fence |
| :--- | :--- | :--- |
| `off` | nothing | no |
| `dry-run` *(default)* | logs `NOT FIT — withholding pet` | no |
| `systemd` | the unit's `WatchdogSec=` fires. Default: systemd kills the agent and restarts it, and the restarted agent kills anything still running locally (startup fence). With `FailureAction=reboot-force` the host reboots, and not only on a missed deadline: systemd runs `FailureAction` on **any** failed result of the unit, so a SIGKILL, crash or OOM kill of the agent reboots the host too (node2, 2026-10-18). That is intended: `fences_by_reboot` promises peers that the host resets once the agent stops petting, and a dead agent has stopped petting (a crash-looping one never re-arms systemd's watchdog). The agent reads its own unit's `FailureAction` to decide what it advertises. `scripts/drill.sh` refuses `restart`/`crash`/`pause` on such a member unless `ALLOW_REBOOT=1`. | only with a reboot `FailureAction` |
| `device` | opens `watchdog.device` (e.g. `/dev/watchdog1`, unused on the author's hosts; `/dev/watchdog0` is systemd's `RuntimeWatchdogSec`) **only with `arm: true`**, else behaves as dry-run. Magic-close disarm only on a clean exit with nothing running. | yes, when armed |

In every mode except `off`, conch's pets come through the daemon's relay (§ Process shape):
the backend is petted only while they are fresh, so the advertised timeout is the backend's
plus half of it.

**The prototype runs `systemd` mode without a `FailureAction`: no machine is ever rebooted.**
