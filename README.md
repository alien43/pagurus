# `pagurus` — at-most-one incus-compose stack, failing over across the cluster

A hermit crab lives in one shell at a time and moves when it has to. `pagurus` keeps a workload
running on exactly one member of an Incus cluster, and moves it when that member fails.

> **Naming.** The project was prototyped as `singleton`; task names still say so. etcd keys are
> `/pagurus/v2/<svc>` (v2: the office became a conch election), and the default state dir is
> `/run/pagurus`.

> **Status: in production on the author's cluster since 2026-10-11** as a canary (a stateless
> `whoami`), one `pagurus@<svc>` systemd unit per member: a NixOS module on two members, a setup
> script on a Debian one. Not yet released; interfaces may still change.

## What it does

One agent runs on each cluster member (asus, dell, rpi5). Between them they keep **one**
incus-compose stack running on **at most one** member at any instant, and on **some** member
whenever at least two of the three are up. The stack has to be stateless and multi-arch. The
prototype runs `traefik/whoami`.

It is the first working slice of the deferred automatic-HA family in `proposals/`. It covers the
**best-effort/FT tiers for stateless work** of `incus_fleet.md`. The election, fencing and
watchdog are as `incus_ha.md` §4/§10 and `stonith_exploration.md` §4–§7 describe them. It moves
no bytes: replication stays out of scope, as in `incus_fleet.md` §13.

```
   etcd (asus-infra, dell-infra, rpi-infra)
     /conch/v1/elect/pagurus/v2/<svc>/<lease>   the office: a conch election (create-rev = epoch)
     /pagurus/v2/<svc>/  leader (who holds it)   ms/<member> may-run|stopped
                         reg/<member>   endpoint   pin
                    ▲                  ▲                  ▲
   ┌────────────────┴─┐    ┌───────────┴──────┐    ┌──────┴───────────┐
   │ pagurus@asus     │    │ pagurus@dell     │    │ pagurus@rpi5     │  one unit per service and host;
   │  └ conch elect   │    │  └ conch elect   │    │  └ conch elect   │  pagurus is main, the only
   │     └ pagurus    │    │     (campaigning)│    │     (campaigning)│  petter of systemd's watchdog
   │       term       │    │                  │    │                  │
   └────────┬─────────┘    └──────────────────┘    └──────────────────┘
            │ incus-compose up / incus stop / cgroup.kill
   project sgl-whoami-asus  sgl-whoami-dell  sgl-whoami-rpi5
   (restricted.cluster.groups = host-asus / host-dell / host-rpi5)
```

## Process shape

One systemd unit per service and host runs the pagurus daemon (the unit's main process). The
daemon keeps the local ground truth, and while this member is eligible it runs
`conch elect <office> --fence 'pagurus fence-self …' --watchdog --campaign-after <rank × grace>
-- pagurus term`. conch (`tools/conch/docs/03_elect.md`) owns the election, the lease, the lease's
local validity bound, the fence before expiry, and the watchdog fitness of a term. When conch
wins, its child `pagurus term` hands the term (CONCH_REV, CONCH_LEASE) back to the daemon over a
unix socket, and the daemon runs it in-process.

- **Watchdog.** conch is given `NOTIFY_SOCKET` pointing at a relay in the daemon. The daemon is
  the only process that pets systemd (`NotifyAccess=main`). While conch runs, the daemon pets
  exactly while conch's last pet is fresh. While conch isn't running, it pets while cgroupfs
  shows nothing of ours running here. It has no other fitness rules. The term's loop touches
  conch's `--watchdog-heartbeat` file, so a wedged term makes conch unfit.
- **Preference.** `--campaign-after <rank × grace>`: a member campaigns only after the office
  has been vacant for that long, unbroken, so a preferred member that comes back within the
  window keeps the office. The member the office is pinned to (`move`) uses 0.
- **No lease resume.** A killed agent's term ends with it. The restarted agent's startup fence
  stops the orphaned copy, and the office comes back once the dead lease expires.
- One conch run is one term. The daemon starts a new conch for each campaign, so eligibility,
  backoff after giving up, and pins stay with the daemon.

## Why per-member projects

A clustered Incus instance has exactly one home (`incus_fleet.md` §2). So the stack does not
move between members. Each member has its own copy of the stack, in its own project pinned to
its own single-member cluster group. Fencing member M then means just "nothing in project
`<prefix>-M` runs". `incus-compose -p <prefix>-M up` is how M runs the stack. The agent refuses
to start, and refuses to campaign, unless the project is pinned to exactly its member. It also
re-checks every instance's `location` after every `up`.

## The safety argument

**Invariant:** at no instant does more than one member's project have a running instance.

1. **One office.** The office is a conch election (etcd's `concurrency.Election`): the candidate
   key with the lowest create revision holds it, and that revision is the **epoch** (CONCH_REV).
   A key that is lowest stays lowest while it exists, so every leader write is a transaction
   guarded on "my candidate key still exists with my epoch". That covers recording the leader,
   recording `may-run`, recording a fence, and publishing the endpoint. So a deposed leader
   cannot authorise anything. Each supervision tick also re-reads the key, so a term whose key
   was deleted outside conch stops.

2. **The leader stops itself before its lease can expire.** This is conch's guarantee
   (`03_elect.md` § Fencing). conch tracks `ValidUntil`: the send time of the newest renewal
   that succeeded, plus the TTL, on this host's monotonic clock. etcd extends a lease from the
   moment it *receives* the renewal, so `ValidUntil` can never be later than the real expiry.
   conch's guardian starts the fence (`pagurus fence-self`) at loss detection, and at the
   latest at `ValidUntil − fence budget − 1 s`, whatever the term is doing. The fence runs the
   member's **self-fence chain** (§ Fences). The default chain is:
   - `incus stop --force` on the local project, within `stop_budget`;
   - then **`cgroup.kill`** on `lxc.payload.<project>_*`, within `kill_budget`. This needs no
     Incus daemon and no cluster quorum, which matters because a partitioned member's Incus has
     lost the dqlite quorum and cannot stop anything;
   - success is confirmed from **cgroupfs** (`cgroup.events: populated 0`), not from Incus.

   conch's budget is the chain's total plus 1 s for `fence-self` to start. A fence that fails
   is not resigned: the lease is left to expire, conch stays unfit, and the next leader must
   fence this member. No rival can acquire before the lease is gone server-side. So a
   successful self-fence always finishes before any rival can start.

3. **A new leader fences every member that may still run the workload, not just the last
   one.** `ms/<member>` is a lease-less record. A member writes `may-run` (guarded) before it
   starts. It is set back to `stopped` only by:
   - a confirmed graceful release;
   - a leader that confirmed the stop through the Incus API;
   - the member itself, after verifying from cgroupfs that nothing runs and while it is not
     leading;
   - an operator's `fence-ack`.

   On acquiring, the new leader takes each other member in turn:
   - **Incus-reachable** → `incus stop --force` through the cluster API, then confirm every
     instance reads `Stopped`. If the cluster can *see* a running instance, no record can
     override that.
   - **Unreachable, record `stopped`** → nothing to fence.
   - **Unreachable, record `may-run`, member advertised a reboot watchdog** → wait for that
     member's watchdog timeout plus `reboot_margin`, counted from acquisition. The member's last
     watchdog pet was before its safe deadline, which was before the lease expired, which was
     before we acquired.
   - **Unreachable, `may-run`, no reboot watchdog** → `fence.unverified`:
     - `block` (default) waits, fail-closed, until the member returns or `fence-ack`;
     - `proceed` waits `unverified_wait` and trusts the member's software self-fence (step 2).

   This chain covers zombies too. A member fenced only by assumption keeps its `may-run`
   record, so every later leader deals with it again.

4. **Nothing else starts it.** The agent enforces `boot.autostart=false` after every `up`.
   (Unset means "restore last state on boot", so a rebooting host would bring a second copy
   back.) No instance may carry a `user.healthcheck.restart` other than `no`, so ic-healthd never
   restarts a fenced instance. (incus-compose sets the *project's* `user.healthcheck.enabled=true`
   even with `--no-healthd`, so that flag is not the thing to check.) A follower checks cgroupfs every `eligible_interval` and stops anything it
   finds. The leader audits the other members' projects every `audit_interval`.

5. **A wedged or unfit agent is not trusted.** systemd's watchdog is fed only while:
   - **conch runs:** conch's own pet is fresh. conch is fit while not holding, while holding
     with the fence's start deadline ahead and the term's heartbeat fresh, while a fence runs
     within its budget, and never after a fence failed (`03_elect.md` § Watchdog);
   - **conch doesn't run:** cgroupfs shows nothing of ours running here (or briefly, while a
     stray copy or a dead term's remains are being stopped).

   This is `sbd`'s idea, using the conch lease as the quorum signal.

### The residual gap, stated plainly

With `fence.unverified=proceed` and no reboot-armed watchdog, a violation needs **all** of
these together:
- the leader's host is cut off from **both** etcd and the Incus cluster;
- **and** its agent is dead or wedged, or both `incus stop` and `cgroup.kill` fail;
- **and** the host stays up.

Under `block`, that case leaves the service down instead. With an armed watchdog
(`FailureAction=reboot-force`, or `device` mode with `arm: true`), the host resets and the gap
closes. That is the `stonith_exploration.md` §7 rung.

A **process pause** between the last "am I still the leader" check and the start (GC,
SIGSTOP) is the classic lease hazard. **Incus has no fencing token to close it.** Its
`PUT /1.0/instances/<name>/state` handler (`cmd/incusd/instance_state.go:153`,
`instanceStatePut`, Incus 7.5.1) never calls `EtagCheck`, unlike `instance_put.go` and most
other PUTs, so a start cannot be made conditional on an `If-Match`/ETag. pagurus narrows the
window instead (`termState.start`):

1. **Prepare first.** `incus-compose up --no-start --recreate --no-healthd` pulls and creates
   this member's instances afresh, with nothing running. This is the slow part (~1 s with the
   image cached), and it comes after the guarded `may-run` write. Recreating every term is
   what makes a compose change (ports, labels, environment) or a newly pulled image reach the
   instance: a plain `up` patches only missing config keys, never devices. Workloads are
   stateless; named volumes survive a recreate.
2. **A fresh linearizable read** that our election key still holds the office, then the
   start. The window is now the launch call alone, not a whole `up`.
3. **Re-check after the start.** If the office moved, stop at once.
4. **Fast audit.** For three health intervals after a start, the new leader audits the other
   members on every tick, not every `audit_interval`.

In the sim, a leader paused in step 1 while it lost the office no longer starts at all
(`TestPausedStart`; before the change, it started a second copy). A pause *inside* the launch
call still lets a late copy start. The re-check stops it within milliseconds: in `TestPausedLaunchWindow`, 2 ms of overlap,
against 57 ms without the re-check (and up to `audit_interval`, 30 s, before conch).

## Timing

```
conch loss detection  D(ttl) = ttl/3 + max(1.5 s, ttl/6)                 = 10 s at ttl 20 s
D(ttl) + fence budget (self chain + 1 s) + 1 s  <  ttl     (config.Validate and conch refuse otherwise)
self-fence starts at  loss detection, at the latest ValidUntil − budget − 1 s
rival can acquire at  real lease expiry ≥ ValidUntil, then waits its --campaign-after
watchdog-fenced peer: new leader waits watchdog_timeout (+ relay freshness) + reboot_margin after acquiring
unverified (proceed): new leader waits unverified_wait after acquiring
```

With the prototype config (TTL 20 s, stop 4 s, kill 2 s, grace 10 s):

| Event | Time to the workload on a new member |
| :--- | :--- |
| planned `move` | stop + start, ~seconds |
| leader agent `systemctl stop` (graceful) | stop + the next member's `grace` + start |
| leader agent crash, host fine | ≤ TTL (20 s) + `grace` + API fence + start |
| leader host partitioned/powered off, `proceed` | ≤ TTL + `unverified_wait` (40 s) + start |
| same, `block` | until the member returns or `fence-ack` |
| preferred member blips back within `grace` | no failover |

Failback is never automatic. Use `pagurus move`.

## Provisioning

`pagurus provision -c cfg` prepares Incus for a service. Each member gets a cluster group
`host-<member>` holding exactly that member, and a project `<prefix>-<member>` pinned to it.
Each project is marked `user.pagurus.service=<service>`, so other tools can tell a pagurus
project apart (the configs repo's standby-sync skips them). Provision adds that key to an existing
project and drops the old, unread `user.pagurus.managed`. A project marked for another service
is refused.
- It plans before it acts. `--dry-run` prints the equivalent `incus` commands and changes
  nothing.
- It is idempotent: an already-right setup reports `[ok]` lines and does nothing.
- It only creates things, or fills an empty group. A group shared with other members, or a
  project pinned elsewhere, is reported and **left alone**.
- Cluster group changes are cluster-wide, so they need `--yes` or a "y" on a terminal.
  Creating projects doesn't.
- With a `replicate` block, it also creates each replicated volume in every member's project, on
  that member (`incus storage volume create <pool> <vol> --project <p> --target <m>`). An
  existing volume is left alone, and a missing pool is refused.

## Reaching Incus

pagurus talks to Incus through its Go client (`github.com/lxc/incus/v7/client`), not by running
the `incus` CLI. Every call carries the caller's deadline, so a hung daemon can delay a fence by
no more than the fence's budget. Only `incus-compose up` / `config` still shell out, because
incus-compose has no library API.

```jsonc
"incus": {}                                         // agents on members: the local unix socket (default)
"incus": {"socket": "/var/lib/incus/unix.socket"}   // a non-default socket
"incus": {                                          // an operator elsewhere (status, audit, move)
  "url": "https://10.0.1.10:8443",
  "client_cert": "~/.config/incus/client.crt", "client_key": "~/.config/incus/client.key",
  "server_cert": "~/.config/incus/servercerts/asus.crt"
}
```

**Least privilege** (verified on Incus 7.5.1, 2026-10-10). A TLS client certificate restricted to
the service's projects is enough to run pagurus:

```sh
incus config trust add-certificate pagurus.crt --restricted --projects <prefix>-<m1>,<prefix>-<m2>,<prefix>-<m3>
```

| Call | Restricted cert |
| :--- | :--- |
| `GET /1.0` (member name, version, architectures) | allowed |
| `GET /1.0/projects/<own>` · `/1.0/cluster/groups/<g>` · `/1.0/cluster/members/<m>` | allowed |
| list, start, stop, state and config of instances in its projects | allowed |
| `POST /1.0/projects`, `POST /1.0/cluster/groups` (`pagurus provision` creating or marking things) | **refused**: run `provision` with an admin certificate |

- 🚨 **List *every* member's project in the certificate.** For a project it may not read, a
  restricted client gets **an empty instance list, not an error**, even when instances run there
  (`GET /1.0/instances?project=X` → 200 OK, 0 items; `GET /1.0/projects/X` → 403).
- An empty list reads as "all stopped", which a peer fence would take as a verified fence. So the
  driver now reads the project first and fails if it can't
  (`TestIncusAPIStopNeverConfirmsAnUnreadableProject`). A misconfigured certificate gives errors
  and blocked failovers, not a second copy.
- Agents on members can keep using the local unix socket (full access).

## etcd security

pagurus talks plain, unauthenticated etcd by default. TLS and etcd's user/password auth are
optional fields under `etcd`:

```jsonc
"etcd": {
  "endpoints": ["https://10.0.1.3:2379"],
  "cacert": "/etc/pagurus/etcd-ca.pem",                                    // verify etcd's server cert
  "cert": "/etc/pagurus/etcd-client.pem", "key": "/etc/pagurus/etcd-client-key.pem",  // client cert
  "user": "pagurus", "password_file": "/run/secrets/pagurus-etcd"          // never the password itself
}
```

`cert`/`key` and `user`/`password_file` go in pairs. The same settings render as conch flags
(`Config.ConchEtcdArgs`: `--cacert --cert --key --user --password-file`, conch ≥ `bea278d`)
for a conch that pagurus spawns. The password travels as a file path, never on a command line.

## Starters

How a member brings its copy up is the configured **starter** (`internal/start`). The agent
itself only fences, records `may-run`, calls `Start`, then checks location and autostart.

```jsonc
"start": {"kind": "compose", "file": "/etc/pagurus/whoami/compose.yaml", "args": [], "up_args": ["--detach", "--no-healthd"]}
"start": {"kind": "instances", "names": ["whoami"]}   // empty names: every instance in the project
```

- **compose** runs `incus-compose -p <project> -f <file> up --detach --no-healthd`. The
  prototype's top-level `"compose"` / `"compose_args"` / `"up_args"` still work, mapped onto
  this, with a deprecation warning.
- **instances** runs `incus start` on containers that already exist in each member's pinned
  project (`examples/whoami-instances/`). The preflight checks each one exists, is a
  container, and has `boot.autostart=false`.

The **instances** starter also runs VMs. compose runs containers only.

## Updates and compose changes

Every term starts from a fresh instance: Prepare is `up --no-start --recreate`, from the compose
file and the image already in the member's project. So:

- **A compose change** (ports, labels, environment) reaches each member at its next term. Use
  `pagurus move` or `pagurus refresh` to apply it now.
- **An image update** is `pagurus refresh -c cfg`. The leader sees the new token at its next
  health tick. It pulls into its own project (on its own Incus, so the image matches its
  architecture), then recreates the workload in place, in the same term. Once the workload is
  healthy it writes `<prefix>/refresh-done`, and `refresh` returns. The followers then pull
  into their own projects, so a later failover starts from the same image.
- **A refresh that fails** is one where the workload stays unhealthy and the leader gives the
  office up, as for any unhealthy workload. The next leader starts from the image already in
  *its* project, which it never pulled, so a bad image rolls back by failover. Nobody
  acknowledges the refresh, and `refresh` exits non-zero ("the term ended before the refresh
  was healthy").
- A follower never recreates on a refresh. Never pull into a member's project from another
  member: Incus pulls for the architecture of the server it talks to.

## Replication (stateful services)

A service whose state is a local custom volume can have it follow the office. It's **opt-in**:
only a config with a `replicate` block replicates anything, and without one the agent makes no
volume call at all (`TestReplicaOffMeansNoVolumeCalls`). The design and its reasoning are in
the configs repo's `proposals/pagurus_stateful.md`. The Incus facts it rests on (an interrupted
refresh is atomic per snapshot; `--refresh` silently destroys a diverged target) are in
`tasks/results/pagurus-replica-spike.md`.

```json
"replicate": {"pool": "zfs-pool", "volumes": ["data"], "interval": "15m", "max_staleness": "2h",
              "keep": 4, "catchup_timeout": "10m",
              "pre_hook": {"instance": "app-1", "command": ["sqlite3", "/data/app.db", ".backup /data/snap.db"]}}
```

The volumes have the same names in every member's project; `pagurus provision` creates the missing
ones and never touches an existing one. The block is part of the config hash, so every member must
agree on it. A config without it hashes as before.

**Snapshots** are named `pagurus-<epoch>-<YYYYMMDDTHHMMSSmmmZ>`, by the term that wrote the data.
`-final` is appended when the term's workload was confirmed stopped first, so nothing can have been
written after it. Only names of exactly that form are pruned (to `keep`).

**The leader**, once its workload runs, snapshots every volume each `interval` (after the
pre-hook, if any) and publishes the round (`<prefix>/replica/source`, a guarded write).

**A follower** pulls the current leader's published snapshot with `incus storage volume copy
--refresh`. It reads the leader's epoch before and after, and a copy made while the office moved
is not published. It never pulls while a term runs on it or its own workload isn't confirmed
stopped (rule 1). A new term waits for an in-flight pull to end, up to `catchup_timeout`, and gives
up the office past that. It waits rather than cancels, because killing the `incus` client doesn't stop
the copy server-side.

**A planned move copies first and stops second.** When the leader sees `pagurus move`'s pin to
another member, it keeps serving and takes one more round at once. It skips its regular rounds and
waits, up to `catchup_timeout`, until the target has pulled that round. Only then does it release.
The target's backlog (an in-flight pull, a burst of writes) crosses while the workload still runs, and
the new term's final catch-up carries only what was written since that round. Live, a move during
a 3 GiB pull left nothing running for 104 s before this; the sim's gap dropped from 2.9 s to 0.7 s.
For a replicating service, `move`'s default `--timeout` grows to `catchup_timeout` + 2 m.

**A leader whose agent shuts down drains first.** A host reboot stops the agent, and the host's Incus
follows within seconds, before the next leader could reach it for the final state. So, after the
workload is confirmed stopped and before the office is released, the leader:
1. snapshots every volume `-final`;
2. publishes that as the round;
3. waits, up to 30 s (capped by `catchup_timeout`), until every follower with a live agent
   (`<prefix>/reg/`) has pulled it.

A new leader whose own copy is the previous holder's `-final` of its last epoch starts from it with
nothing lost and doesn't contact the old host. The returning host shares that `-final` with the new
source, so rule 4 finds nothing diverged. The agent must stop before the host's Incus does: the
NixOS unit and the rpi5 unit are ordered after `incus-startup.service`, whose stop shuts the instances
down and puts incusd into "shutting down" mode. Live on 2026-10-16, two kernel reboots without this
took the unreachable path, exposing 40 s and 3.5 min of writes.

A snapshot round covers all volumes or none: if one volume's snapshot fails, the others' are deleted
again. A follower whose pull completed but whose record didn't (a failed etcd write) publishes the
record on its next tick, without waiting for a new round.

**Rule 4: never destroy the loser's data.** Before a pull overwrites its volume, the member checks
whether the volume holds writes the source lacks. It does when any of these holds:
- it has a snapshot newer than their common snapshot that the source doesn't have;
- it led a term (`<prefix>/replica/led/<member>`) after the common snapshot;
- it took the common snapshot itself and that snapshot isn't `-final`.

If so, it copies the volume to `<vol>-orphaned-<epoch>-<UTC>` first, alerts (`orphaned-data`), and
only then resyncs. Orphans are never merged or pruned; a human decides.

**A new leader**, after fencing and before anything starts:
1. If no member has led with replication yet, or it led last itself, it starts from its own volumes.
2. Otherwise it tries to **catch up**. If the previous holder is Online in Incus and nothing runs
   in its project (it has just been fenced), it snapshots that holder's volumes `-final` and pulls
   them. **Nothing is lost**: a move, an agent crash, an unhealthy give-up. A volume whose pull
   fails is retried while the previous holder stays reachable, within `catchup_timeout`, so the
   volumes don't end up from different rounds.
3. If the previous holder is unreachable (host down, partition), it starts from its own newest
   copy, after two checks:
   - **The freshest copy leads.** If another member with a live agent (`<prefix>/reg/`) has
     published a newer copy, this member steps aside (giving the office up for
     (members + 1) × `grace`) so that member can take it. It does this once per previous term,
     so a fresher member that can't host doesn't keep the office empty.
   - **A consistent copy only.** "Its copy" is the newest round that every volume has. If the
     volumes' own newest snapshots differ (a pull or catch-up that stopped part-way), the copy is
     mixed and the member doesn't start.

   The **staleness gate** applies: if that copy is older than `max_staleness`, is mixed, or there
   is none, the member doesn't start. It alerts (`replica-stale`) and gives the office up with the
   release backoff. `pagurus accept-stale -c cfg [--member m]` lets one term start anyway, and
   that member skips its backoff. The loss is then everything after its copy; the writes stay on
   the previous holder and become its orphan when it returns. The override covers one term: a
   term of that member (or any member's, without `--member`) that starts without needing it,
   say because the previous holder came back and was caught up from, clears it too.

Not built from the proposal: the rollback of a new leader's volume to its newest snapshot. A
half-applied copy can't exist (the spike), and the rollback would destroy that member's own
uncopied writes. Not built yet: a byte counter for pulls (neither the CLI nor the API reports
bytes).

**Multi-volume consistency has a limit.** Each volume is refreshed separately, and a refresh
carries the source's live head. So a follower's volumes match to the round, but their heads can
differ by the seconds between two pulls. An application that needs its volumes from one instant
(say, a database split over two volumes) should keep its state on one volume.

`pagurus status` shows, for a replicating service:
- the last published round;
- a pending `accept-stale`;
- per member: the last epoch it led, its last pull, its newest snapshot's age, and its orphans.

**Bandwidth.** A pull is a ZFS send from the leader's member. Uncapped, it fills the link that etcd
shares, so every member that can lead needs an egress cap toward the others. That is outside
pagurus (the configs repo's `standby-sync-shaper` on asus is the model).

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

Measured on asus (Alpine VM): confirmed in 117 ms, and Incus then reports the VM `STOPPED`
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
| `systemd` | the unit's `WatchdogSec=` fires. Default: systemd kills the agent and restarts it, and the restarted agent kills anything still running locally (startup fence). With `FailureAction=reboot-force` the host reboots; the agent reads its own unit's `FailureAction` to decide what it advertises. | only with a reboot `FailureAction` |
| `device` | opens `watchdog.device` (e.g. `/dev/watchdog1`, unused on asus/dell; `/dev/watchdog0` is systemd's `RuntimeWatchdogSec`) **only with `arm: true`**, else behaves as dry-run. Magic-close disarm only on a clean exit with nothing running. | yes, when armed |

In every mode except `off`, conch's pets come through the daemon's relay (§ Process shape):
the backend is petted only while they are fresh, so the advertised timeout is the backend's
plus half of it.

**The prototype runs `systemd` mode without a `FailureAction`: no machine is ever rebooted.**

## Metrics

`pagurus run --metrics-addr :9478` serves Prometheus metrics on `/metrics` (off by default).
Every series carries `svc` and `member` labels; no label has unbounded values.

| Metric | Type | Meaning |
| :--- | :--- | :--- |
| `pagurus_role` | gauge | 1 while this member holds the office and runs its term |
| `pagurus_conch_running` | gauge | 1 while this member's `conch elect` runs |
| `pagurus_local_running` | gauge | containers of this member's project with live processes, **from cgroupfs** every health interval: each member's own ground truth, independent of the office |
| `pagurus_fit` | gauge | 1 while the agent pets its watchdog backend |
| `pagurus_lease_remaining_seconds` | gauge | server-side TTL left on the holder's lease (0 when not holding) |
| `pagurus_fence_blocked` | gauge | 1 while this leader waits on a member it cannot fence (`block`) |
| `pagurus_self_chain_verified` | gauge | 1 when the self chain can confirm a stop for this project's instance types |
| `pagurus_terms_total` | counter | terms held |
| `pagurus_fence_runs_total{chain,kind,result}` | counter | fences run by the agent (self: `confirmed\|failed\|skipped`; peer: `confirmed\|assumed\|wait\|skipped`). `pagurus fence-self` runs inside conch and isn't counted; its failures show as `pagurus_conch_exits_total{result="fence_failed"}` |
| `pagurus_conch_exits_total{result}` | counter | `ok`, `fence_failed`, `withdrawn` (stopped while campaigning, e.g. by a move), `other` |
| `pagurus_replica_age_seconds` | gauge | replicate only: age of this member's newest snapshot |
| `pagurus_replica_pulls_total{result}` | counter | replicate only: pulls into this member, `ok\|failed\|discarded` (the office moved during it) |
| `pagurus_replica_snapshots_total{result}` | counter | replicate only: the leader's snapshot rounds, `ok\|failed` |
| `pagurus_orphaned_volumes` | gauge | replicate only: `<vol>-orphaned-*` volumes on this member (rule 4) |
| `pagurus_replica_stale_blocked` | gauge | replicate only: 1 while this member's last term refused to start on a stale copy |

`deploy/grafana/pagurus-rules.yaml` holds Grafana rules for the four conditions that need a
human, and two for replicating services: `PagurusReplicaStale` (a member's copy is over an hour old,
for 10 min) and `PagurusOrphanedData` (rule 4 preserved something). None fires on a normal failover or move:
- **two running:** `count by (svc) (pagurus_local_running > 0) > 1`, at once;
- **fence blocked:** for 2 min;
- **unfit:** per member, for 1 min;
- **nobody running:** while ≥ 2 agents report, for 3 min.

`TestMetricsShowTheFourConditions` drives each condition in the sim.

## Layout

```
cmd/pagurus/            CLI: run | validate | status | move | fence-ack | audit | …;
                        fence-self and term are run by conch
cmd/blackhole-proxy/    drill-only TCP proxy; SIGUSR1 = partition the agent from etcd
internal/config/        schema, defaults, the timing validation (conch's rule)
internal/coord/         etcd schema: the office (conch's election keys), member states, registrations, pin
internal/incus/         incus / incus-compose / cgroupfs driver
internal/fence/         fence kinds (incl. qemu-kill), the chain runner, fencetest (fakes + contract)
internal/start/         starters: compose, instances
internal/watchdog/      off | dry-run | systemd (sd_notify) | device (ioctl)
internal/metrics/       Prometheus text exposition (no client library)
deploy/grafana/         alert rules, to move into the fleet's Grafana provisioning
internal/agent/         agent.go: the daemon (ground truth, campaigning); conch.go: conch,
                        the watchdog relay, the term socket; lead.go: a term
examples/whoami/        a generic compose example (placeholder addresses, 192.0.2.0/24)
examples/whoami-instances/  the same with the instances starter
scripts/drill.sh        live drills against pagurus@<svc> units, refereed by `audit`
internal/provision/     `pagurus provision`: per-member cluster groups + pinned projects (+ replicated volumes)
internal/replica/       replication's pure rules: snapshot names, pruning, rule 4 (divergence)
internal/agent/replica.go  replication: the leader's rounds, the follower's pull, the takeover
```

## Running it

```fish
go test ./...                              # needs an `etcd` binary on PATH; ~6 min (~13 under -race). The sim
                                           # builds the conch pinned in go.mod (a tool dep)
pagurus status -c <cfg>                    # on a member: office, records, instances
pagurus refresh -c <cfg>                   # leader pulls, recreates in place; waits until healthy
scripts/drill.sh move                      # live drills, audited (see below); SVC=, CTL= to adapt
```

`pagurus audit -c <cfg> --for 10m` is the independent referee during drills. It polls every
member's project through the cluster API once a second. It reports, and exits non-zero on, any
instant where instances run in more than one project.

## Results (2026-10-10, under conch)

**Simulated:** all 13 original scenarios plus 5 later ones (fence chain, cgroup layout, config
skew) run against a **real conch** (`0609155`, then `1834068`) for every member, plus a relay
test. `go test -race ./... -count=2`: ok. `RestartWithinTTL` now recovers on a new term with no
overlap instead of resuming the old one.

**Live** (`scripts/drill.sh`, audited every 500 ms, **0 violations in 400 polls**; run 1 before the
conch fix: 0 in 447):

| Drill | Outcome |
| :--- | :--- |
| `move` asus→dell→rpi5→asus | 9.1 s, 10.0 s, 11.1 s |
| `graceful` | dell serving 17 s after the stop (incl. its 10 s `--campaign-after`) |
| `restart` (SIGKILL the agent) | a new term on asus 22 s later (no resume: the dead lease expires first) |
| `crash` (SIGKILL, kept down) | dell serving 36 s later, old copy stopped via the Incus API |
| `pause` (SIGSTOP) | systemd's watchdog killed the frozen unit; a new term on asus at 50 s |
| `partition` | **self-fenced at 8 s** (conch: loss detection 10 s after the last good renewal); dell serving at 34 s |

**Found:** a `conch elect` stopped while still campaigning died from SIGTERM's default action
and left its candidate key for a whole TTL. That slowed a `move` to 26 s. Fixed in conch
`1834068`.

## Results (2026-10-06)

**Simulated** (`go test ./...`): 13 scenarios against a real etcd, with a simulated 3-member
Incus cluster that fails the test on any instant with two members running. Graceful handover,
crash fenced via the API, restart-resume inside the TTL, partitioned leader under `proceed`,
partitioned leader under `block`, `fence-ack`, self-fence failure under `block`, self-fence
failure with an armed watchdog resetting the host, planned move, unhealthy leader giving up, a
stray copy on a follower, and follower partitions. Run 4× in all: **0 violations**.

**Live** (`scripts/drill.sh`, audited every 500 ms, **0 violations in 352 polls**):

| Drill | Outcome |
| :--- | :--- |
| `move` asus→dell→rpi5→asus | ~11 s per hop, ~3.5 s with nothing running; arm64 pull on rpi5 worked |
| `graceful` (`systemctl stop` on the leader) | new leader healthy after 16 s (incl. the 10 s preference grace) |
| `resume` (SIGKILL, systemd restarts within 3 s) | term resumed, no failover |
| `crash` (SIGKILL, agent kept down, workload left running) | new leader stopped the orphan via the Incus API, healthy after 27 s |
| `pause` (SIGSTOP 45 s) | systemd's watchdog aborted the frozen agent at 28 s. Dell won, API-fenced asus, and was healthy at 38 s. Asus's restarted agent found its lease gone and came back as a follower. |
| `partition` (agent cut off etcd) | **self-fenced at 9 s** (as computed: 20 − 2 − 9). New leader healthy at 26 s. |

**Found while doing it:**
- incus-compose sets the project's `user.healthcheck.enabled=true` even with `--no-healthd`.
  The guard now checks the instances' restart policy instead.
- rpi5's `/run` is `noexec`.
- A `TimeToLive`-based expiry probe reads 0 up to 1 s early.

## What would have to be true to promote it

- A decision on `fence.unverified` (`block` vs `proceed`) per service, and whether to arm a
  reboot watchdog (`FailureAction=reboot-force`). The latter needs its own drill on `dell` first
  (`stonith_exploration.md` §7.4).
- A NixOS module with a real unit: `Type=notify`, `WatchdogSec`, `Restart=always`, and `--self`
  from the host name. For rpi5 (Debian), a deploy path that is not hand-copying
  (`warm_standby_dell.md` §4 records what happens to hand-copied scripts).
- Routing. Clients today must read `<prefix>/endpoint`, and nothing re-points Caddy. A label
  template or a VIP would have to follow the office.
- `go test -race` on an amd64 host (impossible on rpi5, see the conch notes), and a soak run.
