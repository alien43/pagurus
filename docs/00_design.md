# Design: the safety argument and timing

## What it does

One agent runs on each cluster member (node1, node2, node3). Between them they keep **one**
incus-compose stack running on **at most one** member at any instant, and on **some** member
whenever at least two of the three are up. The stack has to be stateless and multi-arch. The
prototype runs `traefik/whoami`.

It is the stateless failover tier of an Incus cluster without shared storage: one copy runs, the
others stand by, and a dead member's copy is fenced before another starts. It moves no bytes unless
a service opts into replication (`02_replication.md`).

```
   etcd (etcd1, etcd2, etcd3)
     /conch/v1/elect/pagurus/v2/<svc>/<lease>   the office: a conch election (create-rev = epoch)
     /pagurus/v2/<svc>/  leader (who holds it)   ms/<member> may-run|stopped
                         reg/<member>   endpoint   pin
                    ▲                  ▲                  ▲
   ┌────────────────┴─┐    ┌───────────┴──────┐    ┌──────┴───────────┐
   │ pagurus@node1     │    │ pagurus@node2     │    │ pagurus@node3     │  one unit per service and host;
   │  └ conch elect   │    │  └ conch elect   │    │  └ conch elect   │  pagurus is main, the only
   │     └ pagurus    │    │     (campaigning)│    │     (campaigning)│  petter of systemd's watchdog
   │       term       │    │                  │    │                  │
   └────────┬─────────┘    └──────────────────┘    └──────────────────┘
            │ incus-compose up / incus stop / cgroup.kill
   project whoami-node1  whoami-node2  whoami-node3
   (restricted.cluster.groups = host-node1 / host-node2 / host-node3)
```

## Process shape

One systemd unit per service and host runs the pagurus daemon (the unit's main process). The
daemon keeps the local ground truth, and while this member is eligible it runs
`conch elect <office> --fence 'pagurus fence-self …' --watchdog --campaign-after <rank × grace>
-- pagurus term`. conch ([`docs/03_elect.md`](https://github.com/alien43/conch/blob/main/docs/03_elect.md)) owns the election, the lease, the lease's
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

A clustered Incus instance has exactly one home (its cluster member). So the stack does not
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
   (conch [`docs/03_elect.md`](https://github.com/alien43/conch/blob/main/docs/03_elect.md) § Fencing). conch tracks `ValidUntil`: the send time of the newest renewal
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

   This chain covers zombies too. A member fenced only by assumption is recorded `stopped`,
   marked unverified. That holds until it runs a new term, so a later term in the same outage
   doesn't wait it out again.

   **Last look.** Before starting, the leader looks again at every member no fence saw stopped,
   with 4 × `api_timeout`. If the cluster reports the member offline, that's the case the fences
   above handle. If it's online, it has to answer. A running copy is stopped through the API
   (`fence-missed`). Until the member answers, runs nothing or goes offline, the term waits
   (`fence-blocked` on the second try). This covers an incusd too slow for the fence's budget:
   a killed agent and conch, the workload still running, and `proceed` trusting a self-fence
   that never ran (mqtt-lab 2026-10-18).

4. **Nothing else starts it.** The agent enforces `boot.autostart=false` after every `up`.
   (Unset means "restore last state on boot", so a rebooting host would bring a second copy
   back.) No instance may carry a `user.healthcheck.restart` other than `no`, so ic-healthd never
   restarts a fenced instance. (incus-compose sets the *project's* `user.healthcheck.enabled=true`
   even with `--no-healthd`, so that flag is not the thing to check.) A follower checks cgroupfs every `eligible_interval` and stops anything it
   finds. The leader audits the other members' projects every `audit_interval`.

5. **A wedged or unfit agent is not trusted.** systemd's watchdog is fed only while:
   - **conch runs:** conch's own pet is fresh. conch is fit while not holding, while holding
     with the fence's start deadline ahead and the term's heartbeat fresh, while a fence runs
     within its budget, and never after a fence failed (conch [`docs/03_elect.md`](https://github.com/alien43/conch/blob/main/docs/03_elect.md) § Watchdog);
   - **conch doesn't run:** cgroupfs shows nothing of ours running here (or briefly, while a
     stray copy or a dead term's remains are being stopped).

   This is `sbd`'s idea, using the conch lease as the quorum signal.

   The term's heartbeat holds while the term keeps a progress deadline: before each call it may
   block on, the term sets the deadline to that call's own bound plus 30 s. A call that waits on
   a stalled backend until its timeout is progress; one stuck past its bound is a wedge, and
   starves the watchdog (§ Backend stalls).

### The residual gap, stated plainly

With `fence.unverified=proceed` and no reboot-armed watchdog, a violation needs **all** of
these together:
- the leader's host is cut off from **both** etcd and the Incus cluster;
- **and** its agent is dead or wedged, or both `incus stop` and `cgroup.kill` fail;
- **and** the host stays up.

Under `block`, that case leaves the service down instead. With an armed watchdog
(`FailureAction=reboot-force`, or `device` mode with `arm: true`), the host resets and the gap
closes (`03_fencing.md`, Watchdog).

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

## Backend stalls

The Incus cluster database can stall every API call for 90 s (seen four times in three days on
one cluster's database leader, during instance starts). A leader's agent then neither learns
nor acts. Two rules keep it from making things worse:

- **Unavailable is not unhealthy.** The Incus driver marks a read that Incus did not answer (the
  caller's deadline passed, or the socket refused or dropped the connection) as
  `backend.ErrAPIUnavailable`. A health check that gets it counts no failure and resets none,
  restarts nothing and gives nothing up: the term holds its state. Each episode is logged once at
  its start and once at its end, with `pagurus_api_unavailable_seconds` and
  `pagurus_api_unavailable_total`. Anything Incus *answered*, an instance not Running or an HTTP
  check that fails, counts as before.
- **The hold is bounded.** After 3 min (`APIUnavailableMax`, twice the observed stall) of
  unavailable checks, they count as unhealthy, so a dead incusd on the leader ends the term the
  usual way (restart in place, then give the office up), a few minutes late. The bound does not
  touch fencing: the lease is etcd's, and conch's self-fence runs on its loss whatever the Incus
  API does. Its first fence (`incus-stop`) may then time out on the stalled API; `cgroup.kill`
  needs no Incus (`TestIncusStallEtcdLostSelfFencesInTime`).
- **The heartbeat outlives the stall.** A supervise iteration beats before each bounded call
  (health check, each audited member, a snapshot round, a refresh pull, a start) with that call's
  bound, so the gap between beats is one call, never the sum. With a 15 s API timeout and a 5 s
  health interval the longest gap in a 90 s stall is one snapshot round (`interval + 2 ×
  api_timeout`, then 15 s per volume for its cleanup), against a budget of that plus 30 s.
  Before, one budget of `interval + 2 × api_timeout + 30 s` = 65 s covered the whole iteration
  while a snapshot round alone could take 90 s (a 1 min `replicate.interval`): the stall starved
  the watchdog at +65 s, 10 s from a forced reboot on a reboot-armed member.

`TestIncusStall90sHoldsTheTerm`, `TestIncusStall200sHoldsUntilTheBound` and
`TestIncusDeadEndsTheTermAfterTheBound` drive these in the sim at 1/15 of live timing.

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

## Architecture

The core (`internal/agent`, with `coord`, `replica`, `watchdog`, `metrics`, `config` and the
chain runner in `fence`) knows the lease, office, epochs, `prefer`, restart budget, release
backoff, the config-skew guard and replication's rules. It reaches the workload only through
seams named for what they mean, all wired once in `cmd/pagurus` (`agent.Backend`):

| Seam | Means | Implemented by (`internal/backend/incus/…`) |
|---|---|---|
| `Site` | a member's place to run the workload: check, list, stop, no autostart, address | `*incus.Driver`: a project pinned to the member |
| `Starter` | bring the member's copy up: check, prepare, pull, start | `start.Compose` (incus-compose), `start.Instances` |
| `Observer` | does the workload run on this host, without the daemon or quorum | `*incus.Driver`: cgroupfs `lxc.payload.<project>_*` |
| `Membership` | this host's member name; is member *m* online | `*incus.Driver`: Incus cluster member state |
| `Fences` | the member's self and peer fence chains | `fence.Configured` over `*incus.Driver` (kinds below) |
| `Replicator` | snapshot, ship, keep aside, prepare a volume for first use | `*incus.Driver`: custom volumes on a ZFS pool |

`internal/backend` holds the one neutral type, an instance as a site reports it. The fence kinds
stay in `internal/fence`: `config` validates a chain by building it, and none of them imports the
Incus client (`cgroup-kill` and `qemu-kill` work without incusd by design). The provisioner
(`pagurus provision`) is CLI-only, so it has no seam. `go list -deps ./internal/agent` lists
nothing from `lxc/incus` or `internal/backend/incus`.

## Layout

```
cmd/pagurus/            CLI: run | validate | status | move | fence-ack | audit | …;
                        fence-self and term are run by conch
cmd/blackhole-proxy/    drill-only TCP proxy; SIGUSR1 = partition the agent from etcd
internal/config/        schema, defaults, the timing validation (conch's rule)
internal/coord/         etcd schema: the office (conch's election keys), member states, registrations, pin
internal/backend/        backend-neutral types (an instance)
internal/backend/incus/ incus / incus-compose / cgroupfs driver
internal/fence/         fence kinds (incl. qemu-kill), the chain runner, fencetest (fakes + contract)
internal/backend/incus/start/      starters: compose, instances
internal/watchdog/      off | dry-run | systemd (sd_notify) | device (ioctl)
internal/metrics/       Prometheus text exposition (no client library)
deploy/grafana/         alert rules, to move into the fleet's Grafana provisioning
internal/agent/         agent.go: the daemon (ground truth, campaigning); conch.go: conch,
                        the watchdog relay, the term socket; lead.go: a term
examples/whoami/        a generic compose example (placeholder addresses, 192.0.2.0/24)
examples/whoami-instances/  the same with the instances starter
scripts/drill.sh        live drills against pagurus@<svc> units, refereed by `audit`
internal/backend/incus/provision/  `pagurus provision`: per-member cluster groups + pinned projects (+ replicated volumes)
internal/replica/       replication's pure rules: snapshot names, pruning, rule 4 (divergence)
internal/agent/replica.go  replication: the leader's rounds, the follower's pull, the takeover
```
