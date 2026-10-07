# `pagurus` — at-most-one incus-compose stack, failing over across the cluster

A hermit crab lives in one shell at a time and moves when it has to. `pagurus` keeps a workload
running on exactly one member of an Incus cluster, and moves it when that member fails.

> **Naming.** The project was prototyped as `singleton`. The prototype that still runs live keeps
> its old names: transient units `singleton-<svc>`, binaries in `/run/singleton-proto` and
> `/var/lib/singleton-proto`, and etcd keys under `/singleton/v1/` (pinned in
> `examples/whoami/pagurus.json`). `scripts/` drive that prototype, so they use those names too.
> New code and defaults use `pagurus` (`/pagurus/v1/<svc>`, `/run/pagurus`).

> **Status: EXPERIMENTAL PROTOTYPE (2026-10-06).** Not wired into any NixOS module, timer or
> `fleet/` stack. When it runs on the live cluster it does so only as *transient* systemd units
> started by `scripts/proto-up.sh`. On asus and dell the binaries sit on tmpfs
> (`/run/singleton-proto`). On rpi5, whose `/run` is `noexec`, they sit in
> `/var/lib/singleton-proto`. A reboot stops the agents, and `scripts/proto-down.sh --purge`
> removes every trace. Promotion out of `experimental/` is a task, not a commit
> (`experimental/README.md`).

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
            etcd (asus-infra, dell-infra, rpi-infra) — /singleton/v1/<svc>/
                 leader (leased; create-rev = epoch)   ms/<member> may-run|stopped
                 reg/<member> (leased)   endpoint (leased)   pin
                    ▲                ▲                 ▲
          ┌─────────┴──┐     ┌───────┴────┐     ┌──────┴─────┐
          │ agent@asus │     │ agent@dell │     │ agent@rpi5 │   one Go binary per host,
          └─────┬──────┘     └─────┬──────┘     └─────┬──────┘   root, systemd watchdog
                │ incus-compose up / incus stop / cgroup.kill
          project sgl-whoami-asus  sgl-whoami-dell  sgl-whoami-rpi5
          (restricted.cluster.groups = host-asus / host-dell / host-rpi5)
```

## Why per-member projects

A clustered Incus instance has exactly one home (`incus_fleet.md` §2). So the stack does not
move between members. Each member has its own copy of the stack, in its own project pinned to
its own single-member cluster group. Fencing member M then means just "nothing in project
`<prefix>-M` runs". `incus-compose -p <prefix>-M up` is how M runs the stack. The agent refuses
to start, and refuses to campaign, unless the project is pinned to exactly its member. It also
re-checks every instance's `location` after every `up`.

## The safety argument

**Invariant:** at no instant does more than one member's project have a running instance.

1. **One office.** Holding the office means owning `<prefix>/leader`, which is attached to a
   lease. It is created with a compare-and-swap on `CreateRevision == 0`. Its create revision is
   the **epoch**. Every leader write is a transaction guarded on "the leader key still has my
   epoch and my lease". That covers recording `may-run`, recording a fence, and publishing the
   endpoint. So a deposed leader cannot authorise anything.

2. **The leader stops itself before its lease can expire.** `internal/lease` tracks
   `ValidUntil`: the send time of the newest renewal that succeeded, plus the TTL, on this
   host's monotonic clock. etcd extends the lease from the moment it *receives* the renewal, so
   `ValidUntil` can never be later than the real expiry. etcd leader changes only extend leases.
   `TestValidityBoundNeverExceedsServerExpiry` measures this against a real etcd: the bound came
   12–22 ms before the attached key vanished. The **guardian** goroutine starts the self-fence at
   `ValidUntil − margin − (stop_budget + kill_budget)`. It does this itself, so a main loop stuck
   in a slow call cannot delay it. The steps are:
   - `incus stop --force` on the local project, within `stop_budget`;
   - then **`cgroup.kill`** on `lxc.payload.<project>_*`, within `kill_budget`. This needs no
     Incus daemon and no cluster quorum, which matters because a partitioned member's Incus has
     lost the dqlite quorum and cannot stop anything;
   - success is confirmed from **cgroupfs** (`cgroup.events: populated 0`), not from Incus.

   No rival can create the leader key before the lease is gone server-side. So a successful
   self-fence always finishes before any rival can start.

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

5. **A wedged or unfit agent is not trusted.** The watchdog is fed only while `Fit()` holds:
   - **leading:** before the safe deadline, or after it only once the local stop is confirmed;
   - **not leading:** while cgroupfs shows nothing running here;
   - **always:** the main loop has made progress recently.

   This is `sbd`'s idea, using the conch-style lease as the quorum signal.

### The residual gap, stated plainly

With `fence.unverified=proceed` and no reboot-armed watchdog, a violation needs **all** of
these together:
- the leader's host is cut off from **both** etcd and the Incus cluster;
- **and** its agent is dead or wedged, or both `incus stop` and `cgroup.kill` fail;
- **and** the host stays up.

Under `block`, that case leaves the service down instead. With an armed watchdog
(`FailureAction=reboot-force`, or `device` mode with `arm: true`), the host resets and the gap
closes. That is the `stonith_exploration.md` §7 rung.

A **process pause** between a lease check and an `incus-compose up` (GC, SIGSTOP) is the
classic lease hazard. Incus has no fencing-token check to close it. It is narrowed, not closed:
the authorising guarded write comes immediately before `up`, the deposition watch stops the
copy, and the new leader's API fence and audit stop it within seconds.

## Timing

```
renew_interval ×3 + stop_budget + kill_budget + margin  ≤  ttl      (config.Validate refuses otherwise)
self-fence starts at  ValidUntil − margin − (stop+kill)              ≈ last good renewal + 9 s  (prod defaults)
rival can acquire at  real lease expiry ≥ ValidUntil                ≈ last good renewal + 20 s
watchdog-fenced peer: new leader waits watchdog_timeout + reboot_margin after acquiring
unverified (proceed): new leader waits unverified_wait (≥ stop+kill+margin) after acquiring
```

With the prototype config (TTL 20 s, renew 2 s, margin 2 s, stop 6 s, kill 3 s):

| Event | Time to the workload on a new member |
| :--- | :--- |
| planned `move` | stop + start, ~seconds |
| leader agent `systemctl stop` (graceful) | stop + start, ~seconds |
| leader agent crash, host fine | ≤ TTL (20 s) + API fence + start |
| leader host partitioned/powered off, `proceed` | ≤ TTL + `unverified_wait` (40 s) + start |
| same, `block` | until the member returns or `fence-ack` |
| preferred member blips back within `grace` | no failover |

Failback is never automatic. Use `pagurus move`.

## Watchdog

| `watchdog.mode` | What a missed pet does | Advertised as reboot fence |
| :--- | :--- | :--- |
| `off` | nothing | no |
| `dry-run` *(default)* | logs `NOT FIT — withholding pet` | no |
| `systemd` | the unit's `WatchdogSec=` fires. Default: systemd kills the agent and restarts it, and the restarted agent kills anything still running locally (startup fence). With `FailureAction=reboot-force` the host reboots; the agent reads its own unit's `FailureAction` to decide what it advertises. | only with a reboot `FailureAction` |
| `device` | opens `watchdog.device` (e.g. `/dev/watchdog1`, unused on asus/dell; `/dev/watchdog0` is systemd's `RuntimeWatchdogSec`) **only with `arm: true`**, else behaves as dry-run. Magic-close disarm only on a clean exit with nothing running. | yes, when armed |

**The prototype runs `systemd` mode without a `FailureAction`: no machine is ever rebooted.**

## Layout

```
cmd/pagurus/            CLI: run | validate | status | move | fence-ack | audit
cmd/blackhole-proxy/    drill-only TCP proxy; SIGUSR1 = partition the agent from etcd
internal/config/        schema, defaults, the timing validation
internal/lease/         lease keeper with the local validity bound
internal/coord/         etcd schema: leader/epoch, member states, registrations, pin
internal/incus/         incus / incus-compose / cgroupfs driver
internal/watchdog/      off | dry-run | systemd (sd_notify) | device (ioctl)
internal/agent/         the state machine (agent.go: lifecycle; lead.go: a term)
examples/whoami/        the prototype's compose.yaml + pagurus.json
scripts/                provision.sh, proto-up.sh, proto-down.sh, drill.sh
```

## Running it

```fish
go test ./...                              # needs an `etcd` binary on PATH; ~2 min
scripts/proto-up.sh --drill-proxy          # live prototype, from rpi5 (build + copy + transient units)
/var/lib/singleton-proto/singleton status -c /run/singleton-proto/whoami/singleton.json
scripts/drill.sh                           # live drills, audited (see below)
scripts/proto-down.sh --purge              # remove everything
```

`pagurus audit -c <cfg> --for 10m` is the independent referee during drills. It polls every
member's project through the cluster API once a second. It reports, and exits non-zero on, any
instant where instances run in more than one project.

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
