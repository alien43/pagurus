# Changelog

## v0.1.3

- `pagurus-term`: conch's child for a term is now its own binary, linking only the standard
  library (`internal/termshim`). One runs per leading service, and as `pagurus term` each paid
  for the big binary's package init()s (grpc, etcd client, zap, incus): ~3.3 MB of anon per
  process. Install it next to `pagurus` (the release ships `pagurus-term-linux-<arch>`). The
  agent runs it from the directory of its own executable; when it is missing, the agent logs a
  warning ("falling back") and runs `pagurus term`, which stays as an alias. Behaviour and exit
  codes are unchanged.
## v0.1.2

- Whole-instance replication: `replicate.instances` replicates instances (root disk, config and
  devices, a VM's included) under the same rules as volumes (rounds, catch-up, staleness gate,
  orphans, `keep`). Only with `start.kind=instances`, each one in `start.names`. `pagurus
  provision` seeds a member that lacks one with a stopped copy. A volumes-only config hashes as
  before. See docs/02_replication.md § Instances.
- `provision` no longer refuses a plan whose replicated instance exists nowhere yet: it creates the
  projects and prints a `[todo]`.
- `stop_timeout` (default 5 s): how long a planned release (move, unhealthy give-up, agent
  shutdown) lets the workload shut down cleanly before forcing it. It used to be a fixed 5 s, a
  power cut for a VM guest. The agent's systemd `TimeoutStopSec` must exceed it plus the 30 s drain.
- The instances starter retries a busy start every 2 s within the start's bound. A follower pulling
  a replicated instance holds the source busy, and a restart in place used to fail and give the
  office up.
- Fix: a term that started while the follow loop was deciding to withdraw was ended at once ("not
  eligible: local instances not confirmed stopped" right after "won the office"; 12 times in 7
  days on one cluster). A withdrawal now never signals conch once a term has begun.

## v0.1.1

- A stalled or dead backend API is no longer a sick workload. A health check that Incus does not
  answer (timeout, connection refused) is `backend.ErrAPIUnavailable`: the leader holds its state
  (no failure counted, no restart in place) for up to 3 min, then counts it as unhealthy. Before,
  a 90 s Incus cluster-database stall restarted healthy workloads in place.
- The term heartbeat beats before each bounded backend call with that call's own bound. Before,
  one 65 s budget covered a whole health iteration, which a snapshot round alone could exceed, so
  the same stall starved the watchdog (a forced reboot on a reboot-armed member). The peer-fence
  loop's budget is now the peer chain's own, and a failed snapshot round's cleanup is bounded.
- Metrics `pagurus_api_unavailable_seconds`, `pagurus_api_unavailable_total`,
  `pagurus_heartbeat_withheld_total`; the agent logs when it withholds the heartbeat.

## v0.1.0

First public release.

- One agent per Incus cluster member (`pagurus run`, under systemd) keeps one copy of a service
  running on at most one member: per-member projects pinned to single-member cluster groups, the
  office a [conch](https://github.com/alien43/conch) election on an etcd lease.
- Fencing: self-fence before the lease expires (stop, then `cgroup.kill`), startup fence, peer
  fence through the Incus API with `block` / `proceed` policies for an unverifiable member,
  `fence-ack` for the operator; watchdog modes `systemd` and `device`.
- Starters: `compose` (incus-compose) and `instances` (existing containers or VMs).
- Opt-in volume replication for stateful services (`replicate`), with staleness bounds and
  `accept-stale`.
- CLI: `run`, `validate`, `check-config`, `status`, `move`, `refresh`, `fence-ack`,
  `accept-stale`, `audit`, `fence-check`, `fence-run`, `provision`.
- Prometheus metrics; a three-member lab (`lab/`) and live drills (`scripts/drill.sh`) refereed by
  `pagurus audit`.
- Static linux amd64/arm64 binaries.
