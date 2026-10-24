# Changelog

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
