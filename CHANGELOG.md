# Changelog

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
