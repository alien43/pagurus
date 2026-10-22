# pagurus — one copy of a service, failing over across an Incus cluster

A hermit crab lives in one shell at a time and moves when it has to. `pagurus` keeps a container
workload (an incus-compose stack, or plain Incus containers) running on **at most one** member of an
Incus cluster at any instant, and on **some** member whenever a quorum is up. When the member
running it dies, hangs or is cut off, the copy there is fenced before another member starts its own.

It is for clusters **without shared storage**, where Incus' own cluster healing cannot move an
instance off a dead member. Each member keeps its own copy of the stack in its own project;
pagurus decides which one may run.

```
                 etcd (3 nodes)      the office: a conch election on a lease
                 ▲        ▲        ▲
   ┌─────────────┴─┐ ┌────┴──────┐ ┌┴──────────────┐
   │ pagurus@node1 │ │ @node2    │ │ @node3        │   one agent per member, under systemd's watchdog
   │  leads: runs  │ │ standby   │ │ standby       │
   └──────┬────────┘ └───────────┘ └───────────────┘
          │ incus-compose up / incus stop / cgroup.kill
   project whoami-node1   whoami-node2   whoami-node3   (each pinned to its own member)
```

The leader holds a lease in etcd ([conch](https://github.com/alien43/conch) runs the election).
Before its lease can expire it stops its copy; if its agent is gone, peers stop that copy through the
Incus API, and an optional watchdog reboots a host whose agent wedges. The safety argument, and the
gap it leaves, are in [`docs/00_design.md`](docs/00_design.md).

**What it assumes** — read these before relying on it:
- at most one member impaired at a time, and a working etcd quorum;
- containers only for the self-fence's last resort (`cgroup.kill`); VMs only with the `instances`
  starter and an Incus-API fence;
- stateless workloads, or the opt-in volume replication in [`docs/02_replication.md`](docs/02_replication.md),
  which can lose the writes since the last sync;
- **a residual gap**: with no reboot-armed watchdog and `fence.unverified=proceed`, a leader cut off
  from both etcd and Incus whose agent is also dead, on a host that stays up, can overlap a new
  leader. `docs/00_design.md` § The residual gap states it exactly.

## Requirements

- An Incus cluster of ≥ 3 members (or 2 members plus an etcd member elsewhere as a witness), cgroup2,
  systemd.
- etcd v3, three nodes.
- [conch](https://github.com/alien43/conch) on `PATH` (the version in `go.mod`), and
  [incus-compose](https://github.com/lxc/incus-compose) for the compose starter.

## Try it

[`lab/`](docs/06_lab.md) builds a throwaway three-member cluster on one Incus host and runs the
failure drills against it, refereed by `pagurus audit`:

```sh
lab/up.sh && lab/drill.sh    # ~10 min; lab/down.sh removes it
```

The same lab runs in CI on every push (`.github/workflows/lab.yml`).

## Docs

| | |
|---|---|
| [`00_design.md`](docs/00_design.md) | process shape, the safety argument, the residual gap, timing, architecture |
| [`01_operations.md`](docs/01_operations.md) | provisioning, reaching Incus and etcd, starters, updates, metrics, the CLI |
| [`02_replication.md`](docs/02_replication.md) | stateful services: replicated volumes and their limits |
| [`03_fencing.md`](docs/03_fencing.md) | fence policies, VM and peer fences, watchdog modes |
| [`04_comparison.md`](docs/04_comparison.md) | vs Incus cluster healing, Proxmox HA, Pacemaker, keepalived, Nomad |
| [`05_results.md`](docs/05_results.md) | drill results from the author's cluster |
| [`06_lab.md`](docs/06_lab.md) | the lab |

## Status

In production on the author's three-member cluster since October 2026 (stateless and replicated
services). Releases: [GitHub releases](https://github.com/alien43/pagurus/releases) (static
linux amd64/arm64). Interfaces may still change before 1.0; see [`CHANGELOG.md`](CHANGELOG.md).

License: see [`LICENSE`](LICENSE). Security reports: [`SECURITY.md`](SECURITY.md).
