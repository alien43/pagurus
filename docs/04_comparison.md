# How it compares

pagurus does one narrow thing: **at most one** running copy of a container workload across an
Incus cluster without shared storage, with the old copy fenced before a new one starts. The tools
below overlap with parts of that. Claims about them are about their default, documented behaviour;
check your version.

| | What fails over | Storage it needs | Stops the old copy before starting a new one |
|---|---|---|---|
| **pagurus** | one container stack per service | none (each member keeps its own copy; optional volume replication) | yes: self-fence before the lease expires, peer fence through the Incus API, optional watchdog reboot |
| **Incus cluster healing** (`cluster.healing_threshold`) | instances of an offline member, evacuated elsewhere | shared (Ceph, LINSTOR, …): an instance has one home and moves only with its storage | relies on the member being offline; no fencing of a member that is only cut off |
| **Proxmox VE HA** | VMs and containers of the Proxmox cluster | shared or replicated (ZFS replication) storage | yes: watchdog self-fence on quorum loss |
| **Pacemaker + Corosync** | any resource with an agent | whatever the resource needs | yes: STONITH, which it requires for safety |
| **keepalived (VRRP)** | a virtual IP | none | no: a partition can leave two masters |
| **Nomad** | job allocations, rescheduled on lost nodes | per job | only if configured: allocations on a disconnected client keep running unless the job sets how long they may (`disconnect` / `stop_after_client_disconnect`) |

## When to use something else

- **You have shared storage.** Incus cluster healing moves the instance itself; you don't need a
  copy per member.
- **You run Proxmox.** Its HA stack does this for its own guests, with the same fencing idea.
- **The resource isn't an Incus container**, or several resources must move together in order.
  Pacemaker is the general tool, at the cost of its complexity and a real STONITH device.
- **You only need an address to move**, and two holders for a moment is harmless: keepalived.
- **You want a scheduler** for many jobs across many nodes: Nomad or Kubernetes, with their own
  answers to partitions.

## What pagurus adds

- Each member's copy is pinned to that member (a project per member, pinned to a single-member
  cluster group), so "fence member M" means "nothing in M's project runs" — checkable through the
  cluster API from any member, which `pagurus audit` does once a second.
- The election, lease and fencing deadline come from [conch](https://github.com/alien43/conch), a
  small etcd lease tool with its own tests; pagurus owns the Incus side.
- It needs nothing beyond the Incus cluster you already run and a three-node etcd.
