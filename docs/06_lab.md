# The lab

`lab/up.sh` builds a throwaway three-member cluster on **one** Incus host and starts pagurus on it
with the `whoami` example; `lab/drill.sh` runs the failure drills against it; `lab/down.sh` removes
everything. It is the quickest way to watch a failover, and CI runs it on every push
(`.github/workflows/lab.yml`).

```sh
lab/up.sh       # build pagurus + conch, three members, Incus cluster, etcd cluster, whoami served
lab/drill.sh    # move graceful restart crash pause, refereed by `pagurus audit`; exit 0 = all met, 0 violations
lab/down.sh
```

## What it builds

- A bridge `pglbr0` on `10.190.7.0/24` (members at `.11`–`.13`). **Pick a subnet that is free on
  your host and in your LAN** and pass it as `LAB_SUBNET=10.x.y` (the first three octets). The
  workload bridge inside each member is `WORK_SUBNET` (default `10.191.7`).
- Three system containers `pgl-1..3` (Ubuntu 24.04) with Incus from the Zabbly packages, etcd
  (`ETCD_VERSION`), incus-compose (`INCUS_COMPOSE_VERSION`), and pagurus and conch built from the
  checkout (or `PAGURUS_BIN` / `CONCH_BIN`).
- An etcd cluster and an Incus cluster (dir storage, one `incusbr0` per member) across them.
- `pagurus provision`, then one `pagurus@whoami` systemd unit per member.

The members are **privileged, AppArmor-unconfined containers with nesting** so their Incus can run
containers of its own (confined, the nested Incus loses its own database socket once it starts one). That is a lab shortcut, not a production layout: run the lab on a disposable machine, or let CI
run it.

## The drills

| Drill | Fault | Expected |
|---|---|---|
| `move` | `pagurus move` to each member in turn | the office and the workload follow, one copy at a time |
| `graceful` | stop the leader's agent | it stops its copy; a standby takes over |
| `restart` | SIGKILL the leader's agent; systemd restarts it | its startup fence stops the orphan; the office returns after the lease |
| `crash` | SIGKILL and keep it down | a peer fences the copy through the Incus API, then takes over |
| `pause` | SIGSTOP the leader's agent | systemd's watchdog kills it; a standby takes over |

`partition` (cutting a member off etcd and the Incus cluster with nftables) exists in
`scripts/drill.sh` but is not part of the lab's default set.
