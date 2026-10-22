# Operations

## Provisioning

`pagurus provision -c cfg` prepares Incus for a service. Each member gets a cluster group
`host-<member>` holding exactly that member, and a project `<prefix>-<member>` pinned to it.
Each project is marked `user.pagurus.service=<service>`, so other tools can tell a pagurus
project apart. Provision adds that key to an existing
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
  "url": "https://192.0.2.10:8443",
  "client_cert": "~/.config/incus/client.crt", "client_key": "~/.config/incus/client.key",
  "server_cert": "~/.config/incus/servercerts/node1.crt"
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
  "endpoints": ["https://192.0.2.1:2379"],
  "cacert": "/etc/pagurus/etcd-ca.pem",                                    // verify etcd's server cert
  "cert": "/etc/pagurus/etcd-client.pem", "key": "/etc/pagurus/etcd-client-key.pem",  // client cert
  "user": "pagurus", "password_file": "/run/secrets/pagurus-etcd"          // never the password itself
}
```

`cert`/`key` and `user`/`password_file` go in pairs. The same settings render as conch flags
(`Config.ConchEtcdArgs`: `--cacert --cert --key --user --password-file`, conch ≥ `bea278d`)
for a conch that pagurus spawns. The password travels as a file path, never on a command line.

## Starters

How a member brings its copy up is the configured **starter** (`internal/backend/incus/start`). The agent
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

## Day to day

```sh
pagurus status -c cfg                     # the office, every member's record and instances
pagurus move -c cfg --to <member>         # planned move; returns once the target serves (or why it gave up)
pagurus refresh -c cfg                    # pull a new image and recreate in place on the leader
pagurus audit -c cfg --for 10m            # independent referee: exits non-zero on any overlap
pagurus fence-check -c cfg --member <m>   # read-only: what each self-fence step would do (run on <m>)
pagurus validate -c cfg                   # config, this member's project, the compose file
```

- **Maintenance on a member.** `pagurus move` the service off it, then stop its agent
  (`systemctl stop pagurus@<svc>`): a stopped agent stops its local copy and does not campaign.
  Start it again afterwards; the office stays where it is (no fail-back).
- **A member that is down for good, under `fence.unverified=block`.** Failover waits until the
  member is verified down. Once you have checked it is (powered off, or its Incus is gone),
  `pagurus fence-ack -c cfg --member <m>` records that and the next leader proceeds.
- **Replicated services** may refuse to start from a copy older than `replicate.max_staleness`;
  `pagurus accept-stale -c cfg` lets the next term start from it, once (`02_replication.md`).
- **Upgrading pagurus.** Replace the binary and restart the agents one member at a time, standbys
  first, then `move` the office off the leader and restart that one. A new binary that cannot
  load the config exits 78 (`EX_CONFIG`), which a unit like `lab/up.sh`'s (`RestartPreventExitStatus=78`) does not restart (`pagurus check-config`
  runs the same load with no network, for a pre-deploy check). Agents whose safety-relevant config
  differs refuse to campaign rather than run with mismatched fences.
- **Alerts.** `deploy/grafana/pagurus-rules.yaml` is a Grafana alert-rule provisioning file over the metrics above: two copies running, nobody running, and the replication conditions. Set its `datasourceUid` and folder to yours.
