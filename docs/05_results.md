# Results from production and drills

## Results (2026-10-10, under conch)

**Simulated:** all 13 original scenarios plus 5 later ones (fence chain, cgroup layout, config
skew) run against a **real conch** (`0609155`, then `1834068`) for every member, plus a relay
test. `go test -race ./... -count=2`: ok. `RestartWithinTTL` now recovers on a new term with no
overlap instead of resuming the old one.

**Live** (`scripts/drill.sh`, audited every 500 ms, **0 violations in 400 polls**; run 1 before the
conch fix: 0 in 447):

| Drill | Outcome |
| :--- | :--- |
| `move` node1→node2→node3→node1 | 9.1 s, 10.0 s, 11.1 s |
| `graceful` | node2 serving 17 s after the stop (incl. its 10 s `--campaign-after`) |
| `restart` (SIGKILL the agent) | a new term on node1 22 s later (no resume: the dead lease expires first) |
| `crash` (SIGKILL, kept down) | node2 serving 36 s later, old copy stopped via the Incus API |
| `pause` (SIGSTOP) | systemd's watchdog killed the frozen unit; a new term on node1 at 50 s |
| `partition` | **self-fenced at 8 s** (conch: loss detection 10 s after the last good renewal); node2 serving at 34 s |

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
| `move` node1→node2→node3→node1 | ~11 s per hop, ~3.5 s with nothing running; arm64 pull on node3 worked |
| `graceful` (`systemctl stop` on the leader) | new leader healthy after 16 s (incl. the 10 s preference grace) |
| `resume` (SIGKILL, systemd restarts within 3 s) | term resumed, no failover |
| `crash` (SIGKILL, agent kept down, workload left running) | new leader stopped the orphan via the Incus API, healthy after 27 s |
| `pause` (SIGSTOP 45 s) | systemd's watchdog aborted the frozen agent at 28 s. Node2 won, API-fenced node1, and was healthy at 38 s. Node1's restarted agent found its lease gone and came back as a follower. |
| `partition` (agent cut off etcd) | **self-fenced at 9 s** (as computed: 20 − 2 − 9). New leader healthy at 26 s. |

**Found while doing it:**
- incus-compose sets the project's `user.healthcheck.enabled=true` even with `--no-healthd`.
  The guard now checks the instances' restart policy instead.
- node3's `/run` is `noexec`.
- A `TimeToLive`-based expiry probe reads 0 up to 1 s early.
