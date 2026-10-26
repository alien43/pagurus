# Replication (stateful services)


A service whose state is a local custom volume can have it follow the office. It's **opt-in**:
only a config with a `replicate` block replicates anything, and without one the agent makes no
volume call at all (`TestReplicaOffMeansNoVolumeCalls`). Two Incus facts it rests on were established by experiment before the code: an
interrupted refresh is atomic per snapshot, and `--refresh` silently destroys a diverged target.

```json
"replicate": {"pool": "zfs-pool", "volumes": ["data"], "interval": "15m", "max_staleness": "2h",
              "keep": 4, "catchup_timeout": "10m",
              "pre_hook": {"instance": "app-1", "command": ["sqlite3", "/data/app.db", ".backup /data/snap.db"]}}
```

The volumes have the same names in every member's project; `pagurus provision` creates the missing
ones and never touches an existing one. The block is part of the config hash, so every member must
agree on it. A config without it hashes as before.

**Snapshots** are named `pagurus-<epoch>-<YYYYMMDDTHHMMSSmmmZ>`, by the term that wrote the data.
`-final` is appended when the term's workload was confirmed stopped first, so nothing can have been
written after it. Only names of exactly that form are pruned (to `keep`).

**The leader**, once its workload runs, snapshots every volume each `interval` (after the
pre-hook, if any) and publishes the round (`<prefix>/replica/source`, a guarded write).

**A follower** pulls the current leader's published snapshot with `incus storage volume copy
--refresh`. It reads the leader's epoch before and after, and a copy made while the office moved
is not published. It never pulls while a term runs on it or its own workload isn't confirmed
stopped (rule 1). A new term waits for an in-flight pull to end, up to `catchup_timeout`, and gives
up the office past that. It waits rather than cancels, because killing the `incus` client doesn't stop
the copy server-side.

**A planned move copies first and stops second.** When the leader sees `pagurus move`'s pin to
another member, it keeps serving and takes one more round at once. It skips its regular rounds and
waits, up to `catchup_timeout`, until the target has pulled that round. Only then does it release.
The target's backlog (an in-flight pull, a burst of writes) crosses while the workload still runs, and
the new term's final catch-up carries only what was written since that round. Live, a move during
a 3 GiB pull left nothing running for 104 s before this; the sim's gap dropped from 2.9 s to 0.7 s.
For a replicating service, `move`'s default `--timeout` grows to `catchup_timeout` + 2 m.

**A leader whose agent shuts down drains first.** A host reboot stops the agent, and the host's Incus
follows within seconds, before the next leader could reach it for the final state. So, after the
workload is confirmed stopped and before the office is released, the leader:
1. snapshots every volume `-final`;
2. publishes that as the round;
3. waits, up to 30 s (capped by `catchup_timeout`), until every follower with a live agent
   (`<prefix>/reg/`) has pulled it.

A new leader whose own copy is the previous holder's `-final` of its last epoch starts from it with
nothing lost and doesn't contact the old host. The returning host shares that `-final` with the new
source, so rule 4 finds nothing diverged. The agent must stop before the host's Incus does: the
agent's unit must be ordered after `incus-startup.service`, whose stop shuts the instances
down and puts incusd into "shutting down" mode. Live on 2026-10-16, two kernel reboots without this
took the unreachable path, exposing 40 s and 3.5 min of writes.

A snapshot round covers all volumes or none: if one volume's snapshot fails, the others' are deleted
again. A follower whose pull completed but whose record didn't (a failed etcd write) publishes the
record on its next tick, without waiting for a new round.

**Rule 4: never destroy the loser's data.** Before a pull overwrites its volume, the member checks
whether the volume holds writes the source lacks. It does when any of these holds:
- it has a snapshot newer than their common snapshot that the source doesn't have;
- it led a term (`<prefix>/replica/led/<member>`) after the common snapshot;
- it took the common snapshot itself and that snapshot isn't `-final`.

If so, it copies the volume to `<vol>-orphaned-<epoch>-<UTC>` first, alerts (`orphaned-data`), and
only then resyncs. Orphans are never merged or pruned; a human decides.

**A new leader**, after fencing and before anything starts:
1. If no member has led with replication yet, or it led last itself, it starts from its own volumes.
2. Otherwise it tries to **catch up**. If the previous holder is Online in Incus and nothing runs
   in its project (it has just been fenced), it snapshots that holder's volumes `-final` and pulls
   them. **Nothing is lost**: a move, an agent crash, an unhealthy give-up. A volume whose pull
   fails is retried while the previous holder stays reachable, within `catchup_timeout`, so the
   volumes don't end up from different rounds.
3. If the previous holder is unreachable (host down, partition), it starts from its own newest
   copy, after two checks:
   - **The freshest copy leads.** If another member with a live agent (`<prefix>/reg/`) has
     published a newer copy, this member steps aside (giving the office up for
     (members + 1) × `grace`) so that member can take it. It does this once per previous term,
     so a fresher member that can't host doesn't keep the office empty.
   - **A consistent copy only.** "Its copy" is the newest round that every volume has. If the
     volumes' own newest snapshots differ (a pull or catch-up that stopped part-way), the copy is
     mixed and the member doesn't start.

   The **staleness gate** applies: if that copy is older than `max_staleness`, is mixed, or there
   is none, the member doesn't start. It alerts (`replica-stale`) and gives the office up with the
   release backoff. `pagurus accept-stale -c cfg [--member m]` lets one term start anyway, and
   that member skips its backoff. The loss is then everything after its copy; the writes stay on
   the previous holder and become its orphan when it returns. The override covers one term: a
   term of that member (or any member's, without `--member`) that starts without needing it,
   say because the previous holder came back and was caught up from, clears it too.

Not built from the proposal: the rollback of a new leader's volume to its newest snapshot. A
half-applied copy can't exist (the spike), and the rollback would destroy that member's own
uncopied writes. Not built yet: a byte counter for pulls (neither the CLI nor the API reports
bytes).

**Multi-volume consistency has a limit.** Each volume is refreshed separately, and a refresh
carries the source's live head. So a follower's volumes match to the round, but their heads can
differ by the seconds between two pulls. An application that needs its volumes from one instant
(say, a database split over two volumes) should keep its state on one volume.

`pagurus status` shows, for a replicating service:
- the last published round;
- a pending `accept-stale`;
- per member: the last epoch it led, its last pull, its newest snapshot's age, and its orphans.

## Instances

A service started by the instances starter can replicate whole instances: a VM whose state lives on
its own root disk (Home Assistant OS partitions it itself) can't move its data to a custom volume.

```json
"start": {"kind": "instances", "names": ["vm"]},
"replicate": {"instances": ["vm"], "interval": "2m", "max_staleness": "30m"},
"fences": {"self": [{"kind": "incus-stop", "budget": "4s"}, {"kind": "qemu-kill", "budget": "3s"}]}
```

An instance is one more unit next to the volumes, under every rule above. A round is an instance
snapshot (disk only, `pagurus-<epoch>-<UTC>`; Incus's own scheduled snapshots are neither counted
nor pruned, though they travel with a copy). A pull is `incus copy --refresh --stateless`, with
`boot.autostart=false` pinned on the copy. A rule 4 orphan is a stopped instance
`<name>-orphaned-<epoch>-<UTC>`, so the name is capped at 20 characters. The starter never starts it:
every replicated instance must be in `start.names`, and `compose` is refused (it recreates its
instances). The instance must exist in every member's project: make it in one (stopped,
`boot.autostart=false`) and `pagurus provision` copies it to the others.

**The MAC.** A plain `incus copy` gives the copy a new `volatile.eth0.hwaddr`; a `--refresh` carries
the source's (Incus 7.5.1, measured). So from the first pull on, every member's copy answers on the
leader's MAC (and, with DHCP by MAC, its address). Before the first pull, a provisioned copy has
its own: pin `hwaddr` on the NIC device if even that first window matters.

Measured on a 512 MiB Alpine VM, asus ↔ dell: first copy 7 s; a handover round 7–8 s; the `-final`
catch-up ~10 s; a planned move about 25 s end to end, the guest's marker file intact.

**Bandwidth.** A pull is a ZFS send from the leader's member. Uncapped, it fills the link that etcd
shares, so every member that can lead needs an egress cap toward the others. That is outside
pagurus.
