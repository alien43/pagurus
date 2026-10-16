package agent

// Replication of a stateful service's volumes (README "Replication",
// proposals/pagurus_stateful.md). Everything here runs only for a service
// with a replicate block.
//
//   - The leader, inside its term, snapshots every volume each interval and
//     publishes the snapshot (a guarded write).
//   - A follower pulls the current leader's snapshots (`copy --refresh`),
//     never while it is in a term or its own workload may run (rule 1), and
//     first preserves anything of its own the pull would destroy (rule 4).
//   - A new leader, before it starts anything, pulls the previous holder's
//     final state if it can reach it (nothing lost), or else starts from its
//     own newest copy if that is fresh enough (the staleness gate).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/incus"
	"github.com/alien43/pagurus/internal/replica"
)

// Volumes is the storage side of replication. *incus.Driver implements it;
// tests substitute the simulated cluster. Volumes are custom volumes on the
// configured pool, in a member's project, on that member.
type Volumes interface {
	// VolumeSnapshots lists the snapshot names of vol in member's project.
	VolumeSnapshots(ctx context.Context, member, vol string) ([]string, error)
	SnapshotVolume(ctx context.Context, member, vol, snap string) error
	DeleteVolumeSnapshot(ctx context.Context, member, vol, snap string) error
	// RefreshVolume pulls vol from from's project into to's (`incus storage
	// volume copy --refresh`, run where `to` is): the target ends with
	// exactly the source's snapshots and state, whatever it had before.
	RefreshVolume(ctx context.Context, from, to, vol string) error
	// CopyVolume copies vol to a new volume dst, both in member's project.
	CopyVolume(ctx context.Context, member, vol, dst string) error
	// ListVolumes lists the custom volumes in member's project.
	ListVolumes(ctx context.Context, member string) ([]string, error)
	// Exec runs argv in an instance of member's project.
	Exec(ctx context.Context, member, instance string, argv []string) error
}

var errStale = errors.New("replica too stale to start")

var _ Volumes = (*incus.Driver)(nil)

func (a *Agent) replicating() bool { return a.cfg.Replicate != nil }

// preflightReplica: the runtime can replicate and every volume exists here.
func (a *Agent) preflightReplica(ctx context.Context) error {
	if !a.replicating() {
		return nil
	}
	v, ok := a.rt.(Volumes)
	if !ok {
		return errors.New("replicate is set but this runtime has no volume support")
	}
	a.vols = v
	have, err := v.ListVolumes(ctx, a.member)
	if err != nil {
		return fmt.Errorf("replicate: listing volumes: %w", err)
	}
	for _, vol := range a.cfg.Replicate.Volumes {
		if !slices.Contains(have, vol) {
			return fmt.Errorf("replicate: volume %s is missing from project %s on pool %s (run `pagurus provision`)",
				vol, a.cfg.Project(a.member), a.cfg.Replicate.Pool)
		}
	}
	return nil
}

// ------------------------------------------------------------ rule 1 lock -

// beginPull reserves this member's volumes for a pull. It refuses while a
// term runs here or our workload is not confirmed stopped; a term that
// begins later waits for the pull to end (waitPulls).
func (a *Agent) beginPull(ctx context.Context) (context.Context, func(), bool) {
	if !a.pullMu.TryLock() {
		return nil, nil, false
	}
	a.mu.Lock()
	ok := !a.termActive && a.localStopped
	a.mu.Unlock()
	if !ok {
		a.pullMu.Unlock()
		return nil, nil, false
	}
	return ctx, a.pullMu.Unlock, true
}

// waitPulls waits until a follower pull in flight has ended, so nothing
// writes into our volumes once the term goes on. The term is already
// active, so no new pull begins. It waits rather than cancels: killing the
// incus client does not stop the copy server-side (the replica spike), so a
// cancelled pull could go on receiving into a volume the workload is about
// to use. A refresh lands whole or not at all, and fails quickly when its
// source is gone, so waiting is safe and usually short.
func (a *Agent) waitPulls(ctx context.Context, within time.Duration) error {
	deadline := a.now().Add(within)
	for !a.pullMu.TryLock() {
		if a.now().After(deadline) {
			return errors.New("a pull into this member's volumes did not stop")
		}
		if !sleep(ctx, 20*time.Millisecond) {
			return ctx.Err()
		}
	}
	a.pullMu.Unlock()
	return nil
}

// ---------------------------------------------------------------- follower -

// followReplica keeps this member's volumes a recent copy of the leader's.
func (a *Agent) followReplica(ctx context.Context) {
	if !a.replicating() {
		return
	}
	for sleep(ctx, a.cfg.EligibleInterval.D()) {
		a.replicaMetrics(ctx)
		a.pullOnce(ctx)
	}
}

// replicaMetrics: the age of this member's newest copy, and its orphans.
func (a *Agent) replicaMetrics(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, a.cfg.Fence.APITimeout.D())
	defer cancel()
	if age, ok := a.replicaAge(cctx); ok {
		a.m.SetGauge("pagurus_replica_age_seconds", age.Seconds())
	}
	if vs, err := a.vols.ListVolumes(cctx, a.member); err == nil {
		n := 0
		for _, v := range vs {
			if strings.Contains(v, "-orphaned-") {
				n++
			}
		}
		a.m.SetGauge("pagurus_orphaned_volumes", float64(n))
	}
}

// replicaAge is the age of the stalest volume's newest pagurus snapshot.
func (a *Agent) replicaAge(ctx context.Context) (time.Duration, bool) {
	var oldest time.Time
	for _, vol := range a.cfg.Replicate.Volumes {
		names, err := a.vols.VolumeSnapshots(ctx, a.member, vol)
		if err != nil {
			return 0, false
		}
		n, ok := replica.Newest(names)
		if !ok {
			return 0, false
		}
		if oldest.IsZero() || n.At.Before(oldest) {
			oldest = n.At
		}
	}
	return a.now().Sub(oldest), true
}

// pullOnce pulls the current leader's newest published snapshot, if this
// member does not have it yet.
func (a *Agent) pullOnce(ctx context.Context) {
	src, err := a.co.ReplicaSource(ctx)
	if err != nil || src == nil || src.Member == a.member {
		return
	}
	rec, epoch, _, _, err := a.co.Leader(ctx)
	if err != nil || rec == nil || rec.Member != src.Member || epoch != src.Epoch {
		return // only the current holder's own publish is a source
	}
	pctx, done, ok := a.beginPull(ctx)
	if !ok {
		return
	}
	defer done()
	log := a.log.With("member", a.member, "source", src.Member, "epoch", epoch, "snapshot", src.Snapshot)
	led, err := a.co.ReplicaLed(pctx)
	if err != nil {
		return
	}
	mine, err := a.co.ReplicaCopy(pctx, a.member)
	if err != nil {
		return
	}
	if mine == nil {
		mine = &coord.ReplicaCopy{}
	}
	pulled := false
	for _, vol := range a.cfg.Replicate.Volumes {
		local, err := a.vols.VolumeSnapshots(pctx, a.member, vol)
		if err != nil {
			return
		}
		if slices.Contains(local, src.Snapshot) {
			continue
		}
		source, err := a.vols.VolumeSnapshots(pctx, src.Member, vol)
		if err != nil {
			return
		}
		if !slices.Contains(source, src.Snapshot) {
			return // pruned or not there yet: the next round brings another
		}
		if !a.preserve(pctx, vol, local, source, led[a.member], mine, log) {
			return
		}
		cctx, cancel := context.WithTimeout(pctx, a.cfg.Replicate.CatchupTimeout.D()+a.cfg.Replicate.Interval.D())
		err = a.vols.RefreshVolume(cctx, src.Member, a.member, vol)
		cancel()
		if err != nil {
			a.m.Inc("pagurus_replica_pulls_total", "result", "failed")
			log.Warn("pull failed; the volume stays at its last complete snapshot", "volume", vol, "err", err)
			return
		}
		pulled = true
	}
	if !pulled {
		return
	}
	// Rule 1: a copy made while the office moved is not this member's
	// replica state. ZFS kept it whole; it is simply not published, and the
	// next leader's snapshots supersede it.
	_, after, _, _, err := a.co.Leader(ctx)
	if err != nil || after != epoch {
		a.m.Inc("pagurus_replica_pulls_total", "result", "discarded")
		log.Warn("the office moved during the pull; not publishing it", "epoch_after", after, "err", err)
		return
	}
	mine.Snapshot, mine.SourceEpoch, mine.At = src.Snapshot, epoch, a.now().UTC()
	if err := a.co.PutReplicaCopy(ctx, a.member, *mine); err != nil {
		log.Warn("cannot record the pull", "err", err)
		return
	}
	a.m.Inc("pagurus_replica_pulls_total", "result", "ok")
	if s, ok := replica.Parse(src.Snapshot); ok {
		a.m.SetGauge("pagurus_replica_age_seconds", a.now().Sub(s.At).Seconds())
	}
	log.Debug("pulled")
}

// preserve applies rule 4 to this member's vol before a pull from source
// overwrites it: if it holds writes the source lacks, copy it aside as
// <vol>-orphaned-<epoch>-<UTC>, record that, and alert. It returns false when
// the pull must not go ahead.
func (a *Agent) preserve(ctx context.Context, vol string, local, source []string, led int64, mine *coord.ReplicaCopy, log *slog.Logger) bool {
	d := replica.Diverged(local, source, led)
	if !d.Diverged {
		return true
	}
	key := vol + " " + d.Key
	if mine.Orphaned[vol] == key {
		return true // preserved already; the last pull failed after it
	}
	dst := fmt.Sprintf("%s-orphaned-%d-%s", vol, led, a.now().UTC().Format("20060102T150405Z"))
	if err := a.vols.CopyVolume(ctx, a.member, vol, dst); err != nil {
		log.Error("rule 4: cannot preserve diverged data; NOT pulling over it", "volume", vol, "why", d.Why, "err", err)
		return false
	}
	if mine.Orphaned == nil {
		mine.Orphaned = map[string]string{}
	}
	mine.Orphaned[vol] = key
	if err := a.co.PutReplicaCopy(ctx, a.member, *mine); err != nil {
		log.Warn("rule 4: preserved, but could not record it (it may be preserved again)", "err", err)
	}
	a.alerter.Alert("orphaned-data", "this member's volume held writes the leader never had; preserved them as a separate volume before resyncing. A human decides whether anything in it matters",
		"member", a.member, "volume", vol, "orphan", dst, "why", d.Why, "led_epoch", led, "base", d.Base)
	a.m.SetGauge("pagurus_orphaned_volumes", a.m.Get("pagurus_orphaned_volumes")+1)
	return true
}

// ---------------------------------------------------------------- takeover -

// replicaTakeover runs once per term, after the other members are fenced and
// before anything starts. It returns false when the term must not start (the
// reason is set for the release).
func (t *termState) replicaTakeover() bool {
	a := t.a
	if !a.replicating() {
		return true
	}
	r := a.cfg.Replicate
	a.beat(2*r.CatchupTimeout.D() + time.Minute)
	if err := a.waitPulls(t.op, r.CatchupTimeout.D()); err != nil {
		t.giveUp("replicate: "+err.Error(), a.cfg.EligibleInterval.D())
		return false
	}
	led, err := a.co.ReplicaLed(t.op)
	if err != nil {
		t.giveUp("replicate: reading replica/led: "+err.Error(), 0)
		return false
	}
	prev, prevEpoch := "", int64(0)
	for m, e := range led {
		if e > prevEpoch {
			prev, prevEpoch = m, e
		}
	}
	switch {
	case prev == "":
		t.log.Info("replicate: no member has led with replication yet; starting from this member's volumes")
	case prev == a.member:
		t.log.Info("replicate: this member led last; starting from its own volumes", "led_epoch", prevEpoch)
	default:
		err := t.catchUp(prev, prevEpoch, led[a.member])
		if err == nil {
			break
		}
		if t.op.Err() != nil {
			return false
		}
		t.log.Warn("replicate: cannot pull the previous holder's final state; checking this member's own copy", "previous", prev, "err", err)
		if !t.staleGate(prev) {
			return false
		}
	}
	a.m.Bool("pagurus_replica_stale_blocked", false)
	// An accept-stale is for one term: this one, if it named this member,
	// whether or not the gate needed it. Left pending, it would pass a later,
	// unrelated failover's gate without an operator's word.
	if as, err := a.co.GetAcceptStale(t.op); err == nil && as != nil && (as.Member == "" || as.Member == a.member) {
		if err := a.co.ConsumeAcceptStale(t.op, t.epoch, t.lease, as.ModRev); err == nil {
			t.log.Info("replicate: a pending accept-stale was not needed; cleared (it covers one term)", "by", as.By)
		}
	}
	if err := a.co.Guarded(t.op, t.epoch, t.lease, a.co.OpPutReplicaLed(a.member, t.epoch)); err != nil {
		t.log.Warn("replicate: cannot record this term in replica/led; not starting", "err", err)
		return false
	}
	return true
}

// catchUp pulls the previous holder's state as of its fence: snapshot its
// stopped volumes ("-final": nothing can be written after it) and refresh
// ours from them. Nothing is lost.
func (t *termState) catchUp(prev string, prevEpoch, myLed int64) error {
	a := t.a
	ctx, cancel := context.WithTimeout(t.op, a.cfg.Replicate.CatchupTimeout.D())
	defer cancel()
	if on, err := a.rt.MemberOnline(ctx, prev); err != nil || !on {
		return fmt.Errorf("%s is not reachable (online=%v, err=%v)", prev, on, err)
	}
	insts, err := a.rt.List(ctx, prev)
	if err != nil {
		return err
	}
	if r := incus.Running(insts); len(r) > 0 {
		return fmt.Errorf("%s still runs %v", prev, r)
	}
	snap := replica.Name(prevEpoch, a.now(), true)
	for _, vol := range a.cfg.Replicate.Volumes {
		if err := a.vols.SnapshotVolume(ctx, prev, vol, snap); err != nil {
			return fmt.Errorf("snapshot %s on %s: %w", vol, prev, err)
		}
	}
	if err := t.authorised(ctx); err != nil {
		return err
	}
	mine, err := a.co.ReplicaCopy(ctx, a.member)
	if err != nil {
		return err
	}
	if mine == nil {
		mine = &coord.ReplicaCopy{}
	}
	for _, vol := range a.cfg.Replicate.Volumes {
		local, err := a.vols.VolumeSnapshots(ctx, a.member, vol)
		if err != nil {
			return err
		}
		source, err := a.vols.VolumeSnapshots(ctx, prev, vol)
		if err != nil {
			return err
		}
		if !a.preserve(ctx, vol, local, source, myLed, mine, t.log) {
			return errors.New("cannot preserve this member's own diverged data")
		}
		if err := a.vols.RefreshVolume(ctx, prev, a.member, vol); err != nil {
			a.m.Inc("pagurus_replica_pulls_total", "result", "failed")
			return fmt.Errorf("pull %s from %s: %w", vol, prev, err)
		}
	}
	if err := t.authorised(ctx); err != nil {
		return err
	}
	a.m.Inc("pagurus_replica_pulls_total", "result", "ok")
	a.m.SetGauge("pagurus_replica_age_seconds", 0)
	t.log.Info("replicate: pulled the previous holder's final state; nothing lost", "previous", prev, "snapshot", snap)
	return nil
}

// staleGate: start from this member's own newest copy only if it is no
// older than max_staleness, or an operator accepted a stale start.
func (t *termState) staleGate(prev string) bool {
	a := t.a
	limit := a.cfg.Replicate.MaxStaleness.D()
	age, ok := a.replicaAge(t.op)
	if ok && age <= limit {
		t.log.Warn("replicate: starting from this member's newest copy; writes after it on the previous holder are lost here (rule 4 keeps them there)",
			"previous", prev, "age", age.Round(time.Millisecond))
		return true
	}
	what := "no complete copy"
	if ok {
		what = "newest copy is " + age.Round(time.Second).String() + " old"
	}
	if as, err := a.co.GetAcceptStale(t.op); err == nil && as != nil && (as.Member == "" || as.Member == a.member) {
		if err := a.co.ConsumeAcceptStale(t.op, t.epoch, t.lease, as.ModRev); err == nil {
			a.alerter.Alert("accept-stale", "starting past the staleness gate on an operator's word", "member", a.member, "by", as.By, "copy", what)
			return true
		}
	}
	a.m.Bool("pagurus_replica_stale_blocked", true)
	backoff, streak := a.releaseBackoff(false)
	a.alerter.Alert("replica-stale", fmt.Sprintf("not starting: %s (max_staleness %s) and the previous holder %s is unreachable; "+
		"run `pagurus accept-stale` to start from it anyway", what, limit, prev), "member", a.member, "backoff", backoff, "failed_terms", streak)
	t.giveUp(fmt.Sprintf("%v: %s", errStale, what), backoff)
	return false
}

// acceptStaleFor reports a pending accept-stale this member may use.
func (a *Agent) acceptStaleFor(ctx context.Context) bool {
	if !a.replicating() {
		return false
	}
	as, err := a.co.GetAcceptStale(ctx)
	return err == nil && as != nil && (as.Member == "" || as.Member == a.member)
}

// ------------------------------------------------------------------ leader -

// snapshotRound snapshots every volume, prunes, and publishes the round.
func (t *termState) snapshotRound() {
	a := t.a
	r := a.cfg.Replicate
	ctx, cancel := context.WithTimeout(t.op, r.Interval.D()+2*a.cfg.Fence.APITimeout.D())
	defer cancel()
	if h := r.PreHook; h != nil {
		if err := a.vols.Exec(ctx, a.member, h.Instance, h.Command); err != nil {
			t.log.Warn("replicate: pre-hook failed; snapshotting anyway (crash-consistent)", "err", err)
		}
	}
	name := replica.Name(t.epoch, a.now(), false)
	for _, vol := range r.Volumes {
		if err := a.vols.SnapshotVolume(ctx, a.member, vol, name); err != nil {
			a.m.Inc("pagurus_replica_snapshots_total", "result", "failed")
			t.log.Warn("replicate: snapshot failed", "volume", vol, "err", err)
			return
		}
	}
	a.m.Inc("pagurus_replica_snapshots_total", "result", "ok")
	if err := a.co.Guarded(ctx, t.epoch, t.lease, a.co.OpPutReplicaSource(coord.ReplicaSource{
		Member: a.member, Epoch: t.epoch, Snapshot: name, At: a.now().UTC(),
	})); err != nil {
		t.log.Warn("replicate: cannot publish the snapshot", "err", err)
	}
	a.m.SetGauge("pagurus_replica_age_seconds", 0)
	for _, vol := range r.Volumes {
		names, err := a.vols.VolumeSnapshots(ctx, a.member, vol)
		if err != nil {
			continue
		}
		for _, old := range replica.Prune(names, r.Keep) {
			if err := a.vols.DeleteVolumeSnapshot(ctx, a.member, vol, old); err != nil {
				t.log.Warn("replicate: prune failed", "volume", vol, "snapshot", old, "err", err)
			}
		}
	}
}
