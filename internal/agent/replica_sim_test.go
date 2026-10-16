package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/replica"
)

// ------------------------------------------------------- simulated volumes -
//
// Each member's project has custom volumes. A volume's data is one counter:
// the workload, while it runs on a member, increments that member's "data"
// volume every writeTick. So the data a failover loses is exact: the old
// leader's counter when it stopped, minus the new leader's when it started.
//
// A refresh behaves as `incus storage volume copy --refresh` on ZFS did in
// the spike: it takes pullDelay, fails without any change if the source goes
// offline (or the call is cancelled) meanwhile, and otherwise leaves the
// target with exactly the source's snapshots and its head as of the start
// (a forced receive: whatever the target had of its own is gone).

const writeTick = 10 * time.Millisecond

type simVol struct {
	head  int64
	snaps []simSnap
}

type simSnap struct {
	name string
	val  int64
}

func (v *simVol) names() []string {
	var out []string
	for _, s := range v.snaps {
		out = append(out, s.name)
	}
	return out
}

func (v *simVol) clone() *simVol {
	return &simVol{head: v.head, snaps: slices.Clone(v.snaps)}
}

type simRepl struct {
	vols       map[string]map[string]*simVol // member -> volume -> data
	pullDelay  map[string]time.Duration      // per pulling member
	perWrite   map[string]time.Duration      // ... plus this per write the pull carries (its size)
	pulling    map[string]bool
	startHead  map[string]int64 // the data volume's counter at the last start, per member
	stopHead   map[string]int64 // ... and at the last stop
	wasRunning []string
	pulled     map[string]int64 // the counter the last complete refresh left, per member
	refreshes  map[string]int   // complete refreshes into a member
	failed     map[string]int   // failed or cancelled refreshes into a member
	hooks      map[string]int
	snapFail   map[string]bool // volume name -> SnapshotVolume fails
	calls      int // every volume call, any member
}

func (c *simCluster) repl() *simRepl {
	if c.vol == nil {
		c.vol = &simRepl{vols: map[string]map[string]*simVol{}, pullDelay: map[string]time.Duration{}, perWrite: map[string]time.Duration{}, pulling: map[string]bool{},
			startHead: map[string]int64{}, stopHead: map[string]int64{}, pulled: map[string]int64{}, refreshes: map[string]int{}, failed: map[string]int{}, hooks: map[string]int{}, snapFail: map[string]bool{}}
	}
	return c.vol
}

// volume returns member's volume (provisioned on first use, as `pagurus
// provision` does). Callers hold c.mu.
func (c *simCluster) volume(member, name string) *simVol {
	r := c.repl()
	if r.vols[member] == nil {
		r.vols[member] = map[string]*simVol{}
	}
	if r.vols[member][name] == nil {
		r.vols[member][name] = &simVol{}
	}
	return r.vols[member][name]
}

// write is the workload: every running member's data volume counts up.
func (c *simCluster) write() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, m := range c.m {
		if m.running {
			c.volume(name, "data").head++
		}
	}
}

// volOp: a volume call on member's volume from r's Incus.
func (r *simRT) volOp(member string) error {
	if err := r.clusterOp(); err != nil {
		return err
	}
	r.c.repl().calls++
	if !r.c.m[member].online {
		return fmt.Errorf("member %s is offline", member)
	}
	return nil
}

func (r *simRT) VolumeSnapshots(_ context.Context, member, vol string) ([]string, error) {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.volOp(member); err != nil {
		return nil, err
	}
	return r.c.volume(member, vol).names(), nil
}

func (r *simRT) SnapshotVolume(_ context.Context, member, vol, snap string) error {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.volOp(member); err != nil {
		return err
	}
	if r.c.repl().snapFail[vol] {
		return fmt.Errorf("injected: snapshot of %s fails", vol)
	}
	v := r.c.volume(member, vol)
	if slices.Contains(v.names(), snap) {
		return fmt.Errorf("snapshot %s already exists", snap)
	}
	v.snaps = append(v.snaps, simSnap{name: snap, val: v.head})
	return nil
}

func (r *simRT) DeleteVolumeSnapshot(_ context.Context, member, vol, snap string) error {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.volOp(member); err != nil {
		return err
	}
	v := r.c.volume(member, vol)
	v.snaps = slices.DeleteFunc(v.snaps, func(s simSnap) bool { return s.name == snap })
	return nil
}

func (r *simRT) RefreshVolume(ctx context.Context, from, to, vol string) error {
	r.c.mu.Lock()
	if err := r.volOp(from); err != nil {
		r.c.mu.Unlock()
		return err
	}
	if err := r.volOp(to); err != nil {
		r.c.mu.Unlock()
		return err
	}
	rp := r.c.repl()
	src := r.c.volume(from, vol).clone()
	delay := rp.pullDelay[to]
	if d := src.head - r.c.volume(to, vol).head; d > 0 {
		delay += time.Duration(d) * rp.perWrite[to]
	}
	rp.pulling[to] = true
	r.c.mu.Unlock()
	fail := func(err error) error {
		r.c.mu.Lock()
		rp.pulling[to] = false
		rp.failed[to]++
		r.c.log = append(r.c.log, fmt.Sprintf("%s refresh %s <- %s failed: %v", time.Now().Format("05.000"), to, from, err))
		r.c.mu.Unlock()
		return err
	}
	for end := time.Now().Add(delay); ; {
		if ctx.Err() != nil {
			return fail(ctx.Err())
		}
		r.c.mu.Lock()
		gone := !r.c.m[from].online || r.gate() != nil
		r.c.mu.Unlock()
		if gone {
			return fail(fmt.Errorf("source %s went away during the transfer", from))
		}
		if !time.Now().Before(end) {
			break
		}
		time.Sleep(min(10*time.Millisecond, time.Until(end)))
	}
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	rp.pulling[to] = false
	if r.c.m[to].running {
		r.c.t.Errorf("RULE 1 VIOLATED: refresh into %s's %s volume while its workload runs", to, vol)
	}
	rp.vols[to][vol] = src
	rp.refreshes[to]++
	if vol == "data" {
		rp.pulled[to] = src.head
	}
	r.c.log = append(r.c.log, fmt.Sprintf("%s refresh %s <- %s: head %d, %d snapshots", time.Now().Format("05.000"), to, from, src.head, len(src.snaps)))
	return nil
}

func (r *simRT) CopyVolume(_ context.Context, member, vol, dst string) error {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.volOp(member); err != nil {
		return err
	}
	if _, ok := r.c.repl().vols[member][dst]; ok {
		return fmt.Errorf("volume %s already exists", dst)
	}
	r.c.repl().vols[member][dst] = r.c.volume(member, vol).clone()
	r.c.log = append(r.c.log, fmt.Sprintf("%s copy %s/%s -> %s", time.Now().Format("05.000"), member, vol, dst))
	return nil
}

func (r *simRT) ListVolumes(_ context.Context, member string) ([]string, error) {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.volOp(member); err != nil {
		return nil, err
	}
	r.c.volume(member, "data")
	var out []string
	for n := range r.c.repl().vols[member] {
		out = append(out, n)
	}
	slices.Sort(out)
	return out, nil
}

func (r *simRT) Exec(_ context.Context, member, instance string, argv []string) error {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.volOp(member); err != nil {
		return err
	}
	if !r.c.m[member].running {
		return errors.New("instance is not running")
	}
	r.c.repl().hooks[member]++
	return nil
}

// ------------------------------------------------------------------ helpers -

func replicated(maxStale time.Duration) func(*config.Config) {
	return func(c *config.Config) {
		c.Replicate = &config.Replicate{Pool: "p", Volumes: []string{"data"}, Interval: config.Duration(500 * time.Millisecond),
			MaxStaleness: config.Duration(maxStale), Keep: 3, CatchupTimeout: config.Duration(5 * time.Second)}
	}
}

func proceed(c *config.Config) { c.Fence.Unverified = config.UnverifiedProceed }

func both(fs ...func(*config.Config)) func(*config.Config) {
	return func(c *config.Config) {
		for _, f := range fs {
			f(c)
		}
	}
}

// newReplHarness is newHarness with the workload writing its counter.
func newReplHarness(t *testing.T, mutate func(*config.Config)) *harness {
	h := newHarness(t, mutate)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		for sleep(ctx, writeTick) {
			h.sim.write()
		}
	}()
	return h
}

func (h *harness) head(m string) int64 {
	h.sim.mu.Lock()
	defer h.sim.mu.Unlock()
	return h.sim.volume(m, "data").head
}

func (h *harness) stopHead(m string) int64 {
	h.sim.mu.Lock()
	defer h.sim.mu.Unlock()
	return h.sim.repl().stopHead[m]
}

func (h *harness) startHead(m string) int64 {
	h.sim.mu.Lock()
	defer h.sim.mu.Unlock()
	return h.sim.repl().startHead[m]
}

// orphans returns member's preserved volumes and their counters.
func (h *harness) orphans(m string) map[string]int64 {
	h.sim.mu.Lock()
	defer h.sim.mu.Unlock()
	out := map[string]int64{}
	for n, v := range h.sim.repl().vols[m] {
		if strings.Contains(n, "-orphaned-") {
			out[n] = v.head
		}
	}
	return out
}

func (h *harness) replStat(f func(r *simRepl) int) int {
	h.sim.mu.Lock()
	defer h.sim.mu.Unlock()
	return f(h.sim.repl())
}

func (h *harness) waitFor(what string, d time.Duration, cond func() bool) {
	h.t.Helper()
	for end := time.Now().Add(d); !cond(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			h.t.Fatalf("after %s: %s never happened (running on %v, leader %q)", d, what, h.sim.runningOn(), h.leader())
		}
	}
}

// pulledFrom: member has a complete pull recorded from the current leader.
func (h *harness) pulledFromLeader(m string) bool {
	_, epoch, _, _, err := h.leaderEpoch()
	if err != nil {
		return false
	}
	rc, err := h.coord().ReplicaCopy(context.Background(), m)
	return err == nil && rc != nil && rc.SourceEpoch == epoch
}

// powerOff: the host loses power: workload and agent gone, its Incus offline.
func (h *harness) powerOff(m string) int64 {
	h.sim.set(m, func(sm *simMember) { sm.running, sm.online, sm.isolated = false, false, true })
	h.nodes[m].crash()
	return h.head(m)
}

// powerOn: the host is back; its agent starts afresh.
func (h *harness) powerOn(m string) {
	h.sim.set(m, func(sm *simMember) { sm.online, sm.isolated = true, false })
	h.start(m, nil)
}

func (h *harness) epoch() int64 {
	_, e, _, _, _ := h.leaderEpoch()
	return e
}

// ------------------------------------------------------------------- tests -

// §5 "Clean move": the new leader pulls the old leader's final state before
// it starts. Nothing is lost and nothing is orphaned.
func TestReplicaCleanMove(t *testing.T) {
	h := newReplHarness(t, replicated(time.Minute))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("both followers pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") && h.pulledFromLeader("rpi5") })
	if err := h.coord().SetPin(context.Background(), "rpi5"); err != nil {
		t.Fatal(err)
	}
	h.waitRunning("rpi5", 15*time.Second)
	_ = h.coord().SetPin(context.Background(), "")
	if old, got := h.stopHead("asus"), h.startHead("rpi5"); got != old {
		t.Fatalf("rpi5 started from %d, asus stopped at %d: a clean move lost %d writes", got, old, old-got)
	}
	h.waitFor("asus pulled from the new leader", 10*time.Second, func() bool { return h.pulledFromLeader("asus") })
	for _, m := range []string{"asus", "dell", "rpi5"} {
		if o := h.orphans(m); len(o) != 0 {
			t.Errorf("%s orphaned %v after a clean move", m, o)
		}
	}
	h.holdOne("rpi5", time.Second)
}

// The leader's agent dies, its host stays up: the new leader fences it and
// pulls its final state. Nothing is lost.
func TestReplicaAgentCrashLosesNothing(t *testing.T) {
	h := newReplHarness(t, replicated(time.Minute))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("dell pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") })
	h.nodes["asus"].crash()
	h.waitRunning("dell", 20*time.Second)
	if old, got := h.stopHead("asus"), h.startHead("dell"); got != old {
		t.Fatalf("dell started from %d, asus was fenced at %d: lost %d writes", got, old, old-got)
	}
}

// §5 "Leader crashes between snapshots": the host is gone, so the new leader
// starts from its newest complete copy. Loss <= interval + copy time.
func TestReplicaLeaderCrashBetweenSnapshots(t *testing.T) {
	h := newReplHarness(t, both(proceed, replicated(time.Minute)))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("dell pulled twice", 10*time.Second, func() bool { return h.replStat(func(r *simRepl) int { return r.refreshes["dell"] }) >= 2 })
	dead := h.powerOff("asus")
	h.waitRunning("dell", 30*time.Second)
	got := h.startHead("dell")
	lost := dead - got
	bound := int64((h.cfg.Replicate.Interval.D() + 2*h.cfg.EligibleInterval.D()) / writeTick)
	t.Logf("asus died at %d, dell started from %d: lost %d writes (bound %d)", dead, got, lost, bound)
	if got == 0 || lost < 0 || lost > bound {
		t.Fatalf("lost %d writes, want 0 < start and loss <= %d (interval + copy)", lost, bound)
	}
}

// §5 "Failover in the middle of a copy": the source dies mid-transfer. The
// pull fails without changing anything, and the new leader starts from its
// previous complete copy.
func TestReplicaFailoverMidCopy(t *testing.T) {
	h := newReplHarness(t, both(proceed, replicated(time.Minute)))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("dell pulled", 10*time.Second, func() bool { return h.replStat(func(r *simRepl) int { return r.refreshes["dell"] }) >= 1 })
	h.sim.mu.Lock()
	h.sim.repl().pullDelay["dell"] = 3 * time.Second
	h.sim.mu.Unlock()
	h.waitFor("dell mid-copy", 10*time.Second, func() bool {
		return h.replStat(func(r *simRepl) int {
			if r.pulling["dell"] {
				return 1
			}
			return 0
		}) == 1
	})
	before := h.replStat(func(r *simRepl) int { return int(r.pulled["dell"]) })
	h.powerOff("asus")
	h.waitRunning("dell", 30*time.Second)
	if f := h.replStat(func(r *simRepl) int { return r.failed["dell"] }); f == 0 {
		t.Fatal("the interrupted pull did not fail")
	}
	if got := h.startHead("dell"); got != int64(before) {
		t.Fatalf("dell started from %d, its last complete copy was %d", got, before)
	}
}

// A planned move copies first and stops second: the leader keeps serving
// while the target pulls its backlog, and releases only once the target has
// a fresh round, so the gap with nothing running is a small final catch-up,
// not the rest of a big pull (104 s live, tasks/results/pagurus-replica-e2e.md).
func TestReplicaMoveCopiesBeforeStopping(t *testing.T) {
	h := newReplHarness(t, replicated(time.Minute))
	h.sim.mu.Lock()
	h.sim.repl().perWrite["dell"] = 5 * time.Millisecond // 1 s of writes takes 0.5 s to pull
	h.sim.mu.Unlock()
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("dell pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") })
	h.nodes["dell"].crash() // dell falls 6 s behind: its next pull takes ~3 s
	time.Sleep(6 * time.Second)
	h.start("dell", nil)
	h.waitFor("dell mid-copy", 10*time.Second, func() bool {
		return h.replStat(func(r *simRepl) int {
			if r.pulling["dell"] {
				return 1
			}
			return 0
		}) == 1
	})
	if err := h.coord().SetPin(context.Background(), "dell"); err != nil {
		t.Fatal(err)
	}
	// Measured in data, not wall time (the takeover's own overhead varies,
	// ~3x under -race): what dell still had to pull once nothing ran.
	var stopped time.Time
	var atStop int64
	for end := time.Now().Add(20 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		on := h.sim.runningOn()
		if slices.Contains(on, "dell") {
			break
		}
		if len(on) == 0 && stopped.IsZero() {
			stopped = time.Now()
			atStop = int64(h.replStat(func(r *simRepl) int { return int(r.pulled["dell"]) }))
		}
		if time.Now().After(end) {
			t.Fatalf("dell never started (running on %v)", on)
		}
	}
	_ = h.coord().SetPin(context.Background(), "")
	residual := h.startHead("dell") - atStop
	t.Logf("nothing ran for %s; the final catch-up carried %d writes (the backlog was ~600)", time.Since(stopped).Round(time.Millisecond), residual)
	if residual > 300 {
		t.Fatalf("the final catch-up carried %d writes: the move stopped the workload before dell had its backlog", residual)
	}
	if old, got := h.stopHead("asus"), h.startHead("dell"); got != old {
		t.Fatalf("dell started from %d, asus stopped at %d", got, old)
	}
}

// A planned reboot: the leader's agent shuts down gracefully, and its host's
// Incus goes away right after. The leader drains before it releases (stop,
// snapshot -final, wait until the live followers have pulled it), so the next
// leader starts with nothing lost although the old host is unreachable by
// then, and the returning host finds nothing diverged (live 2026-10-16: two
// kernel reboots took the unreachable path, 40 s and 3.5 min of exposure).
func TestReplicaShutdownDrainsBeforeTheHostGoes(t *testing.T) {
	h := newReplHarness(t, replicated(time.Minute))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("both followers pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") && h.pulledFromLeader("rpi5") })
	h.nodes["asus"].cancel()
	<-h.nodes["asus"].done
	h.sim.set("asus", func(sm *simMember) { sm.online, sm.isolated = false, true }) // the host reboots
	h.waitRunning("dell", 30*time.Second)
	if old, got := h.stopHead("asus"), h.startHead("dell"); got != old {
		t.Fatalf("dell started from %d, asus stopped at %d: a planned shutdown lost %d writes", got, old, old-got)
	}
	h.powerOn("asus")
	h.waitFor("asus pulled from the new leader", 15*time.Second, func() bool { return h.pulledFromLeader("asus") })
	if o := h.orphans("asus"); len(o) != 0 {
		t.Fatalf("asus orphaned %v after a drained shutdown", o)
	}
}

// A follower whose pull completed but whose record did not (a transient
// etcd error on the publish) publishes it on its next tick, without waiting
// for a new round: a move or drain waits on exactly that record, and no new
// round comes while it waits (code review 2026-10-16, bug 2).
func TestReplicaUnrecordedPullIsPublishedWithoutANewRound(t *testing.T) {
	h := newReplHarness(t, func(c *config.Config) {
		replicated(time.Minute)(c)
		c.Replicate.Interval = config.Duration(time.Hour) // one round only: the term's first
		c.Replicate.MaxStaleness = config.Duration(2 * time.Hour)
	})
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("dell pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") })
	if _, err := h.obs.Delete(context.Background(), h.coord().ReplicaCopyKey("dell")); err != nil {
		t.Fatal(err)
	}
	h.waitFor("dell re-published its pull", 5*time.Second, func() bool { return h.pulledFromLeader("dell") })
}

// A round is all volumes or none: when the second volume's snapshot fails,
// the first's is deleted again and nothing is published (review bug 4).
func TestReplicaFailedRoundLeavesNoPartialSnapshot(t *testing.T) {
	h := newReplHarness(t, func(c *config.Config) {
		replicated(time.Minute)(c)
		c.Replicate.Volumes = []string{"data", "config"}
	})
	h.sim.mu.Lock()
	for _, m := range []string{"asus", "dell", "rpi5"} {
		h.sim.volume(m, "config") // provisioned
	}
	h.sim.mu.Unlock()
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("dell pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") })
	src0, _ := h.coord().ReplicaSource(context.Background())
	h.sim.mu.Lock()
	h.sim.repl().snapFail["config"] = true
	h.sim.mu.Unlock()
	time.Sleep(2 * time.Second) // several 500 ms rounds, all failing
	h.sim.mu.Lock()
	data, config := h.sim.volume("asus", "data").names(), h.sim.volume("asus", "config").names()
	h.sim.mu.Unlock()
	if !slices.Equal(data, config) {
		t.Fatalf("data has %v, config has %v: a failed round left a partial snapshot", data, config)
	}
	if src, _ := h.coord().ReplicaSource(context.Background()); src == nil || src0 == nil || src.Snapshot != src0.Snapshot {
		t.Fatalf("a failed round was published: before %+v, after %+v", src0, src)
	}
}

// A copy that completes after the office moved is not published.
func TestReplicaPullSpanningAMoveIsNotPublished(t *testing.T) {
	h := newReplHarness(t, replicated(time.Minute))
	h.sim.mu.Lock()
	h.sim.repl().pullDelay["dell"] = 4 * time.Second
	h.sim.mu.Unlock()
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	old := h.epoch()
	h.waitFor("dell mid-copy", 10*time.Second, func() bool {
		return h.replStat(func(r *simRepl) int {
			if r.pulling["dell"] {
				return 1
			}
			return 0
		}) == 1
	})
	if err := h.coord().SetPin(context.Background(), "rpi5"); err != nil {
		t.Fatal(err)
	}
	h.waitRunning("rpi5", 15*time.Second)
	_ = h.coord().SetPin(context.Background(), "")
	h.waitFor("dell's slow copy ended", 10*time.Second, func() bool {
		return h.replStat(func(r *simRepl) int { return r.refreshes["dell"] + r.failed["dell"] }) >= 1
	})
	if rc, _ := h.coord().ReplicaCopy(context.Background(), "dell"); rc != nil && rc.SourceEpoch == old {
		t.Fatalf("dell published a copy from epoch %d that completed after the office moved: %+v", old, rc)
	}
}

// §5 "The old leader returns with uncopied writes": rule 4. Its volume is
// preserved as an orphan (exactly its data at death), then it resyncs from
// the new leader. The same divergence is preserved once.
func TestReplicaOldLeaderReturnsOrphaned(t *testing.T) {
	h := newReplHarness(t, both(proceed, replicated(time.Minute)))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("dell pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") })
	dead := h.powerOff("asus")
	h.waitRunning("dell", 30*time.Second)
	h.powerOn("asus")
	h.waitFor("asus resynced from dell", 15*time.Second, func() bool { return h.pulledFromLeader("asus") })
	o := h.orphans("asus")
	if len(o) != 1 {
		t.Fatalf("asus orphans %v, want exactly one", o)
	}
	for n, v := range o {
		if v != dead {
			t.Fatalf("orphan %s holds %d, asus died at %d", n, v, dead)
		}
	}
	time.Sleep(2 * time.Second) // more rounds: no second orphan
	if o := h.orphans("asus"); len(o) != 1 {
		t.Fatalf("asus orphans %v after more rounds, want still one", o)
	}
	if v := h.nodes["asus"].agent.m.Get("pagurus_orphaned_volumes"); v != 1 {
		t.Errorf("pagurus_orphaned_volumes on asus = %v, want 1", v)
	}
	for _, m := range []string{"dell", "rpi5"} {
		if o := h.orphans(m); len(o) != 0 {
			t.Errorf("%s orphaned %v", m, o)
		}
	}
}

// §5 "The only follower's copy is older than max_staleness": nobody starts;
// `pagurus accept-stale` lets one member start once.
func TestReplicaStaleGateBlocksUntilAcceptStale(t *testing.T) {
	h := newReplHarness(t, both(proceed, replicated(2*time.Second)))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("followers pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") && h.pulledFromLeader("rpi5") })
	h.powerOff("asus")
	h.waitRunning("", 2*time.Second)
	h.holdNobody(25 * time.Second) // ttl + unverified_wait for each, and each refuses
	if v := h.nodes["dell"].agent.m.Get("pagurus_replica_stale_blocked"); v != 1 {
		t.Errorf("pagurus_replica_stale_blocked on dell = %v, want 1", v)
	}
	if err := h.coord().SetAcceptStale(context.Background(), coord.AcceptStale{Member: "dell", By: "test", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	h.waitRunning("dell", 30*time.Second)
	if a, _ := h.coord().GetAcceptStale(context.Background()); a != nil {
		t.Fatalf("accept-stale not consumed: %+v", a)
	}
}

// An accept-stale is for one term. If that term starts without needing it
// (the previous holder came back and was caught up from), it is spent: left
// pending, it would let a later, unrelated failover start stale without
// anyone's word (found live, tasks/results/pagurus-replica-e2e.md).
func TestReplicaAcceptStaleSpentByATermThatDidNotNeedIt(t *testing.T) {
	h := newReplHarness(t, replicated(time.Minute))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("dell pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") })
	if err := h.coord().SetAcceptStale(context.Background(), coord.AcceptStale{Member: "dell", By: "test", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := h.coord().SetPin(context.Background(), "dell"); err != nil {
		t.Fatal(err)
	}
	h.waitRunning("dell", 15*time.Second)
	_ = h.coord().SetPin(context.Background(), "")
	if old, got := h.stopHead("asus"), h.startHead("dell"); got != old {
		t.Fatalf("dell started from %d, asus stopped at %d: the move did not catch up", got, old)
	}
	if a, _ := h.coord().GetAcceptStale(context.Background()); a != nil {
		t.Fatalf("accept-stale still pending after dell's term started without it: %+v", a)
	}
}

// §5 "A partitioned old leader is still running and still writing": it is
// fenced (self-fence), and its writes become orphan material when it returns,
// never flowing anywhere.
func TestReplicaPartitionedLeaderWritesBecomeOrphan(t *testing.T) {
	h := newReplHarness(t, both(proceed, replicated(time.Minute)))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("dell pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") })
	h.partition("asus", true)
	h.waitRunning("dell", 30*time.Second)
	fenced := h.stopHead("asus")
	if fenced <= h.startHead("dell") {
		t.Fatalf("asus stopped at %d, dell started from %d: the partition wrote nothing past the last copy?", fenced, h.startHead("dell"))
	}
	h.partition("asus", false)
	h.waitFor("asus resynced from dell", 20*time.Second, func() bool { return h.pulledFromLeader("asus") })
	o := h.orphans("asus")
	if len(o) != 1 {
		t.Fatalf("asus orphans %v, want one", o)
	}
	for n, v := range o {
		if v != fenced {
			t.Fatalf("orphan %s holds %d, asus self-fenced at %d", n, v, fenced)
		}
	}
}

// §5 "Two failovers in a row before any copy completes": each new leader
// starts from its own newest copy; each older term's writes are an orphan on
// its own member.
func TestReplicaTwoFailoversBeforeAnyCopy(t *testing.T) {
	h := newReplHarness(t, both(proceed, replicated(time.Minute)))
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("followers pulled", 10*time.Second, func() bool { return h.pulledFromLeader("dell") && h.pulledFromLeader("rpi5") })
	h.sim.mu.Lock()
	h.sim.repl().pullDelay["rpi5"] = time.Hour // rpi5 never completes another copy
	h.sim.mu.Unlock()
	asusDead := h.powerOff("asus")
	h.waitRunning("dell", 30*time.Second)
	time.Sleep(time.Second) // dell writes and snapshots; rpi5 can't copy any of it
	dellDead := h.powerOff("dell")
	h.waitRunning("rpi5", 40*time.Second)
	if got, from := h.startHead("rpi5"), h.replStat(func(r *simRepl) int { return int(r.pulled["rpi5"]) }); got != int64(from) {
		t.Fatalf("rpi5 started from %d, its newest copy was %d", got, from)
	}
	h.sim.mu.Lock()
	h.sim.repl().pullDelay["rpi5"] = 0
	h.sim.mu.Unlock()
	h.powerOn("asus")
	h.powerOn("dell")
	h.waitFor("asus and dell resynced", 20*time.Second, func() bool { return h.pulledFromLeader("asus") && h.pulledFromLeader("dell") })
	for m, dead := range map[string]int64{"asus": asusDead, "dell": dellDead} {
		o := h.orphans(m)
		if len(o) != 1 {
			t.Fatalf("%s orphans %v, want one", m, o)
		}
		for n, v := range o {
			if v != dead {
				t.Errorf("%s's orphan %s holds %d, it died at %d", m, n, v, dead)
			}
		}
	}
}

// §5 "Bandwidth: a full first copy of a big volume": until a follower's
// first copy completes it is not ready; the gate keeps it from starting with
// no data. Once the old leader is back, the service resumes from its data.
func TestReplicaSlowFirstCopyIsGatedByStaleness(t *testing.T) {
	h := newReplHarness(t, both(proceed, replicated(time.Minute)))
	h.sim.mu.Lock()
	h.sim.repl().pullDelay["dell"] = time.Hour
	h.sim.repl().pullDelay["rpi5"] = time.Hour
	h.sim.mu.Unlock()
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	time.Sleep(time.Second)
	dead := h.powerOff("asus")
	h.holdNobody(20 * time.Second)
	h.sim.mu.Lock()
	h.sim.repl().pullDelay["dell"], h.sim.repl().pullDelay["rpi5"] = 0, 0
	h.sim.mu.Unlock()
	h.powerOn("asus")
	// Whoever wins now starts from asus's data: asus itself, or a follower
	// that pulls asus's final state first.
	var on []string
	h.waitFor("someone runs", 40*time.Second, func() bool { on = h.sim.runningOn(); return len(on) == 1 })
	if got := h.startHead(on[0]); got != dead {
		t.Fatalf("%s started from %d, asus died at %d", on[0], got, dead)
	}
}

// The leader runs the pre-hook before each snapshot round.
func TestReplicaPreHookRunsBeforeSnapshots(t *testing.T) {
	h := newReplHarness(t, func(c *config.Config) {
		replicated(time.Minute)(c)
		c.Replicate.PreHook = &config.Hook{Instance: "svc-1", Command: []string{"sync"}}
	})
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.waitFor("three snapshot rounds", 10*time.Second, func() bool { return h.replStat(func(r *simRepl) int { return r.hooks["asus"] }) >= 3 })
	h.sim.mu.Lock()
	n := len(replica.Sorted(h.sim.volume("asus", "data").names()))
	h.sim.mu.Unlock()
	if n > h.cfg.Replicate.Keep {
		t.Fatalf("asus keeps %d snapshots, keep is %d", n, h.cfg.Replicate.Keep)
	}
}

// Without a replicate block, nothing touches a volume.
func TestReplicaOffMeansNoVolumeCalls(t *testing.T) {
	h := newReplHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	if err := h.coord().SetPin(context.Background(), "dell"); err != nil {
		t.Fatal(err)
	}
	h.waitRunning("dell", 15*time.Second)
	time.Sleep(time.Second)
	if n := h.replStat(func(r *simRepl) int { return r.calls }); n != 0 {
		t.Fatalf("%d volume calls without replicate", n)
	}
}
