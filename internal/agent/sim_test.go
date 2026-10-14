package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/incus"
	"github.com/alien43/pagurus/internal/testutil"
	"github.com/alien43/pagurus/internal/watchdog"
)

// ------------------------------------------------------- simulated cluster -

// simCluster models the parts of an Incus cluster the agent touches: per
// member, whether it is Online, whether its workload runs, and fault knobs.
// It counts every instant at which more than one member runs the workload.
type simCluster struct {
	t  *testing.T
	mu sync.Mutex
	m  map[string]*simMember

	violations int
	maxRunning int
	log        []string

	// tolerate records overlaps (how long two ran) instead of failing:
	// only for measuring the residual start window.
	tolerate     bool
	overlapSince time.Time
	maxOverlap   time.Duration
}

type simMember struct {
	online    bool // Online in the Incus cluster
	isolated  bool // this member's own Incus cannot reach the cluster
	running   bool
	unhealthy bool // instances exist but are not Running
	stopFail  bool // `incus stop` does nothing
	killFail  bool // cgroup.kill does not empty the cgroup
	upFail    bool
	// prepareFail: `up --no-start --recreate` deletes the instance and the
	// create fails "In use" (an incusd that leaked the volume's mount
	// refcount after a cgroup-kill fence), until incusd restarts.
	prepareFail bool
	// hideCgroup: cgroupfs shows no payload cgroup although Incus says it
	// runs (a liblxc that names payload cgroups differently).
	hideCgroup bool
	// slowPause, once set, pauses the next slow start phase (Prepare, or a
	// Start not preceded by Prepare: a whole `up`) for that long, ignoring
	// cancellation, as a paused process would. paused is set while it waits.
	slowPause time.Duration
	paused    bool
	prepared  bool
	prepares  int // Prepare calls: each recreates the instance (stopped)
	pulls     int // Pull calls
	// launchPause pauses inside the launch itself (the residual window).
	launchPause time.Duration
}

func newSim(t *testing.T, members ...string) *simCluster {
	c := &simCluster{t: t, m: map[string]*simMember{}}
	for _, m := range members {
		c.m[m] = &simMember{online: true}
	}
	return c
}

func (c *simCluster) set(member string, f func(*simMember)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(c.m[member])
	c.check("set " + member)
}

func (c *simCluster) check(what string) {
	n := 0
	var on []string
	for name, m := range c.m {
		if m.running {
			n++
			on = append(on, name)
		}
	}
	slices.Sort(on)
	c.maxRunning = max(c.maxRunning, n)
	switch {
	case n > 1 && c.tolerate:
		if c.overlapSince.IsZero() {
			c.overlapSince = time.Now()
		}
	case n > 1:
		c.violations++
		c.t.Errorf("AT-MOST-ONE VIOLATED after %s: running on %v", what, on)
	case !c.overlapSince.IsZero():
		c.maxOverlap = max(c.maxOverlap, time.Since(c.overlapSince))
		c.overlapSince = time.Time{}
	}
	c.log = append(c.log, fmt.Sprintf("%s %s -> %v", time.Now().Format("05.000"), what, on))
}

func (c *simCluster) runningOn() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var on []string
	for name, m := range c.m {
		if m.running {
			on = append(on, name)
		}
	}
	slices.Sort(on)
	return on
}

// simRT is one agent's view of the cluster. dead=true makes it inert: the
// process is "crashed", every call fails and nothing it does has an effect.
type simRT struct {
	c    *simCluster
	me   string
	mu   sync.Mutex
	dead bool
}

var errDead = errors.New("agent is dead")

func (r *simRT) gate() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dead {
		return errDead
	}
	return nil
}

// clusterOp: an API call that needs this member's Incus to reach the cluster.
func (r *simRT) clusterOp() error {
	if err := r.gate(); err != nil {
		return err
	}
	if r.c.m[r.me].isolated {
		return errors.New("local Incus lost the cluster")
	}
	return nil
}

func (r *simRT) Identity(context.Context) (string, string, error) { return r.me, "x86_64", r.gate() }
func (r *simRT) CheckProject(context.Context, string) error       { return nil }
func (r *simRT) CheckStart(context.Context, string) error         { return nil }
func (r *simRT) CheckCgroup2() error                              { return nil }

func (r *simRT) MemberOnline(_ context.Context, m string) (bool, error) {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.clusterOp(); err != nil {
		return false, err
	}
	return r.c.m[m].online, nil
}

func (r *simRT) List(_ context.Context, m string) ([]incus.Instance, error) {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.clusterOp(); err != nil {
		return nil, err
	}
	sm := r.c.m[m]
	if !sm.online {
		return nil, fmt.Errorf("member %s is offline", m)
	}
	st := incus.StatusStopped
	if sm.running {
		st = incus.StatusRunning
		if sm.unhealthy {
			st = "Error"
		}
	}
	return []incus.Instance{{Name: "svc-1", Status: st, Location: m, Type: "container"}}, nil
}

// pauseIf sleeps for *d (once) without the lock, ignoring cancellation.
func (r *simRT) pauseIf(m string, d *time.Duration) {
	r.c.mu.Lock()
	p := *d
	*d = 0
	if p > 0 {
		r.c.m[m].paused = true
	}
	r.c.mu.Unlock()
	if p > 0 {
		time.Sleep(p)
		r.c.mu.Lock()
		r.c.m[m].paused = false
		r.c.mu.Unlock()
	}
}

func (r *simRT) Prepare(_ context.Context, m string) error {
	r.pauseIf(m, &r.c.m[m].slowPause)
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.clusterOp(); err != nil {
		return err
	}
	r.c.m[m].prepares++
	r.c.m[m].running = false // `up --no-start --recreate`: a fresh, stopped instance
	if r.c.m[m].prepareFail {
		return errors.New("Creating instance: In use")
	}
	r.c.m[m].prepared = true
	return nil
}

func (r *simRT) Pull(_ context.Context, m string) error {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.clusterOp(); err != nil {
		return err
	}
	r.c.m[m].pulls++
	return nil
}

func (r *simRT) Start(_ context.Context, m string) error {
	r.c.mu.Lock()
	unprepared := !r.c.m[m].prepared
	r.c.mu.Unlock()
	if unprepared { // a whole `up`: the slow part is here
		r.pauseIf(m, &r.c.m[m].slowPause)
	}
	r.pauseIf(m, &r.c.m[m].launchPause)
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	r.c.m[m].prepared = false
	if err := r.clusterOp(); err != nil {
		return err
	}
	if r.c.m[m].upFail {
		return errors.New("up failed")
	}
	r.c.m[m].running = true
	r.c.check("up on " + m)
	return nil
}

func (r *simRT) EnforceNoAutostart(context.Context, string) error { return r.clusterOp() }

func (r *simRT) Stop(_ context.Context, m string, _ time.Duration) error {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.clusterOp(); err != nil {
		return err
	}
	sm := r.c.m[m]
	if !sm.online {
		return fmt.Errorf("member %s is offline", m)
	}
	if sm.stopFail {
		return errors.New("stop failed")
	}
	if sm.running {
		sm.running = false
		r.c.check(fmt.Sprintf("stop %s (by %s)", m, r.me))
	}
	return nil
}

func (r *simRT) LocalRunning(m string) ([]string, error) {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.gate(); err != nil {
		return nil, err
	}
	if r.c.m[m].running && !r.c.m[m].hideCgroup {
		return []string{"svc-1"}, nil
	}
	return nil, nil
}

func (r *simRT) Kill(_ context.Context, m string) error {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.gate(); err != nil {
		return err
	}
	sm := r.c.m[m]
	if sm.killFail {
		return errors.New("cgroup.kill: still populated")
	}
	if sm.running {
		sm.running = false
		r.c.check("cgroup.kill " + m)
	}
	return nil
}

func (r *simRT) IPv4(context.Context, string, string) (string, error) { return "192.0.2.1", nil }

// -------------------------------------------------------- simulated watchdog -

// simDog behaves like an armed watchdog whose expiry "resets the host": the
// member's workload stops and its agent dies.
type simDog struct {
	timeout time.Duration
	reboot  bool
	onFire  func()
	mu      sync.Mutex
	fired   bool
}

func (d *simDog) Mode() string             { return "sim" }
func (d *simDog) Timeout() time.Duration   { return d.timeout }
func (d *simDog) FencesByReboot() bool     { return d.reboot }
func (d *simDog) Close(bool)               {}
func (d *simDog) Starving() (bool, string) { return false, "" }
func (d *simDog) Run(ctx context.Context, fit watchdog.Fitness) {
	last := time.Now()
	for ctx.Err() == nil {
		if ok, _ := fit(); ok {
			last = time.Now()
		} else if time.Since(last) > d.timeout && d.reboot {
			d.mu.Lock()
			d.fired = true
			d.mu.Unlock()
			d.onFire()
			return
		}
		sleep(ctx, 50*time.Millisecond)
	}
}

// ------------------------------------------------------------------ harness -

type node struct {
	name    string
	rt      *simRT
	proxies []*testutil.Proxy // one per etcd endpoint: this node's network
	cancel  context.CancelFunc
	done    chan struct{}
	agent   *Agent
	dog     *simDog
}

type harness struct {
	t     *testing.T
	cfg   *config.Config
	sim   *simCluster
	eps   []string // etcd endpoints (one member, or a cluster's client proxies)
	obs   *clientv3.Client
	mu    sync.Mutex // nodes, read by the simulated host
	nodes map[string]*node
	state string
	host  string // the simulated host's socket (sim-fence)
	// nodeCfg, if set for a member, adjusts that member's config only.
	nodeCfg map[string]func(*config.Config)
}

func testConfig(t *testing.T, mutate func(*config.Config)) *config.Config {
	t.Helper()
	raw := `{
	  "service": "svc", "compose": "/dev/null", "project_prefix": "sgl-svc",
	  "members": ["asus", "dell", "rpi5"],
	  "etcd": {"endpoints": ["x"], "op_timeout": "400ms"},
	  "lease": {"ttl": "8s", "renew_interval": "500ms", "margin": "500ms"},
	  "self_fence": {"stop_budget": "1s", "kill_budget": "500ms"},
	  "grace": "1s", "eligible_interval": "300ms", "audit_interval": "1s",
	  "fence": {"api_timeout": "1s", "retry_interval": "500ms", "reboot_margin": "1s", "unverified_wait": "6s"},
	  "health": {"interval": "300ms", "start_period": "1s", "failures": 2, "max_restarts": 1, "release_backoff": "20s"},
	  "watchdog": {"mode": "off"},
	  "state_dir": "/tmp/x"
	}`
	c, err := config.Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if mutate != nil {
		mutate(c)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	return c
}

func newHarness(t *testing.T, mutate func(*config.Config)) *harness {
	// Short: unix socket paths are limited to 108 bytes.
	state, err := os.MkdirTemp("/tmp", "pg-state")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(state) })
	return newHarnessOn(t, mutate, []string{testutil.Etcd(t)}, state)
}

// newClusterHarness runs the sim against a 3-member etcd cluster.
func newClusterHarness(t *testing.T, mutate func(*config.Config)) (*harness, *testutil.Cluster) {
	state, err := os.MkdirTemp("/tmp", "pg-state")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(state) })
	c := testutil.EtcdCluster(t, 3)
	return newHarnessOn(t, mutate, c.Endpoints(), state), c
}

func newHarnessOn(t *testing.T, mutate func(*config.Config), eps []string, state string) *harness {
	h := &harness{t: t, cfg: testConfig(t, mutate), sim: newSim(t, "asus", "dell", "rpi5"),
		eps: eps, nodes: map[string]*node{}, state: state}
	h.obs = testutil.Client(t, eps[0], eps[1:]...)
	h.host = h.serveHost(t)
	t.Cleanup(func() {
		h.mu.Lock()
		nodes := maps.Clone(h.nodes)
		h.mu.Unlock()
		for _, n := range nodes {
			n.kill()
		}
		if t.Failed() {
			h.sim.mu.Lock()
			t.Log("sim timeline:\n" + strings.Join(h.sim.log, "\n"))
			h.sim.mu.Unlock()
		}
	})
	return h
}

func logger(t *testing.T, name string) *slog.Logger {
	if verbose() {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", name)
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func verbose() bool { return os.Getenv("SINGLETON_TEST_LOG") != "" }

func procOutput() io.Writer {
	if verbose() {
		return os.Stderr
	}
	return io.Discard
}

// start launches (or restarts) member's agent. dog may be nil (watchdog off).
func (h *harness) start(name string, dog *simDog) *node {
	cfg := *h.cfg
	cfg.StateDir = h.state + "/" + name
	if f := h.nodeCfg[name]; f != nil {
		f(&cfg)
	}
	var pxs []*testutil.Proxy
	var addrs []string
	for _, ep := range h.eps {
		px := testutil.NewProxy(h.t, ep)
		pxs, addrs = append(pxs, px), append(addrs, px.Addr())
	}
	cfg.Etcd.Endpoints = addrs // conch too: a partition cuts it off
	cli := testutil.Client(h.t, addrs[0], addrs[1:]...)
	rt := &simRT{c: h.sim, me: name}
	self, err := os.Executable()
	if err != nil {
		h.t.Fatal(err)
	}
	a := New(Options{Config: &cfg, Runtime: rt, Etcd: cli, Log: logger(h.t, name), Version: "test",
		Conch: []string{conchBin}, FenceCmd: []string{self, "sim-fence", h.host}, TermCmd: []string{self, "sim-term"},
		ProcOutput: procOutput()})
	if err := a.Preflight(h.t.Context()); err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{name: name, rt: rt, proxies: pxs, cancel: cancel, done: make(chan struct{}), agent: a, dog: dog}
	var wd watchdog.Watchdog
	if dog != nil {
		wd = dog
		dog.onFire = func() { // the host resets: workload gone, agent gone
			h.sim.set(name, func(m *simMember) { m.running = false })
			n.crash()
		}
	} else {
		wd, _ = watchdog.New(watchdog.Options{Mode: "off"})
	}
	go func() { defer close(n.done); _ = a.Run(ctx, wd) }()
	h.mu.Lock()
	h.nodes[name] = n
	h.mu.Unlock()
	return n
}

// crash: the agent's unit dies at once without any cleanup (SIGKILL, OOM):
// conch and the term process are killed, the agent's etcd connection goes
// silent and its runtime handle becomes inert. No fence runs.
func (n *node) crash() {
	n.rt.mu.Lock()
	n.rt.dead = true
	n.rt.mu.Unlock()
	n.blackhole(true)
	n.agent.KillProcs()
	n.cancel() // the in-process agent is inert now; let it wind down, spawning nothing
}

func (n *node) blackhole(on bool) {
	for _, p := range n.proxies {
		p.SetBlackhole(on)
	}
}

// kill stops the goroutine for good (test teardown or after crash).
func (n *node) kill() {
	n.crash()
	n.cancel()
	<-n.done
}

// partition cuts member off etcd and the Incus cluster, the way a dead NIC
// would: it can still act on its own cgroups, nothing else.
func (h *harness) partition(name string, on bool) {
	h.nodes[name].blackhole(on)
	h.sim.set(name, func(m *simMember) { m.online, m.isolated = !on, on })
}

func (h *harness) leader() string {
	co := coord.New(h.obs, h.cfg.EtcdPrefix, time.Second)
	rec, _, _, _, err := co.Leader(context.Background())
	if err != nil || rec == nil {
		return ""
	}
	return rec.Member
}

func (h *harness) coord() *coord.Coord { return coord.New(h.obs, h.cfg.EtcdPrefix, time.Second) }

func (h *harness) leaderEpoch() (*coord.LeaderRecord, int64, clientv3.LeaseID, int64, error) {
	return coord.New(h.obs, h.cfg.EtcdPrefix, time.Second).Leader(context.Background())
}

func (h *harness) memberStates() map[string]coord.MemberState {
	co := coord.New(h.obs, h.cfg.EtcdPrefix, time.Second)
	st, _ := co.MemberStates(context.Background())
	return st
}

// waitRunning waits until exactly want runs the workload ("" = nobody, for
// the full duration d when stable is true).
func (h *harness) waitRunning(want string, d time.Duration) time.Duration {
	h.t.Helper()
	start := time.Now()
	for time.Since(start) < d {
		on := h.sim.runningOn()
		if (want == "" && len(on) == 0) || (len(on) == 1 && on[0] == want) {
			return time.Since(start)
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("after %s: running on %v, want %q (leader=%q)", d, h.sim.runningOn(), want, h.leader())
	return 0
}

// holdNobody asserts nobody runs the workload for all of d.
func (h *harness) holdNobody(d time.Duration) {
	h.t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if on := h.sim.runningOn(); len(on) > 0 {
			h.t.Fatalf("workload started on %v while it had to stay down", on)
		}
	}
}

func (h *harness) startAll() {
	for _, m := range h.cfg.Members {
		h.start(m, nil)
	}
}

// --------------------------------------------------------------------- tests -

func TestPreferredMemberWinsAndOnlyOneRuns(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.holdOne("asus", 3*time.Second)
}

func (h *harness) holdOne(want string, d time.Duration) {
	h.t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if on := h.sim.runningOn(); len(on) != 1 || on[0] != want {
			h.t.Fatalf("running on %v, want only %s", on, want)
		}
	}
}

func TestGracefulShutdownHandsOver(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.nodes["asus"].cancel()
	<-h.nodes["asus"].done
	if on := h.sim.runningOn(); len(on) != 0 && on[0] == "asus" {
		t.Fatal("asus still running after a graceful shutdown")
	}
	took := h.waitRunning("dell", 10*time.Second)
	t.Logf("handover took %s", took)
	if st := h.memberStates()["asus"]; st.State != coord.Stopped {
		t.Fatalf("asus member state %+v, want stopped", st)
	}
}

// The leader's agent crashes; its host and Incus stay up. The workload keeps
// running unsupervised until the lease expires; the new leader must stop it
// through the Incus API before starting its own copy.
func TestCrashedAgentIsFencedViaAPI(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.nodes["asus"].crash()
	if on := h.sim.runningOn(); len(on) != 1 {
		t.Fatal("crash must leave the workload running")
	}
	took := h.waitRunning("dell", 15*time.Second)
	t.Logf("failover after crash took %s", took)
	if st := h.memberStates()["asus"]; st.State != coord.Stopped || !strings.Contains(st.Reason, "fenced via Incus API") {
		t.Fatalf("asus member state %+v, want stopped via API fence", st)
	}
}

// Agent crash + quick restart (within TTL). conch has no lease resume
// (Andrey's Q3 decision: accept it): the restarted agent stops the orphaned
// copy at startup, and the office comes back after the dead term's lease
// expires. asus, queued with no campaign delay, wins it again. Never two.
func TestRestartWithinTTLRecoversWithoutOverlap(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.nodes["asus"].crash()
	time.Sleep(500 * time.Millisecond)
	h.start("asus", nil)
	h.waitRunning("", 3*time.Second) // the startup fence stopped the orphan
	took := h.waitRunning("asus", 2*h.cfg.Lease.TTL.D()+10*time.Second)
	t.Logf("asus running again %s after its restart", took)
	h.holdOne("asus", 2*time.Second)
}

// The leader is partitioned from everything. It must fence itself (Incus
// cannot stop anything without the cluster, so via cgroup.kill) before its
// lease expires; the peers cannot verify that and, under policy=proceed, wait
// unverified_wait before starting.
func TestPartitionedLeaderSelfFences_ProceedPolicy(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.Fence.Unverified = config.UnverifiedProceed })
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	t0 := time.Now()
	h.partition("asus", true)
	h.waitRunning("", 6*time.Second)
	selfFenced := time.Since(t0)
	took := h.waitRunning("dell", 20*time.Second)
	t.Logf("self-fence after %s; dell running %s after partition", selfFenced, time.Since(t0))
	if selfFenced > h.cfg.Lease.TTL.D() {
		t.Fatalf("self-fence took %s, longer than the TTL", selfFenced)
	}
	_ = took
	// Healing: asus comes back as a follower and reports itself stopped.
	h.partition("asus", false)
	deadline := time.Now().Add(10 * time.Second)
	for h.memberStates()["asus"].State != coord.Stopped {
		if time.Now().After(deadline) {
			t.Fatalf("asus never reported itself stopped: %+v", h.memberStates()["asus"])
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.holdOne("dell", 2*time.Second)
}

// A member whose self chain is empty (e.g. a VM with no Incus-free kill)
// advertises no verified self-fence. Partitioned as leader, it cannot stop its
// copy, and under policy=proceed its peers must still refuse to trust it:
// the copy keeps running alone, and nobody starts a second one even well past
// unverified_wait.
func TestNoVerifiedSelfFence_ProceedPolicyStillBlocks(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.Fence.Unverified = config.UnverifiedProceed
		c.Fences.Self = &[]config.FenceStep{}
	})
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.partition("asus", true)
	h.holdOne("asus", h.cfg.Fence.UnverifiedWait.D()+h.cfg.Lease.TTL.D()+2*time.Second)
	if !h.memberStates()["asus"].Caps.NoVerifiedSelfFence {
		t.Fatal("asus did not advertise NoVerifiedSelfFence")
	}
}

// Same partition under the default policy=block: nobody may start until the
// isolated member is back (or acknowledged), even though it did self-fence.
func TestPartitionedLeader_BlockPolicyWaitsForTheMember(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.partition("asus", true)
	h.waitRunning("", 6*time.Second)
	h.holdNobody(8 * time.Second)
	h.partition("asus", false)
	took := h.waitRunning("dell", 15*time.Second)
	t.Logf("dell started %s after the partition healed", took)
}

// Under policy=block, an operator fence-ack unblocks a failover.
func TestFenceAckUnblocks(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.partition("asus", true)
	h.waitRunning("", 6*time.Second)
	h.holdNobody(3 * time.Second)
	// The operator acks once a new leader is blocked on asus: before that,
	// asus's lease is still live and the ack is (rightly) refused.
	for deadline := time.Now().Add(2 * h.cfg.Lease.TTL.D()); h.leader() == "asus" || h.leader() == ""; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the office never left asus (leader %q)", h.leader())
		}
	}
	h.holdNobody(time.Second) // blocked: still nobody runs
	co := coord.New(h.obs, h.cfg.EtcdPrefix, time.Second)
	if err := co.AckFenced(context.Background(), "asus", "operator:test"); err != nil {
		t.Fatal(err)
	}
	h.waitRunning("dell", 10*time.Second)
}

// The partitioned leader cannot stop its workload at all (Incus and
// cgroup.kill both fail). Its fitness must fail after the safe deadline, and
// nobody may start under policy=block.
func TestSelfFenceFailure_BlockPolicyHolds(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.sim.set("asus", func(m *simMember) { m.stopFail, m.killFail = true, true })
	h.partition("asus", true)
	h.holdOne("asus", 10*time.Second)
	if ok, why := h.nodes["asus"].agent.Fit(); ok {
		t.Fatal("asus still reports fit although its stop failed past the deadline")
	} else {
		t.Logf("asus unfit: %s", why)
	}
}

// The same unstoppable, partitioned leader under policy=block, but with a
// power fence (an exec peer fence) in the chain: the new leader runs it, and
// starts only once it has confirmed. The power-off is simulated by the test
// flipping asus's instance to stopped when the command asks for it.
func TestExecPeerFence_PowerOffUnblocksFailover(t *testing.T) {
	dir := t.TempDir()
	requested, off := filepath.Join(dir, "requested"), filepath.Join(dir, "off")
	plugOff := filepath.Join(dir, "plug-off")
	plugIsOff := filepath.Join(dir, "plug-is-off")
	if err := os.WriteFile(plugOff, []byte(fmt.Sprintf("#!/bin/sh\necho \"$1\" >> %q\n", requested)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plugIsOff, []byte(fmt.Sprintf("#!/bin/sh\ngrep -qx \"$1\" %q\n", off)), 0o755); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, func(c *config.Config) {
		c.Fences.Peer = &[]config.FenceStep{
			{Kind: "incus-api-stop", Budget: c.Fence.APITimeout},
			{Kind: "exec", Budget: config.Duration(2 * time.Second), Command: []string{plugOff, "{member}"}, Confirm: []string{plugIsOff, "{member}"}},
		}
	})
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.sim.set("asus", func(m *simMember) { m.stopFail, m.killFail = true, true })
	h.partition("asus", true)
	t0 := time.Now()

	// The "power strip": cut asus when the fence asks for it.
	go func() {
		for time.Since(t0) < 30*time.Second {
			if b, err := os.ReadFile(requested); err == nil && strings.Contains(string(b), "asus") {
				h.sim.set("asus", func(m *simMember) { m.running = false })
				_ = os.WriteFile(off, []byte("asus\n"), 0o644)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	took := h.waitRunning("dell", 30*time.Second)
	t.Logf("dell running %s after the partition (power fence, policy=block)", took)
	if st := h.memberStates()["asus"]; st.State != coord.Stopped || !strings.Contains(st.Reason, "exec") {
		t.Fatalf("asus record %+v, want stopped via exec", st)
	}
}

// asus's cgroupfs hides the payload cgroup of what it runs. Right after its
// first start the agent must notice that Incus and cgroupfs disagree and give
// the term up instead of running a copy its own checks cannot see; another
// member takes over. (Preflight passes: nothing runs yet.)
func TestUnrecognisedCgroupLayoutAfterStartGivesUp(t *testing.T) {
	h := newHarness(t, nil)
	h.sim.set("asus", func(m *simMember) { m.hideCgroup = true })
	h.startAll()
	took := h.waitRunning("dell", 20*time.Second)
	t.Logf("dell running %s after start; asus refused its term", took)
	if on := h.sim.runningOn(); len(on) != 1 || on[0] != "dell" {
		t.Fatalf("running on %v, want only dell", on)
	}
}

// dell runs a different lease TTL from asus and rpi5. Nobody may campaign
// while a live registration disagrees: a leader fencing a peer with another
// TTL would wait the wrong amount of time. Once dell comes back with the
// agreed config, the service starts.
func TestConfigSkewRefusesToCampaign(t *testing.T) {
	h := newHarness(t, nil)
	h.nodeCfg = map[string]func(*config.Config){"dell": func(c *config.Config) { c.Lease.TTL = config.Duration(6 * time.Second) }}
	h.startAll()
	h.holdNobody(8 * time.Second)
	// dell is restarted with the agreed config; its old registration lapses
	// with its lease.
	h.nodes["dell"].crash()
	h.nodeCfg = nil
	h.start("dell", nil)
	took := h.waitRunning("asus", 20*time.Second)
	t.Logf("asus running %s after dell came back with the agreed config", took)
}

// With an armed (simulated) watchdog the same unstoppable leader is reset by
// its watchdog, and the new leader waits out the advertised timeout before
// starting — no overlap.
func TestSelfFenceFailure_WatchdogResets(t *testing.T) {
	h := newHarness(t, nil)
	dogs := map[string]*simDog{}
	for _, m := range h.cfg.Members {
		dogs[m] = &simDog{timeout: 3 * time.Second, reboot: true}
		h.start(m, dogs[m])
	}
	h.waitRunning("asus", 10*time.Second)
	h.sim.set("asus", func(m *simMember) { m.stopFail, m.killFail = true, true })
	t0 := time.Now()
	h.partition("asus", true)
	took := h.waitRunning("dell", 30*time.Second)
	t.Logf("dell running %s after partition (watchdog reset of asus: %v)", took, dogs["asus"].fired)
	if !dogs["asus"].fired {
		t.Fatal("asus watchdog never fired")
	}
	if min := h.cfg.Lease.TTL.D() + 3*time.Second; time.Since(t0) < min {
		t.Fatalf("dell started after %s, before TTL + watchdog timeout (%s)", time.Since(t0), min)
	}
}

func TestPlannedMove(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	co := coord.New(h.obs, h.cfg.EtcdPrefix, time.Second)
	if err := co.SetPin(context.Background(), "rpi5"); err != nil {
		t.Fatal(err)
	}
	took := h.waitRunning("rpi5", 10*time.Second)
	t.Logf("move took %s", took)
	_ = co.SetPin(context.Background(), "")
	h.holdOne("rpi5", 3*time.Second) // no automatic failback
	// The pin made the followers stop their campaigns: withdrawals, not
	// failures.
	var withdrawn, other float64
	var per []string
	for _, m := range []string{"asus", "dell", "rpi5"} {
		w := h.nodes[m].agent.m.Counter("pagurus_conch_exits_total", "result", "withdrawn")
		o := h.nodes[m].agent.m.Counter("pagurus_conch_exits_total", "result", "other")
		ok := h.nodes[m].agent.m.Counter("pagurus_conch_exits_total", "result", "ok")
		withdrawn, other = withdrawn+w, other+o
		per = append(per, fmt.Sprintf("%s: ok=%v withdrawn=%v other=%v", m, ok, w, o))
	}
	if withdrawn == 0 || other != 0 {
		t.Fatalf("conch exits: withdrawn=%v other=%v, want withdrawals only (%s)", withdrawn, other, strings.Join(per, "; "))
	}
}

func TestUnhealthyLeaderGivesUp(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.sim.set("asus", func(m *simMember) { m.unhealthy = true })
	h.waitRunning("dell", 20*time.Second)
	h.sim.set("asus", func(m *simMember) { m.unhealthy = false })
	h.holdOne("dell", 2*time.Second)
}

// Two members whose Incus cannot create the workload (2026-10-14: both
// cgroup-kill fenced, both leaking the volume's mount refcount) must not
// starve the third. With a fixed backoff shorter than a failed tenure the
// poisoned pair took turns for ever: whenever one released, the other was
// already back and ranked ahead of rpi5.
func TestPoisonedPairDoesNotStarveTheThird(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.Health.ReleaseBackoff = config.Duration(1500 * time.Millisecond) })
	for _, m := range []string{"asus", "dell"} {
		h.sim.set(m, func(m *simMember) { m.prepareFail = true })
	}
	h.startAll()
	took := h.waitRunning("rpi5", 60*time.Second)
	t.Logf("rpi5 serving %s after start", took.Round(100*time.Millisecond))
	h.holdOne("rpi5", 2*time.Second)
}

// The control: the same pair with a fixed backoff (max = base) never lets
// rpi5 in, which is what made the escalation necessary.
func TestPoisonedPairStarvesTheThirdWithFixedBackoff(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.Health.ReleaseBackoff = config.Duration(1500 * time.Millisecond)
		c.Health.ReleaseBackoffMax = c.Health.ReleaseBackoff
	})
	for _, m := range []string{"asus", "dell"} {
		h.sim.set(m, func(m *simMember) { m.prepareFail = true })
	}
	h.startAll()
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if on := h.sim.runningOn(); len(on) > 0 {
			t.Fatalf("running on %v: with a fixed backoff the poisoned pair should have starved rpi5", on)
		}
	}
}

func TestStrayInstanceOnFollowerIsStopped(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	// Inject a copy on rpi5 behind everyone's back (a human, a stale
	// autostart). The violation is the injected fault itself; what is tested
	// is that it is removed.
	h.sim.mu.Lock()
	h.sim.m["rpi5"].running = true
	h.sim.mu.Unlock()
	h.waitRunning("asus", 5*time.Second)
}

// Two members partitioned: no etcd quorum is not simulated here (single etcd),
// but the leader must still keep running while the other two are cut off.
func TestFollowerPartitionDoesNotDisturbLeader(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.partition("dell", true)
	h.partition("rpi5", true)
	h.holdOne("asus", 8*time.Second)
}

func (h *harness) pulls(m string) int {
	h.sim.mu.Lock()
	defer h.sim.mu.Unlock()
	return h.sim.m[m].pulls
}

func (h *harness) prepares(m string) int {
	h.sim.mu.Lock()
	defer h.sim.mu.Unlock()
	return h.sim.m[m].prepares
}

func waitRefreshDone(t *testing.T, co *coord.Coord, token string, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < within {
		if done, err := co.GetString(context.Background(), co.RefreshDoneKey()); err == nil && done == token {
			return time.Since(start)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("refresh %q never reported done within %s", token, within)
	return 0
}

// TestRefreshRecreatesOnTheLeaderOnly: a refresh request makes the leader
// recreate its workload in place (same term), and only the leader.
func TestRefreshRecreatesOnTheLeaderOnly(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	co := coord.New(h.obs, h.cfg.EtcdPrefix, time.Second)
	rec, _, _, _, err := co.Leader(context.Background())
	if err != nil || rec == nil {
		t.Fatalf("leader: %v %v", rec, err)
	}
	before := map[string]int{}
	for _, m := range []string{"asus", "dell", "rpi5"} {
		before[m] = h.prepares(m)
	}
	if err := co.PutString(context.Background(), co.RefreshKey(), "t1"); err != nil {
		t.Fatal(err)
	}
	took := waitRefreshDone(t, co, "t1", 20*time.Second)
	t.Logf("refresh done after %s", took)
	if h.prepares("asus") != before["asus"]+1 {
		t.Errorf("asus prepared %d times for one refresh", h.prepares("asus")-before["asus"])
	}
	for _, m := range []string{"dell", "rpi5"} {
		if h.prepares(m) != before[m] {
			t.Errorf("follower %s prepared on a refresh", m)
		}
	}
	after, _, _, _, _ := co.Leader(context.Background())
	if after == nil || after.Epoch != rec.Epoch {
		t.Errorf("refresh changed the term: %v -> %v", rec, after)
	}
	if h.pulls("asus") == 0 {
		t.Error("the leader recreated without pulling first")
	}
	// The followers pull only after the leader reported done.
	deadline := time.Now().Add(10 * time.Second)
	for h.pulls("dell") == 0 || h.pulls("rpi5") == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("followers never pulled after the refresh: dell=%d rpi5=%d", h.pulls("dell"), h.pulls("rpi5"))
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.holdOne("asus", 3*time.Second)
	// The same token again is not a new request.
	time.Sleep(3 * time.Second)
	if h.prepares("asus") != before["asus"]+1 {
		t.Error("an applied token was applied again")
	}
}

// TestRefreshThatStaysUnhealthyFailsOver: a bad refresh makes the leader give
// the office up as any unhealthy workload does. The next leader runs the image
// already in its project (it never pulled): the refresh is not acknowledged,
// and the followers do not pull the bad image either.
func TestRefreshThatStaysUnhealthyFailsOver(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.Health.Failures, c.Health.MaxRestarts = 2, 1
	})
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	co := coord.New(h.obs, h.cfg.EtcdPrefix, time.Second)
	h.sim.set("asus", func(m *simMember) { m.unhealthy = true }) // the "new image" is bad on asus
	if err := co.PutString(context.Background(), co.RefreshKey(), "bad"); err != nil {
		t.Fatal(err)
	}
	h.waitRunning("dell", 60*time.Second)
	h.sim.set("asus", func(m *simMember) { m.unhealthy = false })
	h.holdOne("dell", 5*time.Second)
	if done, _ := co.GetString(context.Background(), co.RefreshDoneKey()); done == "bad" {
		t.Error("a refresh that failed over was acknowledged")
	}
	if h.pulls("dell") != 0 || h.pulls("rpi5") != 0 {
		t.Errorf("followers pulled a refresh that failed: dell=%d rpi5=%d", h.pulls("dell"), h.pulls("rpi5"))
	}
}
