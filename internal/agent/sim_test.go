package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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
}

type simMember struct {
	online    bool // Online in the Incus cluster
	isolated  bool // this member's own Incus cannot reach the cluster
	running   bool
	unhealthy bool // instances exist but are not Running
	stopFail  bool // `incus stop` does nothing
	killFail  bool // cgroup.kill does not empty the cgroup
	upFail    bool
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
	if n > 1 {
		c.violations++
		c.t.Errorf("AT-MOST-ONE VIOLATED after %s: running on %v", what, on)
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
func (r *simRT) CheckCompose(context.Context, string) error       { return nil }
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

func (r *simRT) Up(_ context.Context, m string) error {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
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
	if r.c.m[m].running {
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
	name   string
	rt     *simRT
	proxy  *testutil.Proxy
	cancel context.CancelFunc
	done   chan struct{}
	agent  *Agent
	dog    *simDog
}

type harness struct {
	t     *testing.T
	cfg   *config.Config
	sim   *simCluster
	etcd  string
	nodes map[string]*node
	state string
}

func testConfig(t *testing.T, mutate func(*config.Config)) *config.Config {
	t.Helper()
	raw := `{
	  "service": "svc", "compose": "/dev/null", "project_prefix": "sgl-svc",
	  "members": ["asus", "dell", "rpi5"],
	  "etcd": {"endpoints": ["x"], "op_timeout": "400ms"},
	  "lease": {"ttl": "5s", "renew_interval": "500ms", "margin": "500ms"},
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
	h := &harness{t: t, cfg: testConfig(t, mutate), sim: newSim(t, "asus", "dell", "rpi5"),
		etcd: testutil.Etcd(t), nodes: map[string]*node{}, state: t.TempDir()}
	t.Cleanup(func() {
		for _, n := range h.nodes {
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
	if os.Getenv("SINGLETON_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("node", name)
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// start launches (or restarts) member's agent. dog may be nil (watchdog off).
func (h *harness) start(name string, dog *simDog) *node {
	cfg := *h.cfg
	cfg.StateDir = h.state + "/" + name
	px := testutil.NewProxy(h.t, h.etcd)
	cli := testutil.Client(h.t, px.Addr())
	rt := &simRT{c: h.sim, me: name}
	a := New(Options{Config: &cfg, Runtime: rt, Etcd: cli, Log: logger(h.t, name), Version: "test"})
	if err := a.Preflight(h.t.Context()); err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{name: name, rt: rt, proxy: px, cancel: cancel, done: make(chan struct{}), agent: a, dog: dog}
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
	h.nodes[name] = n
	return n
}

// crash: the agent process dies without running any cleanup. Its etcd
// connection goes silent and its runtime handle becomes inert.
func (n *node) crash() {
	n.rt.mu.Lock()
	n.rt.dead = true
	n.rt.mu.Unlock()
	n.proxy.SetBlackhole(true)
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
	h.nodes[name].proxy.SetBlackhole(on)
	h.sim.set(name, func(m *simMember) { m.online, m.isolated = !on, on })
}

func (h *harness) leader() string {
	co := coord.New(testutil.Client(h.t, h.etcd), h.cfg.EtcdPrefix, time.Second)
	rec, _, _, _, err := co.Leader(context.Background())
	if err != nil || rec == nil {
		return ""
	}
	return rec.Member
}

func (h *harness) memberStates() map[string]coord.MemberState {
	co := coord.New(testutil.Client(h.t, h.etcd), h.cfg.EtcdPrefix, time.Second)
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

// Agent crash + quick restart (within TTL): the term resumes, no failover.
func TestRestartWithinTTLResumes(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	h.nodes["asus"].crash()
	time.Sleep(500 * time.Millisecond)
	h.start("asus", nil)
	h.holdOne("asus", 8*time.Second) // > TTL: a failover would have happened
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
	co := coord.New(testutil.Client(t, h.etcd), h.cfg.EtcdPrefix, time.Second)
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
	co := coord.New(testutil.Client(t, h.etcd), h.cfg.EtcdPrefix, time.Second)
	if err := co.SetPin(context.Background(), "rpi5"); err != nil {
		t.Fatal(err)
	}
	took := h.waitRunning("rpi5", 10*time.Second)
	t.Logf("move took %s", took)
	_ = co.SetPin(context.Background(), "")
	h.holdOne("rpi5", 3*time.Second) // no automatic failback
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
