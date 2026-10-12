// Package agent is the per-member daemon. It keeps this member's local ground
// truth (nothing of ours runs here unless we hold the office), and, while
// eligible, runs `conch elect` for the office. conch owns the election, the
// lease, its validity bound, the self-fence before expiry (`pagurus
// fence-self`) and the watchdog fitness of a term. When conch wins, it starts
// `pagurus term`, which hands the term back to this daemon: fence every other
// member that may still run the workload, start it here, supervise it.
//
// The invariant (README.md "Safety argument"): at most one member's project has
// running instances at any time, as long as at most one member is partitioned
// or wedged at once and the watchdog policy in force covers that member.
package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/fence"
	"github.com/alien43/pagurus/internal/incus"
	"github.com/alien43/pagurus/internal/metrics"
	"github.com/alien43/pagurus/internal/watchdog"
)

// Runtime is everything the agent needs from Incus. *incus.Driver implements
// it; tests substitute a simulated cluster.
type Runtime interface {
	Identity(ctx context.Context) (member, arch string, err error)
	CheckProject(ctx context.Context, member string) error
	// CheckStart / Start are the configured starter (internal/start).
	CheckStart(ctx context.Context, member string) error
	Prepare(ctx context.Context, member string) error
	Pull(ctx context.Context, member string) error
	CheckCgroup2() error
	MemberOnline(ctx context.Context, member string) (bool, error)
	List(ctx context.Context, member string) ([]incus.Instance, error)
	Start(ctx context.Context, member string) error
	EnforceNoAutostart(ctx context.Context, member string) error
	Stop(ctx context.Context, member string, graceful time.Duration) error
	LocalRunning(member string) ([]string, error)
	Kill(ctx context.Context, member string) error
	IPv4(ctx context.Context, member, name string) (string, error)
}

const (
	roleStarting = "starting"
	roleFollower = "follower"
	roleLeader   = "leader"
)

// UpTimeout bounds one `incus-compose up` (image pulls included).
var UpTimeout = 5 * time.Minute

type Agent struct {
	cfg      *config.Config
	rt       Runtime
	cli      *clientv3.Client
	co       *coord.Coord
	wd       watchdog.Watchdog
	log      *slog.Logger
	alerter  *Alerter
	http     *http.Client
	version  string
	versions map[string]string
	conchCmd []string // conch binary (argv prefix)
	fenceCmd []string // `pagurus fence-self -c <cfg>` (argv prefix; --member is appended)
	termCmd  []string // `pagurus term --socket` (argv prefix; the socket is appended)
	procOut  io.Writer

	member string
	arch   string
	// types are the instance types in this member's project, learned at
	// preflight and after each start; the self-fence chain is chosen by them.
	typesMu      sync.Mutex
	types        []string
	persistTypes bool // record types for fence-self (Run only)
	agentID      string

	// now is replaceable in tests.
	now func() time.Time

	relay *notifyRelay // conch's watchdog pets (nil with the watchdog off)
	m     *metrics.Set

	mu               sync.Mutex
	role             string
	localStopped     bool      // last ground-truth check found nothing running here
	skewAlerted      string    // the last config-skew reason alerted, to alert once
	unfitAfter       time.Time // while conch is not running and not localStopped, fit until then
	progressDeadline time.Time // a term's loop must report progress before this (heartbeat)
	backoffUntil     time.Time
	conch            *exec.Cmd // the running `conch elect`, nil if none
	termActive       bool      // conch holds the office and its term runs here
	termPID          int       // the `pagurus term` process of the active term
	termRan          bool      // this conch's term has run: it is resigning, not campaigning

	localMu sync.Mutex // serialises guardLocal against the start of a term
	fenceMu sync.Mutex // serialises self-fences
}

type Options struct {
	Config  *config.Config
	Runtime Runtime
	Etcd    *clientv3.Client
	Log     *slog.Logger
	Version string
	// Versions of the tools the agent drives, recorded in its registration.
	Versions map[string]string
	// Member overrides Identity() (tests).
	Member string
	// Conch is the conch argv prefix (default: config conch.path).
	Conch []string
	// FenceCmd runs this member's self chain: `pagurus fence-self -c <cfg>`;
	// the agent appends --member <member>. TermCmd is `pagurus term
	// --socket`; the agent appends the socket path.
	FenceCmd, TermCmd []string
	// ProcOutput receives conch's (and its children's) output (default stderr).
	ProcOutput io.Writer
	// Metrics, if set, receives the agent's metrics (else a private set).
	Metrics *metrics.Set
}

func New(o Options) *Agent {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if len(o.Conch) == 0 {
		o.Conch = []string{o.Config.Conch.Path}
	}
	if o.ProcOutput == nil {
		o.ProcOutput = os.Stderr
	}
	host, _ := os.Hostname()
	if o.Metrics == nil {
		o.Metrics = metrics.New(o.Config.Service, o.Member)
	}
	return &Agent{
		m:        o.Metrics,
		cfg:      o.Config,
		rt:       o.Runtime,
		cli:      o.Etcd,
		co:       coord.New(o.Etcd, o.Config.EtcdPrefix, o.Config.Etcd.OpTimeout.D()),
		log:      o.Log,
		alerter:  NewAlerter(o.Config.AlertURL, o.Config.Service, o.Log),
		http:     &http.Client{Timeout: o.Config.Health.Timeout.D()},
		version:  o.Version,
		versions: o.Versions,
		conchCmd: o.Conch,
		fenceCmd: o.FenceCmd,
		termCmd:  o.TermCmd,
		procOut:  o.ProcOutput,
		member:   o.Member,
		agentID:  fmt.Sprintf("%s/%d/%d", host, os.Getpid(), time.Now().UnixNano()),
		now:      time.Now,
		role:     roleStarting,
	}
}

func (a *Agent) Member() string { return a.member }

// ---------------------------------------------------------------- fitness --

func (a *Agent) setRole(r string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.role = r
	a.m.Bool("pagurus_role", r == roleLeader)
}

// fitness is Fit, recorded as pagurus_fit each time the petter evaluates it.
func (a *Agent) fitness() (bool, string) {
	ok, why := a.Fit()
	a.m.Bool("pagurus_fit", ok)
	return ok, why
}

// sampleLocal records pagurus_local_running from cgroupfs every health
// interval: each member's own ground truth, independent of the office.
func (a *Agent) sampleLocal(ctx context.Context) {
	for {
		if running, err := a.rt.LocalRunning(a.member); err == nil {
			a.m.SetGauge("pagurus_local_running", float64(len(running)))
		}
		a.m.Bool("pagurus_self_chain_verified", a.selfChain().Verified(a.selfTarget()))
		a.fitness() // records pagurus_fit even when no watchdog backend evaluates it
		if !sleep(ctx, a.cfg.Health.Interval.D()) {
			return
		}
	}
}

// beat records a term's progress; the next beat must come within d (plus
// slack) or the term heartbeat goes stale and conch counts the term unfit.
func (a *Agent) beat(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.progressDeadline = a.now().Add(d + 30*time.Second)
}

func (a *Agent) setLocalStopped(ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !ok && a.localStopped {
		a.unfitAfter = a.now().Add(a.cfg.SelfFenceLead() + 2*a.cfg.EligibleInterval.D())
	}
	a.localStopped = ok
}

// Fit is the watchdog predicate. pagurus keeps no fitness rules of its own
// for a term: while conch runs, it is fit exactly while conch's last pet
// (relayed) is fresh. While conch is not running, nothing of ours may run
// here: fit while that is confirmed, or briefly while a stray copy (or the
// remains of a term whose conch died) is being stopped.
func (a *Agent) Fit() (bool, string) {
	a.mu.Lock()
	running := a.conch != nil
	// conch's pets matter only while it holds a term here: the term runs,
	// or it ended without our workload confirmed stopped. A conch that is
	// merely campaigning (or withdrawing, which can take as long as etcd is
	// unreachable) guards nothing, and its silence must not starve the
	// watchdog: on a reboot-armed member that would reset a healthy follower
	// on every etcd outage longer than the watchdog timeout.
	holding := running && (a.termActive || (a.termRan && !a.localStopped))
	stopped, until := a.localStopped, a.unfitAfter
	a.mu.Unlock()
	if holding && a.relay != nil {
		return a.relay.Fresh()
	}
	if (running && a.relay == nil) || stopped || a.now().Before(until) {
		return true, ""
	}
	return false, "local instances are not confirmed stopped and no term guards them"
}

// relayFreshness is how stale conch's last pet may be: conch pets every
// quarter of it (it is given WATCHDOG_USEC = half of it).
func (a *Agent) relayFreshness() time.Duration { return a.wd.Timeout() / 2 }

func (a *Agent) caps() coord.Caps {
	c := coord.Caps{SelfFenceLead: a.cfg.ConchFenceBudget(), NoVerifiedSelfFence: !a.selfChain().Verified(a.selfTarget()), Margin: a.cfg.Lease.Margin.D()}
	if a.wd != nil {
		c.WatchdogMode = a.wd.Mode()
		c.WatchdogTimeout = a.wd.Timeout()
		if a.relay != nil {
			// conch stops petting; the relay notices within its freshness
			// window; then the backend's own timeout runs.
			c.WatchdogTimeout += a.relayFreshness()
		}
		c.FencesByReboot = a.wd.FencesByReboot()
	}
	return c
}

// ------------------------------------------------------------------- run ---

// Preflight establishes identity and refuses to run on a misconfigured
// cluster. Every check here guards an assumption of the safety argument.
func (a *Agent) Preflight(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	m, arch, err := a.rt.Identity(cctx)
	if err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	if a.member != "" && a.member != m {
		return fmt.Errorf("configured member %q but local Incus is %q", a.member, m)
	}
	a.member, a.arch = m, arch
	a.m.SetMember(m)
	if !slices.Contains(a.cfg.Members, m) {
		return fmt.Errorf("member %q is not in members %v", m, a.cfg.Members)
	}
	if err := a.rt.CheckCgroup2(); err != nil {
		return err
	}
	if err := a.rt.CheckProject(cctx, m); err != nil {
		return err
	}
	insts, err := a.rt.List(cctx, m)
	if err != nil {
		return err
	}
	a.setTypes(insts)
	if err := a.checkCgroupLayout(insts); err != nil {
		return err
	}
	if t := a.selfTarget(); len(t.Types) > 0 && !a.selfChain().Verified(t) {
		a.log.Warn("no fence in the self chain applies to this project's instances: no verified self-fence (advertised; peers will not trust it under proceed)",
			"types", t.Types, "chain", a.cfg.SelfFences())
	}
	return a.rt.CheckStart(cctx, m)
}

// Run is the agent's lifetime. It returns when ctx ends, after a held term
// has been released and conch has exited.
func (a *Agent) Run(ctx context.Context, wd watchdog.Watchdog) error {
	a.wd = wd
	if len(a.fenceCmd) == 0 || len(a.termCmd) == 0 {
		return errors.New("agent: FenceCmd and TermCmd are required")
	}
	if err := os.MkdirAll(a.cfg.StateDir, 0o700); err != nil {
		return fmt.Errorf("state dir: %w", err)
	}
	a.typesMu.Lock()
	a.persistTypes = true // validate runs Preflight unprivileged; only Run records
	a.typesMu.Unlock()
	if err := WriteTypes(a.cfg, a.selfTarget().Types); err != nil {
		return fmt.Errorf("recording instance types for fence-self: %w", err)
	}
	if wd.Mode() != config.WatchdogOff {
		r, err := listenNotify(filepath.Join(a.cfg.StateDir, a.cfg.Service+".notify"), a.relayFreshness)
		if err != nil {
			return err
		}
		defer r.Close()
		a.relay = r
	}
	ts, err := a.listenTerms(ctx)
	if err != nil {
		return err
	}
	defer ts.Close()

	a.mu.Lock()
	a.unfitAfter = a.now().Add(a.cfg.SelfFenceLead() + 30*time.Second)
	a.mu.Unlock()
	wctx, wcancel := context.WithCancel(context.Background())
	wdDone := make(chan struct{})
	go func() { defer close(wdDone); wd.Run(wctx, a.fitness) }()
	go a.sampleLocal(wctx)
	defer func() { wcancel(); <-wdDone }()

	log := a.log.With("member", a.member)
	log.Info("agent starting", "version", a.version, "agent_id", a.agentID, "watchdog", wd.Mode(),
		"watchdog_timeout", wd.Timeout(), "fences_by_reboot", wd.FencesByReboot(),
		"ttl", a.cfg.Lease.TTL.D(), "unverified_policy", a.cfg.Fence.Unverified, "office", a.co.Office())

	go a.registerLoop(ctx)
	go a.followRefreshes(ctx)
	a.startupFence(ctx)
	watchdog.Ready()
	a.setRole(roleFollower)
	a.follow(ctx)
	watchdog.Stopping()
	return nil
}

// followRefreshes pulls into this member's own project once the leader has
// applied a refresh and reported it healthy, so a follower that takes over
// later starts from the same image, while a refresh that fails on the leader
// leaves the followers on the old one (a rollback by failover). A follower
// never recreates or starts anything here.
func (a *Agent) followRefreshes(ctx context.Context) {
	seen, seeded := "", false
	tick := time.NewTicker(a.cfg.EligibleInterval.D())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		done, err := a.co.GetString(ctx, a.co.RefreshDoneKey())
		if err != nil {
			continue
		}
		if !seeded { // what was done before this agent started is not news
			seen, seeded = done, true
			continue
		}
		if done == seen || a.inTerm() {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, UpTimeout)
		err = a.rt.Pull(pctx, a.member)
		cancel()
		if err != nil {
			a.log.Warn("pull after the leader's refresh failed; retrying", "member", a.member, "token", done, "err", err)
			continue
		}
		a.log.Info("pulled after the leader's refresh", "member", a.member, "token", done)
		seen = done
	}
}

// registerLoop keeps reg/<member> (config hash, caps, versions) under a lease
// of its own. Not safety-critical: peers read it for the config-skew check.
func (a *Agent) registerLoop(ctx context.Context) {
	ttl := int64(a.cfg.Lease.TTL.D() / time.Second)
	for ctx.Err() == nil {
		cctx, cancel := context.WithTimeout(ctx, a.cfg.Etcd.OpTimeout.D())
		g, err := a.cli.Grant(cctx, ttl)
		cancel()
		if err != nil {
			sleep(ctx, a.cfg.EligibleInterval.D())
			continue
		}
		reg := coord.Registration{AgentID: a.agentID, Version: a.version, Arch: a.arch, Caps: a.caps(), Started: a.now().UTC(), Versions: a.versions, ConfigHash: a.cfg.SafetyHash()}
		if err := a.co.Register(ctx, a.member, g.ID, reg); err != nil {
			a.log.Warn("register failed", "err", err)
		}
		ka, err := a.cli.KeepAlive(ctx, g.ID)
		if err == nil {
			for range ka {
			}
		}
		rctx, cancel := context.WithTimeout(context.Background(), a.cfg.Etcd.OpTimeout.D())
		_, _ = a.cli.Revoke(rctx, g.ID)
		cancel()
		sleep(ctx, time.Second)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ------------------------------------------------------- local ground truth -

// guardLocal verifies (from cgroupfs) that nothing of ours runs here while we
// do not lead, stops anything that does, and reports a confirmed stop to etcd
// so future leaders need not fence us.
//
// It never runs during a term (that copy is the term's), and a term never
// starts while it runs.
func (a *Agent) guardLocal(ctx context.Context) {
	a.localMu.Lock()
	defer a.localMu.Unlock()
	if a.inTerm() {
		return
	}
	running, err := a.rt.LocalRunning(a.member)
	if err != nil {
		a.log.Error("cannot read local cgroups", "err", err)
		a.setLocalStopped(false)
		return
	}
	if len(running) > 0 {
		a.setLocalStopped(false)
		a.alerter.Alert("stray-local", "instances running here without the office — stopping them", "member", a.member, "instances", running)
		a.stopLocal(ctx, "stray instances while not leading")
		return
	}
	a.setLocalStopped(true)
	a.reportStopped(ctx)
}

func (a *Agent) reportStopped(ctx context.Context) {
	states, err := a.co.MemberStates(ctx)
	if err != nil {
		return
	}
	ms, ok := states[a.member]
	if !ok || ms.State == coord.Stopped {
		return
	}
	rec := coord.MemberState{State: coord.Stopped, Epoch: ms.Epoch, By: a.member, Reason: "self-verified", At: a.now().UTC(), Caps: a.caps()}
	if err := a.co.ReportSelfStopped(ctx, a.member, ms.ModRev, rec); err != nil {
		a.log.Warn("could not record self-verified stop", "err", err)
		return
	}
	a.log.Info("recorded member state stopped (self-verified)", "epoch", ms.Epoch)
}

// selfChain is this member's configured self-fence chain over the runtime.
func (a *Agent) selfChain() fence.Chain {
	c, err := fence.BuildSelf(a.rt, a.cfg.SelfFences())
	if err != nil {
		panic(err) // config.Validate builds the same chain
	}
	return c
}

// peerChain is the configured peer-fence chain over the runtime.
func (a *Agent) peerChain() fence.PeerChain {
	c, err := fence.BuildPeer(a.rt, a.cfg.Fence.RebootMargin.D(), a.cfg.PeerFences())
	if err != nil {
		panic(err) // config.Validate builds the same chain
	}
	return c
}

func (a *Agent) selfTarget() fence.Target {
	a.typesMu.Lock()
	defer a.typesMu.Unlock()
	return fence.Target{Member: a.member, Project: a.cfg.Project(a.member), Types: slices.Clone(a.types)}
}

// errCgroupLayout: Incus says a container runs here but cgroupfs shows no
// payload cgroup for it. LocalRunning would then report a running member as
// stopped, which every self-check and self-fence relies on, so the agent
// refuses rather than fall back to "nothing running".
var errCgroupLayout = errors.New("cgroup layout not recognised")

// checkCgroupLayout requires that every container Incus reports Running in
// this member's project has a live payload cgroup (lxc.payload.<project>_<name>)
// that LocalRunning sees. VMs have no payload cgroup and are not checked here.
func (a *Agent) checkCgroupLayout(insts []incus.Instance) error {
	var running []string
	for _, in := range insts {
		if in.Status == incus.StatusRunning && (in.Type == "" || in.Type == "container") {
			running = append(running, in.Name)
		}
	}
	if len(running) == 0 {
		return nil
	}
	local, err := a.rt.LocalRunning(a.member)
	if err != nil {
		return fmt.Errorf("%w: reading cgroupfs: %v", errCgroupLayout, err)
	}
	for _, n := range running {
		if !slices.Contains(local, n) {
			return fmt.Errorf("%w: Incus reports container %s of project %s Running, but cgroupfs has no live lxc.payload.%s_%s; "+
				"is this a liblxc/Incus version with a different payload cgroup name? Refusing: the self-fence would see it as stopped",
				errCgroupLayout, n, a.cfg.Project(a.member), a.cfg.Project(a.member), n)
		}
	}
	return nil
}

// setTypes records the distinct instance types of insts (sorted).
func (a *Agent) setTypes(insts []incus.Instance) {
	var ts []string
	for _, in := range insts {
		if in.Type != "" && !slices.Contains(ts, in.Type) {
			ts = append(ts, in.Type)
		}
	}
	slices.Sort(ts)
	a.typesMu.Lock()
	a.types = ts
	persist := a.persistTypes
	a.typesMu.Unlock()
	if !persist {
		return
	}
	if err := WriteTypes(a.cfg, ts); err != nil {
		a.log.Warn("cannot record instance types for fence-self", "err", err)
	}
}

func stepResult(st fence.Step) string {
	switch {
	case st.Skipped:
		return "skipped"
	case st.Stopped:
		return "confirmed"
	default:
		return "failed"
	}
}

func typesPath(cfg *config.Config) string { return filepath.Join(cfg.StateDir, cfg.Service+".types") }

// WriteTypes records the project's instance types for `pagurus fence-self`,
// which must choose its chain without waiting on Incus.
func WriteTypes(cfg *config.Config, ts []string) error {
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return err
	}
	tmp := typesPath(cfg) + ".tmp"
	if err := os.WriteFile(tmp, []byte(strings.Join(ts, "\n")), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, typesPath(cfg))
}

// ReadTypes is what the agent last recorded; ok=false if nothing was.
func ReadTypes(cfg *config.Config) (ts []string, ok bool) {
	b, err := os.ReadFile(typesPath(cfg))
	if err != nil {
		return nil, false
	}
	for _, t := range strings.Split(string(b), "\n") {
		if t != "" {
			ts = append(ts, t)
		}
	}
	return ts, true
}

// stopLocal runs this member's self-fence chain (by default incus-stop, then
// cgroup.kill), each fence within its budget. It returns whether a fence
// confirmed that nothing runs.
func (a *Agent) stopLocal(ctx context.Context, reason string) bool {
	a.fenceMu.Lock()
	defer a.fenceMu.Unlock()
	log := a.log.With("member", a.member, "reason", reason)
	start := a.now()
	res := a.selfChain().Run(context.WithoutCancel(ctx), a.selfTarget(), log)
	for _, st := range res.Steps {
		a.m.Inc("pagurus_fence_runs_total", "chain", "self", "kind", st.Kind, "result", stepResult(st))
	}
	ok := res.Stopped
	a.setLocalStopped(ok)
	if ok {
		log.Info("local instances stopped", "by", res.By, "took", a.now().Sub(start).Round(time.Millisecond))
	} else {
		running, lerr := a.rt.LocalRunning(a.member)
		a.alerter.Alert("self-fence-failed", "CANNOT confirm local instances stopped — watchdog is the backstop",
			"member", a.member, "chain", res.String(), "running", running, "err", lerr, "watchdog", a.caps().WatchdogMode)
	}
	return ok
}

// startupFence: a fresh agent holds no lease, so nothing may run here. Retry
// until cgroupfs confirms it; the watchdog stops being fed if that takes long.
func (a *Agent) startupFence(ctx context.Context) {
	for ctx.Err() == nil {
		a.beat(a.cfg.SelfFenceLead() + a.cfg.EligibleInterval.D())
		running, err := a.rt.LocalRunning(a.member)
		if err == nil && len(running) == 0 {
			a.setLocalStopped(true)
			return
		}
		a.log.Warn("startup: local instances present without a lease; stopping", "running", running, "err", err)
		if a.stopLocal(ctx, "startup without a lease") {
			return
		}
		sleep(ctx, a.cfg.EligibleInterval.D())
	}
}

// -------------------------------------------------------------- follower ---

// campaign is how this member should campaign right now, if at all.
type campaign struct {
	delay time.Duration // --campaign-after
	why   string        // non-empty: do not campaign
}

// desired decides whether to campaign, and with what delay: rank × grace,
// or none for the member the office is pinned to.
func (a *Agent) desired(ctx context.Context) campaign {
	pin, err := a.co.Pin(ctx)
	if err != nil {
		return campaign{why: "etcd unreachable: " + err.Error()}
	}
	if pin != "" && pin != a.member {
		return campaign{why: "office pinned to " + pin}
	}
	a.mu.Lock()
	backoff := a.backoffUntil
	a.mu.Unlock()
	if d := backoff.Sub(a.now()); d > 0 {
		return campaign{why: fmt.Sprintf("backing off after giving up the office (%s left)", d.Round(time.Second))}
	}
	if err := a.eligible(ctx); err != nil {
		return campaign{why: "not eligible: " + err.Error()}
	}
	if pin == a.member {
		return campaign{}
	}
	return campaign{delay: time.Duration(a.cfg.Rank(a.member)) * a.cfg.Grace.D()}
}

// follow guards the local ground truth and keeps `conch elect` running while
// this member should campaign. It returns when ctx ends and conch is gone.
func (a *Agent) follow(ctx context.Context) {
	log := a.log.With("member", a.member)
	lastWhy := ""
	why := func(s string) {
		if s != lastWhy {
			log.Info("not campaigning: " + s)
			lastWhy = s
		}
	}
	var (
		done      chan error // conch's exit, while it runs
		running   campaign
		spawnedAt time.Time
		failures  int
	)
	tick := time.NewTicker(a.cfg.EligibleInterval.D())
	defer tick.Stop()
	for {
		if done == nil {
			if ctx.Err() != nil {
				return
			}
			a.guardLocal(ctx)
			want := a.desired(ctx)
			if want.why != "" {
				why(want.why)
			} else {
				lastWhy = ""
				var err error
				if done, err = a.spawnConch(want.delay); err != nil {
					log.Error("cannot start conch", "err", err)
					done = nil
				} else {
					running, spawnedAt = want, a.now()
				}
			}
		} else if a.inTerm() {
			// conch holds the office: the term owns this member now.
		} else if a.conchTermRan() {
			// The term is over and conch is resigning (fence, resign, exit):
			// not a campaign to withdraw. Signalling it here would cut that
			// short and make a clean release exit non-zero.
		} else if ctx.Err() != nil {
			a.stopConch("agent shutdown")
		} else {
			a.guardLocal(ctx)
			if want := a.desired(ctx); want.why != "" || want.delay != running.delay {
				reason := want.why
				if reason == "" {
					reason = fmt.Sprintf("campaign delay changed from %s to %s", running.delay, want.delay)
				}
				log.Info("withdrawing from the campaign", "reason", reason)
				a.stopConch(reason)
			}
		}

		var ctxDone <-chan struct{} // once ctx has ended, wait on conch and the tick only
		if ctx.Err() == nil {
			ctxDone = ctx.Done()
		}
		select {
		case err := <-done:
			a.conchExited(err)
			done = nil
			if a.now().Sub(spawnedAt) < 2*time.Second {
				failures++
			} else {
				failures = 0
			}
			if failures > 0 {
				d := min(time.Duration(failures)*a.cfg.EligibleInterval.D(), 30*time.Second)
				log.Warn("conch exited quickly; backing off", "err", err, "for", d)
				sleep(ctx, d)
			}
		case <-tick.C:
		case <-ctxDone:
			if done == nil {
				return
			}
		}
	}
}

func (a *Agent) conchTermRan() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.conch != nil && a.termRan
}

func (a *Agent) inTerm() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.termActive
}

// eligible: may this member host the workload right now?
func (a *Agent) eligible(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, a.cfg.Fence.APITimeout.D())
	defer cancel()
	online, err := a.rt.MemberOnline(cctx, a.member)
	if err != nil {
		return fmt.Errorf("local Incus: %w", err)
	}
	if !online {
		return errors.New("this member is not Online in the Incus cluster")
	}
	if err := a.rt.CheckProject(cctx, a.member); err != nil {
		return err
	}
	a.mu.Lock()
	ok := a.localStopped
	a.mu.Unlock()
	if !ok {
		return errors.New("local instances not confirmed stopped")
	}
	return a.checkConfigSkew(cctx)
}

// checkConfigSkew refuses to campaign while a live peer registration carries
// a different config.SafetyHash: a leader fencing a peer that runs another
// TTL, margin or fence chain would do so on the wrong assumptions.
func (a *Agent) checkConfigSkew(ctx context.Context) error {
	regs, err := a.co.Registrations(ctx)
	if err != nil {
		return fmt.Errorf("reading registrations: %w", err)
	}
	mine := a.cfg.SafetyHash()
	for m, r := range regs {
		if m == a.member || r.ConfigHash == "" || r.ConfigHash == mine {
			continue
		}
		why := fmt.Sprintf("config skew: %s runs config %s, this member %s; refusing to campaign until they agree", m, r.ConfigHash, mine)
		a.mu.Lock()
		first := a.skewAlerted != why
		a.skewAlerted = why
		a.mu.Unlock()
		if first {
			a.alerter.Alert("config-skew", why, "peer", m, "peer_hash", r.ConfigHash, "hash", mine)
		}
		return errors.New(why)
	}
	a.mu.Lock()
	a.skewAlerted = ""
	a.mu.Unlock()
	return nil
}
