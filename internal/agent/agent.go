// Package agent is the per-member state machine: campaign for the office,
// fence every member that may still run the workload, start it here, supervise
// it, and fence ourselves before our lease can expire.
//
// The invariant (README.md "Safety argument"): at most one member's project has
// running instances at any time, as long as at most one member is partitioned
// or wedged at once and the watchdog policy in force covers that member.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/incus"
	"github.com/alien43/pagurus/internal/lease"
	"github.com/alien43/pagurus/internal/watchdog"
)

// Runtime is everything the agent needs from Incus. *incus.Driver implements
// it; tests substitute a simulated cluster.
type Runtime interface {
	Identity(ctx context.Context) (member, arch string, err error)
	CheckProject(ctx context.Context, member string) error
	CheckCompose(ctx context.Context, member string) error
	CheckCgroup2() error
	MemberOnline(ctx context.Context, member string) (bool, error)
	List(ctx context.Context, member string) ([]incus.Instance, error)
	Up(ctx context.Context, member string) error
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
	cfg     *config.Config
	rt      Runtime
	cli     *clientv3.Client
	co      *coord.Coord
	wd      watchdog.Watchdog
	log     *slog.Logger
	alerter *Alerter
	http    *http.Client
	version string

	member  string
	arch    string
	agentID string

	// now is replaceable in tests.
	now func() time.Time

	mu               sync.Mutex
	role             string
	keeper           *lease.Keeper // current leadership term's lease
	epoch            int64
	localStopped     bool      // last ground-truth check found nothing running here
	unfitAfter       time.Time // while not leading and not localStopped, fit until then
	progressDeadline time.Time // main loop must report progress before this
	backoffUntil     time.Time

	fenceMu sync.Mutex // serialises self-fences (guardian vs. release)
}

type Options struct {
	Config  *config.Config
	Runtime Runtime
	Etcd    *clientv3.Client
	Log     *slog.Logger
	Version string
	// Member overrides Identity() (tests).
	Member string
}

func New(o Options) *Agent {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	host, _ := os.Hostname()
	return &Agent{
		cfg:     o.Config,
		rt:      o.Runtime,
		cli:     o.Etcd,
		co:      coord.New(o.Etcd, o.Config.EtcdPrefix, o.Config.Etcd.OpTimeout.D()),
		log:     o.Log,
		alerter: NewAlerter(o.Config.AlertURL, o.Config.Service, o.Log),
		http:    &http.Client{Timeout: o.Config.Health.Timeout.D()},
		version: o.Version,
		member:  o.Member,
		agentID: fmt.Sprintf("%s/%d/%d", host, os.Getpid(), time.Now().UnixNano()),
		now:     time.Now,
		role:    roleStarting,
	}
}

func (a *Agent) Member() string { return a.member }

// ---------------------------------------------------------------- fitness --

func (a *Agent) setRole(r string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.role = r
}

// beat records main-loop progress; the next beat must come within d (plus
// slack) or the agent is unfit — a wedged agent must not keep the host alive.
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

// Fit is the watchdog predicate. Leader: fit until the lease's safe deadline,
// after that only if the local stop is confirmed. Otherwise: fit while nothing
// runs here, or briefly while a stray copy is being stopped.
func (a *Agent) Fit() (bool, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	if now.After(a.progressDeadline) {
		return false, "main loop stalled"
	}
	if a.role == roleLeader && a.keeper != nil {
		if now.Before(a.keeper.SafeDeadline()) || a.localStopped {
			return true, ""
		}
		return false, "lease safe deadline passed and local stop is unconfirmed"
	}
	if a.localStopped || now.Before(a.unfitAfter) {
		return true, ""
	}
	return false, "not leading, but local instances are not confirmed stopped"
}

func (a *Agent) caps() coord.Caps {
	c := coord.Caps{SelfFenceLead: a.cfg.SelfFenceLead()}
	if a.wd != nil {
		c.WatchdogMode = a.wd.Mode()
		c.WatchdogTimeout = a.wd.Timeout()
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
	for _, in := range insts {
		if in.Type != "container" {
			return fmt.Errorf("instance %s is a %s; only containers can be fenced via cgroup.kill", in.Name, in.Type)
		}
	}
	return a.rt.CheckCompose(cctx, m)
}

// Run is the agent's lifetime. It returns when ctx ends, after releasing the
// office if held.
func (a *Agent) Run(ctx context.Context, wd watchdog.Watchdog) error {
	a.wd = wd
	a.mu.Lock()
	a.unfitAfter = a.now().Add(a.cfg.SelfFenceLead() + 30*time.Second)
	a.mu.Unlock()
	a.beat(time.Minute)
	wctx, wcancel := context.WithCancel(context.Background())
	wdDone := make(chan struct{})
	go func() { defer close(wdDone); wd.Run(wctx, a.Fit) }()
	defer func() { wcancel(); <-wdDone }()

	log := a.log.With("member", a.member)
	log.Info("agent starting", "version", a.version, "agent_id", a.agentID, "watchdog", wd.Mode(),
		"watchdog_timeout", wd.Timeout(), "fences_by_reboot", wd.FencesByReboot(),
		"ttl", a.cfg.Lease.TTL.D(), "unverified_policy", a.cfg.Fence.Unverified)

	// A crashed agent may have left a live term behind: resume it if etcd
	// still has our lease and office, so an agent restart is not a failover.
	resumed, epoch := a.tryResume(ctx)
	if resumed == nil {
		a.startupFence(ctx)
	}
	watchdog.Ready()

	for ctx.Err() == nil {
		var k *lease.Keeper
		if resumed != nil {
			k, resumed = resumed, nil
		} else {
			var err error
			if k, err = a.grant(ctx); err != nil {
				return nil // ctx done
			}
			if epoch, err = a.follow(ctx, k); err != nil {
				_ = k.Revoke(context.Background())
				if !errors.Is(err, lease.ErrLost) && ctx.Err() == nil {
					log.Warn("follower loop ended", "err", err)
				}
				continue
			}
		}
		a.lead(ctx, k, epoch)
		_ = k.Revoke(context.Background())
		a.clearResume()
	}
	watchdog.Stopping()
	return nil
}

// grant obtains a fresh lease (retrying while etcd is unreachable), starts its
// keeper, and registers this agent under it.
func (a *Agent) grant(ctx context.Context) (*lease.Keeper, error) {
	backoff := time.Second
	for {
		a.beat(a.cfg.EligibleInterval.D() + backoff)
		a.guardLocal(ctx)
		k, err := lease.Grant(ctx, a.cli, a.leaseOpts())
		if err == nil {
			go k.Run()
			reg := coord.Registration{AgentID: a.agentID, Version: a.version, Arch: a.arch, Caps: a.caps(), Started: a.now().UTC()}
			if err := a.co.Register(ctx, a.member, k.ID(), reg); err != nil {
				a.log.Warn("register failed", "err", err)
			}
			return k, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		a.log.Warn("cannot grant lease; etcd unreachable?", "err", err, "retry_in", backoff)
		if !sleep(ctx, backoff) {
			return nil, ctx.Err()
		}
		backoff = min(2*backoff, a.cfg.EligibleInterval.D())
	}
}

func (a *Agent) leaseOpts() lease.Options {
	return lease.Options{
		TTL: a.cfg.Lease.TTL.D(), RenewInterval: a.cfg.Lease.RenewInterval.D(),
		Margin: a.cfg.Lease.Margin.D(), OpTimeout: a.cfg.Etcd.OpTimeout.D(),
		Log: a.log.With("member", a.member),
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
func (a *Agent) guardLocal(ctx context.Context) {
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

// stopLocal stops this member's instances: Incus first, then cgroup.kill, each
// within its budget. It returns whether cgroupfs confirms nothing runs.
func (a *Agent) stopLocal(ctx context.Context, reason string) bool {
	a.fenceMu.Lock()
	defer a.fenceMu.Unlock()
	log := a.log.With("member", a.member, "reason", reason)
	start := a.now()
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.SelfFence.StopBudget.D())
	err := a.rt.Stop(sctx, a.member, 0)
	cancel()
	running, lerr := a.rt.LocalRunning(a.member)
	if err != nil || lerr != nil || len(running) > 0 {
		log.Warn("incus stop did not confirm; escalating to cgroup.kill", "stop_err", err, "still_running", running)
		kctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.cfg.SelfFence.KillBudget.D())
		kerr := a.rt.Kill(kctx, a.member)
		cancel()
		running, lerr = a.rt.LocalRunning(a.member)
		if kerr != nil {
			log.Error("cgroup.kill failed", "err", kerr)
		}
	}
	ok := lerr == nil && len(running) == 0
	a.setLocalStopped(ok)
	if ok {
		log.Info("local instances stopped", "took", a.now().Sub(start).Round(time.Millisecond))
	} else {
		a.alerter.Alert("self-fence-failed", "CANNOT stop local instances — watchdog is the backstop",
			"member", a.member, "running", running, "err", lerr, "watchdog", a.caps().WatchdogMode)
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

// ---------------------------------------------------------------- resume ---

type resumeState struct {
	Lease  int64  `json:"lease"`
	Epoch  int64  `json:"epoch"`
	Member string `json:"member"`
}

func (a *Agent) resumePath() string {
	return filepath.Join(a.cfg.StateDir, a.cfg.Service+".json")
}

func (a *Agent) saveResume(k *lease.Keeper, epoch int64) {
	if err := os.MkdirAll(a.cfg.StateDir, 0o700); err != nil {
		a.log.Warn("state dir", "err", err)
		return
	}
	b, _ := json.Marshal(resumeState{Lease: int64(k.ID()), Epoch: epoch, Member: a.member})
	tmp := a.resumePath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err == nil {
		_ = os.Rename(tmp, a.resumePath())
	}
}

func (a *Agent) clearResume() { _ = os.Remove(a.resumePath()) }

func (a *Agent) tryResume(ctx context.Context) (*lease.Keeper, int64) {
	b, err := os.ReadFile(a.resumePath())
	if err != nil {
		return nil, 0
	}
	a.clearResume()
	var st resumeState
	if json.Unmarshal(b, &st) != nil || st.Member != a.member {
		return nil, 0
	}
	k, err := lease.Resume(ctx, a.cli, clientv3.LeaseID(st.Lease), a.leaseOpts())
	if err != nil {
		a.log.Info("previous term not resumable", "lease", st.Lease, "err", err)
		return nil, 0
	}
	rec, epoch, lid, _, err := a.co.Leader(ctx)
	if err != nil || rec == nil || rec.Member != a.member || lid != k.ID() || epoch != st.Epoch {
		a.log.Info("previous term superseded; not resuming", "err", err)
		_ = k.Revoke(ctx)
		return nil, 0
	}
	go k.Run()
	a.log.Warn("resuming leadership term after agent restart", "epoch", epoch, "remaining", k.Remaining().Round(time.Millisecond))
	return k, epoch
}

// -------------------------------------------------------------- follower ---

// follow waits as a follower until this member wins the office. It keeps
// guarding the local ground truth meanwhile.
func (a *Agent) follow(ctx context.Context, k *lease.Keeper) (int64, error) {
	a.setRole(roleFollower)
	log := a.log.With("member", a.member)
	lastWhy := ""
	why := func(s string, kv ...any) {
		if s != lastWhy {
			log.Info("not campaigning: "+s, kv...)
			lastWhy = s
		}
	}
	for {
		a.beat(2 * a.cfg.EligibleInterval.D())
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if k.IsLost() {
			return 0, lease.ErrLost
		}
		a.guardLocal(ctx)

		rec, _, _, rev, err := a.co.Leader(ctx)
		if err != nil {
			why("etcd unreachable", "err", err)
			sleep(ctx, a.cfg.EligibleInterval.D())
			continue
		}
		if rec != nil {
			why("office held by "+rec.Member, "holder", rec.Member)
			a.waitOffice(ctx, rev, a.cfg.EligibleInterval.D())
			continue
		}
		pin, err := a.co.Pin(ctx)
		if err != nil {
			continue
		}
		if pin != "" && pin != a.member {
			why("office pinned to "+pin, "pin", pin)
			a.waitOffice(ctx, rev, a.cfg.EligibleInterval.D())
			continue
		}
		a.mu.Lock()
		backoff := a.backoffUntil
		a.mu.Unlock()
		if d := backoff.Sub(a.now()); d > 0 {
			why("backing off after giving up the office", "for", d.Round(time.Second))
			sleep(ctx, min(d, a.cfg.EligibleInterval.D()))
			continue
		}
		if err := a.eligible(ctx); err != nil {
			why("not eligible", "err", err)
			sleep(ctx, a.cfg.EligibleInterval.D())
			continue
		}
		delay := time.Duration(a.cfg.Rank(a.member)) * a.cfg.Grace.D()
		if pin == a.member {
			delay = 0
		}
		if delay > 0 && a.officeTakenWithin(ctx, rev, delay) {
			continue
		}
		won, epoch, err := a.co.Campaign(ctx, k.ID(), coord.LeaderRecord{Member: a.member, AgentID: a.agentID, Acquired: a.now().UTC()})
		if err != nil {
			why("campaign failed", "err", err)
			continue
		}
		if won {
			log.Info("won the office", "epoch", epoch)
			return epoch, nil
		}
		lastWhy = ""
	}
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
	return nil
}

// waitOffice blocks until something under the service prefix changes after
// rev, or d elapses.
func (a *Agent) waitOffice(ctx context.Context, rev int64, d time.Duration) {
	wctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	for resp := range a.co.WatchOffice(wctx, rev) {
		if resp.Err() != nil || len(resp.Events) > 0 {
			return
		}
	}
}

// officeTakenWithin sleeps d (the preference delay) but returns true early if
// someone takes the office meanwhile.
func (a *Agent) officeTakenWithin(ctx context.Context, rev int64, d time.Duration) bool {
	deadline := a.now().Add(d)
	for {
		left := deadline.Sub(a.now())
		if left <= 0 {
			return false
		}
		a.waitOffice(ctx, rev, left)
		if ctx.Err() != nil {
			return true
		}
		rec, _, _, r, err := a.co.Leader(ctx)
		if err != nil || rec != nil {
			return true
		}
		rev = r
	}
}
