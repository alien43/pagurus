package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/fence"
	"github.com/alien43/pagurus/internal/incus"
)

// lead runs one term, handed over by `pagurus term` (conch's child). epoch is
// CONCH_REV and lease conch's session lease. It returns the term process's
// exit code: 0 after a confirmed local stop, so conch's fence confirms at once
// and conch resigns; non-zero otherwise.
//
// Two contexts govern a term:
//   - ctx, the agent's lifetime. Its end asks for a graceful release.
//   - lost, ended when the term process goes (conch lost the lease or died).
//     Its end means "stop acting now": conch's fence stops the workload.
func (a *Agent) lead(ctx, lost context.Context, epoch int64, lease clientv3.LeaseID) int {
	log := a.log.With("member", a.member, "epoch", epoch)
	acquired := a.now()
	a.mu.Lock()
	// From here on the workload may run here: only a confirmed stop may set
	// localStopped again.
	a.role, a.localStopped, a.unfitAfter = roleLeader, false, time.Time{}
	a.mu.Unlock()
	defer a.setRole(roleFollower)
	a.beat(time.Minute)
	hctx, stopHB := context.WithCancel(context.Background())
	defer stopHB()
	go a.heartbeat(hctx)

	op, cancelOp := context.WithCancel(lost)
	defer cancelOp()
	stopAfter := context.AfterFunc(ctx, cancelOp)
	defer stopAfter()

	t := &termState{a: a, lease: lease, epoch: epoch, acquired: acquired, log: log, op: op}
	log.Info("won the office", "lease", fmt.Sprintf("%x", int64(lease)))

	finish := func() int {
		if lost.Err() != nil {
			log.Warn("term lost (conch ended it); conch's fence stops the workload")
			return 70
		}
		reason, backoff := "agent shutdown", time.Duration(0)
		switch {
		case t.releaseReason != "":
			reason, backoff = t.releaseReason, t.releaseBackoff
		case ctx.Err() == nil:
			reason = "term ended"
		}
		if a.release(epoch, lease, reason, backoff, log) {
			return 0
		}
		return 1
	}

	if err := a.co.Guarded(op, epoch, lease, a.co.OpPutLeader(coord.LeaderRecord{
		Member: a.member, Epoch: epoch, AgentID: a.agentID, Acquired: acquired.UTC(),
	}, lease)); err != nil {
		log.Warn("cannot record the office; not starting", "err", err)
		return finish()
	}
	// What made us campaign may have changed while conch waited.
	if pin, err := a.co.Pin(op); err == nil && pin != "" && pin != a.member {
		t.giveUp("office pinned to "+pin, 0)
		return finish()
	}
	if err := a.checkConfigSkew(op); err != nil {
		t.giveUp(err.Error(), a.cfg.EligibleInterval.D())
		return finish()
	}
	if err := t.fenceOthers(); err != nil {
		return finish()
	}
	if err := a.co.Guarded(op, epoch, lease, a.co.OpPutMS(a.member, coord.MemberState{
		State: coord.MayRun, Epoch: epoch, By: a.member, Reason: "starting", At: a.now().UTC(), Caps: a.caps(),
	})); err != nil {
		log.Warn("cannot record may-run; not starting", "err", err)
		return finish()
	}
	t.supervise()
	return finish()
}

// release gives the office up gracefully: stop here, confirm, record. conch
// then runs the fence (which confirms at once) and resigns. If the stop cannot
// be confirmed the member state stays may-run: conch's fence tries again, and
// failing that the next leader must fence us.
func (a *Agent) release(epoch int64, lease clientv3.LeaseID, reason string, backoff time.Duration, log *slog.Logger) bool {
	log.Info("releasing the office", "reason", reason)
	bg := context.Background()
	sctx, cancel := context.WithTimeout(bg, a.cfg.SelfFence.StopBudget.D()+5*time.Second)
	err := a.rt.Stop(sctx, a.member, 5*time.Second)
	cancel()
	running, lerr := a.rt.LocalRunning(a.member)
	ok := err == nil && lerr == nil && len(running) == 0
	if !ok {
		ok = a.stopLocal(bg, "release: "+reason)
	} else {
		a.setLocalStopped(true)
	}
	a.mu.Lock()
	a.backoffUntil = a.now().Add(backoff)
	a.mu.Unlock()
	if !ok {
		log.Warn("release: local stop unconfirmed; leaving member state may-run for conch's fence and the next leader")
		return false
	}
	if err := a.co.Guarded(bg, epoch, lease, a.co.OpPutMS(a.member, coord.MemberState{
		State: coord.Stopped, Epoch: epoch, By: a.member, Reason: "released: " + reason, At: a.now().UTC(), Caps: a.caps(),
	})); err != nil {
		log.Warn("release: could not record stopped", "err", err)
	}
	log.Info("office released", "reason", reason)
	return true
}

// termState carries one term's working state.
type termState struct {
	a        *Agent
	lease    clientv3.LeaseID
	epoch    int64
	acquired time.Time
	log      *slog.Logger
	op       context.Context

	releaseReason  string
	releaseBackoff time.Duration
}

func (t *termState) giveUp(reason string, backoff time.Duration) {
	t.releaseReason, t.releaseBackoff = reason, backoff
}

// ------------------------------------------------------------ fence others -

// fenceOthers makes sure no other member runs the workload before we start
// it. It returns nil once every other member is fenced, or the op error.
func (t *termState) fenceOthers() error {
	a := t.a
	var pending []string
	for _, m := range a.cfg.Members {
		if m != a.member {
			pending = append(pending, m)
		}
	}
	alerted := map[string]bool{}
	for {
		a.beat(a.cfg.Fence.RetryInterval.D() + time.Duration(len(pending)+1)*a.cfg.Fence.APITimeout.D())
		next := a.cfg.Fence.RetryInterval.D()
		states, err := a.co.MemberStates(t.op)
		if err == nil {
			var left []string
			for _, m := range pending {
				done, why, retry := t.fenceMember(m, states[m])
				if !done {
					next = min(next, max(retry, 100*time.Millisecond))
					left = append(left, m)
					if why != "" && !alerted[m+why] {
						alerted[m+why] = true
						if strings.HasPrefix(why, "BLOCKED") {
							a.alerter.Alert("fence-blocked", why, "member", m, "epoch", t.epoch)
						} else {
							t.log.Warn("fence pending", "peer", m, "why", why)
						}
					}
				}
			}
			pending = left
		} else if t.op.Err() == nil {
			t.log.Warn("cannot read member states", "err", err)
		}
		if len(pending) == 0 {
			return nil
		}
		if !sleep(t.op, next) {
			return t.op.Err()
		}
	}
}

// fenceVia names a peer fence in a member record; the API stop keeps the
// prototype's wording, which records and operators already know.
func fenceVia(kind string) string {
	if kind == fence.KindIncusAPIStop {
		return "Incus API"
	}
	return kind
}

// fenceMember tries to establish that m runs nothing. It returns done=true
// when that is confirmed or covered by a policy, else a reason to wait.
//
// It runs the configured peer chain (by default incus-api-stop, then
// watchdog-wait); the first fence that does not skip decides. When every
// fence skips (m unreachable and no fence applies), the record and
// fence.unverified decide, as before.
func (t *termState) fenceMember(m string, st coord.MemberState) (done bool, why string, retry time.Duration) {
	retry = t.a.cfg.Fence.RetryInterval.D()
	a := t.a
	since := a.now().Sub(t.acquired)
	pt := fence.PeerTarget{
		Target:          fence.Target{Member: m, Project: a.cfg.Project(m)},
		MayRun:          st.State == coord.MayRun,
		FencesByReboot:  st.Caps.FencesByReboot,
		WatchdogTimeout: st.Caps.WatchdogTimeout,
		SinceAcquired:   since,
	}
	out, by := a.peerChain().Attempt(t.op, pt)
	switch out.Status {
	case fence.Wait:
		if out.Retry > 0 {
			retry = min(retry, out.Retry)
		}
		return false, out.Why, retry
	case fence.Confirmed:
		if !out.Verified {
			t.log.Warn("member unreachable; treating as fenced", "peer", m, "by", by, "waited", since.Round(time.Second))
			return true, "", 0
		}
		if st.State == coord.MayRun {
			err := a.co.Guarded(t.op, t.epoch, t.lease, a.co.OpPutMS(m, coord.MemberState{
				State: coord.Stopped, Epoch: st.Epoch, By: a.member, At: a.now().UTC(), Caps: st.Caps,
				Reason: fmt.Sprintf("fenced via %s by epoch %d", fenceVia(by), t.epoch),
			}))
			if err != nil {
				return false, "cannot record fence: " + err.Error(), retry
			}
			t.log.Info("fenced (confirmed stopped)", "peer", m, "by", by, "their_epoch", st.Epoch)
		}
		return true, "", 0
	}

	reach := out.Why
	if reach == "" {
		reach = "no peer fence applies"
	}
	if st.State != coord.MayRun {
		t.log.Info("member unreachable but recorded stopped; nothing to fence", "peer", m, "reach", reach, "record", st.State)
		return true, "", 0
	}
	if a.cfg.Fence.Unverified == config.UnverifiedProceed && st.Caps.NoVerifiedSelfFence {
		return false, fmt.Sprintf("BLOCKED: %s may still run the workload (epoch %d), is unreachable (%s), and advertises no verified self-fence, so policy=proceed cannot trust it; "+
			"run `pagurus fence-ack --member %s` once you have verified it is down", m, st.Epoch, reach, m), retry
	}
	if a.cfg.Fence.Unverified == config.UnverifiedProceed {
		// The victim's own self-fence timing, if it advertised it: it may
		// still be fencing itself until its lead + margin past its expiry.
		wait := max(a.cfg.Fence.UnverifiedWait.D(), st.Caps.SelfFenceLead+st.Caps.Margin)
		if since >= wait {
			a.alerter.Alert("fence-unverified", "proceeding WITHOUT a verified fence (policy=proceed): trusting the member's self-fence",
				"member", m, "reach", reach, "their_epoch", st.Epoch, "waited", since.Round(time.Second))
			return true, "", 0
		}
		return false, fmt.Sprintf("waiting %s before trusting %s's self-fence (%s)", (wait - since).Round(time.Second), m, reach), min(retry, wait-since)
	}
	return false, fmt.Sprintf("BLOCKED: %s may still run the workload (epoch %d), is unreachable (%s), and no peer fence confirmed; "+
		"waiting for it to come back, or run `pagurus fence-ack --member %s` once you have verified it is down", m, st.Epoch, reach, m), retry
}

// ---------------------------------------------------------------- supervise -

// start brings the workload up in three steps, so the window in which a
// deposed leader can still start a copy is one quick call, not a whole `up`:
//
//  1. Prepare: pull and create, nothing runs (the slow part);
//  2. a linearizable read that our election key still holds the office;
//  3. Start; then the same read again, stopping at once if it moved.
//
// Incus has no fencing token on start (PUT /1.0/instances/<n>/state ignores
// If-Match), so this narrows the window; it cannot close it.
func (t *termState) start() error {
	a := t.a
	a.beat(UpTimeout)
	uctx, cancel := context.WithTimeout(t.op, UpTimeout)
	defer cancel()
	if err := a.rt.Prepare(uctx, a.member); err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	if err := t.authorised(uctx); err != nil {
		return err
	}
	if err := a.rt.Start(uctx, a.member); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	if still, err := a.co.StillLeader(context.WithoutCancel(uctx), t.epoch, t.lease); err == nil && !still {
		a.alerter.Alert("deposed-at-start", "the office moved while starting; stopping at once", "member", a.member, "epoch", t.epoch)
		a.stopLocal(context.WithoutCancel(uctx), "deposed while starting")
		return fmt.Errorf("%w: the office moved while starting", errDeposed)
	}
	if err := a.rt.EnforceNoAutostart(uctx, a.member); err != nil {
		return fmt.Errorf("enforce boot.autostart=false: %w", err)
	}
	insts, err := a.rt.List(uctx, a.member)
	if err != nil {
		return err
	}
	if len(insts) == 0 {
		return errors.New("start left no instances in the project")
	}
	for _, in := range insts {
		if in.Location != a.member {
			return fmt.Errorf("%w: instance %s landed on %s, not %s", errMisplaced, in.Name, in.Location, a.member)
		}
	}
	a.setTypes(insts)
	if err := a.checkCgroupLayout(insts); err != nil {
		return fmt.Errorf("%w: %v", errMisplaced, err)
	}
	t.publish()
	return nil
}

var (
	errMisplaced = errors.New("refusing to run")
	errDeposed   = errors.New("deposed")
)

// authorised is the last check before anything may run: the term is still
// live and our election key still holds the office (a fresh read).
func (t *termState) authorised(ctx context.Context) error {
	if t.op.Err() != nil {
		return fmt.Errorf("%w: term ended before start", errDeposed)
	}
	still, err := t.a.co.StillLeader(ctx, t.epoch, t.lease)
	if err != nil {
		return fmt.Errorf("%w: cannot confirm the office before start: %v", errDeposed, err)
	}
	if !still {
		return fmt.Errorf("%w: the office moved before start", errDeposed)
	}
	return nil
}

func (t *termState) publish() {
	a := t.a
	ep := coord.Endpoint{Member: a.member, Epoch: t.epoch, Addresses: map[string]string{}}
	if insts, err := a.rt.List(t.op, a.member); err == nil {
		for _, in := range insts {
			if ip, err := a.rt.IPv4(t.op, a.member, in.Name); err == nil {
				ep.Addresses[in.Name] = ip
			}
		}
	}
	if err := a.co.Guarded(t.op, t.epoch, t.lease, a.co.OpPutEndpoint(ep, t.lease)); err != nil {
		t.log.Warn("cannot publish endpoint", "err", err)
	}
}

func (t *termState) health() error {
	a := t.a
	cctx, cancel := context.WithTimeout(t.op, a.cfg.Fence.APITimeout.D())
	defer cancel()
	insts, err := a.rt.List(cctx, a.member)
	if err != nil {
		return err
	}
	if len(insts) == 0 {
		return errors.New("no instances")
	}
	for _, in := range insts {
		if in.Status != incus.StatusRunning {
			return fmt.Errorf("%s is %s", in.Name, in.Status)
		}
	}
	if a.cfg.Health.HTTP == "" {
		return nil
	}
	ip, err := a.rt.IPv4(cctx, a.member, a.cfg.Health.Instance)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, strings.ReplaceAll(a.cfg.Health.HTTP, "{ip}", ip), nil)
	if err != nil {
		return err
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("health %s: HTTP %d", a.cfg.Health.HTTP, resp.StatusCode)
	}
	return nil
}

// audit stops copies of the workload on other members that no leader started
// (e.g. by hand). Stopping them only ever strengthens the invariant.
func (t *termState) audit() {
	a := t.a
	for _, m := range a.cfg.Members {
		if m == a.member {
			continue
		}
		cctx, cancel := context.WithTimeout(t.op, a.cfg.Fence.APITimeout.D())
		if on, err := a.rt.MemberOnline(cctx, m); err == nil && on {
			if insts, err := a.rt.List(cctx, m); err == nil {
				if r := incus.Running(insts); len(r) > 0 {
					a.alerter.Alert("rogue", "instances running on a non-leader member — stopping them", "member", m, "instances", r, "epoch", t.epoch)
					if err := a.rt.Stop(cctx, m, 0); err != nil {
						t.log.Error("could not stop rogue instances", "peer", m, "err", err)
					}
				}
			}
		}
		cancel()
	}
}

// supervise starts the workload and keeps it healthy for the rest of the term.
func (t *termState) supervise() {
	a := t.a
	hc := a.cfg.Health
	restarts, failures := 0, 0
	startOnce := func() bool {
		if err := t.start(); err != nil {
			if t.op.Err() != nil {
				return false
			}
			t.log.Error("start failed", "err", err)
			if errors.Is(err, errMisplaced) {
				a.alerter.Alert("misplaced", err.Error(), "member", a.member)
				t.giveUp(err.Error(), time.Hour)
				return false
			}
			if errors.Is(err, errDeposed) {
				t.log.Warn("not starting", "err", err)
				t.giveUp(err.Error(), 0)
				return false
			}
			return true // counted as unhealthy below
		}
		t.log.Info("workload started")
		return true
	}
	if !startOnce() {
		return
	}
	startedAt, lastAudit := a.now(), time.Time{}
	tick := time.NewTicker(hc.Interval.D())
	defer tick.Stop()
	healthy := false
	for {
		a.beat(hc.Interval.D() + 2*a.cfg.Fence.APITimeout.D())
		select {
		case <-t.op.Done():
			return
		case <-tick.C:
		}
		if pin, err := a.co.Pin(t.op); err == nil && pin != "" && pin != a.member {
			t.giveUp("office pinned to "+pin, 0)
			return
		}
		// The office can also be taken away outside conch (its key deleted):
		// conch only notices a lost lease.
		if still, err := a.co.StillLeader(t.op, t.epoch, t.lease); err == nil && !still {
			a.alerter.Alert("deposed", "office taken away (election key gone); stopping", "member", a.member, "epoch", t.epoch)
			t.giveUp("deposed", 0)
			return
		}
		err := t.health()
		if t.op.Err() != nil {
			return
		}
		switch {
		case err == nil:
			if !healthy {
				t.log.Info("workload healthy")
			}
			healthy, failures = true, 0
		case a.now().Sub(startedAt) < hc.StartPeriod.D():
			// still starting
		default:
			healthy = false
			failures++
			t.log.Warn("workload unhealthy", "err", err, "consecutive", failures)
			if failures < hc.Failures {
				break
			}
			if restarts >= hc.MaxRestarts {
				a.alerter.Alert("unhealthy", "workload stays unhealthy; giving the office up", "member", a.member, "err", err.Error())
				t.giveUp("unhealthy: "+err.Error(), hc.ReleaseBackoff.D())
				return
			}
			restarts++
			failures = 0
			t.log.Warn("restarting workload in place", "attempt", restarts)
			if !startOnce() {
				return
			}
			startedAt = a.now()
		}
		// Right after a start, audit every tick: a deposed leader's late
		// start is caught in one health interval, not one audit interval.
		fast := a.now().Sub(startedAt) < 3*hc.Interval.D()
		if fast || a.now().Sub(lastAudit) >= a.cfg.AuditInterval.D() {
			lastAudit = a.now()
			t.audit()
		}
	}
}
