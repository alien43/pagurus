package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/incus"
	"github.com/alien43/pagurus/internal/lease"
)

var (
	errReleased  = errors.New("office released")
	errSelfFence = errors.New("self-fence")
)

// lead runs one leadership term. It returns once the workload is stopped here
// (or, if that cannot be confirmed, once the failure is alerted and left to the
// watchdog) and the term is over.
//
// Two contexts govern a term:
//   - ctx, the agent's lifetime. Its end asks for a graceful release.
//   - term, ended by the guardian when the lease cannot be kept or the office
//     was taken away. Its end means "stop now", and the guardian does the
//     stopping itself, so a main loop stuck in a slow call cannot delay it.
func (a *Agent) lead(ctx context.Context, k *lease.Keeper, epoch int64) {
	log := a.log.With("member", a.member, "epoch", epoch)
	acquired := a.now()
	a.mu.Lock()
	// From here on the workload may run here: only a confirmed stop may set
	// localStopped again (it is what keeps the watchdog fed past the deadline).
	// unfitAfter is zeroed so an unconfirmed stop at the end of the term is
	// unfit at once, not after some window left over from before.
	a.role, a.keeper, a.epoch, a.localStopped, a.unfitAfter = roleLeader, k, epoch, false, time.Time{}
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.role, a.keeper = roleFollower, nil
		a.mu.Unlock()
	}()
	a.saveResume(k, epoch)

	term, endTerm := context.WithCancelCause(context.Background())
	defer endTerm(errReleased)
	var releasing atomic.Bool
	fenced := make(chan struct{})
	go a.guardian(term, endTerm, k, epoch, &releasing, fenced)

	// op is cancelled by either: every leader-side operation runs under it.
	op, cancelOp := context.WithCancel(term)
	defer cancelOp()
	stopAfter := context.AfterFunc(ctx, cancelOp)
	defer stopAfter()

	t := &termState{a: a, k: k, epoch: epoch, acquired: acquired, log: log, op: op}

	finish := func() {
		if errors.Is(context.Cause(term), errSelfFence) {
			<-fenced
			return
		}
		reason, backoff := "agent shutdown", time.Duration(0)
		switch {
		case t.releaseReason != "":
			reason, backoff = t.releaseReason, t.releaseBackoff
		case ctx.Err() == nil:
			reason = "term ended"
		}
		releasing.Store(true)
		a.release(k, epoch, reason, backoff, log)
		endTerm(errReleased)
	}

	if err := t.fenceOthers(); err != nil {
		finish()
		return
	}
	if err := a.co.Guarded(op, epoch, k.ID(), a.co.OpPutMS(a.member, coord.MemberState{
		State: coord.MayRun, Epoch: epoch, By: a.member, Reason: "starting", At: a.now().UTC(), Caps: a.caps(),
	})); err != nil {
		log.Warn("cannot record may-run; not starting", "err", err)
		if errors.Is(err, coord.ErrNotLeader) {
			// Deposed already. A resumed term may still have the workload
			// running from before the restart: stop it, but there is no
			// office left to release.
			releasing.Store(true)
			endTerm(errReleased)
			<-fenced
			a.stopLocal(context.WithoutCancel(ctx), "deposed before start")
			return
		}
		finish()
		return
	}
	t.supervise()
	finish()
}

// guardian ends the term and fences locally when the lease is lost, its safe
// deadline approaches without renewals, or the office is taken from us.
func (a *Agent) guardian(term context.Context, endTerm context.CancelCauseFunc, k *lease.Keeper, epoch int64, releasing *atomic.Bool, fenced chan<- struct{}) {
	defer close(fenced)
	deposed := a.watchDeposed(term, epoch)
	lead := a.cfg.SelfFenceLead()
	for {
		trigger := k.SafeDeadline().Add(-lead)
		timer := time.NewTimer(max(trigger.Sub(a.now()), 0))
		var reason string
		select {
		case <-term.Done():
			timer.Stop()
			return
		case <-k.Lost():
			reason = "lease lost"
		case <-deposed:
			if releasing.Load() {
				timer.Stop()
				return
			}
			reason = "office taken away (leader key deleted or replaced)"
		case <-timer.C:
			if a.now().Before(k.SafeDeadline().Add(-lead)) {
				continue // a renewal moved the deadline
			}
			reason = fmt.Sprintf("lease renewals failing (last error: %v); %s to safe deadline", k.LastError(), k.Remaining().Round(time.Millisecond))
		}
		timer.Stop()
		endTerm(fmt.Errorf("%w: %s", errSelfFence, reason))
		a.alerter.Alert("self-fence", "self-fencing: "+reason, "member", a.member, "epoch", epoch)
		a.stopLocal(context.Background(), "self-fence: "+reason)
		return
	}
}

// watchDeposed signals when the leader key no longer carries our epoch.
// Watch failures are retried; partitions are the lease's job, not this one's.
func (a *Agent) watchDeposed(term context.Context, epoch int64) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for term.Err() == nil {
			_, cur, _, rev, err := a.co.Leader(term)
			if err == nil && cur != epoch {
				close(ch)
				return
			}
			if err != nil {
				sleep(term, time.Second)
				continue
			}
			for resp := range a.co.Client().Watch(clientv3.WithRequireLeader(term), a.co.LeaderKey(), clientv3.WithRev(rev+1)) {
				if resp.Err() != nil {
					break
				}
				for _, ev := range resp.Events {
					if ev.Type == clientv3.EventTypeDelete || ev.Kv.CreateRevision != epoch {
						close(ch)
						return
					}
				}
			}
			sleep(term, time.Second)
		}
	}()
	return ch
}

// release gives the office up gracefully: stop here, confirm, record, resign.
// If the stop cannot be confirmed the office is not resigned explicitly and
// the member state stays may-run: the next leader must fence us.
func (a *Agent) release(k *lease.Keeper, epoch int64, reason string, backoff time.Duration, log *slog.Logger) {
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
		log.Warn("release: local stop unconfirmed; leaving member state may-run for the next leader to fence")
		return
	}
	if err := a.co.Guarded(bg, epoch, k.ID(), a.co.OpPutMS(a.member, coord.MemberState{
		State: coord.Stopped, Epoch: epoch, By: a.member, Reason: "released: " + reason, At: a.now().UTC(), Caps: a.caps(),
	})); err != nil {
		log.Warn("release: could not record stopped", "err", err)
	}
	if err := a.co.Resign(bg, epoch); err != nil {
		log.Warn("release: resign failed; the lease will expire on its own", "err", err)
	}
	log.Info("office released", "reason", reason)
}

// termState carries one term's working state.
type termState struct {
	a        *Agent
	k        *lease.Keeper
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

// fenceMember tries to establish that m runs nothing. It returns done=true
// when that is confirmed or covered by a policy, else a reason to wait.
func (t *termState) fenceMember(m string, st coord.MemberState) (done bool, why string, retry time.Duration) {
	retry = t.a.cfg.Fence.RetryInterval.D()
	a := t.a
	cctx, cancel := context.WithTimeout(t.op, a.cfg.Fence.APITimeout.D())
	defer cancel()

	online, err := a.rt.MemberOnline(cctx, m)
	if err == nil && online {
		insts, lerr := a.rt.List(cctx, m)
		if lerr == nil && !incus.AllStopped(insts) {
			t.log.Warn("fencing: stopping instances on member via Incus API", "peer", m, "running", incus.Running(insts))
			if serr := a.rt.Stop(cctx, m, 0); serr != nil {
				t.log.Warn("fencing: stop via API failed", "peer", m, "err", serr)
			}
			insts, lerr = a.rt.List(cctx, m)
		}
		if lerr == nil && incus.AllStopped(insts) {
			for _, in := range insts {
				if in.Location != m {
					return false, fmt.Sprintf("BLOCKED: instance %s of project %s is located on %s — project pinning is broken", in.Name, a.cfg.Project(m), in.Location), retry
				}
			}
			if st.State == coord.MayRun {
				err := a.co.Guarded(t.op, t.epoch, t.k.ID(), a.co.OpPutMS(m, coord.MemberState{
					State: coord.Stopped, Epoch: st.Epoch, By: a.member, At: a.now().UTC(), Caps: st.Caps,
					Reason: fmt.Sprintf("fenced via Incus API by epoch %d", t.epoch),
				}))
				if err != nil {
					return false, "cannot record fence: " + err.Error(), retry
				}
				t.log.Info("fenced via Incus API (confirmed stopped)", "peer", m, "their_epoch", st.Epoch)
			}
			return true, "", 0
		}
		if lerr == nil {
			// The cluster sees m and its instances, and they are not stopped:
			// no record can override that.
			return false, fmt.Sprintf("instances on %s still not stopped after API stop", m), retry
		}
		err = lerr
	}
	reach := "offline in the Incus cluster"
	if err != nil {
		reach = "Incus API error: " + err.Error()
	}

	if st.State != coord.MayRun {
		t.log.Info("member unreachable but recorded stopped; nothing to fence", "peer", m, "reach", reach, "record", st.State)
		return true, "", 0
	}
	since := a.now().Sub(t.acquired)
	if st.Caps.FencesByReboot {
		wait := st.Caps.WatchdogTimeout + a.cfg.Fence.RebootMargin.D()
		if since >= wait {
			t.log.Warn("member unreachable; its armed watchdog has had time to reset it — treating as fenced", "peer", m, "waited", since.Round(time.Second))
			return true, "", 0
		}
		return false, fmt.Sprintf("waiting %s for %s's watchdog (%s)", (wait - since).Round(time.Second), m, reach), min(retry, wait-since)
	}
	if a.cfg.Fence.Unverified == config.UnverifiedProceed {
		wait := a.cfg.Fence.UnverifiedWait.D()
		if since >= wait {
			a.alerter.Alert("fence-unverified", "proceeding WITHOUT a verified fence (policy=proceed): trusting the member's self-fence",
				"member", m, "reach", reach, "their_epoch", st.Epoch, "waited", since.Round(time.Second))
			return true, "", 0
		}
		return false, fmt.Sprintf("waiting %s before trusting %s's self-fence (%s)", (wait - since).Round(time.Second), m, reach), min(retry, wait-since)
	}
	return false, fmt.Sprintf("BLOCKED: %s may still run the workload (epoch %d), is %s, and has no reboot watchdog; "+
		"waiting for it to come back, or run `pagurus fence-ack --member %s` once you have verified it is down", m, st.Epoch, reach, m), retry
}

// ---------------------------------------------------------------- supervise -

func (t *termState) start() error {
	a := t.a
	a.beat(UpTimeout)
	uctx, cancel := context.WithTimeout(t.op, UpTimeout)
	defer cancel()
	if err := a.rt.Up(uctx, a.member); err != nil {
		return fmt.Errorf("up: %w", err)
	}
	if err := a.rt.EnforceNoAutostart(uctx, a.member); err != nil {
		return fmt.Errorf("enforce boot.autostart=false: %w", err)
	}
	insts, err := a.rt.List(uctx, a.member)
	if err != nil {
		return err
	}
	if len(insts) == 0 {
		return errors.New("up created no instances")
	}
	for _, in := range insts {
		if in.Location != a.member {
			return fmt.Errorf("%w: instance %s landed on %s, not %s", errMisplaced, in.Name, in.Location, a.member)
		}
		if in.Type != "container" {
			return fmt.Errorf("%w: instance %s is a %s", errMisplaced, in.Name, in.Type)
		}
	}
	t.publish()
	return nil
}

var errMisplaced = errors.New("refusing to run")

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
	if err := a.co.Guarded(t.op, t.epoch, t.k.ID(), a.co.OpPutEndpoint(ep, t.k.ID())); err != nil {
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
			return true // counted as unhealthy below
		}
		t.log.Info("workload started")
		return true
	}
	if !startOnce() {
		return
	}
	startedAt, lastAudit := a.now(), a.now()
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
		if a.now().Sub(lastAudit) >= a.cfg.AuditInterval.D() {
			lastAudit = a.now()
			t.audit()
		}
	}
}
