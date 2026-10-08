package fence

import (
	"context"
	"fmt"
	"time"

	"github.com/alien43/pagurus/internal/incus"
)

// Peer fences are what a new leader does to another member that may still
// run the workload. Unlike a self-fence, a peer fence is retried across the
// leader's fencing loop and may have to wait (for a watchdog to fire, for an
// instance the cluster still sees running), so it reports one of three
// outcomes instead of a plain stop/no-stop.

const (
	KindIncusAPIStop = "incus-api-stop"
	KindWatchdogWait = "watchdog-wait"
)

// PeerTarget is a member to fence, with what the leader knows about it.
type PeerTarget struct {
	Target
	// MayRun: the member's record says it may be running the workload.
	MayRun bool
	// FencesByReboot and WatchdogTimeout come from the member's advertised
	// capabilities.
	FencesByReboot  bool
	WatchdogTimeout time.Duration
	// SinceAcquired is how long ago this leader acquired the office.
	SinceAcquired time.Duration
}

// Status of one peer fence attempt.
type Status int

const (
	// Skip: this fence cannot act on the target (not applicable,
	// unreachable, failed); the chain moves on.
	Skip Status = iota
	// Wait: not fenced yet and no later fence may override this one (e.g. the
	// cluster sees the instance still running); the chain stops, retry later.
	Wait
	// Confirmed: the target runs nothing. Verified distinguishes an observed
	// stop (recorded as stopped) from one assumed after a wait (the may-run
	// record is kept, so every later leader deals with it again).
	Confirmed
)

// Outcome of one peer fence attempt.
type Outcome struct {
	Status   Status
	Verified bool
	// Why explains a Skip or Wait; a Wait whose Why starts with "BLOCKED"
	// needs a human.
	Why   string
	Retry time.Duration
}

// PeerFence is one way a leader fences another member.
type PeerFence interface {
	Kind() string
	Budget() time.Duration
	Attempt(ctx context.Context, t PeerTarget) Outcome
}

// Remote is what incus-api-stop needs: the cluster's view of a member.
type Remote interface {
	MemberOnline(ctx context.Context, member string) (bool, error)
	List(ctx context.Context, member string) ([]incus.Instance, error)
	Stop(ctx context.Context, member string, graceful time.Duration) error
}

// IncusAPIStop stops the member's project through the Incus cluster API and
// confirms from the cluster's view. It applies whatever the record says: if
// the cluster can see an instance running, no record can override that.
type IncusAPIStop struct {
	R Remote
	B time.Duration
}

func (f IncusAPIStop) Kind() string          { return KindIncusAPIStop }
func (f IncusAPIStop) Budget() time.Duration { return f.B }
func (f IncusAPIStop) Attempt(ctx context.Context, t PeerTarget) Outcome {
	ctx, cancel := context.WithTimeout(ctx, f.B)
	defer cancel()
	online, err := f.R.MemberOnline(ctx, t.Member)
	if err != nil {
		return Outcome{Status: Skip, Why: "Incus API error: " + err.Error()}
	}
	if !online {
		return Outcome{Status: Skip, Why: "offline in the Incus cluster"}
	}
	insts, err := f.R.List(ctx, t.Member)
	if err == nil && !incus.AllStopped(insts) {
		_ = f.R.Stop(ctx, t.Member, 0) // judged by the re-list below
		insts, err = f.R.List(ctx, t.Member)
	}
	if err != nil {
		return Outcome{Status: Skip, Why: "Incus API error: " + err.Error()}
	}
	if !incus.AllStopped(insts) {
		return Outcome{Status: Wait, Why: fmt.Sprintf("instances on %s still not stopped after API stop", t.Member)}
	}
	for _, in := range insts {
		if in.Location != t.Member {
			return Outcome{Status: Wait, Why: fmt.Sprintf("BLOCKED: instance %s of project %s is located on %s — project pinning is broken", in.Name, t.Project, in.Location)}
		}
	}
	return Outcome{Status: Confirmed, Verified: true}
}

// WatchdogWait trusts a member whose armed watchdog resets its host: once the
// watchdog timeout plus a margin has passed since acquisition, the member has
// either been fenced by its own watchdog or stopped itself.
type WatchdogWait struct {
	Margin time.Duration
}

func (f WatchdogWait) Kind() string          { return KindWatchdogWait }
func (f WatchdogWait) Budget() time.Duration { return 0 }
func (f WatchdogWait) Attempt(_ context.Context, t PeerTarget) Outcome {
	if !t.MayRun || !t.FencesByReboot {
		return Outcome{Status: Skip}
	}
	wait := t.WatchdogTimeout + f.Margin
	if t.SinceAcquired >= wait {
		return Outcome{Status: Confirmed}
	}
	return Outcome{Status: Wait, Why: fmt.Sprintf("waiting %s for %s's watchdog", (wait - t.SinceAcquired).Round(time.Second), t.Member), Retry: wait - t.SinceAcquired}
}

// ExecPeer runs an operator command against a member that may still run the
// workload: a power fence (smart plug, IPMI, PDU). Only a member recorded
// may-run is fenced; a confirmed command counts as a verified stop.
type ExecPeer struct {
	*Exec
}

func (f ExecPeer) Attempt(ctx context.Context, t PeerTarget) Outcome {
	if !t.MayRun || !f.Applies(t.Target) {
		return Outcome{Status: Skip}
	}
	ctx, cancel := context.WithTimeout(ctx, f.B)
	defer cancel()
	runErr := f.Run(ctx, t.Target)
	ok, _ := f.Confirm(ctx, t.Target)
	if !ok {
		why := "exec fence did not confirm"
		if runErr != nil {
			why += ": " + runErr.Error()
		}
		return Outcome{Status: Skip, Why: why}
	}
	return Outcome{Status: Confirmed, Verified: true}
}

// PeerChain is an ordered list of peer fences.
type PeerChain []PeerFence

// Attempt runs the chain once: the first fence that does not Skip decides.
// If every fence skips, it returns Skip with the reasons joined, and the
// caller applies its unverified policy.
func (c PeerChain) Attempt(ctx context.Context, t PeerTarget) (Outcome, string) {
	var why string
	for _, f := range c {
		out := f.Attempt(ctx, t)
		if out.Status != Skip {
			return out, f.Kind()
		}
		if out.Why != "" {
			if why != "" {
				why += "; "
			}
			why += f.Kind() + ": " + out.Why
		}
	}
	return Outcome{Status: Skip, Why: why}, ""
}

// BuildPeer builds a peer chain from specs.
func BuildPeer(r Remote, watchdogMargin time.Duration, specs []Spec) (PeerChain, error) {
	var c PeerChain
	for i, s := range specs {
		switch s.Kind {
		case KindIncusAPIStop:
			if s.Budget <= 0 {
				return nil, fmt.Errorf("fences.peer[%d] (%s): budget must be > 0", i, s.Kind)
			}
			c = append(c, IncusAPIStop{R: r, B: s.Budget})
		case KindWatchdogWait:
			c = append(c, WatchdogWait{Margin: watchdogMargin})
		case KindExec:
			if s.Budget <= 0 || len(s.Command) == 0 {
				return nil, fmt.Errorf("fences.peer[%d] (exec): budget > 0 and command required", i)
			}
			c = append(c, ExecPeer{&Exec{Argv: s.Command, ConfirmArgv: s.Confirm, B: s.Budget, For: s.For}})
		default:
			return nil, fmt.Errorf("fences.peer[%d]: unknown kind %q (want %s, %s or %s)", i, s.Kind, KindIncusAPIStop, KindExec, KindWatchdogWait)
		}
	}
	return c, nil
}
