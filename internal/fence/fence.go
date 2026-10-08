// Package fence holds pagurus's fences: the ways one member's copy of the
// workload is stopped, each a plugin with its own budget, run as an ordered
// chain that stops at the first fence that confirms.
//
// A self chain runs on the member itself, before its lease can expire
// (incus-stop, cgroup-kill, exec). An empty chain, or one whose every fence
// skips the target, is allowed: the member then has no verified self-fence and
// its peers must rely on their own fences or the unverified policy.
package fence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Target is what a fence acts on: one member's project.
type Target struct {
	Member  string
	Project string
	// Types are the instance types the starter runs ("container",
	// "virtual-machine"); a fence that cannot act on one of them skips.
	Types []string
}

// Fence is one way of stopping a target.
type Fence interface {
	// Kind is the config name, e.g. "cgroup-kill".
	Kind() string
	// Budget is the hard deadline the chain gives Run plus Confirm.
	Budget() time.Duration
	// Applies reports whether this fence can act on t at all (e.g.
	// cgroup-kill: containers only). A fence that does not apply is skipped.
	Applies(t Target) bool
	// Run tries to stop t. It must be idempotent: running it on a stopped
	// target is not an error.
	Run(ctx context.Context, t Target) error
	// Confirm reports whether t is stopped. It must not change anything.
	Confirm(ctx context.Context, t Target) (stopped bool, err error)
}

// Step is the outcome of one fence in a chain run.
type Step struct {
	Kind    string
	Skipped bool
	Stopped bool
	Err     error
	Took    time.Duration
}

// Result is the outcome of a chain run.
type Result struct {
	Stopped bool
	By      string // the kind that confirmed, if any
	Steps   []Step
}

// Chain is an ordered list of fences.
type Chain []Fence

// Budget is the sum of the budgets: the longest a chain run can take.
func (c Chain) Budget() time.Duration {
	var d time.Duration
	for _, f := range c {
		d += f.Budget()
	}
	return d
}

// Verified reports whether some fence in the chain applies to t, i.e.
// whether the chain can confirm a stop at all.
func (c Chain) Verified(t Target) bool {
	for _, f := range c {
		if f.Applies(t) {
			return true
		}
	}
	return false
}

// Run tries each applicable fence in order, each within its budget, and
// stops at the first whose Confirm reports the target stopped. The context
// is only a parent: each fence still gets no more than its own budget.
func (c Chain) Run(ctx context.Context, t Target, log *slog.Logger) Result {
	var res Result
	for _, f := range c {
		st := Step{Kind: f.Kind()}
		if !f.Applies(t) {
			st.Skipped = true
			res.Steps = append(res.Steps, st)
			continue
		}
		start := time.Now()
		fctx, cancel := context.WithTimeout(ctx, f.Budget())
		runErr := f.Run(fctx, t)
		stopped, confErr := f.Confirm(fctx, t)
		cancel()
		st.Took = time.Since(start)
		st.Stopped = stopped && confErr == nil
		st.Err = errors.Join(runErr, confErr)
		res.Steps = append(res.Steps, st)
		if log != nil {
			log.Info("fence step", "kind", st.Kind, "member", t.Member, "stopped", st.Stopped, "err", st.Err, "took", st.Took.Round(time.Millisecond))
		}
		if st.Stopped {
			res.Stopped, res.By = true, f.Kind()
			return res
		}
	}
	return res
}

// String renders a result for logs and the CLI.
func (r Result) String() string {
	s := fmt.Sprintf("stopped=%v", r.Stopped)
	if r.By != "" {
		s += " by=" + r.By
	}
	for _, st := range r.Steps {
		switch {
		case st.Skipped:
			s += fmt.Sprintf(" [%s: skipped]", st.Kind)
		case st.Err != nil:
			s += fmt.Sprintf(" [%s: stopped=%v err=%v %s]", st.Kind, st.Stopped, st.Err, st.Took.Round(time.Millisecond))
		default:
			s += fmt.Sprintf(" [%s: stopped=%v %s]", st.Kind, st.Stopped, st.Took.Round(time.Millisecond))
		}
	}
	return s
}

// Spec configures one fence of a chain.
type Spec struct {
	Kind    string
	Budget  time.Duration
	Command []string // exec
	Confirm []string // exec, optional
	For     []string // exec: target types it applies to; empty = any
}

// BuildSelf builds a self chain from specs over l.
func BuildSelf(l Local, specs []Spec) (Chain, error) {
	var c Chain
	for i, s := range specs {
		if s.Budget <= 0 {
			return nil, fmt.Errorf("fences.self[%d] (%s): budget must be > 0", i, s.Kind)
		}
		switch s.Kind {
		case KindIncusStop:
			c = append(c, IncusStop{L: l, B: s.Budget})
		case KindCgroupKill:
			c = append(c, CgroupKill{L: l, B: s.Budget})
		case KindQemuKill:
			c = append(c, QemuKill{B: s.Budget})
		case KindExec:
			if len(s.Command) == 0 {
				return nil, fmt.Errorf("fences.self[%d] (exec): command required", i)
			}
			c = append(c, &Exec{Argv: s.Command, ConfirmArgv: s.Confirm, B: s.Budget, For: s.For})
		default:
			return nil, fmt.Errorf("fences.self[%d]: unknown kind %q (want %s, %s, %s or %s)", i, s.Kind, KindIncusStop, KindCgroupKill, KindQemuKill, KindExec)
		}
	}
	return c, nil
}
