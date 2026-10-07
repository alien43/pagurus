package fence

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Local is what the self-fences need from the Incus driver. internal/incus's
// Driver implements it, and so does the agent's simulated runtime.
type Local interface {
	// Stop stops every instance of member's project through Incus.
	Stop(ctx context.Context, member string, graceful time.Duration) error
	// Kill writes cgroup.kill for every running instance of member's project.
	Kill(ctx context.Context, member string) error
	// LocalRunning lists member's running instances from cgroupfs, without
	// Incus.
	LocalRunning(member string) ([]string, error)
}

const (
	KindIncusStop  = "incus-stop"
	KindCgroupKill = "cgroup-kill"
	KindExec       = "exec"
)

func containersOnly(t Target) bool {
	if len(t.Types) == 0 {
		return true // the compose starter: containers
	}
	for _, ty := range t.Types {
		if ty != "container" {
			return false
		}
	}
	return true
}

// confirmLocal is the cgroupfs check both container fences confirm with: it
// needs neither incusd nor cluster quorum.
func confirmLocal(l Local, t Target) (bool, error) {
	running, err := l.LocalRunning(t.Member)
	if err != nil {
		return false, err
	}
	return len(running) == 0, nil
}

// IncusStop is `incus stop --force` of the member's project, confirmed from
// cgroupfs. Containers only for now: a VM has no lxc.payload cgroup, so
// cgroupfs cannot confirm it stopped (see singleton-vm-support).
type IncusStop struct {
	L Local
	B time.Duration
}

func (f IncusStop) Kind() string          { return KindIncusStop }
func (f IncusStop) Budget() time.Duration { return f.B }
func (f IncusStop) Applies(t Target) bool { return containersOnly(t) }
func (f IncusStop) Run(ctx context.Context, t Target) error {
	return f.L.Stop(ctx, t.Member, 0)
}
func (f IncusStop) Confirm(_ context.Context, t Target) (bool, error) { return confirmLocal(f.L, t) }

// CgroupKill writes cgroup.kill on lxc.payload.<project>_*: no incusd, no
// quorum. Containers only.
type CgroupKill struct {
	L Local
	B time.Duration
}

func (f CgroupKill) Kind() string          { return KindCgroupKill }
func (f CgroupKill) Budget() time.Duration { return f.B }
func (f CgroupKill) Applies(t Target) bool { return containersOnly(t) }
func (f CgroupKill) Run(ctx context.Context, t Target) error {
	return f.L.Kill(ctx, t.Member)
}
func (f CgroupKill) Confirm(_ context.Context, t Target) (bool, error) { return confirmLocal(f.L, t) }

// Exec runs an operator-supplied command: a power fence, a site script. Argv
// elements may contain {member} and {project}, substituted per element, never
// through a shell. With ConfirmArgv, that command's exit 0 confirms; without
// it, the Run's own exit 0 does.
type Exec struct {
	Argv        []string
	ConfirmArgv []string
	B           time.Duration
	// For selects target types; empty means any.
	For []string

	mu     sync.Mutex
	lastOK map[string]bool
}

func (f *Exec) Kind() string          { return KindExec }
func (f *Exec) Budget() time.Duration { return f.B }
func (f *Exec) Applies(t Target) bool {
	if len(f.For) == 0 {
		return true
	}
	for _, ty := range t.Types {
		if !slices.Contains(f.For, ty) {
			return false
		}
	}
	return true
}

func (f *Exec) Run(ctx context.Context, t Target) error {
	err := runArgv(ctx, f.Argv, t)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastOK == nil {
		f.lastOK = map[string]bool{}
	}
	f.lastOK[t.Member] = err == nil
	return err
}

func (f *Exec) Confirm(ctx context.Context, t Target) (bool, error) {
	if len(f.ConfirmArgv) > 0 {
		err := runArgv(ctx, f.ConfirmArgv, t)
		return err == nil, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastOK[t.Member], nil
}

// Expand substitutes {member} and {project} in each argv element.
func Expand(argv []string, t Target) []string {
	out := make([]string, len(argv))
	r := strings.NewReplacer("{member}", t.Member, "{project}", t.Project)
	for i, a := range argv {
		out[i] = r.Replace(a)
	}
	return out
}

// runArgv runs argv in its own process group, killing the whole group when
// ctx ends, with no stdin.
func runArgv(ctx context.Context, argv []string, t Target) error {
	if len(argv) == 0 {
		return fmt.Errorf("exec fence: empty command")
	}
	args := Expand(argv, t)
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("exec fence %s: %w", args[0], ctx.Err())
		}
		return fmt.Errorf("exec fence %s: %w: %s", args[0], err, strings.TrimSpace(errb.String()))
	}
	return nil
}
