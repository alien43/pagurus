package fence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alien43/pagurus/internal/qemuproc"
	"golang.org/x/sys/unix"
)

const KindQemuKill = "qemu-kill"

// QemuKill is the VM self-fence that needs neither incusd nor cluster quorum.
// An Incus API stop does not work without quorum: it opens cluster-DB
// transactions before reaching the qemu driver (Incus v7.5.1
// internal/server/instance/instance_utils.go:335 LoadByProjectAndName,
// internal/server/operations/linux.go:20 registerDBOperation). And a VM's qemu
// has no lxc.payload cgroup: it runs in incus.service's.
//
// So QemuKill reads the pidfile Incus keeps at
// <RunRoot>/<project>_<instance>/qemu.pid and opens a pidfd on that PID. It
// accepts the process only if its cmdline looks like qemu (qemu-system or
// qemu-kvm) and names that same pidfile, the way Incus checks its own pid()
// (driver_qemu.go:6517). It then sends SIGKILL through the pidfd, so a reused
// PID can't be hit, and confirms when no such process is left. VMs only.
type QemuKill struct {
	B time.Duration
	// RunRoot is Incus's run directory (default /run/incus).
	RunRoot string
}

func (f QemuKill) Kind() string          { return KindQemuKill }
func (f QemuKill) Budget() time.Duration { return f.B }

// Applies: every instance type of the target is a VM.
func (f QemuKill) Applies(t Target) bool {
	if len(t.Types) == 0 {
		return false
	}
	for _, ty := range t.Types {
		if ty != "virtual-machine" {
			return false
		}
	}
	return true
}

func (f QemuKill) root() string {
	if f.RunRoot == "" {
		return qemuproc.DefaultRunRoot
	}
	return f.RunRoot
}

// live returns the target project's verified, live qemu processes, each with
// an open pidfd (qemuproc.Live).
func (f QemuKill) live(t Target) ([]qemuproc.Proc, error) {
	return qemuproc.Live(f.root(), t.Project)
}

func (f QemuKill) Run(ctx context.Context, t Target) error {
	ps, err := f.live(t)
	defer qemuproc.CloseAll(ps)
	if err != nil {
		return err
	}
	var errs []error
	for _, p := range ps {
		if err := unix.PidfdSendSignal(p.FD, unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
			errs = append(errs, fmt.Errorf("SIGKILL qemu %d (%s): %w", p.PID, p.PIDFile, err))
		}
	}
	// Wait for the processes to go (a pidfd turns readable when it exits).
	for _, p := range ps {
		for {
			n, err := unix.Poll([]unix.PollFd{{Fd: int32(p.FD), Events: unix.POLLIN}}, 50)
			if n > 0 || (err != nil && !errors.Is(err, unix.EINTR)) {
				break
			}
			if ctx.Err() != nil {
				errs = append(errs, fmt.Errorf("qemu %d still alive: %w", p.PID, ctx.Err()))
				break
			}
		}
	}
	return errors.Join(errs...)
}

// Confirm: no live, verified qemu of the project is left.
func (f QemuKill) Confirm(_ context.Context, t Target) (bool, error) {
	ps, err := f.live(t)
	defer qemuproc.CloseAll(ps)
	if err != nil {
		return false, err
	}
	return len(ps) == 0, nil
}
