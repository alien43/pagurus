// Package qemuproc finds the live qemu processes of an Incus project's VMs
// without asking Incus. A VM's qemu has no lxc.payload cgroup (it runs in
// incus.service's), so cgroupfs can't see it; Incus's pidfile can.
package qemuproc

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// DefaultRunRoot is where Incus keeps <project>_<instance>/qemu.pid.
const DefaultRunRoot = "/run/incus"

// Proc is one live, verified qemu, with an open pidfd the caller closes.
type Proc struct {
	Instance string
	PIDFile  string
	PID      int
	FD       int
}

// Live returns the project's verified, live qemu processes. A pidfile counts
// only if its PID is alive and that process's cmdline is qemu and names the
// same pidfile, as Incus checks its own pid() (driver_qemu.go:6517). The check
// runs after the pidfd is open, so a PID reused before the open fails it and
// one reused after cannot be signalled through the pidfd.
func Live(runRoot, project string) ([]Proc, error) {
	if runRoot == "" {
		runRoot = DefaultRunRoot
	}
	files, err := filepath.Glob(filepath.Join(runRoot, project+"_*", "qemu.pid"))
	if err != nil {
		return nil, err
	}
	var out []Proc
	for _, pf := range files {
		b, err := os.ReadFile(pf)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return out, err
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil || pid <= 1 {
			continue
		}
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			continue // ESRCH: gone
		}
		cmd, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		isQemu := bytes.Contains(cmd, []byte("qemu-system")) || bytes.Contains(cmd, []byte("qemu-kvm"))
		if err != nil || !isQemu || !bytes.Contains(cmd, []byte(pf)) {
			unix.Close(fd)
			continue
		}
		name := strings.TrimPrefix(filepath.Base(filepath.Dir(pf)), project+"_")
		out = append(out, Proc{Instance: name, PIDFile: pf, PID: pid, FD: fd})
	}
	return out, nil
}

// CloseAll closes every Proc's pidfd.
func CloseAll(ps []Proc) {
	for _, p := range ps {
		unix.Close(p.FD)
	}
}
