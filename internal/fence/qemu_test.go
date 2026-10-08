package fence_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/fence"
	"github.com/alien43/pagurus/internal/fence/fencetest"
)

var vmTarget = fence.Target{Member: "asus", Project: "p-asus", Types: []string{"virtual-machine"}}

func TestQemuKillContract(t *testing.T) {
	fencetest.Contract(t, func(t *testing.T, b time.Duration) (fence.Fence, fencetest.Env) {
		root := t.TempDir()
		var q *fencetest.Qemu
		e := fencetest.Env{Target: vmTarget}
		e.Running = func(t testing.TB) { q = fencetest.FakeQemu(t, root, "p-asus", "vm1") }
		e.Mutations = func(testing.TB) int {
			if q != nil && !q.Alive() {
				return 1
			}
			return 0
		}
		// No Hang: nothing can make SIGKILL wait (a D-state qemu cannot be faked).
		return fence.QemuKill{B: b, RunRoot: root}, e
	})
}

func TestQemuKillAppliesToVMsOnly(t *testing.T) {
	f := fence.QemuKill{B: time.Second}
	if !f.Applies(vmTarget) {
		t.Fatal("does not apply to a VM")
	}
	for _, ty := range [][]string{nil, {"container"}, {"virtual-machine", "container"}} {
		if f.Applies(fence.Target{Types: ty}) {
			t.Errorf("applies to %v", ty)
		}
	}
}

// TestQemuKillSparesWhatIsNotItsQemu: a pidfile pointing at a process that is
// not this instance's qemu (PID reuse, a stale file) must never be signalled.
func TestQemuKillSparesWhatIsNotItsQemu(t *testing.T) {
	root := t.TempDir()
	other := fencetest.FakeQemu(t, root, "p-dell", "vm1") // another project's qemu
	plain := exec.Command("sleep", "1000")                // a reused PID: not qemu at all
	if err := plain.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Process.Kill(); _, _ = plain.Process.Wait() })
	dir := filepath.Join(root, "p-asus_vm2")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "qemu.pid"), []byte(fmt.Sprint(plain.Process.Pid)), 0o644); err != nil {
		t.Fatal(err)
	}
	mine := fencetest.FakeQemu(t, root, "p-asus", "vm1")

	res := fence.Chain{fence.QemuKill{B: 2 * time.Second, RunRoot: root}}.Run(context.Background(), vmTarget, nil)
	if !res.Stopped {
		t.Fatalf("own qemu not confirmed killed: %s", res)
	}
	if mine.Alive() {
		t.Fatal("own qemu survived")
	}
	if !other.Alive() {
		t.Fatal("killed another project's qemu")
	}
	if err := plain.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("killed a non-qemu process whose PID sat in a pidfile: %v", err)
	}
}
