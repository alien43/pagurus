package fence_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/backend/incus"
	"github.com/alien43/pagurus/internal/fence"
	"github.com/alien43/pagurus/internal/fence/fencetest"
)

// containerEnv: one container "web" of project p-asus in a FakeIncus whose
// Stop empties the container's cgroup (as the kernel would), and a fake
// cgroupfs. The real internal/backend/incus Driver sits in between.
type containerEnv struct {
	fencetest.Env
	Driver *incus.Driver
	CG     *fencetest.CgroupFS
	Incus  *fencetest.FakeIncus
}

func newContainerEnv(t *testing.T) containerEnv {
	t.Helper()
	cg := fencetest.FakeCgroupFS(t)
	api := fencetest.NewFakeIncus("asus", "dell")
	api.AddInstance("p-asus", incus.Instance{Name: "web", Status: incus.StatusStopped, Location: "asus", Type: "container"})
	api.OnStop = func(project, name string) { cg.SetPopulated(t, project, name, false) }
	d := incus.New(incus.Options{ProjectPrefix: "p", CgroupRoot: cg.Root, API: api})
	e := containerEnv{Driver: d, CG: cg, Incus: api}
	e.Target = fence.Target{Member: "asus", Project: "p-asus", Types: []string{"container"}}
	e.Running = func(t testing.TB) {
		cg.Run(t, "p-asus", "web")
		api.SetStatus("p-asus", "web", incus.StatusRunning)
	}
	e.Mutations = func(t testing.TB) int {
		n := api.Mutations()
		if cg.Killed("p-asus", "web") {
			n++
		}
		return n
	}
	return e
}

func TestIncusStopContract(t *testing.T) {
	fencetest.Contract(t, func(t *testing.T, b time.Duration) (fence.Fence, fencetest.Env) {
		e := newContainerEnv(t)
		e.Hang = func(testing.TB) { e.Incus.SetHang(true) }
		return fence.IncusStop{L: e.Driver, B: b}, e.Env
	})
}

func TestCgroupKillContract(t *testing.T) {
	fencetest.Contract(t, func(t *testing.T, b time.Duration) (fence.Fence, fencetest.Env) {
		e := newContainerEnv(t)
		e.Hang = func(testing.TB) { e.CG.SetKillWorks(false) }
		return fence.CgroupKill{L: e.Driver, B: b}, e.Env
	})
}

func TestExecContract(t *testing.T) {
	fencetest.Contract(t, func(t *testing.T, b time.Duration) (fence.Fence, fencetest.Env) {
		dir := t.TempDir()
		state, hang := filepath.Join(dir, "plug"), filepath.Join(dir, "hang")
		off := fencetest.FakeBin(t, dir, "plug-off", fmt.Sprintf(`[ -e %q ] && exec sleep 100; echo off > %q`, hang, state))
		check := fencetest.FakeBin(t, dir, "plug-is-off", fmt.Sprintf(`grep -q off %q`, state))
		e := fencetest.Env{Target: fence.Target{Member: "dell", Project: "p-dell"}}
		e.Running = func(t testing.TB) {
			if err := os.WriteFile(state, []byte("on"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		e.Mutations = func(t testing.TB) int { return len(off.Calls(t)) }
		e.Hang = func(t testing.TB) {
			if err := os.WriteFile(hang, nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return &fence.Exec{Argv: []string{off.Path, "{member}"}, ConfirmArgv: []string{check.Path}, B: b}, e
	})
}

// TestExecArgvSubstitution: placeholders are expanded per argv element and
// never reach a shell.
func TestExecArgvSubstitution(t *testing.T) {
	dir := t.TempDir()
	bin := fencetest.FakeBin(t, dir, "fence", "")
	f := &fence.Exec{Argv: []string{bin.Path, "--host={member}", "{project}", "a b;rm -rf /"}, B: time.Second}
	tg := fence.Target{Member: "rpi5", Project: "sgl-rpi5"}
	res := fence.Chain{f}.Run(context.Background(), tg, nil)
	if !res.Stopped {
		t.Fatalf("exit 0 without a confirm command must confirm: %s", res)
	}
	if got := bin.Calls(t); len(got) != 1 || got[0] != "--host=rpi5 sgl-rpi5 a b;rm -rf /" {
		t.Fatalf("argv %q", got)
	}
}

// TestExecFailureIsNotConfirmed: a non-zero exit is a failed fence.
func TestExecFailureIsNotConfirmed(t *testing.T) {
	bin := fencetest.FakeBin(t, t.TempDir(), "fence", "exit 3")
	res := fence.Chain{&fence.Exec{Argv: []string{bin.Path}, B: time.Second}}.Run(context.Background(), fence.Target{Member: "x"}, nil)
	if res.Stopped || res.Steps[0].Err == nil {
		t.Fatalf("exit 3 confirmed: %s", res)
	}
}

// TestCgroupKillStuck: a kill that never empties the cgroup is not confirmed,
// and is cut off at the budget.
func TestCgroupKillStuck(t *testing.T) {
	e := newContainerEnv(t)
	e.Running(t)
	e.CG.SetKillWorks(false)
	start := time.Now()
	res := fence.Chain{fence.CgroupKill{L: e.Driver, B: 300 * time.Millisecond}}.Run(context.Background(), e.Target, nil)
	if res.Stopped {
		t.Fatalf("stuck kill confirmed: %s", res)
	}
	if !e.CG.Killed("p-asus", "web") {
		t.Fatal("cgroup.kill was never written")
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %s with a 300ms budget", took)
	}
}

// TestContainerFencesSkipVMs: cgroupfs cannot see a VM, so neither container
// fence may claim to have stopped one.
func TestContainerFencesSkipVMs(t *testing.T) {
	vm := fence.Target{Member: "asus", Project: "p-asus", Types: []string{"virtual-machine"}}
	for _, f := range []fence.Fence{fence.IncusStop{}, fence.CgroupKill{}} {
		if f.Applies(vm) {
			t.Errorf("%s applies to a VM", f.Kind())
		}
	}
	if !(&fence.Exec{}).Applies(vm) {
		t.Error("exec without a type filter must apply to anything")
	}
	if (&fence.Exec{For: []string{"container"}}).Applies(vm) {
		t.Error("exec limited to containers applies to a VM")
	}
}
