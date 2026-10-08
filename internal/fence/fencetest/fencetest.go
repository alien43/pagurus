// Package fencetest is a fake environment for testing fences without a real
// Incus, cgroupfs or host: fake binaries on a temp PATH, a fake cgroup2 tree,
// a real child process with a pidfile, and a contract every fence kind must
// pass.
package fencetest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/fence"
)

// Bin is a fake binary: a shell script that appends its argv, one line per
// call, to Log, then runs the body it was given.
type Bin struct {
	Path string
	Log  string
}

// FakeBin writes a fake binary named name into dir. body is POSIX sh; "$@"
// holds the arguments.
func FakeBin(t testing.TB, dir, name, body string) *Bin {
	t.Helper()
	b := &Bin{Path: filepath.Join(dir, name), Log: filepath.Join(dir, name+".calls")}
	src := fmt.Sprintf("#!/bin/sh\necho \"$*\" >> %q\n%s\n", b.Log, body)
	if err := os.WriteFile(b.Path, []byte(src), 0o755); err != nil {
		t.Fatal(err)
	}
	return b
}

// Calls returns the argv lines the binary was called with.
func (b *Bin) Calls(t testing.TB) []string {
	t.Helper()
	data, err := os.ReadFile(b.Log)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(data), "\n"), "\n")
}

// CgroupFS is a fake cgroup2 root with lxc.payload.<project>_<name>
// directories, the layout internal/incus reads.
type CgroupFS struct {
	Root string
	// KillWorks: writing "1" to cgroup.kill empties the cgroup (default
	// true). Set false to model a kill that never takes.
	KillWorks bool

	mu   sync.Mutex
	stop chan struct{}
}

// FakeCgroupFS creates an empty fake cgroup root and a watcher that applies
// cgroup.kill writes.
func FakeCgroupFS(t testing.TB) *CgroupFS {
	t.Helper()
	c := &CgroupFS{Root: t.TempDir(), KillWorks: true, stop: make(chan struct{})}
	t.Cleanup(func() { close(c.stop) })
	go c.watch()
	return c
}

func (c *CgroupFS) dir(project, name string) string {
	return filepath.Join(c.Root, "lxc.payload."+project+"_"+name)
}

// Run adds a running container.
func (c *CgroupFS) Run(t testing.TB, project, name string) {
	t.Helper()
	d := c.dir(project, name)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	// cgroup.kill exists on a real cgroup2 directory; writers open it, they
	// do not create it (internal/incus writes it with mode 0).
	if err := os.WriteFile(filepath.Join(d, "cgroup.kill"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	c.SetPopulated(t, project, name, true)
}

// SetPopulated flips a container's cgroup.events, as a real stop would.
func (c *CgroupFS) SetPopulated(t testing.TB, project, name string, on bool) {
	t.Helper()
	v := "0"
	if on {
		v = "1"
	}
	if err := writeEvents(c.dir(project, name), v); err != nil {
		t.Fatal(err)
	}
}

// writeEvents replaces cgroup.events atomically, as the kernel does: a reader
// never sees a half-written file. (os.WriteFile truncates first, and a reader
// racing it sees an empty file; found by -race on 2026-10-08.)
func writeEvents(dir, populated string) error {
	tmp := filepath.Join(dir, ".cgroup.events.tmp")
	if err := os.WriteFile(tmp, []byte("populated "+populated+"\nfrozen 0\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "cgroup.events"))
}

// Killed reports whether anything wrote cgroup.kill for the container.
func (c *CgroupFS) Killed(project, name string) bool {
	b, err := os.ReadFile(filepath.Join(c.dir(project, name), "cgroup.kill"))
	return err == nil && strings.TrimSpace(string(b)) == "1"
}

// SetKillWorks changes whether cgroup.kill takes effect.
func (c *CgroupFS) SetKillWorks(on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.KillWorks = on
}

func (c *CgroupFS) watch() {
	for {
		select {
		case <-c.stop:
			return
		case <-time.After(10 * time.Millisecond):
		}
		c.mu.Lock()
		works := c.KillWorks
		c.mu.Unlock()
		if !works {
			continue
		}
		kills, _ := filepath.Glob(filepath.Join(c.Root, "lxc.payload.*", "cgroup.kill"))
		for _, k := range kills {
			if b, err := os.ReadFile(k); err == nil && strings.TrimSpace(string(b)) == "1" {
				_ = writeEvents(filepath.Dir(k), "0")
			}
		}
	}
}

// Proc is a real child process (sleep) with a pidfile, for PID-based fences.
type Proc struct {
	Cmd     *exec.Cmd
	PIDFile string
}

// FakeProc starts `sleep 1000` and writes its pid to a pidfile.
func FakeProc(t testing.TB) *Proc {
	t.Helper()
	cmd := exec.Command("sleep", "1000")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &Proc{Cmd: cmd, PIDFile: filepath.Join(t.TempDir(), "qemu.pid")}
	if err := os.WriteFile(p.PIDFile, []byte(fmt.Sprint(cmd.Process.Pid)), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	return p
}

// Qemu is a fake VM process: a real `sleep` whose argv[0] reads like a qemu
// command line naming its pidfile, laid out the way Incus keeps it:
// <RunRoot>/<project>_<name>/qemu.pid.
type Qemu struct {
	RunRoot string
	PIDFile string
	Cmd     *exec.Cmd
}

// FakeQemu starts one fake qemu for project/name under runRoot (created if
// empty: pass "" for a fresh temp dir).
func FakeQemu(t testing.TB, runRoot, project, name string) *Qemu {
	t.Helper()
	if runRoot == "" {
		runRoot = t.TempDir()
	}
	dir := filepath.Join(runRoot, project+"_"+name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	pf := filepath.Join(dir, "qemu.pid")
	cmd := exec.Command("bash", "-c", `exec -a "qemu-system-x86_64 -name `+name+` -pidfile `+pf+`" sleep 1000`)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	// Reap it as soon as it dies, as init does for a real qemu.
	go func() { _, _ = cmd.Process.Wait() }()
	if err := os.WriteFile(pf, []byte(fmt.Sprint(cmd.Process.Pid)), 0o644); err != nil {
		t.Fatal(err)
	}
	// Wait until the exec has replaced bash: argv[0] must *start* with
	// qemu-system. (bash's own cmdline already *contains* the string, inside
	// its -c argument, so a Contains check returned before the exec, and on a
	// slow CI runner a fence then raced the exec: CI run for f09c991.)
	q := &Qemu{RunRoot: runRoot, PIDFile: pf, Cmd: cmd}
	for deadline := time.Now().Add(5 * time.Second); !q.Alive(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("fake qemu %d never exec'd", cmd.Process.Pid)
		}
	}
	return q
}

// Alive reports whether the fake qemu still runs.
func (q *Qemu) Alive() bool {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", q.Cmd.Process.Pid))
	return err == nil && strings.HasPrefix(string(b), "qemu-system")
}

// Env is one ready-made target environment for a contract run.
type Env struct {
	Target fence.Target
	// Running brings the target back to "running" before a case.
	Running func(t testing.TB)
	// Mutations counts calls that change state (for the read-only check).
	Mutations func(t testing.TB) int
	// Hang makes the next Run hang until its context ends.
	Hang func(t testing.TB)
}

// Setup builds a fresh environment and the fence under test, with the given
// budget, for one contract case.
type Setup func(t *testing.T, budget time.Duration) (fence.Fence, Env)

// Contract checks the properties every fence kind must have.
func Contract(t *testing.T, setup Setup) {
	t.Run("confirms after run", func(t *testing.T) {
		f, e := setup(t, 2*time.Second)
		e.Running(t)
		if !f.Applies(e.Target) {
			t.Skip("fence does not apply to this target")
		}
		if res := (fence.Chain{f}).Run(context.Background(), e.Target, nil); !res.Stopped {
			t.Fatalf("not confirmed: %s", res)
		}
	})
	t.Run("confirm is read-only", func(t *testing.T) {
		f, e := setup(t, 2*time.Second)
		e.Running(t)
		before := e.Mutations(t)
		stopped, err := f.Confirm(context.Background(), e.Target)
		if err != nil {
			t.Fatal(err)
		}
		if stopped {
			t.Fatal("Confirm reported a running target stopped")
		}
		if after := e.Mutations(t); after != before {
			t.Fatalf("Confirm changed state: %d mutating calls", after-before)
		}
	})
	t.Run("idempotent on a stopped target", func(t *testing.T) {
		f, e := setup(t, 2*time.Second)
		e.Running(t)
		for i := 0; i < 2; i++ {
			if res := (fence.Chain{f}).Run(context.Background(), e.Target, nil); !res.Stopped {
				t.Fatalf("run %d: %s", i+1, res)
			}
		}
	})
	t.Run("honours its budget", func(t *testing.T) {
		budget := 300 * time.Millisecond
		f, e := setup(t, budget)
		if e.Hang == nil {
			t.Skip("this environment cannot make the fence hang")
		}
		e.Running(t)
		e.Hang(t)
		start := time.Now()
		res := fence.Chain{f}.Run(context.Background(), e.Target, nil)
		took := time.Since(start)
		if res.Stopped {
			t.Fatalf("a hung fence confirmed: %s", res)
		}
		if took > budget+1500*time.Millisecond {
			t.Fatalf("took %s with a %s budget", took, budget)
		}
	})
	t.Run("errors name the kind", func(t *testing.T) {
		f, e := setup(t, 200*time.Millisecond)
		if e.Hang == nil {
			t.Skip("this environment cannot make the fence hang")
		}
		e.Running(t)
		e.Hang(t)
		res := fence.Chain{f}.Run(context.Background(), e.Target, nil)
		if len(res.Steps) != 1 || res.Steps[0].Kind != f.Kind() || res.Steps[0].Err == nil {
			t.Fatalf("steps %+v: want one failed step of kind %s", res.Steps, f.Kind())
		}
	})
}
