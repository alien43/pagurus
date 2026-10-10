package incus_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/fence/fencetest"
	"github.com/alien43/pagurus/internal/incus"
)

func cgDriver(cg *fencetest.CgroupFS) *incus.Driver {
	return incus.New(incus.Options{ProjectPrefix: "p", CgroupRoot: cg.Root, API: fencetest.NewFakeIncus("asus")})
}

// TestLocalRunningReadsOnlyThisProjectsPopulatedCgroups: the self-fence's
// ground truth. Other projects' containers, emptied cgroups and stray files
// are not "ours, running".
func TestLocalRunningReadsOnlyThisProjectsPopulatedCgroups(t *testing.T) {
	cg := fencetest.FakeCgroupFS(t)
	cg.Run(t, "p-asus", "web")
	cg.Run(t, "p-asus", "db")
	cg.Run(t, "p-dell", "web") // another member's project
	cg.Run(t, "p-asus", "gone")
	cg.SetPopulated(t, "p-asus", "gone", false)
	if err := os.WriteFile(filepath.Join(cg.Root, "lxc.payload.p-asus_file"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := cgDriver(cg).LocalRunning("asus")
	if err != nil || !slices.Equal(got, []string{"db", "web"}) {
		t.Fatalf("LocalRunning = %v %v, want [db web]", got, err)
	}
}

func TestLocalRunningRefusesAnUnreadableEventsFile(t *testing.T) {
	cg := fencetest.FakeCgroupFS(t)
	cg.Run(t, "p-asus", "web")
	if err := os.WriteFile(filepath.Join(cg.Root, "lxc.payload.p-asus_web", "cgroup.events"), []byte("frozen 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := cgDriver(cg).LocalRunning("asus"); err == nil {
		t.Fatal("a cgroup.events without populated was taken as an answer")
	}
	if _, err := incus.New(incus.Options{ProjectPrefix: "p", CgroupRoot: filepath.Join(t.TempDir(), "nope"), API: fencetest.NewFakeIncus("asus")}).LocalRunning("asus"); err == nil {
		t.Fatal("a missing cgroup root was taken as nothing running")
	}
}

func TestKillWritesCgroupKillAndWaits(t *testing.T) {
	cg := fencetest.FakeCgroupFS(t)
	cg.Run(t, "p-asus", "web")
	d := cgDriver(cg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Kill(ctx, "asus"); err != nil {
		t.Fatal(err)
	}
	if !cg.Killed("p-asus", "web") {
		t.Fatal("cgroup.kill not written")
	}
	if left, _ := d.LocalRunning("asus"); len(left) != 0 {
		t.Fatalf("still running: %v", left)
	}
}

func TestKillGivesUpAtTheDeadline(t *testing.T) {
	cg := fencetest.FakeCgroupFS(t)
	cg.SetKillWorks(false)
	cg.Run(t, "p-asus", "web")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := cgDriver(cg).Kill(ctx, "asus")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Kill = %v, want the deadline", err)
	}
}

func TestCheckCgroup2(t *testing.T) {
	if err := incus.New(incus.Options{CgroupRoot: t.TempDir(), API: fencetest.NewFakeIncus("asus")}).CheckCgroup2(); err == nil {
		t.Fatal("a plain directory passed as a cgroup2 mount")
	}
	if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
		if err := incus.New(incus.Options{API: fencetest.NewFakeIncus("asus")}).CheckCgroup2(); err != nil {
			t.Fatalf("the host's cgroup2 mount: %v", err)
		}
	}
}

func TestMemberOnlineAndRunning(t *testing.T) {
	api := fencetest.NewFakeIncus("asus", "dell")
	api.Members["dell"] = "Offline"
	d := incus.New(incus.Options{ProjectPrefix: "p", CgroupRoot: t.TempDir(), API: api})
	if on, err := d.MemberOnline(context.Background(), "asus"); err != nil || !on {
		t.Fatalf("asus: %v %v", on, err)
	}
	if on, err := d.MemberOnline(context.Background(), "dell"); err != nil || on {
		t.Fatalf("dell: %v %v", on, err)
	}
	r := incus.Running([]incus.Instance{{Name: "a", Status: incus.StatusRunning}, {Name: "b", Status: incus.StatusStopped}})
	if !slices.Equal(r, []string{"a"}) {
		t.Fatalf("Running = %v", r)
	}
}

func TestRunKillsTheGroupAtTheDeadline(t *testing.T) {
	out, err := incus.Run(context.Background(), "sh", "-c", "echo hi")
	if err != nil || string(out) != "hi\n" {
		t.Fatalf("Run: %q %v", out, err)
	}
	if _, err := incus.Run(context.Background(), "sh", "-c", "echo oops >&2; exit 3"); err == nil {
		t.Fatal("a failing command succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := incus.Run(ctx, "sh", "-c", "sleep 30 & sleep 30"); err == nil {
		t.Fatal("a command past its deadline succeeded")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("Run returned %v after a 300ms deadline: the group was not killed", took)
	}
}
