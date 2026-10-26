package incus_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/backend/incus"
	"github.com/alien43/pagurus/internal/fence/fencetest"
)

func driver(api *fencetest.FakeIncus) *incus.Driver {
	return incus.New(incus.Options{ProjectPrefix: "p", CgroupRoot: "/nonexistent", API: api})
}

func TestIdentity(t *testing.T) {
	api := fencetest.NewFakeIncus("asus", "dell")
	m, arch, err := driver(api).Identity(context.Background())
	if err != nil || m != "asus" || arch != "x86_64" {
		t.Fatalf("got %q %q %v", m, arch, err)
	}
	api.Archs = nil
	if _, _, err := driver(api).Identity(context.Background()); err == nil {
		t.Fatal("no architecture accepted")
	}
}

func TestCheckProject(t *testing.T) {
	ctx := context.Background()
	api := fencetest.NewFakeIncus("asus", "dell")
	api.AddInstance("p-asus", incus.Instance{Name: "web", Status: incus.StatusStopped, Location: "asus", Type: "container"})
	d := driver(api)
	if err := d.CheckProject(ctx, "asus"); err != nil {
		t.Fatalf("pinned project refused: %v", err)
	}
	// Pinned to a group with two members: the per-member fence would be wrong.
	api.Groups["host-asus"] = []string{"asus", "dell"}
	if err := d.CheckProject(ctx, "asus"); err == nil || !strings.Contains(err.Error(), "want exactly") {
		t.Fatalf("two-member group: %v", err)
	}
	api.Groups["host-asus"] = []string{"asus"}
	// An instance ic-healthd could restart after a fence.
	if err := api.SetConfig(ctx, "p-asus", "web", "user.healthcheck.restart", "always"); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckProject(ctx, "asus"); err == nil || !strings.Contains(err.Error(), "ic-healthd") {
		t.Fatalf("restart policy: %v", err)
	}
	// Not pinned at all.
	api.Projects["p-dell"] = map[string]string{}
	if err := d.CheckProject(ctx, "dell"); err == nil || !strings.Contains(err.Error(), "unset") {
		t.Fatalf("unpinned: %v", err)
	}
}

func TestStopFallsBackToForce(t *testing.T) {
	api := fencetest.NewFakeIncus("asus")
	api.AddInstance("p-asus", incus.Instance{Name: "a", Status: incus.StatusRunning, Location: "asus", Type: "container"})
	api.AddInstance("p-asus", incus.Instance{Name: "b", Status: incus.StatusStopped, Location: "asus", Type: "container"})
	d := driver(api)
	if err := d.Stop(context.Background(), "asus", time.Second); err != nil {
		t.Fatal(err)
	}
	insts, _ := d.List(context.Background(), "asus")
	if !incus.AllStopped(insts) {
		t.Fatalf("not all stopped: %+v", insts)
	}
	// Already stopped: nothing to do, no error (the API errors on a stopped one).
	if err := d.Stop(context.Background(), "asus", 0); err != nil {
		t.Fatalf("stop of a stopped project: %v", err)
	}
}

func TestStopHonoursContext(t *testing.T) {
	api := fencetest.NewFakeIncus("asus")
	api.AddInstance("p-asus", incus.Instance{Name: "a", Status: incus.StatusRunning, Location: "asus", Type: "container"})
	api.SetHang(true)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := driver(api).Stop(ctx, "asus", 0)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("hung API: err %v after %s, want the caller's deadline", err, time.Since(start))
	}
}

func TestEnforceNoAutostartAndStart(t *testing.T) {
	ctx := context.Background()
	api := fencetest.NewFakeIncus("asus")
	api.AddInstance("p-asus", incus.Instance{Name: "a", Status: incus.StatusStopped, Location: "asus", Type: "container"})
	d := driver(api)
	if err := d.EnforceNoAutostart(ctx, "asus"); err != nil {
		t.Fatal(err)
	}
	insts, _ := d.List(ctx, "asus")
	if insts[0].Expanded["boot.autostart"] != "false" {
		t.Fatalf("boot.autostart = %q", insts[0].Expanded["boot.autostart"])
	}
	n := api.Mutations()
	if err := d.EnforceNoAutostart(ctx, "asus"); err != nil || api.Mutations() != n {
		t.Fatalf("second enforce changed something (err %v)", err)
	}
	if err := d.StartInstances(ctx, "asus", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	insts, _ = d.List(ctx, "asus")
	if insts[0].Status != incus.StatusRunning {
		t.Fatalf("status %s after start", insts[0].Status)
	}
}

func TestIPv4PicksAGlobalNonLoopbackAddress(t *testing.T) {
	api := fencetest.NewFakeIncus("asus")
	api.Addrs["p-asus/web"] = map[string][]incus.Address{
		"lo":   {{Family: "inet", Address: "127.0.0.1", Scope: "local"}},
		"eth0": {{Family: "inet6", Address: "fd00::1", Scope: "global"}, {Family: "inet", Address: "10.101.92.7", Scope: "global"}},
	}
	ip, err := driver(api).IPv4(context.Background(), "asus", "web")
	if err != nil || ip != "10.101.92.7" {
		t.Fatalf("got %q %v", ip, err)
	}
}

// TestListRefusesAnUnreadableProject: a restricted client sees an empty
// instance list for a project it may not read. The driver must fail rather
// than report "nothing running" (which a peer fence would take as fenced).
func TestListRefusesAnUnreadableProject(t *testing.T) {
	api := fencetest.NewFakeIncus("asus", "dell")
	api.AddInstance("p-dell", incus.Instance{Name: "web", Status: incus.StatusRunning, Location: "dell", Type: "container"})
	api.Forbidden = map[string]bool{"p-dell": true}
	insts, err := driver(api).List(context.Background(), "dell")
	if err == nil {
		t.Fatalf("listed %v from an unreadable project; want an error", insts)
	}
	if !strings.Contains(err.Error(), "cannot be trusted") {
		t.Fatalf("err %v", err)
	}
}

// A leader's start while a follower pulls the instance (`incus copy
// --refresh` holds the source busy) is retried, not failed: in the VM lab a
// restart in place hit it and gave the office up. Any other error is not.
func TestStartInstancesRetriesABusyInstance(t *testing.T) {
	defer func(d time.Duration) { incus.BusyRetry = d }(incus.BusyRetry)
	incus.BusyRetry = 10 * time.Millisecond
	api := fencetest.NewFakeIncus("asus")
	api.AddInstance("svc-asus", incus.Instance{Name: "vm", Status: incus.StatusStopped, Location: "asus", Type: "virtual-machine"})
	api.StartBusy = 3
	drv := incus.New(incus.Options{ProjectPrefix: "svc", API: api})
	if err := drv.StartInstances(context.Background(), "asus", []string{"vm"}); err != nil {
		t.Fatalf("start after 3 busy refusals: %v", err)
	}
	api.StartBusy = 1000
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := drv.StartInstances(ctx, "asus", []string{"vm"}); err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("still busy at the deadline: err %v, want the busy error", err)
	}
	api.StartBusy = 0
	if err := drv.StartInstances(context.Background(), "asus", []string{"ghost"}); err == nil {
		t.Fatal("a missing instance was retried into success")
	}
}
