package fence_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/fence"
	"github.com/alien43/pagurus/internal/fence/fencetest"
	"github.com/alien43/pagurus/internal/incus"
)

// fakeRemote is the cluster's view of one member.
type fakeRemote struct {
	online, running, stopWorks bool
	location                   string
	err                        error
	stops                      int
}

func (r *fakeRemote) MemberOnline(context.Context, string) (bool, error) { return r.online, r.err }
func (r *fakeRemote) List(_ context.Context, m string) ([]incus.Instance, error) {
	if r.err != nil {
		return nil, r.err
	}
	st, loc := incus.StatusStopped, m
	if r.running {
		st = incus.StatusRunning
	}
	if r.location != "" {
		loc = r.location
	}
	return []incus.Instance{{Name: "web", Status: st, Location: loc, Type: "container"}}, nil
}
func (r *fakeRemote) Stop(context.Context, string, time.Duration) error {
	r.stops++
	if r.stopWorks {
		r.running = false
	}
	return nil
}

var dell = fence.PeerTarget{Target: fence.Target{Member: "dell", Project: "p-dell"}, MayRun: true}

func TestIncusAPIStopOutcomes(t *testing.T) {
	for _, c := range []struct {
		name     string
		r        fakeRemote
		want     fence.Status
		verified bool
		blocked  bool
	}{
		{"running, stop works", fakeRemote{online: true, running: true, stopWorks: true}, fence.Confirmed, true, false},
		{"already stopped", fakeRemote{online: true}, fence.Confirmed, true, false},
		{"stop does nothing: cluster sees it running, nothing may override", fakeRemote{online: true, running: true}, fence.Wait, false, false},
		{"pinning broken", fakeRemote{online: true, location: "asus"}, fence.Wait, false, true},
		{"offline", fakeRemote{online: false}, fence.Skip, false, false},
		{"API error", fakeRemote{err: errors.New("no quorum")}, fence.Skip, false, false},
	} {
		r := c.r
		out := fence.IncusAPIStop{R: &r, B: time.Second}.Attempt(context.Background(), dell)
		if out.Status != c.want || out.Verified != c.verified || strings.HasPrefix(out.Why, "BLOCKED") != c.blocked {
			t.Errorf("%s: %+v", c.name, out)
		}
	}
}

func TestWatchdogWait(t *testing.T) {
	w := fence.WatchdogWait{Margin: 10 * time.Second}
	tg := dell
	if out := w.Attempt(context.Background(), tg); out.Status != fence.Skip {
		t.Fatalf("no reboot watchdog: %+v, want skip", out)
	}
	tg.FencesByReboot, tg.WatchdogTimeout, tg.SinceAcquired = true, 60*time.Second, 30*time.Second
	if out := w.Attempt(context.Background(), tg); out.Status != fence.Wait || out.Retry != 40*time.Second {
		t.Fatalf("mid-wait: %+v, want wait 40s", out)
	}
	tg.SinceAcquired = 71 * time.Second
	if out := w.Attempt(context.Background(), tg); out.Status != fence.Confirmed || out.Verified {
		t.Fatalf("after the wait: %+v, want an unverified confirm", out)
	}
	tg.MayRun = false
	if out := w.Attempt(context.Background(), tg); out.Status != fence.Skip {
		t.Fatalf("recorded stopped: %+v, want skip", out)
	}
}

// TestExecPeer: a power fence runs only against a may-run member, and only a
// confirmed run counts.
func TestExecPeer(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "plug")
	off := fencetest.FakeBin(t, dir, "plug-off", fmt.Sprintf(`echo off > %q`, state))
	isOff := fencetest.FakeBin(t, dir, "plug-is-off", fmt.Sprintf(`grep -q off %q`, state))
	broken := fencetest.FakeBin(t, dir, "plug-broken", "exit 1")

	f := fence.ExecPeer{Exec: &fence.Exec{Argv: []string{off.Path, "{member}"}, ConfirmArgv: []string{isOff.Path}, B: time.Second}}
	stopped := dell
	stopped.MayRun = false
	if out := f.Attempt(context.Background(), stopped); out.Status != fence.Skip || len(off.Calls(t)) != 0 {
		t.Fatalf("recorded-stopped member was power-fenced: %+v", out)
	}
	if err := os.WriteFile(state, []byte("on"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out := f.Attempt(context.Background(), dell); out.Status != fence.Confirmed || !out.Verified {
		t.Fatalf("working power fence: %+v", out)
	}
	if got := off.Calls(t); len(got) != 1 || got[0] != "dell" {
		t.Fatalf("argv %q", got)
	}
	bad := fence.ExecPeer{Exec: &fence.Exec{Argv: []string{broken.Path}, B: time.Second}}
	if out := bad.Attempt(context.Background(), dell); out.Status != fence.Skip || out.Why == "" {
		t.Fatalf("failed power fence: %+v, want skip with a reason", out)
	}
}

// stubPeer is a scripted peer fence.
type stubPeer struct {
	kind string
	out  fence.Outcome
	ran  *[]string
}

func (s stubPeer) Kind() string          { return s.kind }
func (s stubPeer) Budget() time.Duration { return time.Second }
func (s stubPeer) Attempt(context.Context, fence.PeerTarget) fence.Outcome {
	*s.ran = append(*s.ran, s.kind)
	return s.out
}

func TestPeerChainFirstNonSkipDecides(t *testing.T) {
	var ran []string
	c := fence.PeerChain{
		stubPeer{"a", fence.Outcome{Status: fence.Skip, Why: "offline"}, &ran},
		stubPeer{"b", fence.Outcome{Status: fence.Wait, Why: "watchdog"}, &ran},
		stubPeer{"c", fence.Outcome{Status: fence.Confirmed, Verified: true}, &ran},
	}
	out, by := c.Attempt(context.Background(), dell)
	if out.Status != fence.Wait || by != "b" || len(ran) != 2 {
		t.Fatalf("got %+v by %q after %v, want b's wait after [a b]", out, by, ran)
	}
	ran = nil
	out, by = fence.PeerChain{
		stubPeer{"a", fence.Outcome{Status: fence.Skip, Why: "offline"}, &ran},
		stubPeer{"b", fence.Outcome{Status: fence.Skip, Why: "no watchdog"}, &ran},
	}.Attempt(context.Background(), dell)
	if out.Status != fence.Skip || by != "" || out.Why != "a: offline; b: no watchdog" {
		t.Fatalf("all skip: %+v by %q", out, by)
	}
}

func TestBuildPeer(t *testing.T) {
	for _, s := range []fence.Spec{
		{Kind: "ipmi", Budget: time.Second},
		{Kind: fence.KindIncusAPIStop},
		{Kind: fence.KindExec, Budget: time.Second},
	} {
		if _, err := fence.BuildPeer(nil, 0, []fence.Spec{s}); err == nil {
			t.Errorf("accepted %+v", s)
		}
	}
	c, err := fence.BuildPeer(nil, time.Second, []fence.Spec{{Kind: fence.KindIncusAPIStop, Budget: time.Second}, {Kind: fence.KindWatchdogWait}})
	if err != nil || len(c) != 2 {
		t.Fatalf("default chain: %v %v", c, err)
	}
}

// TestIncusAPIStopNeverConfirmsAnUnreadableProject: through a client
// restricted to other projects, the real server lists a peer's project as
// empty. The fence must not take that as "stopped" (a second copy would
// start): the driver errors, and incus-api-stop skips, leaving the decision
// to the next fence or fence.unverified.
func TestIncusAPIStopNeverConfirmsAnUnreadableProject(t *testing.T) {
	api := fencetest.NewFakeIncus("asus", "dell")
	api.AddInstance("p-dell", incus.Instance{Name: "web", Status: incus.StatusRunning, Location: "dell", Type: "container"})
	api.Forbidden = map[string]bool{"p-dell": true}
	d := incus.New(incus.Options{ProjectPrefix: "p", CgroupRoot: t.TempDir(), API: api})
	out := fence.IncusAPIStop{R: d, B: time.Second}.Attempt(context.Background(), dell)
	if out.Status == fence.Confirmed {
		t.Fatalf("confirmed a fence on a project it cannot read: %+v", out)
	}
	if out.Status != fence.Skip || !strings.Contains(out.Why, "cannot be trusted") {
		t.Fatalf("got %+v, want a skip naming the unreadable project", out)
	}
}
