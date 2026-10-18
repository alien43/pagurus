package incus_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/backend/incus"
)

// TestLiveClient exercises the real Incus client against a real cluster. It
// only runs when PAGURUS_LIVE_INCUS_PROJECT names a scratch project holding
// one stopped instance PAGURUS_LIVE_INCUS_INSTANCE; the connection comes from
// PAGURUS_LIVE_INCUS_URL / _CERT / _KEY / _SERVERCERT (or the local socket).
// It flips boot.autostart and back, starts and stops the instance.
func TestLiveClient(t *testing.T) {
	project, name := os.Getenv("PAGURUS_LIVE_INCUS_PROJECT"), os.Getenv("PAGURUS_LIVE_INCUS_INSTANCE")
	if project == "" || name == "" {
		t.Skip("set PAGURUS_LIVE_INCUS_PROJECT and _INSTANCE to run against a real Incus")
	}
	c := incus.NewClient(incus.ClientOptions{URL: os.Getenv("PAGURUS_LIVE_INCUS_URL"),
		ClientCert: os.Getenv("PAGURUS_LIVE_INCUS_CERT"), ClientKey: os.Getenv("PAGURUS_LIVE_INCUS_KEY"),
		ServerCert: os.Getenv("PAGURUS_LIVE_INCUS_SERVERCERT")})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	get := func() incus.Instance {
		insts, err := c.Instances(ctx, project)
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range insts {
			if in.Name == name {
				return in
			}
		}
		t.Fatalf("no instance %s in %s", name, project)
		return incus.Instance{}
	}
	for _, v := range []string{"true", "false"} {
		if err := c.SetConfig(ctx, project, name, "boot.autostart", v); err != nil {
			t.Fatalf("SetConfig %s: %v", v, err)
		}
		if got := get().Expanded["boot.autostart"]; got != v {
			t.Fatalf("boot.autostart = %q after setting %q", got, v)
		}
	}
	if err := c.Start(ctx, project, name); err != nil {
		t.Fatal(err)
	}
	if st := get().Status; st != incus.StatusRunning {
		t.Fatalf("status %s after Start", st)
	}
	if err := c.Stop(ctx, project, name, 0, true); err != nil {
		t.Fatal(err)
	}
	if st := get().Status; st != incus.StatusStopped {
		t.Fatalf("status %s after Stop", st)
	}
	t.Logf("SetConfig true/false, Start, Stop(force) all confirmed on %s/%s", project, name)
}
