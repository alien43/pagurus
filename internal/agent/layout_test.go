package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/backend/incus"
)

// layoutRT is a runtime where Incus's view (insts) and cgroupfs's view
// (local) can disagree.
type layoutRT struct {
	insts []incus.Instance
	local []string
}

func (r *layoutRT) Identity(context.Context) (string, string, error)   { return "asus", "x86_64", nil }
func (r *layoutRT) CheckProject(context.Context, string) error         { return nil }
func (r *layoutRT) CheckStart(context.Context, string) error           { return nil }
func (r *layoutRT) CheckCgroup2() error                                { return nil }
func (r *layoutRT) MemberOnline(context.Context, string) (bool, error) { return true, nil }
func (r *layoutRT) List(context.Context, string) ([]incus.Instance, error) {
	return r.insts, nil
}
func (r *layoutRT) Start(context.Context, string) error                  { return nil }
func (r *layoutRT) Prepare(context.Context, string) error                { return nil }
func (r *layoutRT) Pull(context.Context, string) error                   { return nil }
func (r *layoutRT) EnforceNoAutostart(context.Context, string) error     { return nil }
func (r *layoutRT) Stop(context.Context, string, time.Duration) error    { return nil }
func (r *layoutRT) LocalRunning(string) ([]string, error)                { return r.local, nil }
func (r *layoutRT) Kill(context.Context, string) error                   { return nil }
func (r *layoutRT) IPv4(context.Context, string, string) (string, error) { return "", nil }

func TestPreflightRefusesAnUnrecognisedCgroupLayout(t *testing.T) {
	running := incus.Instance{Name: "svc-1", Status: incus.StatusRunning, Location: "asus", Type: "container"}
	for name, c := range map[string]struct {
		rt   *layoutRT
		want bool // want errCgroupLayout
	}{
		"Incus Running, no payload cgroup (renamed layout)": {&layoutRT{insts: []incus.Instance{running}}, true},
		"Incus Running, cgroup seen":                        {&layoutRT{insts: []incus.Instance{running}, local: []string{"svc-1"}}, false},
		"nothing running":                                   {&layoutRT{insts: []incus.Instance{{Name: "svc-1", Status: incus.StatusStopped, Type: "container"}}}, false},
		"a running VM (no payload cgroup by design)":        {&layoutRT{insts: []incus.Instance{{Name: "vm", Status: incus.StatusRunning, Type: "virtual-machine"}}}, false},
	} {
		cfg := testConfig(t, nil)
		a := New(Options{Config: cfg, Runtime: c.rt, Fences: fencesOver(cfg, c.rt)})
		err := a.Preflight(context.Background())
		if got := errors.Is(err, errCgroupLayout); got != c.want {
			t.Errorf("%s: err %v, want layout error %v", name, err, c.want)
		}
	}
}
