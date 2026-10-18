package start_test

import (
	"context"
	"strings"
	"testing"

	"github.com/alien43/pagurus/internal/backend/incus"
	"github.com/alien43/pagurus/internal/backend/incus/start"
	"github.com/alien43/pagurus/internal/fence/fencetest"
)

func project(m string) string { return "p-" + m }

func TestComposeArgv(t *testing.T) {
	bin := fencetest.FakeBin(t, t.TempDir(), "incus-compose", "")
	c := start.Compose{Run: incus.Run, Bin: bin.Path, Project: project, File: "/x/compose.yaml", Args: []string{"--env-file", "/x/.env"}, UpArgs: []string{"--detach", "--no-healthd"}}
	if err := c.CheckStart(context.Background(), "asus"); err != nil {
		t.Fatal(err)
	}
	if err := c.Prepare(context.Background(), "asus"); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background(), "asus"); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"-p p-asus -f /x/compose.yaml --env-file /x/.env config",
		"-p p-asus -f /x/compose.yaml --env-file /x/.env up --no-start --recreate --detach --no-healthd",
		"-p p-asus -f /x/compose.yaml --env-file /x/.env up --detach --no-healthd",
	}
	if got := bin.Calls(t); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("argv\n got %q\nwant %q", got, want)
	}
}

func TestComposeFailurePropagates(t *testing.T) {
	bin := fencetest.FakeBin(t, t.TempDir(), "incus-compose", "echo boom >&2; exit 2")
	c := start.Compose{Run: incus.Run, Bin: bin.Path, Project: project, File: "f"}
	if err := c.Start(context.Background(), "asus"); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err %v, want the tool's stderr", err)
	}
}

type fakeList []incus.Instance

func (f fakeList) List(context.Context, string) ([]incus.Instance, error) { return f, nil }

// started records StartInstances calls on a fakeList.
var started [][]string

func (f fakeList) StartInstances(_ context.Context, member string, names []string) error {
	started = append(started, append([]string{member}, names...))
	return nil
}

func inst(name, status, typ, autostart string) incus.Instance {
	return incus.Instance{Name: name, Status: status, Type: typ, Expanded: map[string]string{"boot.autostart": autostart}}
}

func TestInstancesStartsOnlyWhatIsNotRunning(t *testing.T) {
	started = nil
	s := start.Instances{Project: project, L: fakeList{
		inst("web", incus.StatusStopped, "container", "false"),
		inst("db", incus.StatusRunning, "container", "false"),
		inst("tmp", incus.StatusStopped, "container", "false"),
	}, Names: []string{"web", "db"}}
	if err := s.CheckStart(context.Background(), "dell"); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background(), "dell"); err != nil {
		t.Fatal(err)
	}
	if len(started) != 1 || strings.Join(started[0], " ") != "dell web" {
		t.Fatalf("started %q, want only web on dell (db runs, tmp not selected)", started)
	}
	started = nil
	s.L, s.Names = fakeList{inst("web", incus.StatusRunning, "container", "false")}, nil
	if err := s.Start(context.Background(), "dell"); err != nil || len(started) != 0 {
		t.Fatalf("all running: err %v started %q, want nothing to do", err, started)
	}
}

func TestInstancesCheckRefuses(t *testing.T) {
	for name, c := range map[string]struct {
		l     fakeList
		names []string
		want  string
	}{
		"missing name":    {fakeList{inst("web", "Stopped", "container", "false")}, []string{"web", "db"}, "no instance(s) db"},
		"empty project":   {fakeList{}, nil, "no instances"},
		"unknown type":    {fakeList{inst("x", "Stopped", "potato", "false")}, nil, "containers and VMs"},
		"autostart unset": {fakeList{inst("web", "Stopped", "container", "")}, nil, "boot.autostart"},
	} {
		s := start.Instances{Project: project, L: c.l, Names: c.names}
		if err := s.CheckStart(context.Background(), "rpi5"); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want %q", name, err, c.want)
		}
	}
}

func TestInstancesAcceptsVMs(t *testing.T) {
	s := start.Instances{Project: project, L: fakeList{inst("vm", "Stopped", "virtual-machine", "false")}}
	if err := s.CheckStart(context.Background(), "asus"); err != nil {
		t.Fatalf("a VM was refused: %v", err)
	}
}

// A recreate never runs without --no-healthd, whatever up_args say.
func TestComposePrepareAlwaysNoHealthd(t *testing.T) {
	bin := fencetest.FakeBin(t, t.TempDir(), "incus-compose", "")
	c := start.Compose{Run: incus.Run, Bin: bin.Path, Project: project, File: "/x/compose.yaml", UpArgs: []string{"--detach"}}
	if err := c.Prepare(context.Background(), "asus"); err != nil {
		t.Fatal(err)
	}
	want := "-p p-asus -f /x/compose.yaml up --no-start --recreate --detach --no-healthd"
	if got := bin.Calls(t); len(got) != 1 || got[0] != want {
		t.Fatalf("argv %q, want %q", got, want)
	}
}
