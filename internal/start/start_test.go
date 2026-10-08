package start_test

import (
	"context"
	"strings"
	"testing"

	"github.com/alien43/pagurus/internal/fence/fencetest"
	"github.com/alien43/pagurus/internal/incus"
	"github.com/alien43/pagurus/internal/start"
)

func project(m string) string { return "p-" + m }

func TestComposeArgv(t *testing.T) {
	bin := fencetest.FakeBin(t, t.TempDir(), "incus-compose", "")
	c := start.Compose{Run: incus.Run, Bin: bin.Path, Project: project, File: "/x/compose.yaml", Args: []string{"--env-file", "/x/.env"}, UpArgs: []string{"--detach", "--no-healthd"}}
	if err := c.CheckStart(context.Background(), "asus"); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background(), "asus"); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"-p p-asus -f /x/compose.yaml --env-file /x/.env config",
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

func inst(name, status, typ, autostart string) incus.Instance {
	return incus.Instance{Name: name, Status: status, Type: typ, Expanded: map[string]string{"boot.autostart": autostart}}
}

func TestInstancesStartsOnlyWhatIsNotRunning(t *testing.T) {
	bin := fencetest.FakeBin(t, t.TempDir(), "incus", "")
	s := start.Instances{Run: incus.Run, Bin: bin.Path, Project: project, L: fakeList{
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
	if got := bin.Calls(t); len(got) != 1 || got[0] != "start --project p-dell web" {
		t.Fatalf("argv %q, want only web started (db runs, tmp not selected)", got)
	}
	bin2 := fencetest.FakeBin(t, t.TempDir(), "incus", "")
	s.Bin, s.L = bin2.Path, fakeList{inst("web", incus.StatusRunning, "container", "false")}
	s.Names = nil
	if err := s.Start(context.Background(), "dell"); err != nil || len(bin2.Calls(t)) != 0 {
		t.Fatalf("all running: err %v calls %q, want nothing to do", err, bin2.Calls(t))
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
		s := start.Instances{Run: incus.Run, Project: project, L: c.l, Names: c.names}
		if err := s.CheckStart(context.Background(), "rpi5"); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want %q", name, err, c.want)
		}
	}
}

func TestInstancesAcceptsVMs(t *testing.T) {
	s := start.Instances{Run: incus.Run, Project: project, L: fakeList{inst("vm", "Stopped", "virtual-machine", "false")}}
	if err := s.CheckStart(context.Background(), "asus"); err != nil {
		t.Fatalf("a VM was refused: %v", err)
	}
}
