package provision_test

import (
	"context"
	"strings"
	"testing"

	"github.com/alien43/pagurus/internal/fence/fencetest"
	"github.com/alien43/pagurus/internal/provision"
)

var ctx = context.Background()

func TestFreshClusterIsProvisionedAndThenIdempotent(t *testing.T) {
	api := fencetest.NewFakeIncus("asus", "dell")
	delete(api.Groups, "host-dell") // dell has no group yet
	p, err := provision.Make(ctx, api, "svc", "svc", []string{"asus", "dell"}, provision.Volumes{})
	if err != nil {
		t.Fatal(err)
	}
	var cmds []string
	for _, a := range p.Actions {
		cmds = append(cmds, a.Command)
	}
	got := strings.Join(cmds, "\n")
	for _, want := range []string{
		"incus project create svc-asus",
		"restricted.cluster.groups=host-asus",
		"incus cluster group create host-dell && incus cluster group assign-add dell host-dell",
		"incus project create svc-dell",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("plan lacks %q:\n%s", want, got)
		}
	}
	if !p.NeedsConfirmation() {
		t.Error("creating a cluster group must need confirmation")
	}
	if api.Mutations() != 0 {
		t.Fatal("planning changed something")
	}
	if err := p.Apply(ctx, api); err != nil {
		t.Fatal(err)
	}
	if g := api.Groups["host-dell"]; len(g) != 1 || g[0] != "dell" {
		t.Fatalf("host-dell = %v", g)
	}
	if api.Projects["svc-dell"]["restricted.cluster.groups"] != "host-dell" || api.Projects["svc-dell"]["user.pagurus.service"] != "svc" || api.Projects["svc-dell"]["user.pagurus.managed"] != "" {
		t.Fatalf("svc-dell config %v", api.Projects["svc-dell"])
	}
	again, err := provision.Make(ctx, api, "svc", "svc", []string{"asus", "dell"}, provision.Volumes{})
	if err != nil || len(again.Actions) != 0 || len(again.OK) != 6 {
		t.Fatalf("second run: %d actions, %d ok, err %v; want nothing to do", len(again.Actions), len(again.OK), err)
	}
}

func TestRefusesWhatItWillNotFix(t *testing.T) {
	api := fencetest.NewFakeIncus("asus", "dell")
	api.Groups["host-asus"] = []string{"asus", "dell"}
	if _, err := provision.Make(ctx, api, "svc", "svc", []string{"asus"}, provision.Volumes{}); err == nil || !strings.Contains(err.Error(), "Not changing it") {
		t.Fatalf("shared group: %v", err)
	}
	api = fencetest.NewFakeIncus("asus")
	api.Projects["svc-asus"] = map[string]string{"restricted.cluster.groups": "everyone"}
	if _, err := provision.Make(ctx, api, "svc", "svc", []string{"asus"}, provision.Volumes{}); err == nil || !strings.Contains(err.Error(), "Not changing it") {
		t.Fatalf("project pinned elsewhere: %v", err)
	}
}

func TestEmptyGroupIsFilledWithConfirmation(t *testing.T) {
	api := fencetest.NewFakeIncus("asus")
	api.Groups["host-asus"] = nil
	p, err := provision.Make(ctx, api, "svc", "svc", []string{"asus"}, provision.Volumes{})
	if err != nil || !p.NeedsConfirmation() {
		t.Fatalf("err %v, confirm %v", err, p.NeedsConfirmation())
	}
	if err := p.Apply(ctx, api); err != nil {
		t.Fatal(err)
	}
	if g := api.Groups["host-asus"]; len(g) != 1 || g[0] != "asus" {
		t.Fatalf("host-asus = %v", g)
	}
}

// Replicated volumes: created in every member's project on that member, an
// existing one left alone, a missing pool refused.
func TestReplicatedVolumes(t *testing.T) {
	api := fencetest.NewFakeIncus("asus", "dell")
	api.Pools["zp"] = true
	api.Projects["svc-asus"] = map[string]string{"restricted.cluster.groups": "host-asus"}
	if err := api.CreateVolume(ctx, "svc-asus", "zp", "asus", "data"); err != nil {
		t.Fatal(err)
	}
	vols := provision.Volumes{Pool: "zp", Names: []string{"data"}}
	p, err := provision.Make(ctx, api, "svc", "svc", []string{"asus", "dell"}, vols)
	if err != nil {
		t.Fatal(err)
	}
	var cmds []string
	for _, a := range p.Actions {
		cmds = append(cmds, a.Command)
	}
	got := strings.Join(cmds, "\n")
	if !strings.Contains(got, "incus storage volume create zp data --project svc-dell --target dell") {
		t.Fatalf("plan lacks dell's volume:\n%s", got)
	}
	if strings.Contains(got, "--project svc-asus --target asus") {
		t.Fatalf("plan recreates asus's existing volume:\n%s", got)
	}
	if err := p.Apply(ctx, api); err != nil {
		t.Fatal(err)
	}
	if _, ok := api.Vols["svc-dell/zp/dell/data"]; !ok {
		t.Fatalf("dell's volume not created: %v", api.Vols)
	}
	again, err := provision.Make(ctx, api, "svc", "svc", []string{"asus", "dell"}, vols)
	if err != nil || len(again.Actions) != 0 {
		t.Fatalf("second run: %d actions, err %v", len(again.Actions), err)
	}
	if _, err := provision.Make(ctx, api, "svc", "svc", []string{"asus"}, provision.Volumes{Pool: "nope", Names: []string{"data"}}); err == nil {
		t.Fatal("a missing pool was not refused")
	}
}

// A project made before the marker was user.pagurus.service (it was
// user.pagurus.managed=true, which nothing read) gets the new key and loses
// the old one; a project marked for another service is refused.
func TestServiceMarker(t *testing.T) {
	api := fencetest.NewFakeIncus("asus")
	api.Projects["svc-asus"] = map[string]string{"restricted.cluster.groups": "host-asus", "user.pagurus.managed": "true"}
	p, err := provision.Make(ctx, api, "web", "svc", []string{"asus"}, provision.Volumes{})
	if err != nil {
		t.Fatal(err)
	}
	var cmds []string
	for _, a := range p.Actions {
		cmds = append(cmds, a.Command)
	}
	if got := strings.Join(cmds, "\n"); !strings.Contains(got, "incus project set svc-asus user.pagurus.service=web") ||
		!strings.Contains(got, "incus project unset svc-asus user.pagurus.managed") {
		t.Fatalf("plan:\n%s", got)
	}
	if p.NeedsConfirmation() {
		t.Error("marking a project must not need confirmation")
	}
	if err := p.Apply(ctx, api); err != nil {
		t.Fatal(err)
	}
	if c := api.Projects["svc-asus"]; c["user.pagurus.service"] != "web" || c["user.pagurus.managed"] != "" {
		t.Fatalf("svc-asus config %v", c)
	}
	if again, err := provision.Make(ctx, api, "web", "svc", []string{"asus"}, provision.Volumes{}); err != nil || len(again.Actions) != 0 {
		t.Fatalf("second run: %v, err %v", again, err)
	}
	if _, err := provision.Make(ctx, api, "other", "svc", []string{"asus"}, provision.Volumes{}); err == nil || !strings.Contains(err.Error(), "Not changing it") {
		t.Fatalf("a project of another service: %v", err)
	}
}
