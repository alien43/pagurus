package incus_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/alien43/pagurus/internal/backend/incus"
	"github.com/alien43/pagurus/internal/fence/fencetest"
)

func TestDriverVolumes(t *testing.T) {
	api := fencetest.NewFakeIncus("asus", "dell")
	api.Projects["svc-asus"], api.Projects["svc-dell"] = map[string]string{}, map[string]string{}
	api.Pools["zp"] = true
	var calls []string
	drv := incus.New(incus.Options{ProjectPrefix: "svc", API: api, Pool: "zp",
		RunCLI: func(_ context.Context, name string, args ...string) ([]byte, error) {
			calls = append(calls, name+" "+strings.Join(args, " "))
			return nil, nil
		}})
	ctx := context.Background()
	if err := api.CreateVolume(ctx, "svc-asus", "zp", "asus", "data"); err != nil {
		t.Fatal(err)
	}
	if vs, err := drv.ListVolumes(ctx, "asus"); err != nil || !slices.Equal(vs, []string{"data"}) {
		t.Fatalf("ListVolumes = %v, %v", vs, err)
	}
	if vs, _ := drv.ListVolumes(ctx, "dell"); len(vs) != 0 {
		t.Fatalf("dell lists asus's volume: %v", vs)
	}
	if err := drv.SnapshotVolume(ctx, "asus", "data", "s1"); err != nil {
		t.Fatal(err)
	}
	if s, err := drv.VolumeSnapshots(ctx, "asus", "data"); err != nil || !slices.Equal(s, []string{"s1"}) {
		t.Fatalf("VolumeSnapshots = %v, %v", s, err)
	}
	if err := drv.DeleteVolumeSnapshot(ctx, "asus", "data", "s1"); err != nil {
		t.Fatal(err)
	}
	if err := drv.RefreshVolume(ctx, "asus", "dell", "data"); err != nil {
		t.Fatal(err)
	}
	if err := drv.CopyVolume(ctx, "dell", "data", "data-orphaned-7-20261015T220000Z"); err != nil {
		t.Fatal(err)
	}
	if err := drv.Exec(ctx, "asus", "app-1", []string{"sqlite3", "/data/app.db", ".backup /data/x"}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"incus storage volume copy zp/data zp/data --project svc-asus --target asus --target-project svc-dell --destination-target dell --refresh",
		"incus storage volume copy zp/data zp/data-orphaned-7-20261015T220000Z --project svc-dell --target dell --destination-target dell",
		"incus exec app-1 --project svc-asus -- sqlite3 /data/app.db .backup /data/x",
	}
	if !slices.Equal(calls, want) {
		t.Fatalf("CLI calls:\n%s\nwant:\n%s", strings.Join(calls, "\n"), strings.Join(want, "\n"))
	}
	if _, err := incus.New(incus.Options{ProjectPrefix: "svc", API: api}).ListVolumes(ctx, "asus"); err == nil {
		t.Fatal("a driver without a pool listed volumes")
	}
}

// A volume never attached (volatile.idmap.last empty: fresh from provision,
// or pulled) gets the containers' map before the first start, so Incus does
// not shift it by +1000000. A stamped volume is left alone; VMs only: nothing.
func TestEnsureVolumeIdmap(t *testing.T) {
	const m1 = `[{"Isuid":true,"Isgid":false,"Hostid":1000000,"Nsid":0,"Maprange":1000000000}]`
	ctx := context.Background()
	setup := func(insts ...incus.Instance) (*fencetest.FakeIncus, *incus.Driver) {
		api := fencetest.NewFakeIncus("asus")
		api.Projects["svc-asus"] = map[string]string{}
		api.Pools["zp"] = true
		api.Insts["svc-asus"] = insts
		if err := api.CreateVolume(ctx, "svc-asus", "zp", "asus", "data"); err != nil {
			t.Fatal(err)
		}
		return api, incus.New(incus.Options{ProjectPrefix: "svc", API: api, Pool: "zp"})
	}
	ct := func(name, idmap string) incus.Instance {
		return incus.Instance{Name: name, Type: "container", Location: "asus", Status: "Stopped",
			Expanded: map[string]string{"volatile.idmap.next": idmap}}
	}
	last := func(api *fencetest.FakeIncus) string {
		c, err := api.VolumeConfig(ctx, "svc-asus", "zp", "asus", "data")
		if err != nil {
			t.Fatal(err)
		}
		return c[incus.IdmapLastKey]
	}

	api, drv := setup(ct("app-1", m1), ct("app-2", m1))
	if got, err := drv.EnsureVolumeIdmap(ctx, "asus", "data"); err != nil || got != m1 || last(api) != m1 {
		t.Fatalf("empty volume: stamped %q, err %v, now %q; want %q", got, err, last(api), m1)
	}
	if got, err := drv.EnsureVolumeIdmap(ctx, "asus", "data"); err != nil || got != "" {
		t.Fatalf("stamped volume touched again: %q, %v", got, err)
	}

	api, drv = setup(ct("app-1", m1))
	_ = api.SetVolumeConfig(ctx, "svc-asus", "zp", "asus", "data", incus.IdmapLastKey, "[other]")
	if got, err := drv.EnsureVolumeIdmap(ctx, "asus", "data"); err != nil || got != "" || last(api) != "[other]" {
		t.Fatalf("a set volume was changed: %q, %v, now %q", got, err, last(api))
	}

	api, drv = setup(incus.Instance{Name: "vm", Type: "virtual-machine", Location: "asus"})
	if got, err := drv.EnsureVolumeIdmap(ctx, "asus", "data"); err != nil || got != "" || last(api) != "" {
		t.Fatalf("VMs only: %q, %v, now %q", got, err, last(api))
	}

	_, drv = setup(ct("app-1", m1), ct("app-2", "[different]"))
	if _, err := drv.EnsureVolumeIdmap(ctx, "asus", "data"); err == nil {
		t.Fatal("containers with different maps: no error")
	}
}
