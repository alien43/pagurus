package incus_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/alien43/pagurus/internal/fence/fencetest"
	"github.com/alien43/pagurus/internal/incus"
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
