package incus

import (
	"context"
	"errors"
)

// Replication (agent.Volumes): custom volumes on Options.Pool, in member's
// project, on member. Snapshots go through the API; copies and exec through
// the incus CLI, as standby-sync runs them (proven live since 2026-09), with
// the source and destination members always named: the pool is
// member-local, and without --target the CLI looks for the volume on the
// member it talks to.

func (d *Driver) pool() (string, error) {
	if d.o.Pool == "" {
		return "", errors.New("incus driver: no replicate pool configured")
	}
	return d.o.Pool, nil
}

// VolumeSnapshots lists the snapshot names of vol in member's project.
func (d *Driver) VolumeSnapshots(ctx context.Context, member, vol string) ([]string, error) {
	p, err := d.pool()
	if err != nil {
		return nil, err
	}
	return d.o.API.VolumeSnapshots(ctx, d.Project(member), p, member, vol)
}

func (d *Driver) SnapshotVolume(ctx context.Context, member, vol, snap string) error {
	p, err := d.pool()
	if err != nil {
		return err
	}
	return d.o.API.CreateVolumeSnapshot(ctx, d.Project(member), p, member, vol, snap)
}

func (d *Driver) DeleteVolumeSnapshot(ctx context.Context, member, vol, snap string) error {
	p, err := d.pool()
	if err != nil {
		return err
	}
	return d.o.API.DeleteVolumeSnapshot(ctx, d.Project(member), p, member, vol, snap)
}

// ListVolumes lists the custom volumes in member's project on member.
func (d *Driver) ListVolumes(ctx context.Context, member string) ([]string, error) {
	p, err := d.pool()
	if err != nil {
		return nil, err
	}
	return d.o.API.Volumes(ctx, d.Project(member), p, member)
}

// RefreshVolume pulls vol from from's project into to's: `incus storage
// volume copy --refresh` (an incremental ZFS send; a forced receive on the
// target, tasks/results/pagurus-replica-spike.md). Cancelling ctx kills the
// CLI.
func (d *Driver) RefreshVolume(ctx context.Context, from, to, vol string) error {
	p, err := d.pool()
	if err != nil {
		return err
	}
	_, err = d.o.RunCLI(ctx, "incus", "storage", "volume", "copy", p+"/"+vol, p+"/"+vol,
		"--project", d.Project(from), "--target", from,
		"--target-project", d.Project(to), "--destination-target", to, "--refresh")
	return err
}

// CopyVolume copies vol to a new volume dst in member's project, on member.
func (d *Driver) CopyVolume(ctx context.Context, member, vol, dst string) error {
	p, err := d.pool()
	if err != nil {
		return err
	}
	_, err = d.o.RunCLI(ctx, "incus", "storage", "volume", "copy", p+"/"+vol, p+"/"+dst,
		"--project", d.Project(member), "--target", member, "--destination-target", member)
	return err
}

// Exec runs argv in instance of member's project.
func (d *Driver) Exec(ctx context.Context, member, instance string, argv []string) error {
	args := append([]string{"exec", instance, "--project", d.Project(member), "--"}, argv...)
	_, err := d.o.RunCLI(ctx, "incus", args...)
	return err
}
