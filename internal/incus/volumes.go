package incus

import (
	"context"
	"errors"
	"fmt"
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

// IdmapLastKey is where Incus records the idmap a custom volume's contents
// are shifted to. Empty means "never attached": the next attach shifts the
// whole volume by the container's base (+1000000), whatever is on it.
const IdmapLastKey = "volatile.idmap.last"

// EnsureVolumeIdmap stamps vol's volatile.idmap.last with the idmap of the
// containers in member's project when it is empty, and returns what it
// stamped ("" when it left the volume alone). A replicated volume's contents
// are always in container form: seeded chowned to 1001000 (runbook § 6), or
// pulled from a leader whose containers wrote them. A pull does not carry the
// key, and `pagurus provision` cannot know the map before an instance exists,
// so without this the first attach on each member shifted the data by
// +1000000 (gokapi-dev 2026-10-16: 1001000 -> 2001000). Run it after the
// instances exist (Prepare) and before they start. No container (VMs only):
// nothing to stamp. Containers with different maps: an error, for a human.
func (d *Driver) EnsureVolumeIdmap(ctx context.Context, member, vol string) (string, error) {
	p, err := d.pool()
	if err != nil {
		return "", err
	}
	cfg, err := d.o.API.VolumeConfig(ctx, d.Project(member), p, member, vol)
	if err != nil {
		return "", err
	}
	if cfg[IdmapLastKey] != "" {
		return "", nil
	}
	insts, err := d.List(ctx, member)
	if err != nil {
		return "", err
	}
	idmap := ""
	for _, in := range insts {
		if in.Type != "" && in.Type != "container" {
			continue
		}
		next := in.Expanded["volatile.idmap.next"]
		if next == "" {
			return "", fmt.Errorf("container %s has no volatile.idmap.next yet; cannot stamp %s", in.Name, vol)
		}
		if idmap != "" && next != idmap {
			return "", fmt.Errorf("containers in %s have different idmaps; set %s on volume %s by hand (runbook § 6)",
				d.Project(member), IdmapLastKey, vol)
		}
		idmap = next
	}
	if idmap == "" {
		return "", nil
	}
	if err := d.o.API.SetVolumeConfig(ctx, d.Project(member), p, member, vol, IdmapLastKey, idmap); err != nil {
		return "", err
	}
	return idmap, nil
}
