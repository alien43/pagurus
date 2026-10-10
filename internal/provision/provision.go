// Package provision prepares the Incus side of a pagurus service: one cluster
// group per member holding exactly that member (host-<member>), and one
// project per member pinned to it (restricted.cluster.groups). It replaces
// scripts/provision.sh.
//
// It plans first and applies second, so --dry-run shows exactly what would
// happen. It only ever creates things or fills an empty group; it never
// removes a member from a group, never changes an existing project's pin,
// and refuses (rather than repairs) anything that contradicts the layout.
package provision

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/alien43/pagurus/internal/incus"
)

// GroupPrefix names the per-member cluster groups: host-<member>.
const GroupPrefix = "host-"

// ProjectConfig is what a new per-member project gets, besides its pin.
var ProjectConfig = map[string]string{
	"features.images":          "true",
	"features.profiles":        "true",
	"features.storage.volumes": "true",
	"features.storage.buckets": "true",
	"user.pagurus.managed":     "true",
}

// Action is one change, with the equivalent incus command for --dry-run.
type Action struct {
	// Group is true for cluster-group changes, which touch cluster-wide
	// placement and need explicit confirmation.
	Group   bool
	Command string
	apply   func(ctx context.Context, api incus.API) error
}

// Plan is what provisioning would do: the actions, plus the facts that were
// already right.
type Plan struct {
	Actions []Action
	OK      []string
}

// Make plans provisioning of projectPrefix-<member> for members. It returns an
// error for anything it will not fix: a group with other members in it, or a
// project pinned elsewhere.
func Make(ctx context.Context, api incus.API, projectPrefix string, members []string) (*Plan, error) {
	p := &Plan{}
	for _, m := range members {
		group := GroupPrefix + m
		gm, err := api.ClusterGroupMembers(ctx, group)
		switch {
		case incus.IsNotFound(err):
			members := []string{m}
			p.Actions = append(p.Actions, Action{Group: true,
				Command: fmt.Sprintf("incus cluster group create %s && incus cluster group assign-add %s %s", group, m, group),
				apply: func(ctx context.Context, api incus.API) error {
					return api.CreateClusterGroup(ctx, group, members)
				}})
		case err != nil:
			return nil, fmt.Errorf("cluster group %s: %w", group, err)
		case len(gm) == 0:
			members := []string{m}
			p.Actions = append(p.Actions, Action{Group: true,
				Command: fmt.Sprintf("incus cluster group assign-add %s %s", m, group),
				apply: func(ctx context.Context, api incus.API) error {
					return api.SetClusterGroupMembers(ctx, group, members)
				}})
		case len(gm) == 1 && gm[0] == m:
			p.OK = append(p.OK, fmt.Sprintf("cluster group %s = [%s]", group, m))
		default:
			return nil, fmt.Errorf("cluster group %s has members %v; a pin needs exactly [%s]. Not changing it: fix it by hand or use another group", group, gm, m)
		}

		project := projectPrefix + "-" + m
		cfg, err := api.ProjectConfig(ctx, project)
		switch {
		case incus.IsNotFound(err):
			pc := map[string]string{"restricted.cluster.groups": group}
			for k, v := range ProjectConfig {
				pc[k] = v
			}
			keys := make([]string, 0, len(pc))
			for k := range pc {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			var flags []string
			for _, k := range keys {
				flags = append(flags, fmt.Sprintf("-c %s=%s", k, pc[k]))
			}
			p.Actions = append(p.Actions, Action{
				Command: fmt.Sprintf("incus project create %s %s", project, strings.Join(flags, " ")),
				apply: func(ctx context.Context, api incus.API) error {
					return api.CreateProject(ctx, project, pc)
				}})
		case err != nil:
			return nil, fmt.Errorf("project %s: %w", project, err)
		case cfg["restricted.cluster.groups"] != group:
			return nil, fmt.Errorf("project %s exists with restricted.cluster.groups=%q, want %q. Not changing it", project, cfg["restricted.cluster.groups"], group)
		default:
			p.OK = append(p.OK, fmt.Sprintf("project %s pinned to %s", project, group))
		}
	}
	return p, nil
}

// NeedsConfirmation reports whether any action changes cluster groups.
func (p *Plan) NeedsConfirmation() bool {
	return slices.ContainsFunc(p.Actions, func(a Action) bool { return a.Group })
}

// Apply runs the actions in order, stopping at the first error.
func (p *Plan) Apply(ctx context.Context, api incus.API) error {
	for _, a := range p.Actions {
		if err := a.apply(ctx, api); err != nil {
			return fmt.Errorf("%s: %w", a.Command, err)
		}
	}
	return nil
}
