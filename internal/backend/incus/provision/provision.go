// Package provision prepares the Incus side of a pagurus service: one cluster
// group per member holding exactly that member (host-<member>), and one
// project per member pinned to it (restricted.cluster.groups), and, for a
// service with a replicate block, each replicated custom volume in every
// member's project on that member. It replaces scripts/provision.sh.
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

	"github.com/alien43/pagurus/internal/backend/incus"
)

// GroupPrefix names the per-member cluster groups: host-<member>.
const GroupPrefix = "host-"

// ProjectConfig is what a new per-member project gets, besides its pin and
// ServiceKey.
var ProjectConfig = map[string]string{
	"features.images":          "true",
	"features.profiles":        "true",
	"features.storage.volumes": "true",
	"features.storage.buckets": "true",
}

// ServiceKey marks a project as the home of one pagurus service on one
// member; its value is the service. Other tools key off it (standby-sync
// skips these projects: pagurus moves their data itself, if at all).
const ServiceKey = "user.pagurus.service"

// oldMarker is what projects were marked with before ServiceKey. Nothing
// read it; provision replaces it.
const oldMarker = "user.pagurus.managed"

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
	// Todo is what provisioning cannot do and an operator must, before a
	// second run finishes the job (a replicated instance no member has yet).
	Todo []string
}

// Volumes are the replicated custom volumes to create, on Pool (none for a
// service without replication), and the replicated instances to seed.
type Volumes struct {
	Pool  string
	Names []string
	// Instances must exist in at least one member's project already (made by
	// an operator); each member that lacks one gets a stopped copy of it
	// with boot.autostart=false, from the first member in order that has it.
	// An existing one is never touched. Copy runs the copy (`incus copy`).
	Instances []string
	Copy      func(ctx context.Context, fromProject, toProject, toMember, name string) error
}

// Make plans provisioning of projectPrefix-<member> for members. It returns an
// error for anything it will not fix: a group with other members in it, or a
// project pinned elsewhere, or a volume pool that does not exist.
func Make(ctx context.Context, api incus.API, service, projectPrefix string, members []string, vols Volumes) (*Plan, error) {
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
		newProject := incus.IsNotFound(err)
		switch {
		case incus.IsNotFound(err):
			pc := map[string]string{"restricted.cluster.groups": group, ServiceKey: service}
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
		case cfg[ServiceKey] != "" && cfg[ServiceKey] != service:
			return nil, fmt.Errorf("project %s belongs to service %q (%s), not %q. Not changing it", project, cfg[ServiceKey], ServiceKey, service)
		default:
			p.OK = append(p.OK, fmt.Sprintf("project %s pinned to %s", project, group))
			planMarker(p, project, service, cfg)
		}
		if err := planVolumes(ctx, api, p, project, m, newProject, vols); err != nil {
			return nil, err
		}
	}
	if err := planInstances(ctx, api, p, projectPrefix, members, vols); err != nil {
		return nil, err
	}
	return p, nil
}

// planInstances seeds each replicated instance on the members that lack it,
// copied from the first member in order that has it. It runs after the
// projects are planned: a new project has no instances.
func planInstances(ctx context.Context, api incus.API, p *Plan, projectPrefix string, members []string, vols Volumes) error {
	if len(vols.Instances) == 0 {
		return nil
	}
	have := map[string][]string{} // member -> instance names
	for _, m := range members {
		insts, err := api.Instances(ctx, projectPrefix+"-"+m)
		if err != nil && !incus.IsNotFound(err) {
			return fmt.Errorf("instances of %s-%s: %w", projectPrefix, m, err)
		}
		for _, in := range insts {
			have[m] = append(have[m], in.Name)
		}
	}
	for _, name := range vols.Instances {
		src := slices.IndexFunc(members, func(m string) bool { return slices.Contains(have[m], name) })
		if src < 0 {
			// The projects may be created by this very run: say what to do
			// next rather than refuse the plan.
			p.Todo = append(p.Todo, fmt.Sprintf("replicated instance %s exists in no member's project: create it in one (stopped, boot.autostart=false), then run provision again to copy it to the others", name))
			continue
		}
		from := projectPrefix + "-" + members[src]
		for _, m := range members {
			project := projectPrefix + "-" + m
			if slices.Contains(have[m], name) {
				p.OK = append(p.OK, fmt.Sprintf("instance %s in %s", name, project))
				continue
			}
			if vols.Copy == nil {
				return fmt.Errorf("instance %s: %s lacks it and this provisioner cannot copy instances", name, project)
			}
			to, member, n := project, m, name
			p.Actions = append(p.Actions, Action{
				Command: fmt.Sprintf("incus copy %s %s --project %s --target-project %s --target %s --stateless -c boot.autostart=false", n, n, from, to, member),
				apply: func(ctx context.Context, api incus.API) error {
					return vols.Copy(ctx, from, to, member, n)
				}})
		}
	}
	return nil
}

// planMarker marks an existing project with ServiceKey and drops oldMarker.
func planMarker(p *Plan, project, service string, cfg map[string]string) {
	if cfg[ServiceKey] == service {
		p.OK = append(p.OK, fmt.Sprintf("project %s marked %s=%s", project, ServiceKey, service))
	} else {
		p.Actions = append(p.Actions, Action{
			Command: fmt.Sprintf("incus project set %s %s=%s", project, ServiceKey, service),
			apply: func(ctx context.Context, api incus.API) error {
				return api.SetProjectConfig(ctx, project, ServiceKey, service)
			}})
	}
	if _, ok := cfg[oldMarker]; ok {
		p.Actions = append(p.Actions, Action{
			Command: fmt.Sprintf("incus project unset %s %s", project, oldMarker),
			apply: func(ctx context.Context, api incus.API) error {
				return api.SetProjectConfig(ctx, project, oldMarker, "")
			}})
	}
}

// planVolumes adds each missing replicated volume of member's project. An
// existing volume is never touched: its data may be the only copy.
func planVolumes(ctx context.Context, api incus.API, p *Plan, project, member string, newProject bool, vols Volumes) error {
	if len(vols.Names) == 0 {
		return nil
	}
	var have []string
	if !newProject {
		var err error
		if have, err = api.Volumes(ctx, project, vols.Pool, member); err != nil {
			return fmt.Errorf("volumes of %s on pool %s (%s): %w", project, vols.Pool, member, err)
		}
	}
	for _, v := range vols.Names {
		if slices.Contains(have, v) {
			p.OK = append(p.OK, fmt.Sprintf("volume %s/%s in %s on %s", vols.Pool, v, project, member))
			continue
		}
		pool, name := vols.Pool, v
		p.Actions = append(p.Actions, Action{
			Command: fmt.Sprintf("incus storage volume create %s %s --project %s --target %s", pool, name, project, member),
			apply: func(ctx context.Context, api incus.API) error {
				return api.CreateVolume(ctx, project, pool, member, name)
			}})
	}
	return nil
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
