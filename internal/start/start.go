// Package start holds the ways a member brings its copy of the workload up in
// its own pinned project. The agent is starter-agnostic: it fences, records
// may-run, then calls Start; everything about *what* runs lives here.
package start

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/alien43/pagurus/internal/backend"
)

const (
	KindCompose   = "compose"
	KindInstances = "instances"
)

// Starter brings one member's project up.
type Starter interface {
	Kind() string
	// CheckStart is the preflight: the workload is startable on member.
	CheckStart(ctx context.Context, member string) error
	// Prepare does everything slow that runs nothing (pulls, creates), so
	// that the leader's last authorising check comes right before Start.
	// It must be idempotent.
	Prepare(ctx context.Context, member string) error
	// Pull refreshes the member's image store from the registry, creating
	// and starting nothing. It must run against the member's own Incus: an
	// image is pulled for the architecture of the server it talks to.
	Pull(ctx context.Context, member string) error
	// Start brings member's project up. It must be idempotent, and after
	// Prepare it should be quick: the time it takes is the window in which a
	// deposed leader can still start a copy (Incus has no fencing token).
	Start(ctx context.Context, member string) error
	// Types are the Incus instance types it may run ("container", ...).
	Types() []string
}

// Runner runs a CLI and returns stdout (incus.Run, or a fake in tests).
type Runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// Lister is the cluster's view of a member's project.
type Lister interface {
	List(ctx context.Context, member string) ([]backend.Instance, error)
}

// InstanceStarter lists and starts instances of a member's project
// (incus.Driver, over the Incus API).
type InstanceStarter interface {
	Lister
	StartInstances(ctx context.Context, member string, names []string) error
}

// Compose runs `incus-compose up` in the member's project.
type Compose struct {
	Run     Runner
	Bin     string // default incus-compose
	Project func(member string) string
	File    string
	Args    []string // global flags
	UpArgs  []string // flags after `up`
}

func (c Compose) Kind() string    { return KindCompose }
func (c Compose) Types() []string { return []string{"container"} }

func (c Compose) bin() string {
	if c.Bin == "" {
		return "incus-compose"
	}
	return c.Bin
}

func (c Compose) base(member string) []string {
	return append([]string{"-p", c.Project(member), "-f", c.File}, c.Args...)
}

// CheckStart renders the compose file, proving it parses.
func (c Compose) CheckStart(ctx context.Context, member string) error {
	_, err := c.Run(ctx, c.bin(), append(c.base(member), "config")...)
	return err
}

// Prepare is `incus-compose up --no-start --recreate`: pull, and create this
// member's instances afresh from the current compose file and local image,
// starting nothing. A term starts only when nothing of ours runs here (or to
// restart an unhealthy copy), so recreating costs ~1 s and is what makes a
// compose change (a port, a label) or a freshly pulled image reach the
// instance: `up` alone only patches missing config keys, never devices.
// --no-healthd always: a recreate must not touch the project's ic-healthd.
func (c Compose) Prepare(ctx context.Context, member string) error {
	args := append(append(c.base(member), "up", "--no-start", "--recreate"), c.UpArgs...)
	if !slices.Contains(args, "--no-healthd") {
		args = append(args, "--no-healthd")
	}
	_, err := c.Run(ctx, c.bin(), args...)
	return err
}

// Pull is `incus-compose pull --no-healthd`: new images into the project's
// store; no instance changes until the next Prepare.
func (c Compose) Pull(ctx context.Context, member string) error {
	_, err := c.Run(ctx, c.bin(), append(c.base(member), "pull", "--no-healthd")...)
	return err
}

func (c Compose) Start(ctx context.Context, member string) error {
	args := append(append(c.base(member), "up"), c.UpArgs...)
	_, err := c.Run(ctx, c.bin(), args...)
	return err
}

// Instances starts instances that already exist in the member's project
// (created by an operator, or by `pagurus provision` later). Names selects
// them; empty means every instance in the project.
type Instances struct {
	Project func(member string) string
	L       InstanceStarter
	Names   []string
}

func (s Instances) Kind() string    { return KindInstances }
func (s Instances) Types() []string { return []string{"container", "virtual-machine"} }

// selected returns the instances Start acts on, checking the names exist.
func (s Instances) selected(ctx context.Context, member string) ([]backend.Instance, error) {
	insts, err := s.L.List(ctx, member)
	if err != nil {
		return nil, err
	}
	if len(s.Names) == 0 {
		if len(insts) == 0 {
			return nil, fmt.Errorf("project %s has no instances to start", s.Project(member))
		}
		return insts, nil
	}
	var out []backend.Instance
	var missing []string
	for _, n := range s.Names {
		i := slices.IndexFunc(insts, func(in backend.Instance) bool { return in.Name == n })
		if i < 0 {
			missing = append(missing, n)
			continue
		}
		out = append(out, insts[i])
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("project %s has no instance(s) %s", s.Project(member), strings.Join(missing, ", "))
	}
	return out, nil
}

// CheckStart: every selected instance exists, is a container or a VM, and
// will not start on its own at boot. Which fences can confirm it stopped is
// the self chain's business (cgroup-kill: containers; qemu-kill: VMs).
func (s Instances) CheckStart(ctx context.Context, member string) error {
	insts, err := s.selected(ctx, member)
	if err != nil {
		return err
	}
	for _, in := range insts {
		if in.Type != "" && in.Type != "container" && in.Type != "virtual-machine" {
			return fmt.Errorf("instance %s is a %s; the instances starter runs containers and VMs", in.Name, in.Type)
		}
		if in.Expanded["boot.autostart"] != "false" {
			return fmt.Errorf("instance %s: boot.autostart must be \"false\" (unset restores the last state on boot)", in.Name)
		}
	}
	return nil
}

// Prepare has nothing to do: the instances exist already.
func (s Instances) Prepare(context.Context, string) error { return nil }

// Pull: instances are created outside pagurus; there is nothing to pull.
func (s Instances) Pull(context.Context, string) error { return nil }

// Start starts every selected instance that is not running.
func (s Instances) Start(ctx context.Context, member string) error {
	insts, err := s.selected(ctx, member)
	if err != nil {
		return err
	}
	var names []string
	for _, in := range insts {
		if in.Status != backend.StatusRunning {
			names = append(names, in.Name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	return s.L.StartInstances(ctx, member, names)
}
