// Package start holds the ways a member brings its copy of the workload up in
// its own pinned project. The agent is starter-agnostic: it fences, records
// may-run, then calls Start; everything about *what* runs lives here.
package start

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/alien43/pagurus/internal/incus"
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
	// Start brings member's project up. It must be idempotent.
	Start(ctx context.Context, member string) error
	// Types are the Incus instance types it may run ("container", ...).
	Types() []string
}

// Runner runs a CLI and returns stdout (incus.Run, or a fake in tests).
type Runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// Lister is the cluster's view of a member's project.
type Lister interface {
	List(ctx context.Context, member string) ([]incus.Instance, error)
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

func (c Compose) Start(ctx context.Context, member string) error {
	args := append(append(c.base(member), "up"), c.UpArgs...)
	_, err := c.Run(ctx, c.bin(), args...)
	return err
}

// Instances starts containers that already exist in the member's project
// (created by an operator, or by `pagurus provision` later). Names selects
// them; empty means every instance in the project.
type Instances struct {
	Run     Runner
	Bin     string // default incus
	Project func(member string) string
	L       Lister
	Names   []string
}

func (s Instances) Kind() string    { return KindInstances }
func (s Instances) Types() []string { return []string{"container"} }

func (s Instances) bin() string {
	if s.Bin == "" {
		return "incus"
	}
	return s.Bin
}

// selected returns the instances Start acts on, checking the names exist.
func (s Instances) selected(ctx context.Context, member string) ([]incus.Instance, error) {
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
	var out []incus.Instance
	var missing []string
	for _, n := range s.Names {
		i := slices.IndexFunc(insts, func(in incus.Instance) bool { return in.Name == n })
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

// CheckStart: every selected instance exists, is a container (the fences
// confirm from cgroupfs), and will not start on its own at boot.
func (s Instances) CheckStart(ctx context.Context, member string) error {
	insts, err := s.selected(ctx, member)
	if err != nil {
		return err
	}
	for _, in := range insts {
		if in.Type != "" && in.Type != "container" {
			return fmt.Errorf("instance %s is a %s; the instances starter runs containers only", in.Name, in.Type)
		}
		if in.Expanded["boot.autostart"] != "false" {
			return fmt.Errorf("instance %s: boot.autostart must be \"false\" (unset restores the last state on boot)", in.Name)
		}
	}
	return nil
}

// Start starts every selected instance that is not running.
func (s Instances) Start(ctx context.Context, member string) error {
	insts, err := s.selected(ctx, member)
	if err != nil {
		return err
	}
	var names []string
	for _, in := range insts {
		if in.Status != incus.StatusRunning {
			names = append(names, in.Name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	_, err = s.Run(ctx, s.bin(), append([]string{"start", "--project", s.Project(member)}, names...)...)
	return err
}
