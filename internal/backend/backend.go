// Package backend holds what the core knows about a workload without knowing
// how it runs: an instance, as a member's site reports it. The Incus
// implementation of every seam is internal/backend/incus.
package backend

import (
	"errors"
	"strings"
)

// ErrAPIUnavailable marks a read the backend's API did not answer in time,
// or refused to connect for: the daemon is stalled or down, so the answer
// says nothing about the workload either way. The Incus cluster database
// stalls every API read for 90 s on some instance starts
// (docs/00_design.md § Backend stalls).
var ErrAPIUnavailable = errors.New("backend API unavailable")

type Instance struct {
	Name     string            `json:"name"`
	Status   string            `json:"status"`
	Location string            `json:"location"`
	Type     string            `json:"type"`
	Expanded map[string]string `json:"expanded_config"`
}

const (
	StatusRunning = "Running"
	StatusStopped = "Stopped"
)

// AllStopped is true when every instance is Stopped (an empty project counts).
func AllStopped(insts []Instance) bool {
	for _, in := range insts {
		if in.Status != StatusStopped {
			return false
		}
	}
	return true
}

// Running returns the names of instances in a Running state.
func Running(insts []Instance) []string {
	var out []string
	for _, in := range insts {
		if in.Status == StatusRunning {
			out = append(out, in.Name)
		}
	}
	return out
}

// instancePrefix marks an instance among replica units (a replicated custom
// volume's name or an instance). Volume names cannot contain "/".
const instancePrefix = "instance/"

// InstanceUnit is the replica unit of instance name.
func InstanceUnit(name string) string { return instancePrefix + name }

// ParseUnit splits a replica unit: an instance's name and true, or the
// volume's name and false.
func ParseUnit(u string) (string, bool) {
	if n, ok := strings.CutPrefix(u, instancePrefix); ok {
		return n, true
	}
	return u, false
}
