// Package backend holds what the core knows about a workload without knowing
// how it runs: an instance, as a member's site reports it. The Incus
// implementation of every seam is internal/backend/incus.
package backend

import "errors"

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
