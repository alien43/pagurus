// Package backend holds what the core knows about a workload without knowing
// how it runs: an instance, as a member's site reports it. The Incus
// implementation of every seam is internal/backend/incus.
package backend

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
