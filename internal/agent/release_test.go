package agent

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/config"
)

// A planned release stops the workload gracefully for stop_timeout (a VM
// guest's own shutdown), not the fixed 5 s it used to: HAOS needs ~47 s, and
// a forced stop on every move is a power cut.
func TestReleaseStopsGracefullyForStopTimeout(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.StopTimeout = config.Duration(3 * time.Second) })
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	if err := h.coord().SetPin(context.Background(), "dell"); err != nil {
		t.Fatal(err)
	}
	h.waitRunning("dell", 20*time.Second)
	h.sim.mu.Lock()
	got := slices.Clone(h.sim.graceful)
	h.sim.mu.Unlock()
	if !slices.Contains(got, 3*time.Second) {
		t.Fatalf("graceful stop timeouts %v: the move did not use stop_timeout 3s", got)
	}
}
