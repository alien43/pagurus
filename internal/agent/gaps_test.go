package agent

import (
	"os"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/config"
)

// waitPaused waits until member's start is paused in its slow phase.
func (h *harness) waitPaused(m string, d time.Duration) {
	h.t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		h.sim.mu.Lock()
		p := h.sim.m[m].paused
		h.sim.mu.Unlock()
		if p {
			return
		}
	}
	h.t.Fatalf("%s never paused in its start", m)
}

// TestPausedStart (gap 1): the leader pauses in the slow part of its start
// (pulling, creating) after its guarded may-run write, and meanwhile loses
// the office: partitioned past its TTL, fenced (nothing ran yet), and the next
// leader starts. When it resumes it must not start a second copy. With a whole
// `up` after the write, it did.
func TestPausedStart(t *testing.T) {
	h := newHarness(t, nil)
	h.sim.set("asus", func(m *simMember) { m.slowPause = 20 * time.Second })
	h.startAll()
	h.waitPaused("asus", 15*time.Second)
	h.partition("asus", true)
	time.Sleep(h.cfg.Lease.TTL.D() + 3*time.Second) // asus's lease is gone, dell holds (blocked on asus)
	h.partition("asus", false)
	took := h.waitRunning("dell", 15*time.Second)
	t.Logf("dell running %s after asus came back, while asus is still paused", took)
	h.holdOne("dell", 12*time.Second) // asus's pause ends in here
}

// TestPausedLaunchWindow (gap 1, residual): the pause lands inside the
// launch itself, after the last check. Incus has no fencing token, so a late
// copy can start; the post-start re-check stops it at once. Measures how
// long two ran.
func TestPausedLaunchWindow(t *testing.T) {
	h := newHarness(t, nil)
	h.sim.tolerate = true
	h.sim.set("asus", func(m *simMember) { m.launchPause = 20 * time.Second })
	h.startAll()
	h.waitPaused("asus", 15*time.Second)
	h.partition("asus", true)
	time.Sleep(h.cfg.Lease.TTL.D() + 3*time.Second)
	h.partition("asus", false)
	h.waitRunning("dell", 15*time.Second)
	time.Sleep(12 * time.Second) // asus's launch completes in here
	h.sim.mu.Lock()
	overlap, since := h.sim.maxOverlap, h.sim.overlapSince
	h.sim.mu.Unlock()
	if !since.IsZero() {
		t.Fatalf("two members still running %s after the late start", time.Since(since).Round(time.Millisecond))
	}
	t.Logf("residual window: two ran for %s after a launch that resumed after deposition", overlap.Round(time.Millisecond))
	if overlap > time.Second {
		t.Fatalf("the late copy ran alongside the new leader for %s; want < 1s", overlap)
	}
	h.holdOne("dell", 2*time.Second)
}

// TestTwoServices (gap 3): two services on one cluster and one etcd, one
// agent each per member (the pagurus@<svc> template unit). A crash of one
// service's agent fails that service over and leaves the other alone.
func TestTwoServices(t *testing.T) {
	a := newHarness(t, nil)
	state, err := os.MkdirTemp("/tmp", "pg-state")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(state) })
	b := newHarnessOn(t, func(c *config.Config) {
		c.Service, c.ProjectPrefix, c.EtcdPrefix = "web", "sgl-web", "/pagurus/v2/web"
	}, a.eps, state)
	a.startAll()
	b.startAll()
	a.waitRunning("asus", 15*time.Second)
	b.waitRunning("asus", 15*time.Second)

	a.nodes["asus"].crash()
	took := a.waitRunning("dell", 20*time.Second)
	t.Logf("svc failed over to dell in %s", took)
	b.holdOne("asus", 5*time.Second)
	if b.leader() != "asus" {
		t.Fatalf("web's office moved to %q", b.leader())
	}
}
