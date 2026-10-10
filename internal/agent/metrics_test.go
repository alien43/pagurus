package agent

import (
	"strings"
	"testing"
	"time"
)

// gaugeSum adds a gauge over every member's agent.
func (h *harness) gaugeSum(name string) float64 {
	var s float64
	for _, m := range h.cfg.Members {
		s += h.nodes[m].agent.m.Get(name)
	}
	return s
}

func (h *harness) waitGauge(what string, d time.Duration, cond func() bool) {
	h.t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if cond() {
			return
		}
	}
	var b strings.Builder
	for _, m := range h.cfg.Members {
		_ = h.nodes[m].agent.m.Write(&b)
	}
	h.t.Fatalf("metrics never showed: %s\n%s", what, b.String())
}

// TestMetricsShowTheFourConditions: the gauges behind the four alert rules
// (deploy/grafana/pagurus-rules.yaml) move as the rules assume.
func TestMetricsShowTheFourConditions(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 15*time.Second)
	asus := h.nodes["asus"].agent.m
	h.waitGauge("steady state", 5*time.Second, func() bool {
		return asus.Get("pagurus_role") == 1 && asus.Get("pagurus_local_running") == 1 &&
			h.gaugeSum("pagurus_local_running") == 1 && h.gaugeSum("pagurus_fit") == 3 &&
			asus.Get("pagurus_lease_remaining_seconds") > 0 && asus.Get("pagurus_self_chain_verified") == 1
	})
	if asus.Counter("pagurus_terms_total") != 1 {
		t.Fatalf("terms_total %v", asus.Counter("pagurus_terms_total"))
	}

	// 1 and 3: a copy on rpi5 that rpi5 cannot stop. Two members' cgroupfs
	// show the workload; rpi5 goes unfit.
	h.sim.mu.Lock()
	h.sim.tolerate = true
	h.sim.m["rpi5"].running, h.sim.m["rpi5"].stopFail, h.sim.m["rpi5"].killFail = true, true, true
	h.sim.mu.Unlock()
	rpi5 := h.nodes["rpi5"].agent.m
	h.waitGauge("two running (rule 1)", 5*time.Second, func() bool { return h.gaugeSum("pagurus_local_running") >= 2 })
	h.waitGauge("rpi5 unfit (rule 3)", 10*time.Second, func() bool { return rpi5.Get("pagurus_fit") == 0 })
	if rpi5.Counter("pagurus_fence_runs_total", "chain", "self", "kind", "incus-stop", "result", "failed") == 0 {
		t.Fatal("no failed self-fence counted on rpi5")
	}
	// The leader's audit cannot stop it either (stopFail): clear the fault.
	h.sim.set("rpi5", func(m *simMember) { m.running, m.stopFail, m.killFail = false, false, false })
	h.waitGauge("rpi5 fit again", 10*time.Second, func() bool { return rpi5.Get("pagurus_fit") == 1 })

	// 2 and 4: the leader partitioned under fence.unverified=block. Nobody
	// runs, and the new leader is blocked on asus.
	h.partition("asus", true)
	h.waitGauge("nobody running (rule 4)", 15*time.Second, func() bool { return h.gaugeSum("pagurus_local_running") == 0 })
	h.waitGauge("fence blocked (rule 2)", 20*time.Second, func() bool { return h.gaugeSum("pagurus_fence_blocked") == 1 })
	h.partition("asus", false)
	h.waitGauge("unblocked after the heal", 20*time.Second, func() bool {
		return h.gaugeSum("pagurus_fence_blocked") == 0 && h.gaugeSum("pagurus_local_running") == 1
	})
}
