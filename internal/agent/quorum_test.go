package agent

import (
	"testing"
	"time"
)

// waitSomeone waits until exactly one member runs the workload, and it is the
// member holding the office (a stray, unleased copy does not count), and
// returns it.
func (h *harness) waitSomeone(d time.Duration) string {
	h.t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if on := h.sim.runningOn(); len(on) == 1 && h.leader() == on[0] {
			return on[0]
		}
	}
	h.t.Fatalf("after %s: running on %v, want exactly one (leader=%q)", d, h.sim.runningOn(), h.leader())
	return ""
}

// TestQuorumLossSelfFencesAndNobodyWins: two of three etcd members freeze.
// Nobody can renew or win anything: the leader must fence itself within the
// TTL, nobody may start while quorum is gone, and after it returns exactly one
// member runs.
func TestQuorumLossSelfFencesAndNobodyWins(t *testing.T) {
	h, c := newClusterHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 15*time.Second)
	ttl := h.cfg.Lease.TTL.D()

	c.Members[1].Pause()
	c.Members[2].Pause()
	t0 := time.Now()
	h.waitRunning("", ttl)
	t.Logf("leader self-fenced %s after quorum loss (TTL %s)", time.Since(t0).Round(time.Millisecond), ttl)
	h.holdNobody(3 * ttl)

	c.Members[1].Resume()
	c.Members[2].Resume()
	healed := time.Now()
	m := h.waitSomeone(4 * ttl)
	t.Logf("%s running %s after quorum returned", m, time.Since(healed).Round(time.Millisecond))
	h.holdOne(m, 3*time.Second)
}

// TestQuorumOneMemberUnreachableNoFailover: each etcd member in turn becomes
// unreachable to every client (its peers still reach it, so quorum holds).
// The likeliest false failover in production: asus-infra, dell-infra and
// rpi-infra reboot independently. The leader must keep the office, without a
// self-fence, throughout.
func TestQuorumOneMemberUnreachableNoFailover(t *testing.T) {
	h, c := newClusterHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 15*time.Second)
	_, epoch, _, _, err := h.leaderEpoch()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range c.Members {
		t.Logf("etcd member %s unreachable to clients for %s", m.Name, 3*h.cfg.Lease.TTL.D())
		m.Client.SetBlackhole(true)
		h.holdOne("asus", 3*h.cfg.Lease.TTL.D())
		m.Client.SetBlackhole(false)
	}
	if _, e, _, _, err := h.leaderEpoch(); err != nil || e != epoch {
		t.Fatalf("epoch %d -> %d (%v): the office changed hands although only one etcd member was ever unreachable", epoch, e, err)
	}
}
