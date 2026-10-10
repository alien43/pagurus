package agent

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestRandomFaults injects a seeded random sequence of faults, one member
// impaired at a time (the design's assumption), each healed after a random
// while. Throughout, the simulated cluster fails the test on any instant with
// two members running. After each heal, exactly one member must be running
// within the settle bound: 3×TTL + 2×grace + unverified_wait, plus slack for a
// start.
//
//	SINGLETON_CHAOS_DURATION  how long to keep injecting (default 30s)
//	SINGLETON_CHAOS_SEED      replay a run (default: the clock; always logged)
func TestRandomFaults(t *testing.T) {
	dur := 30 * time.Second
	if v := os.Getenv("SINGLETON_CHAOS_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
		dur = d
	}
	seed := time.Now().UnixNano()
	if v := os.Getenv("SINGLETON_CHAOS_SEED"); v != "" {
		s, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seed = s
	}
	t.Logf("chaos seed %d, duration %s (replay: SINGLETON_CHAOS_SEED=%d)", seed, dur, seed)
	rng := rand.New(rand.NewSource(seed))

	h := newHarness(t, nil)
	h.startAll()
	h.waitSomeone(15 * time.Second)
	ttl := h.cfg.Lease.TTL.D()
	settle := 3*ttl + 2*h.cfg.Grace.D() + h.cfg.Fence.UnverifiedWait.D() + 15*time.Second
	members := h.cfg.Members
	co := h.coord()

	pick := func() string { return members[rng.Intn(len(members))] }
	hold := func() time.Duration { return time.Duration(2+rng.Intn(14)) * time.Second }
	counts := map[string]int{}
	end := time.Now().Add(dur)
	for time.Now().Before(end) {
		m := pick()
		if rng.Intn(2) == 0 { // half the faults hit whoever leads: the interesting case
			if l := h.leader(); l != "" {
				m = l
			}
		}
		d := hold()
		var fault string
		switch rng.Intn(7) {
		case 0:
			fault = "crash+restart"
			h.nodes[m].crash()
			time.Sleep(time.Duration(rng.Intn(3000)) * time.Millisecond)
			h.start(m, nil)
		case 1:
			fault = "crash, stay down"
			h.nodes[m].crash()
			time.Sleep(d)
			h.start(m, nil)
		case 2:
			fault = "partition"
			h.partition(m, true)
			time.Sleep(d)
			h.partition(m, false)
		case 3:
			fault = "partition, stop and kill fail"
			h.sim.set(m, func(s *simMember) { s.stopFail, s.killFail = true, true })
			h.partition(m, true)
			time.Sleep(d)
			h.partition(m, false)
			h.sim.set(m, func(s *simMember) { s.stopFail, s.killFail = false, false })
		case 4:
			fault = "unhealthy"
			h.sim.set(m, func(s *simMember) { s.unhealthy = true })
			time.Sleep(d)
			h.sim.set(m, func(s *simMember) { s.unhealthy = false })
		case 5:
			fault = "move"
			if err := co.SetPin(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			for until := time.Now().Add(settle); time.Now().Before(until) && h.leader() != m; time.Sleep(200 * time.Millisecond) {
			}
			if err := co.SetPin(context.Background(), ""); err != nil {
				t.Fatal(err)
			}
		case 6:
			fault = "graceful restart"
			n := h.nodes[m]
			n.cancel()
			<-n.done
			h.start(m, nil)
		}
		counts[fault]++
		healed := time.Now()
		who := h.waitSomeone(settle)
		t.Logf("%-30s on %-5s (%2ds): %s runs %s after the heal", fault, m, int(d.Seconds()), who, time.Since(healed).Round(100*time.Millisecond))
	}
	total := 0
	for _, n := range counts {
		total += n
	}
	t.Logf("chaos seed %d: %d faults %v, %d violations, max running %d", seed, total, counts, h.sim.violations, h.sim.maxRunning)
}
