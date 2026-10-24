package agent

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/backend"
	"github.com/alien43/pagurus/internal/config"
)

// The Incus cluster database stalls every API call for 90.0 s on some
// instance starts (asus 2026-10-21..23, tasks/asus-incusd-api-stall.md). The
// leader's agent must hold through it: no heartbeat starve (a reboot on a
// reboot-armed member), no in-place restart of a healthy workload, no
// failover. These sims run production's timings at 1/stallScale: the test
// config's api_timeout is 1 s against 15 s live, so a 90 s freeze is 6 s here.

const stallScale = 15

func prod(d time.Duration) time.Duration { return d / stallScale }

// scaledStall sets the timing knobs the test config does not carry to the
// same scale, for this test only. Call it before the harness, so they are
// restored after its nodes are gone.
func scaledStall(t *testing.T) {
	slack, every, stale, hold := beatSlack, heartbeatEvery, heartbeatStale, APIUnavailableMax
	beatSlack = prod(30 * time.Second)
	heartbeatEvery = 100 * time.Millisecond
	heartbeatStale = 500 * time.Millisecond
	APIUnavailableMax = prod(3 * time.Minute)
	t.Cleanup(func() { beatSlack, heartbeatEvery, heartbeatStale, APIUnavailableMax = slack, every, stale, hold })
}

// stallHarness: three members with armed, reboot-on-expiry watchdogs (dell's
// shape: systemd acts 30 s after the last pet), asus leading and healthy. repl
// makes the service replicate like mqtt-ha (a round every minute): its round
// is the longest Incus call a health iteration makes.
func stallHarness(t *testing.T, repl bool) (*harness, map[string]*simDog) {
	scaledStall(t)
	var h *harness
	if repl {
		h = newReplHarness(t, func(c *config.Config) {
			replicated(time.Hour)(c)
			c.Replicate.Interval = config.Duration(prod(time.Minute))
			c.Replicate.CatchupTimeout = config.Duration(prod(10 * time.Minute))
		})
	} else {
		h = newHarness(t, nil)
	}
	dogs := map[string]*simDog{}
	for _, m := range h.cfg.Members {
		dogs[m] = &simDog{timeout: prod(30 * time.Second), reboot: true}
		h.start(m, dogs[m])
	}
	h.waitRunning("asus", 15*time.Second)
	// Past the start period, healthy, and (replicating) a few rounds in.
	h.waitFor("asus healthy", 10*time.Second, func() bool {
		return h.memberStates()["asus"].Reason == "healthy"
	})
	time.Sleep(prod(time.Minute))
	return h, dogs
}

// alignWithRound returns just before asus's next snapshot round, so a freeze
// begun now catches the whole round: live, a 90 s freeze always does (mqtt-ha
// snapshots every minute), and the round's budget is the longest wait.
func (h *harness) alignWithRound() {
	m := h.nodes["asus"].agent.m
	n := m.Counter("pagurus_replica_snapshots_total", "result", "ok")
	h.waitFor("a snapshot round", 10*time.Second, func() bool {
		return m.Counter("pagurus_replica_snapshots_total", "result", "ok") > n
	})
	time.Sleep(h.cfg.Replicate.Interval.D() - 200*time.Millisecond)
}

func (h *harness) freeze(m string, d time.Duration) {
	h.sim.set(m, func(sm *simMember) { sm.frozenUntil = time.Now().Add(d) })
}

// noStarve fails if any watchdog fired or went unfit, or asus withheld its
// term heartbeat.
func noStarve(t *testing.T, h *harness, dogs map[string]*simDog) {
	t.Helper()
	if n := h.nodes["asus"].agent.m.Counter("pagurus_heartbeat_withheld_total"); n != 0 {
		t.Errorf("asus withheld its term heartbeat %v times (watchdog-starve)", n)
	}
	for m, d := range dogs {
		d.mu.Lock()
		fired, unfit := d.fired, d.unfit
		d.mu.Unlock()
		if fired || unfit > 0 {
			t.Errorf("%s: watchdog fired=%v, unfit for %s", m, fired, unfit)
		}
	}
}

// A 90 s freeze of the leader's Incus: the term holds and nothing moves.
func TestIncusStall90sHoldsTheTerm(t *testing.T) {
	for _, repl := range []bool{false, true} {
		t.Run(fmt.Sprintf("replicating=%v", repl), func(t *testing.T) {
			h, dogs := stallHarness(t, repl)
			if repl {
				h.alignWithRound()
			}
			prep, epoch := h.prepares("asus"), h.epoch()
			freeze := prod(90 * time.Second)
			h.freeze("asus", freeze)
			h.holdOne("asus", freeze+prod(30*time.Second))
			noStarve(t, h, dogs)
			if p := h.prepares("asus"); p != prep {
				t.Errorf("asus recreated its workload %d times during the freeze (restarting workload in place)", p-prep)
			}
			if e := h.epoch(); e != epoch {
				t.Errorf("the office changed hands: epoch %d -> %d", epoch, e)
			}
			if n := h.nodes["asus"].agent.m.Counter("pagurus_api_unavailable_total"); n != 1 {
				t.Errorf("api unavailable episodes = %v, want 1 (the freeze was not seen as one)", n)
			}
		})
	}
}

// A 200 s freeze outlasts APIUnavailableMax (3 min): no starve at any point,
// and nothing is restarted before the bound. After it, the health check
// counts as unhealthy, so the usual restart-in-place may follow.
func TestIncusStall200sHoldsUntilTheBound(t *testing.T) {
	h, dogs := stallHarness(t, true)
	h.alignWithRound()
	prep := h.prepares("asus")
	freeze := prod(200 * time.Second)
	t0 := time.Now()
	h.freeze("asus", freeze)
	var restartAt time.Duration
	for time.Since(t0) < freeze+prod(time.Minute) {
		if restartAt == 0 && h.prepares("asus") != prep {
			restartAt = time.Since(t0)
		}
		if on := h.sim.runningOn(); len(on) != 1 || on[0] != "asus" {
			if time.Since(t0) < APIUnavailableMax {
				t.Fatalf("running on %v %s into the freeze, before the bound", on, time.Since(t0).Round(time.Millisecond))
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("first in-place restart %s into a %s freeze (bound %s)", restartAt.Round(time.Millisecond), freeze, APIUnavailableMax)
	if restartAt != 0 && restartAt < APIUnavailableMax {
		t.Errorf("restarted in place %s into the freeze, before the %s bound", restartAt, APIUnavailableMax)
	}
	noStarve(t, h, dogs)
	h.waitRunning("asus", 10*time.Second)
}

// incusd is dead on the leader (every call refused): the term holds for the
// bound, then the unhealthy accounting ends it the usual way and the office
// moves. A dead incusd must not keep a term forever.
func TestIncusDeadEndsTheTermAfterTheBound(t *testing.T) {
	h, dogs := stallHarness(t, false)
	t0 := time.Now()
	h.sim.set("asus", func(sm *simMember) { sm.apiDown = true })
	h.holdOne("asus", APIUnavailableMax-prod(15*time.Second))
	took := h.waitRunning("dell", APIUnavailableMax+20*time.Second)
	t.Logf("dell running %s after asus's incusd died (bound %s)", time.Since(t0).Round(time.Millisecond), APIUnavailableMax)
	_ = took
	noStarve(t, h, dogs)
}

// etcd is lost while the leader's Incus is frozen: the self-fence must still
// stop the workload before the lease expires. Its first fence (incus-stop)
// hangs on the frozen API until its budget; cgroup.kill needs no Incus.
func TestIncusStallEtcdLostSelfFencesInTime(t *testing.T) {
	h, _ := stallHarness(t, false)
	h.freeze("asus", prod(90*time.Second))
	t0 := time.Now()
	h.nodes["asus"].blackhole(true)
	for len(h.sim.runningOn()) != 0 {
		if time.Since(t0) > 2*h.cfg.Lease.TTL.D() {
			t.Fatalf("asus still runs %s after losing etcd", time.Since(t0))
		}
		time.Sleep(20 * time.Millisecond)
	}
	fenced := time.Since(t0)
	t.Logf("self-fenced %s after losing etcd (ttl %s)", fenced.Round(time.Millisecond), h.cfg.Lease.TTL.D())
	if fenced > h.cfg.Lease.TTL.D() {
		t.Fatalf("self-fence took %s, longer than the TTL", fenced)
	}
	h.sim.mu.Lock()
	by := strings.Join(h.sim.log, "\n")
	h.sim.mu.Unlock()
	if !strings.Contains(by, "cgroup.kill asus") {
		t.Fatalf("asus was not stopped by its own cgroup.kill:\n%s", by)
	}
	h.waitRunning("dell", 30*time.Second)
}

// errAPIDown is what a refused Incus socket looks like through the driver.
var errAPIDown = fmt.Errorf("%w: dial unix /var/lib/incus/unix.socket: connect: connection refused", backend.ErrAPIUnavailable)
