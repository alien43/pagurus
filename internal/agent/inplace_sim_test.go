package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/coord"
)

// reexec upgrades member's agent in place inside the test process, as
// `pagurus reexec` and the agent's exec() do on a host: the old agent parks
// and hands over (Reexec); its exec stops what exec() stops and never
// returns; with next, a new agent resumes from the handover. Without next
// the new image never comes up, and nobody pets the node's watchdog.
func (h *harness) reexec(name string, next bool) *node {
	h.t.Helper()
	old := h.nodes[name]
	got := make(chan string, 1)
	old.agent.exec = func(_ string, _ []string, env []string) error {
		for _, kv := range env {
			if v, ok := strings.CutPrefix(kv, HandoverEnv+"="); ok {
				got <- v
			}
		}
		select {} // exec() does not return
	}
	req := Request{Argv: []string{"pagurus"}, SafetyHash: old.agent.cfg.SafetyHash(), FenceHash: old.agent.cfg.FenceHash()}
	go func() { _ = old.agent.Reexec(req) }()
	var s string
	select {
	case s = <-got:
	case <-time.After(10 * time.Second):
		h.t.Fatal("the agent did not exec")
	}
	old.retire()
	hd, err := parseHandover(s)
	if err != nil {
		h.t.Fatal(err)
	}
	if !next {
		if old.dog != nil {
			go old.dog.Run(context.Background(), func() (bool, string) { return false, "no image pets" })
		}
		return old
	}
	return h.launch(name, old.agent.cfg, old.proxies, old.dog, hd)
}

// retire ends what exec() ends of an image: its watchdog loop, its own
// handles on the sockets (the dups live on), its etcd session. Its other
// loops stay blocked on the gate for good, as no code of the old image runs
// after exec().
func (n *node) retire() {
	a := n.agent
	a.stopPetting()
	if a.relay != nil {
		a.relay.conn.Close() // not relay.Close(): the path is the new image's
	}
	a.terms.ln.SetUnlinkOnClose(false)
	a.terms.ln.Close()
	a.mu.Lock()
	if a.termConn != nil {
		a.termConn.Close()
	}
	a.mu.Unlock()
	n.cancel()
	a.cli.Close()
	gone := make(chan struct{})
	close(gone)
	n.done = gone // its Run never returns
}

func (h *harness) office(t *testing.T) (string, int64, int64) {
	t.Helper()
	rec, epoch, lease, _, err := h.leaderEpoch()
	if err != nil || rec == nil {
		t.Fatalf("no leader: %v", err)
	}
	return rec.Member, epoch, int64(lease)
}

func (d *simDog) hasFired() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.fired
}

func (c *simCluster) startsOn(m string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[m].starts + c.m[m].prepares
}

// Gate 1: the leader's agent and a follower's are exec'd in place (twice
// for the leader). The workload never stops, nothing starts it again, the
// epoch and lease stay, and the sim's audit never sees two copies. Then a
// graceful stop of the resumed leader still hands over: the adopted conch
// and the inherited term connection do their part.
func TestInPlaceUpgradeKeepsTheTerm(t *testing.T) {
	h := newHarness(t, nil)
	for _, m := range h.cfg.Members {
		h.start(m, &simDog{timeout: 4 * time.Second, reboot: true})
	}
	h.waitRunning("asus", 10*time.Second)
	h.holdOne("asus", 2*time.Second) // healthy, past the start's audits
	member, epoch, lease := h.office(t)
	starts := h.sim.startsOn("asus")

	h.reexec("asus", true)
	h.holdOne("asus", 3*time.Second)
	h.reexec("dell", true) // a follower, campaigning
	h.holdOne("asus", 2*time.Second)
	h.reexec("asus", true) // the resumed image upgrades again
	h.holdOne("asus", 4*time.Second)

	if m, e, l := h.office(t); m != member || e != epoch || l != lease {
		t.Fatalf("office %s epoch %d lease %x after the upgrades, was %s %d %x", m, e, l, member, epoch, lease)
	}
	if n := h.sim.startsOn("asus"); n != starts {
		t.Fatalf("the workload was started again (%d start calls, was %d)", n, starts)
	}
	for _, m := range h.cfg.Members {
		if h.nodes[m].dog.hasFired() {
			t.Fatalf("%s's watchdog fired", m)
		}
	}

	h.nodes["asus"].cancel()
	<-h.nodes["asus"].done
	took := h.waitRunning("dell", 15*time.Second)
	t.Logf("handover after the upgrades took %s", took)
	if st := h.memberStates()["asus"]; st.State != coord.Stopped {
		t.Fatalf("asus member state %+v, want stopped", st)
	}
}

// Gate 2: the new image never comes back. Nobody pets the leader's
// (reboot-armed) watchdog, which fences the host within its timeout, and
// another member takes over with never two copies.
func TestInPlaceUpgradeThatNeverComesBackIsFenced(t *testing.T) {
	h := newHarness(t, nil)
	dog := &simDog{timeout: 3 * time.Second, reboot: true}
	h.start("asus", dog)
	h.start("dell", &simDog{timeout: 3 * time.Second, reboot: true})
	h.waitRunning("asus", 10*time.Second)
	h.holdOne("asus", time.Second)

	began := time.Now()
	h.reexec("asus", false)
	h.waitRunning("", dog.timeout+2*time.Second)
	t.Logf("fenced %s after the exec (watchdog %s)", time.Since(began).Round(10*time.Millisecond), dog.timeout)
	if !dog.hasFired() {
		t.Fatal("the leader stopped, but not by its watchdog")
	}
	took := h.waitRunning("dell", 30*time.Second)
	t.Logf("dell took over %s later", took)
}

// Gate 3: a request whose config changes a safety hash is not applied in
// place: Reexec says restart and never execs. The restart releases the term
// as `systemctl restart` does.
func TestInPlaceUpgradeWithASafetyChangeRestarts(t *testing.T) {
	h := newHarness(t, nil)
	h.startAll()
	h.waitRunning("asus", 10*time.Second)
	_, epoch, _ := h.office(t)
	a := h.nodes["asus"].agent
	a.exec = func(string, []string, []string) error {
		t.Error("exec'd although the safety config changes")
		return errors.New("no")
	}
	for _, req := range []Request{
		{Argv: []string{"pagurus"}, SafetyHash: "other", FenceHash: a.cfg.FenceHash()},
		{Argv: []string{"pagurus"}, SafetyHash: a.cfg.SafetyHash(), FenceHash: "other"},
	} {
		if err := a.Reexec(req); !errors.Is(err, ErrRestart) {
			t.Fatalf("Reexec(%+v) = %v, want ErrRestart", req, err)
		}
	}
	// What cmdRun does on ErrRestart, then systemd's restart.
	h.nodes["asus"].cancel()
	<-h.nodes["asus"].done
	if st := h.memberStates()["asus"]; st.State != coord.Stopped {
		t.Fatalf("asus member state %+v, want stopped (released)", st)
	}
	h.start("asus", nil)
	// A new term, on whichever member wins it (the restarted asus is still
	// the preferred one).
	for end := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if rec, e, _, _, err := h.leaderEpoch(); err == nil && rec != nil && rec.Member != "" && e != epoch {
			h.waitRunning(rec.Member, 10*time.Second)
			break
		}
		if time.Now().After(end) {
			t.Fatal("no new term after the restart")
		}
	}
}
