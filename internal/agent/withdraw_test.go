package agent

import (
	"os/exec"
	"testing"
	"time"
)

// A withdrawal decided while still campaigning must not end a term that
// started meanwhile: lead clears localStopped as the term begins, so follow's
// eligibility check, run just after, read the term's own state as "local
// instances not confirmed stopped" and signalled conch, ending the fresh term
// (12 times in 7 days live, 2026-10-26). withdraw signals only while no term
// has begun.
func TestWithdrawSparesAStartedTerm(t *testing.T) {
	cfg := testConfig(t, nil)
	rt := &simRT{c: newSim(t, "asus"), me: "asus"}
	a := New(Options{Config: cfg, Backend: simBackend(cfg, rt), Member: "asus", ProcOutput: procOutput()})
	// One waiter per process: exec.Cmd.Wait must not run twice.
	spawn := func() <-chan struct{} {
		cmd := exec.Command("sleep", "30")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
		a.mu.Lock()
		a.conch = cmd
		a.mu.Unlock()
		return done
	}
	exited := func(done <-chan struct{}) bool {
		select {
		case <-done:
			return true
		case <-time.After(300 * time.Millisecond):
			return false
		}
	}

	term := spawn()
	a.mu.Lock()
	a.termActive, a.termRan, a.localStopped = true, true, false // as lead leaves it
	a.mu.Unlock()
	if a.withdraw("not eligible: local instances not confirmed stopped") {
		t.Fatal("withdraw signalled conch while its term runs")
	}
	if exited(term) {
		t.Fatal("conch exited: the term was ended by a withdrawal")
	}

	a.mu.Lock()
	a.termActive, a.termRan = false, false
	a.mu.Unlock()
	camp := spawn()
	if !a.withdraw("campaign delay changed") {
		t.Fatal("withdraw did not signal a conch that is only campaigning")
	}
	if !exited(camp) {
		t.Fatal("a campaigning conch was not stopped")
	}
}
