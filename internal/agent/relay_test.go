package agent

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRelayFollowsConch: while conch runs, the agent is fit exactly while
// conch's relayed pets are fresh. A conch that stops petting (alive, but
// unfit or wedged) makes the agent unfit within the freshness window: the
// relay never pets on conch's behalf.
func TestRelayFollowsConch(t *testing.T) {
	cfg := testConfig(t, nil)
	dir, err := os.MkdirTemp("/tmp", "pg-relay")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	cfg.StateDir = dir
	self, _ := os.Executable()
	rt := &simRT{c: newSim(t, "asus"), me: "asus"}
	a := New(Options{Config: cfg, Runtime: rt, Fences: fencesOver(cfg, rt), Member: "asus",
		Conch: []string{self, "sim-pet", "10"}, FenceCmd: []string{"true"}, TermCmd: []string{"true"}, ProcOutput: procOutput()})
	a.wd = &simDog{timeout: time.Second} // freshness: 500ms
	r, err := listenNotify(filepath.Join(dir, "svc.notify"), a.relayFreshness)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	a.relay = r

	done, err := a.spawnConch(0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { a.KillProcs(); a.conchExited(<-done) }()
	a.mu.Lock()
	a.termActive = true // conch holds a term: its pets are what count
	a.mu.Unlock()

	// Petting every 100ms for 1s: fit throughout.
	for end := time.Now().Add(800 * time.Millisecond); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if ok, why := a.Fit(); !ok {
			t.Fatalf("unfit while conch pets: %s", why)
		}
	}
	// conch goes silent but stays alive: unfit within freshness + one pet.
	silentBy := time.Now().Add(300 * time.Millisecond)
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		ok, why := a.Fit()
		if !ok {
			if took := time.Since(silentBy); took > 600*time.Millisecond {
				t.Fatalf("unfit only %s after conch's last pet (freshness 500ms): %s", took, why)
			}
			t.Logf("unfit after conch went silent: %s", why)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("still fit 3s after conch stopped petting: the relay is petting on its own")
		}
	}
	// And stays unfit while that conch lives.
	for end := time.Now().Add(time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if ok, _ := a.Fit(); ok {
			t.Fatal("fit again although conch never petted again")
		}
	}
}

// TestCampaigningConchSilenceIsNotUnfit: a conch that only campaigns (or is
// slowly withdrawing while etcd is unreachable) guards nothing. While our
// local instances are confirmed stopped, its silence must not starve the
// watchdog; on a reboot-armed member that would reset a healthy follower on
// every etcd outage longer than the watchdog timeout (seen live 2026-10-12).
func TestCampaigningConchSilenceIsNotUnfit(t *testing.T) {
	cfg := testConfig(t, nil)
	dir, err := os.MkdirTemp("/tmp", "pg-relay")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	cfg.StateDir = dir
	self, _ := os.Executable()
	rt := &simRT{c: newSim(t, "asus"), me: "asus"}
	a := New(Options{Config: cfg, Runtime: rt, Fences: fencesOver(cfg, rt), Member: "asus",
		Conch: []string{self, "sim-pet", "0"}, FenceCmd: []string{"true"}, TermCmd: []string{"true"}, ProcOutput: procOutput()})
	a.wd = &simDog{timeout: time.Second} // freshness: 500ms
	r, err := listenNotify(filepath.Join(dir, "svc.notify"), a.relayFreshness)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	a.relay = r
	a.setLocalStopped(true)

	done, err := a.spawnConch(0) // never pets, never holds a term
	if err != nil {
		t.Fatal(err)
	}
	defer func() { a.KillProcs(); a.conchExited(<-done) }()
	for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if ok, why := a.Fit(); !ok {
			t.Fatalf("unfit while only campaigning with nothing running: %s", why)
		}
	}
	// The same silence while it holds a term is unfit.
	a.mu.Lock()
	a.termActive = true
	a.mu.Unlock()
	if ok, _ := a.Fit(); ok {
		t.Fatal("fit while holding a term with a silent conch")
	}
}
