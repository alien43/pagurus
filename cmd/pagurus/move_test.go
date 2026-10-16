package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/fence"
	"github.com/alien43/pagurus/internal/testutil"
)

const movePrefix = "/pagurus/v2/svc"

func moveSetup(t *testing.T) (*config.Config, *clientv3.Client, *coord.Coord) {
	cli := testutil.Client(t, testutil.Etcd(t))
	cfg := &config.Config{
		Members:    []string{"asus", "dell", "rpi5"},
		EtcdPrefix: movePrefix,
		Etcd:       config.Etcd{OpTimeout: config.Duration(2 * time.Second)},
	}
	return cfg, cli, coord.New(cli, movePrefix, 2*time.Second)
}

// release writes the member-state record an agent's release() writes.
func release(t *testing.T, cli *clientv3.Client, co *coord.Coord, member, reason string) {
	t.Helper()
	b, _ := json.Marshal(coord.MemberState{State: coord.Stopped, Epoch: 7, By: member, Reason: "released: " + reason, At: time.Now().UTC()})
	if _, err := cli.Put(context.Background(), co.MSKey(member), string(b)); err != nil {
		t.Fatal(err)
	}
}

type moveResult struct {
	err error
	at  time.Time
}

func startMove(cfg *config.Config, cli *clientv3.Client, ctx context.Context, to string, timeout time.Duration) <-chan moveResult {
	ch := make(chan moveResult, 1)
	go func() {
		err := cmdMove(ctx, cfg, cli, to, timeout)
		ch <- moveResult{err, time.Now()}
	}()
	return ch
}

func waitPin(t *testing.T, co *coord.Coord, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if p, _ := co.Pin(context.Background()); p == want {
			return
		}
		if time.Now().After(deadline) {
			p, _ := co.Pin(context.Background())
			t.Fatalf("pin %q, want %q", p, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestMoveFailsFastWhenTargetGivesUp: the target wins, can't host and gives
// up. move must stop waiting at once, drop its pin so the others can
// campaign, and say why: not hold the office empty until its timeout.
func TestMoveFailsFastWhenTargetGivesUp(t *testing.T) {
	cfg, cli, co := moveSetup(t)
	done := startMove(cfg, cli, context.Background(), "dell", 30*time.Second)
	waitPin(t, co, "dell")
	time.Sleep(2 * time.Second)
	released := time.Now()
	release(t, cli, co, "dell", "start failed: In use")
	select {
	case r := <-done:
		if d := r.at.Sub(released); d > 2*time.Second {
			t.Errorf("move returned %s after the target gave up, want < 2s", d.Round(time.Millisecond))
		}
		if r.err == nil || !strings.Contains(r.err.Error(), "dell gave up: start failed: In use") {
			t.Errorf("err = %v, want the target's reason", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("move still waiting 5s after the target gave up")
	}
	if p, _ := co.Pin(context.Background()); p != "" {
		t.Errorf("pin %q left after the target gave up", p)
	}
}

// TestMoveIgnoresOlderRelease: a released record from before the move is not
// a give-up.
func TestMoveIgnoresOlderRelease(t *testing.T) {
	cfg, cli, co := moveSetup(t)
	release(t, cli, co, "dell", "old")
	start := time.Now()
	r := <-startMove(cfg, cli, context.Background(), "dell", 3*time.Second)
	if r.err == nil || !strings.Contains(r.err.Error(), "timed out") {
		t.Errorf("err = %v, want a timeout", r.err)
	}
	if d := r.at.Sub(start); d < 3*time.Second {
		t.Errorf("move returned after %s on an old release record", d.Round(time.Millisecond))
	}
	if p, _ := co.Pin(context.Background()); p != "" {
		t.Errorf("pin %q left after the timeout", p)
	}
}

// TestMoveLeavesAnotherPin: a pin written by someone else mid-move (another
// move) is not this move's to clear, whichever way this one ends.
func TestMoveLeavesAnotherPin(t *testing.T) {
	cfg, cli, co := moveSetup(t)
	done := startMove(cfg, cli, context.Background(), "dell", 3*time.Second)
	waitPin(t, co, "dell")
	if err := co.SetPin(context.Background(), "rpi5"); err != nil {
		t.Fatal(err)
	}
	release(t, cli, co, "dell", "start failed")
	<-done
	if p, _ := co.Pin(context.Background()); p != "rpi5" {
		t.Errorf("pin %q, want the other move's rpi5 left alone", p)
	}
}

// TestMoveInterruptedDropsItsPin: Ctrl-C/SIGTERM (ctx cancelled) clears the
// pin rather than leaving it to its lease.
func TestMoveInterruptedDropsItsPin(t *testing.T) {
	cfg, cli, co := moveSetup(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := startMove(cfg, cli, ctx, "dell", 30*time.Second)
	waitPin(t, co, "dell")
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("move did not return after cancel")
	}
	if p, _ := co.Pin(context.Background()); p != "" {
		t.Errorf("pin %q left after an interrupted move", p)
	}
}

// fence-self with no recorded types still confirms for a compose service:
// every built-in container fence applies to its fallback target (review 6B).
func TestFenceSelfFallbackTypesForCompose(t *testing.T) {
	cfg := &config.Config{Start: config.Start{Kind: "compose"}}
	tg := fence.Target{Member: "asus", Types: fallbackTypes(cfg)}
	for _, f := range []fence.Fence{&fence.IncusStop{}, &fence.CgroupKill{}} {
		if !f.Applies(tg) {
			t.Errorf("%s does not apply to the compose fallback %v", f.Kind(), tg.Types)
		}
	}
}
