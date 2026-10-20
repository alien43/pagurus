package agent

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Two holders never overlap; a waiter gives up when its context ends.
func TestHostStartLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "start.lock")
	var in, maxIn atomic.Int32
	done := make(chan struct{})
	for i := 0; i < 3; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			release, _, err := hostStartLock(context.Background(), path, false)
			if err != nil {
				t.Error(err)
				return
			}
			n := in.Add(1)
			if n > maxIn.Load() {
				maxIn.Store(n)
			}
			time.Sleep(150 * time.Millisecond)
			in.Add(-1)
			release()
		}()
	}
	for i := 0; i < 3; i++ {
		<-done
	}
	if maxIn.Load() != 1 {
		t.Fatalf("%d holders at once, want 1", maxIn.Load())
	}

	release, _, err := hostStartLock(context.Background(), path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, waited, err := hostStartLock(ctx, path, false); err == nil || waited < 250*time.Millisecond {
		t.Fatalf("second taker: err %v after %s, want a context error after ~300ms", err, waited)
	}
	if r, _, err := hostStartLock(context.Background(), "", false); err != nil {
		t.Fatal(err)
	} else {
		r()
	}
}

// A priority service that starts waiting after an ordinary one still gets the
// lock first when the holder releases (mqtt-ha behind five services, 2026-10-20).
func TestStartLockPriorityGoesFirst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "start.lock")
	hold, _, err := hostStartLock(context.Background(), path, false)
	if err != nil {
		t.Fatal(err)
	}
	order := make(chan string, 2)
	take := func(name string, prio bool) {
		release, _, err := hostStartLock(context.Background(), path, prio)
		if err != nil {
			t.Error(err)
			return
		}
		order <- name
		time.Sleep(50 * time.Millisecond)
		release()
	}
	go take("ordinary", false)
	time.Sleep(300 * time.Millisecond) // the ordinary one is already waiting
	go take("priority", true)
	time.Sleep(300 * time.Millisecond)
	hold()
	if first := <-order; first != "priority" {
		t.Fatalf("%s got the lock first, want priority", first)
	}
	if second := <-order; second != "ordinary" {
		t.Fatalf("then %s, want ordinary", second)
	}
}
