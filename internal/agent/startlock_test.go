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
			release, _, err := hostStartLock(context.Background(), path)
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

	release, _, err := hostStartLock(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, waited, err := hostStartLock(ctx, path); err == nil || waited < 250*time.Millisecond {
		t.Fatalf("second taker: err %v after %s, want a context error after ~300ms", err, waited)
	}
	if r, _, err := hostStartLock(context.Background(), ""); err != nil {
		t.Fatal(err)
	} else {
		r()
	}
}
