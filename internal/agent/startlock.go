package agent

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

// hostStartLock takes the host-wide start lock at path: the agents of
// different services on one host start their workloads one at a time. A
// deploy restarts every agent on a host at once, and six concurrent `up`s each
// made every peer's incusd restart its dnsmasq for the new DNS records; the
// restarts collided ("Address already in use") and an `up` stalled ~2 min
// (2026-10-18: a stack down 113 s, a deploy rolled back twice). It waits
// until ctx ends; the returned func releases it (the kernel does too if the
// agent dies). An empty path is no lock (tests).
func hostStartLock(ctx context.Context, path string, priority bool) (release func(), waited time.Duration, err error) {
	if path == "" {
		return func() {}, 0, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, 0, err
	}
	// A priority service (config start_priority: the broker everything else
	// talks through) holds path.prio while it waits and starts; the others
	// don't try the lock meanwhile. Without it mqtt-ha waited 32 s behind five
	// services after a deploy (2026-10-20), MQTT down ~45 s instead of ~12 s.
	// At worst it now waits for the one start already running.
	p, err := os.OpenFile(path+".prio", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	closeAll := func() { _ = f.Close(); _ = p.Close() }
	t0 := time.Now()
	if priority {
		for {
			err := syscall.Flock(int(p.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				break
			}
			if !errors.Is(err, syscall.EWOULDBLOCK) {
				closeAll()
				return nil, 0, err
			}
			if !sleep(ctx, 100*time.Millisecond) {
				closeAll()
				return nil, time.Since(t0), ctx.Err()
			}
		}
	}
	for {
		yield := false
		if !priority {
			// Held by a priority service waiting or starting: let it go first.
			if err := syscall.Flock(int(p.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err == nil {
				_ = syscall.Flock(int(p.Fd()), syscall.LOCK_UN)
			} else if errors.Is(err, syscall.EWOULDBLOCK) {
				yield = true
			}
		}
		if !yield {
			err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				return func() {
					_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
					closeAll()
				}, time.Since(t0), nil
			}
			if !errors.Is(err, syscall.EWOULDBLOCK) {
				closeAll()
				return nil, 0, err
			}
		}
		if !sleep(ctx, 100*time.Millisecond) {
			closeAll()
			return nil, time.Since(t0), ctx.Err()
		}
	}
}
