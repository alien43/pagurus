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
// (asus, 2026-10-18, homarr-ha down 113 s, deploy rolled back twice). It waits
// until ctx ends; the returned func releases it (the kernel does too if the
// agent dies). An empty path is no lock (tests).
func hostStartLock(ctx context.Context, path string) (release func(), waited time.Duration, err error) {
	if path == "" {
		return func() {}, 0, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, 0, err
	}
	t0 := time.Now()
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, time.Since(t0), nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			_ = f.Close()
			return nil, 0, err
		}
		if !sleep(ctx, 100*time.Millisecond) {
			_ = f.Close()
			return nil, time.Since(t0), ctx.Err()
		}
	}
}
