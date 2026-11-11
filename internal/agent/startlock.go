package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/alien43/pagurus/internal/config"
)

// hostStartLock takes the host-wide start lock at path: the agents of
// different services on one host start their workloads one at a time. A
// deploy restarts every agent on a host at once, and six concurrent `up`s each
// made every peer's incusd restart its dnsmasq for the new DNS records; the
// restarts collided ("Address already in use") and an `up` stalled ~2 min
// (2026-10-18: a stack down 113 s, a deploy rolled back twice). It waits
// until ctx ends; the returned func releases it (the kernel does too if the
// agent dies). An empty path is no lock (tests).
func hostStartLock(ctx context.Context, path string, level int) (release func(), waited time.Duration, err error) {
	if path == "" {
		return func() {}, 0, nil
	}
	// Priority levels (config start_priority, higher first, as Incus's
	// boot.autostart.priority): a service at level L > 0 holds prioPath(L)
	// while it waits and starts, so one service per level is in line at a
	// time, and a service yields while any level above its own is held.
	// Without it mqtt-ha waited 32 s behind five services after a deploy
	// (2026-10-20), MQTT down ~45 s instead of ~12 s. With one level the rest
	// started in no particular order after a host-wide fence, the last of 14
	// after ~2 min (2026-11-11). At worst a service now waits for the one
	// start already running.
	var files []*os.File
	closeAll := func() {
		for _, f := range files {
			_ = f.Close()
		}
	}
	open := func(p string) (*os.File, error) {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o600)
		if err == nil {
			files = append(files, f)
		}
		return f, err
	}
	f, err := open(path)
	if err != nil {
		return nil, 0, err
	}
	var above []*os.File // the levels this one yields to
	for l := level + 1; l <= config.MaxStartPriority; l++ {
		p, err := open(prioPath(path, l))
		if err != nil {
			closeAll()
			return nil, 0, err
		}
		above = append(above, p)
	}
	t0 := time.Now()
	if level > 0 {
		own, err := open(prioPath(path, level))
		if err != nil {
			closeAll()
			return nil, 0, err
		}
		for {
			err := syscall.Flock(int(own.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
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
		for _, p := range above {
			// Held by a higher level waiting or starting: let it go first.
			if err := syscall.Flock(int(p.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err == nil {
				_ = syscall.Flock(int(p.Fd()), syscall.LOCK_UN)
			} else if errors.Is(err, syscall.EWOULDBLOCK) {
				yield = true
				break
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

// prioPath is level l's lock file. Level 1 keeps the name the boolean
// start_priority used, so old and new agents agree during a rolling deploy.
func prioPath(path string, l int) string {
	if l == 1 {
		return path + ".prio"
	}
	return fmt.Sprintf("%s.prio%d", path, l)
}
