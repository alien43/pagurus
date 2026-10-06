// Package watchdog turns "this agent is a fit custodian of the workload" into a
// watchdog pet (the sbd / Proxmox-LRM pattern, stonith_exploration.md §7).
//
// The agent supplies a fitness predicate. The watchdog pets only while it
// holds; once it fails — or the agent wedges and stops calling — petting stops
// and whatever backs the watchdog acts. Four backends:
//
//	off      no watchdog.
//	dry-run  evaluates fitness and logs when it would have stopped petting.
//	systemd  sd_notify(WATCHDOG=1) to the agent's own unit (WatchdogSec=).
//	         What a missed deadline does is the unit's choice: restart the
//	         agent (default) or FailureAction=reboot-force.
//	device   a /dev/watchdogN the agent owns. Opens the device only with
//	         arm=true: an armed hardware watchdog resets the host.
package watchdog

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Fitness reports whether the agent may keep the host alive, and why not.
type Fitness func() (ok bool, reason string)

type Watchdog interface {
	Mode() string
	// Timeout is the interval after the last pet at which the backend acts
	// (0 for off).
	Timeout() time.Duration
	// FencesByReboot: a missed deadline resets the host.
	FencesByReboot() bool
	// Run pets while fit() holds, until ctx ends.
	Run(ctx context.Context, fit Fitness)
	// Close releases the backend. clean=true disarms it (magic close); a
	// non-clean close leaves an armed device to fire.
	Close(clean bool)
	// Starving reports whether the last evaluation withheld the pet.
	Starving() (bool, string)
}

type Options struct {
	Mode    string
	Device  string
	Timeout time.Duration
	Arm     bool
	Log     *slog.Logger
}

func New(o Options) (Watchdog, error) {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	switch o.Mode {
	case "off":
		return &off{}, nil
	case "dry-run":
		return &petter{mode: "dry-run", timeout: o.Timeout, log: o.Log, pet: func() error { return nil }}, nil
	case "systemd":
		return newSystemd(o)
	case "device":
		if !o.Arm {
			o.Log.Warn("watchdog: mode=device without arm=true — running as dry-run, the device is NOT opened", "device", o.Device)
			return &petter{mode: "device(dry-run)", timeout: o.Timeout, log: o.Log, pet: func() error { return nil }}, nil
		}
		return openDevice(o)
	}
	return nil, fmt.Errorf("unknown watchdog mode %q", o.Mode)
}

type off struct{}

func (off) Mode() string                       { return "off" }
func (off) Timeout() time.Duration             { return 0 }
func (off) FencesByReboot() bool               { return false }
func (off) Run(ctx context.Context, _ Fitness) { <-ctx.Done() }
func (off) Close(bool)                         {}
func (off) Starving() (bool, string)           { return false, "" }

// petter is the shared loop: evaluate fitness every timeout/4, pet if fit.
type petter struct {
	mode     string
	timeout  time.Duration
	reboot   bool
	log      *slog.Logger
	pet      func() error
	closeFn  func(clean bool)
	mu       sync.Mutex
	starving bool
	reason   string
}

func (p *petter) Mode() string           { return p.mode }
func (p *petter) Timeout() time.Duration { return p.timeout }
func (p *petter) FencesByReboot() bool   { return p.reboot }

func (p *petter) Starving() (bool, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starving, p.reason
}

func (p *petter) Close(clean bool) {
	if p.closeFn != nil {
		p.closeFn(clean)
	}
}

func (p *petter) Run(ctx context.Context, fit Fitness) {
	every := p.timeout / 4
	if every <= 0 || every > 5*time.Second {
		every = min(max(every, 500*time.Millisecond), 5*time.Second)
	}
	t := time.NewTicker(every)
	defer t.Stop()
	var since time.Time
	for {
		ok, reason := fit()
		p.mu.Lock()
		was := p.starving
		p.starving, p.reason = !ok, reason
		p.mu.Unlock()
		switch {
		case ok:
			if err := p.pet(); err != nil {
				p.log.Error("watchdog pet failed", "mode", p.mode, "err", err)
			}
			if was {
				p.log.Warn("watchdog: fit again, petting resumed", "mode", p.mode, "starved_for", time.Since(since).Round(time.Millisecond))
			}
		case !was:
			since = time.Now()
			p.log.Error("watchdog: NOT FIT — withholding pet", "mode", p.mode, "reason", reason, "acts_after", p.timeout, "reboots", p.reboot)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// --- systemd ---------------------------------------------------------------

func sdNotify(state string) error {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return fmt.Errorf("NOTIFY_SOCKET unset")
	}
	if strings.HasPrefix(sock, "@") {
		sock = "\x00" + sock[1:]
	}
	c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer c.Close()
	_, err = c.Write([]byte(state))
	return err
}

// Ready tells systemd (Type=notify) the agent finished starting. No-op
// outside systemd.
func Ready() {
	if os.Getenv("NOTIFY_SOCKET") != "" {
		_ = sdNotify("READY=1")
	}
}

// Stopping tells systemd the agent is shutting down cleanly.
func Stopping() {
	if os.Getenv("NOTIFY_SOCKET") != "" {
		_ = sdNotify("STOPPING=1")
	}
}

func newSystemd(o Options) (Watchdog, error) {
	usec, err := strconv.ParseInt(os.Getenv("WATCHDOG_USEC"), 10, 64)
	if err != nil || usec <= 0 {
		return nil, fmt.Errorf("watchdog mode=systemd: WATCHDOG_USEC not set — run under a unit with WatchdogSec= and NotifyAccess=main")
	}
	if pid := os.Getenv("WATCHDOG_PID"); pid != "" && pid != strconv.Itoa(os.Getpid()) {
		return nil, fmt.Errorf("watchdog mode=systemd: WATCHDOG_PID=%s is not this process", pid)
	}
	timeout := time.Duration(usec) * time.Microsecond
	reboot, action := unitRebootsOnFailure()
	o.Log.Info("watchdog: systemd", "timeout", timeout, "failure_action", action, "reboots", reboot)
	return &petter{mode: "systemd", timeout: timeout, reboot: reboot, log: o.Log,
		pet: func() error { return sdNotify("WATCHDOG=1") }}, nil
}

// unitRebootsOnFailure inspects this process's own unit: a watchdog timeout
// only fences by reboot if FailureAction is one of the reboot actions.
func unitRebootsOnFailure() (bool, string) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return false, "unknown"
	}
	unit := ""
	for _, seg := range strings.Split(strings.TrimSpace(string(b)), "/") {
		if strings.HasSuffix(seg, ".service") {
			unit = seg
		}
	}
	if unit == "" {
		return false, "unknown"
	}
	out, err := exec.Command("systemctl", "show", "--value", "-p", "FailureAction", unit).Output()
	if err != nil {
		return false, "unknown"
	}
	action := strings.TrimSpace(string(out))
	return strings.HasPrefix(action, "reboot") || strings.HasPrefix(action, "poweroff"), action
}

// --- device ----------------------------------------------------------------

func openDevice(o Options) (Watchdog, error) {
	fd, err := unix.Open(o.Device, unix.O_WRONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w (systemd may own it via RuntimeWatchdogSec)", o.Device, err)
	}
	secs := int(o.Timeout / time.Second)
	if err := unix.IoctlSetPointerInt(fd, unix.WDIOC_SETTIMEOUT, secs); err != nil {
		// Still open: an open device is armed. Disarm before failing.
		_, _ = unix.Write(fd, []byte("V"))
		unix.Close(fd)
		return nil, fmt.Errorf("%s: set timeout %ds: %w", o.Device, secs, err)
	}
	got, err := unix.IoctlGetInt(fd, unix.WDIOC_GETTIMEOUT)
	if err != nil || got != secs {
		_, _ = unix.Write(fd, []byte("V"))
		unix.Close(fd)
		return nil, fmt.Errorf("%s: timeout readback %d (err %v), want %d", o.Device, got, err, secs)
	}
	o.Log.Warn("watchdog: ARMED hardware device — host resets if the agent stops petting", "device", o.Device, "timeout", o.Timeout)
	var once sync.Once
	return &petter{mode: "device", timeout: o.Timeout, reboot: true, log: o.Log,
		pet: func() error { return unix.IoctlWatchdogKeepalive(fd) },
		closeFn: func(clean bool) {
			once.Do(func() {
				if clean {
					// Magic close: drivers without nowayout disarm on "V".
					_, _ = unix.Write(fd, []byte("V"))
				}
				unix.Close(fd)
			})
		}}, nil
}
