package watchdog

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// notifySocket listens like systemd's NOTIFY_SOCKET and records every frame.
type notifySocket struct {
	mu     sync.Mutex
	frames []string
	at     []time.Time
}

func listenNotify(t *testing.T, name string) (*notifySocket, string) {
	t.Helper()
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: name, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	n := &notifySocket{}
	go func() {
		buf := make([]byte, 1024)
		for {
			k, err := conn.Read(buf)
			if err != nil {
				return
			}
			n.mu.Lock()
			n.frames, n.at = append(n.frames, string(buf[:k])), append(n.at, time.Now())
			n.mu.Unlock()
		}
	}()
	return n, name
}

// count is how many frames equal to f arrived in [from, to).
func (n *notifySocket) count(f string, from, to time.Time) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	c := 0
	for i, x := range n.frames {
		if x == f && !n.at[i].Before(from) && n.at[i].Before(to) {
			c++
		}
	}
	return c
}

func (n *notifySocket) has(f string) bool {
	return n.count(f, time.Time{}, time.Now().Add(time.Hour)) > 0
}

// TestSystemdPetsOnlyWhileFit: WATCHDOG=1 frames arrive while fitness holds,
// stop at once when it fails (Starving reports why), and resume when it holds
// again.
func TestSystemdPetsOnlyWhileFit(t *testing.T) {
	ns, path := listenNotify(t, filepath.Join(t.TempDir(), "notify"))
	t.Setenv("NOTIFY_SOCKET", path)
	t.Setenv("WATCHDOG_USEC", "400000") // 400ms: evaluated every 100ms
	t.Setenv("WATCHDOG_PID", strconv.Itoa(os.Getpid()))
	wd, err := New(Options{Mode: "systemd", Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	if wd.Mode() != "systemd" || wd.Timeout() != 400*time.Millisecond {
		t.Fatalf("mode %q timeout %v", wd.Mode(), wd.Timeout())
	}
	var fit atomic.Bool
	fit.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wd.Run(ctx, func() (bool, string) {
		if fit.Load() {
			return true, ""
		}
		return false, "test says unfit"
	})

	t0 := time.Now()
	time.Sleep(500 * time.Millisecond)
	if c := ns.count("WATCHDOG=1", t0, time.Now()); c < 3 {
		t.Fatalf("%d pets in 500ms while fit", c)
	}
	fit.Store(false)
	time.Sleep(150 * time.Millisecond) // one evaluation to notice
	t1 := time.Now()
	time.Sleep(600 * time.Millisecond)
	if c := ns.count("WATCHDOG=1", t1, time.Now()); c != 0 {
		t.Fatalf("%d pets while unfit", c)
	}
	if starving, why := wd.Starving(); !starving || why != "test says unfit" {
		t.Fatalf("Starving() = %v %q", starving, why)
	}
	fit.Store(true)
	time.Sleep(150 * time.Millisecond)
	t2 := time.Now()
	time.Sleep(400 * time.Millisecond)
	if c := ns.count("WATCHDOG=1", t2, time.Now()); c < 2 {
		t.Fatalf("%d pets after fitness returned", c)
	}
	if starving, _ := wd.Starving(); starving {
		t.Fatal("still starving after fitness returned")
	}
}

func TestSystemdRefusesWithoutItsEnvironment(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "/nonexistent")
	t.Setenv("WATCHDOG_USEC", "")
	if _, err := New(Options{Mode: "systemd", Log: quiet()}); err == nil || !strings.Contains(err.Error(), "WATCHDOG_USEC") {
		t.Fatalf("no WATCHDOG_USEC: %v", err)
	}
	t.Setenv("WATCHDOG_USEC", "1000000")
	t.Setenv("WATCHDOG_PID", "1")
	if _, err := New(Options{Mode: "systemd", Log: quiet()}); err == nil || !strings.Contains(err.Error(), "not this process") {
		t.Fatalf("WATCHDOG_PID of another process: %v", err)
	}
}

// TestReadyStoppingFraming covers the plain and the abstract socket forms.
func TestReadyStoppingFraming(t *testing.T) {
	for _, name := range []string{filepath.Join(t.TempDir(), "n"), "@pagurus-test-" + strconv.Itoa(os.Getpid())} {
		ns, path := listenNotify(t, name)
		t.Setenv("NOTIFY_SOCKET", path)
		Ready()
		Stopping()
		deadline := time.Now().Add(2 * time.Second)
		for !(ns.has("READY=1") && ns.has("STOPPING=1")) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: frames %v", name, ns.frames)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Setenv("NOTIFY_SOCKET", "")
	Ready() // outside systemd: a no-op, not a panic
	if err := sdNotify("X=1"); err == nil {
		t.Fatal("sdNotify without NOTIFY_SOCKET succeeded")
	}
}

func TestOffAndDryRun(t *testing.T) {
	off, err := New(Options{Mode: "off"})
	if err != nil || off.Mode() != "off" || off.Timeout() != 0 || off.FencesByReboot() {
		t.Fatalf("off: %v %+v", err, off)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	off.Run(ctx, nil) // returns at once on a done context
	off.Close(true)

	dr, err := New(Options{Mode: "dry-run", Timeout: 400 * time.Millisecond, Log: quiet()})
	if err != nil || dr.Mode() != "dry-run" || dr.FencesByReboot() {
		t.Fatalf("dry-run: %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); dr.Run(ctx, func() (bool, string) { return false, "nope" }) }()
	time.Sleep(200 * time.Millisecond)
	if s, why := dr.Starving(); !s || why != "nope" {
		t.Fatalf("dry-run Starving() = %v %q", s, why)
	}
	cancel()
	<-done
	dr.Close(true)

	if _, err := New(Options{Mode: "bogus"}); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

// TestDeviceWithoutArmNeverOpensTheDevice: device mode without arm=true runs
// as dry-run. The device path here does not exist: opening it would fail.
func TestDeviceWithoutArmNeverOpensTheDevice(t *testing.T) {
	wd, err := New(Options{Mode: "device", Device: filepath.Join(t.TempDir(), "no-such-watchdog"), Timeout: time.Minute, Log: quiet()})
	if err != nil {
		t.Fatalf("unarmed device mode tried the device: %v", err)
	}
	if wd.Mode() != "device(dry-run)" || wd.FencesByReboot() {
		t.Fatalf("mode %q reboot %v", wd.Mode(), wd.FencesByReboot())
	}
}

func TestUnitRebootsOnFailureOutsideAUnit(t *testing.T) {
	// Under `go test` this process is rarely in a .service; whatever it is
	// in, the answer must be consistent: reboot only for a reboot/poweroff
	// action.
	reboot, action := unitRebootsOnFailure()
	if reboot != (strings.HasPrefix(action, "reboot") || strings.HasPrefix(action, "poweroff")) {
		t.Fatalf("reboot=%v for action %q", reboot, action)
	}
}
