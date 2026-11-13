package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sys/unix"

	"github.com/alien43/pagurus/internal/termshim"
)

// ------------------------------------------------------------------ conch --

func (a *Agent) heartbeatPath() string {
	return filepath.Join(a.cfg.StateDir, a.cfg.Service+".heartbeat")
}

func (a *Agent) termSocket() string { return filepath.Join(a.cfg.StateDir, a.cfg.Service+".term") }

// heartbeatStale is conch's --watchdog-stale: the heartbeat is touched every
// heartbeatEvery while the term's progress deadline holds.
var (
	heartbeatStale = 5 * time.Second
	heartbeatEvery = time.Second
)

// conchArgs is the `conch elect` command line for one campaign.
func (a *Agent) conchArgs(delay time.Duration) []string {
	fenceArgv := append(append([]string(nil), a.fenceCmd...), "--member", a.member)
	args := append(append([]string(nil), a.conchCmd...), "elect", a.co.Office())
	args = append(args, a.cfg.ConchEtcdArgs()...)
	args = append(args,
		"--ttl", a.cfg.Lease.TTL.D().String(),
		"--fence", shellJoin(fenceArgv),
		"--fence-budget", a.cfg.ConchFenceBudget().String(),
	)
	if a.relay != nil {
		args = append(args, "--watchdog", "--watchdog-heartbeat", a.heartbeatPath(), "--watchdog-stale", heartbeatStale.String())
	}
	if delay > 0 {
		args = append(args, "--campaign-after", delay.String())
	}
	args = append(args, "--")
	args = append(args, a.termCmd...)
	return append(args, a.termSocket())
}

// conchEnv is our environment for conch, with systemd's notify variables
// replaced by the relay's: conch pets the relay, never systemd directly.
func (a *Agent) conchEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); k == "NOTIFY_SOCKET" || k == "WATCHDOG_USEC" || k == "WATCHDOG_PID" {
			continue
		}
		env = append(env, kv)
	}
	if a.relay != nil {
		// No WATCHDOG_PID: conch only rejects a PID naming another process.
		env = append(env, "NOTIFY_SOCKET="+a.relay.path,
			fmt.Sprintf("WATCHDOG_USEC=%d", (a.wd.Timeout()/2).Microseconds()))
	}
	return env
}

// conchExit is how conch ended: Code is its exit status, -1 if a signal
// killed it.
type conchExit struct {
	Code int    `json:"code"`
	Err  string `json:"err,omitempty"`
}

// spawnConch starts `conch elect` for this campaign. The returned channel
// receives its exit.
func (a *Agent) spawnConch(delay time.Duration) (chan conchExit, error) {
	argv := a.conchArgs(delay)
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = a.conchEnv()
	// A file, not a pipe into this process: conch outlives an in-place exec.
	cmd.Stdout, cmd.Stderr = a.procOut, a.procOut
	// Its own process group, so a crash test can kill conch and the term
	// together, and a signal to the agent's group is not sent to conch twice.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if a.relay != nil {
		a.relay.Reset() // grace until conch's first pet
		a.touchHeartbeat()
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.conch, a.termRan, a.conchExit = cmd, false, nil
	a.mu.Unlock()
	a.m.Bool("pagurus_conch_running", true)
	a.log.Info("campaigning", "member", a.member, "office", a.co.Office(), "campaign_after", delay, "conch_pid", cmd.Process.Pid)
	done := make(chan conchExit, 1)
	go a.waitConch(cmd.Process.Pid, done)
	return done, nil
}

// waitConch reaps conch and reports its exit on done (and in a.conchExit,
// which an in-place exec hands over until conchExited has seen it). It
// waits on a pidfd instead of in wait4, so that while an exec is pending it
// can hold off reaping: the new image adopts conch by its PID.
func (a *Agent) waitConch(pid int, done chan<- conchExit) {
	pfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		// Not our live child: nothing to reap.
		ex := conchExit{Code: -1, Err: "pidfd: " + err.Error()}
		a.mu.Lock()
		a.conchExit = &ex
		a.mu.Unlock()
		done <- ex
		return
	}
	defer unix.Close(pfd)
	for {
		n, err := unix.Poll([]unix.PollFd{{Fd: int32(pfd), Events: unix.POLLIN}}, 1000)
		if n <= 0 || err != nil {
			continue
		}
		a.mu.Lock()
		if a.execing {
			a.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		var ws unix.WaitStatus
		_, err = unix.Wait4(pid, &ws, unix.WNOHANG, nil)
		ex := conchExit{Code: ws.ExitStatus()}
		switch {
		case err != nil:
			ex = conchExit{Code: -1, Err: err.Error()}
		case ws.Signaled():
			ex = conchExit{Code: -1, Err: "killed by " + ws.Signal().String()}
		}
		a.conchExit = &ex
		a.mu.Unlock()
		done <- ex
		return
	}
}

// stopConch asks conch to stop (SIGTERM). Holding, it ends the term first:
// stops the child, runs the fence, resigns.
func (a *Agent) stopConch(reason string) {
	a.mu.Lock()
	cmd := a.conch
	a.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		a.log.Info("stopping conch", "member", a.member, "reason", reason)
		_ = cmd.Process.Signal(syscall.SIGTERM)
	}
}

// withdraw stops conch for a campaign that should not go on, unless a term
// has begun meanwhile: it reports whether it signalled. follow decides to
// withdraw after its own inTerm check, its ground-truth check and an etcd
// read, and a term that starts in between has already cleared localStopped
// (lead), so the verdict "not eligible: local instances not confirmed
// stopped" was about the term itself; signalling would end it at once
// (seen live 12 times in 7 days on two members, and in the VM lab). The
// check and the signal happen under a.mu, which serveTerm holds to start a
// term, so no term begins between them.
func (a *Agent) withdraw(reason string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	cmd := a.conch
	if a.termActive || a.termRan || cmd == nil || cmd.Process == nil {
		return false
	}
	a.log.Info("stopping conch", "member", a.member, "reason", reason)
	_ = cmd.Process.Signal(syscall.SIGTERM)
	return true
}

// exitWithdrawn is conch's exit after a SIGTERM while still campaigning
// (stopConch on a withdrawal): nothing ran.
const exitWithdrawn = 143

// conchExited records conch's exit. A term still running here has lost its
// conch (and with it the lease and the fence): end it now.
func (a *Agent) conchExited(ex conchExit) {
	code := ex.Code
	a.mu.Lock()
	a.conch, a.conchExit = nil, nil
	termPID := a.termPID
	if !a.localStopped {
		a.unfitAfter = a.now().Add(a.cfg.SelfFenceLead() + 2*a.cfg.EligibleInterval.D())
	}
	a.mu.Unlock()
	if termPID > 0 {
		_ = syscall.Kill(termPID, syscall.SIGKILL)
	}
	a.m.Bool("pagurus_conch_running", false)
	switch code {
	case 0:
		a.m.Inc("pagurus_conch_exits_total", "result", "ok")
	case 71:
		a.m.Inc("pagurus_conch_exits_total", "result", "fence_failed")
	case exitWithdrawn:
		a.m.Inc("pagurus_conch_exits_total", "result", "withdrawn")
	default:
		a.m.Inc("pagurus_conch_exits_total", "result", "other")
	}
	switch code {
	case 0, exitWithdrawn:
		a.log.Info("conch exited", "member", a.member, "code", code)
	case 71:
		a.alerter.Alert("conch-fence-failed", "conch's self-fence did not confirm; the lease was left to expire", "member", a.member)
	default:
		a.log.Warn("conch exited", "member", a.member, "code", code, "err", ex.Err)
	}
}

// KillProcs kills conch and the term process outright, as a host crash or
// an OOM kill of the unit would (tests).
func (a *Agent) KillProcs() {
	a.mu.Lock()
	cmd, termPID := a.conch, a.termPID
	a.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	if termPID > 0 {
		_ = syscall.Kill(termPID, syscall.SIGKILL)
	}
}

// shellJoin quotes argv for `/bin/sh -c` (conch runs --fence through sh).
func shellJoin(argv []string) string {
	q := make([]string, len(argv))
	for i, s := range argv {
		q[i] = "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
	}
	return strings.Join(q, " ")
}

// ------------------------------------------------------------- heartbeat --

func (a *Agent) touchHeartbeat() {
	now := time.Now()
	if err := os.Chtimes(a.heartbeatPath(), now, now); err != nil {
		_ = os.WriteFile(a.heartbeatPath(), nil, 0o600)
	}
}

// heartbeat touches conch's --watchdog-heartbeat file every second while the
// term's progress deadline (beat) holds. A wedged term stops it; conch then
// counts the term unfit and stops petting.
func (a *Agent) heartbeat(ctx context.Context) {
	if a.relay == nil {
		return
	}
	t := time.NewTicker(heartbeatEvery)
	defer t.Stop()
	withheld := false
	for {
		a.mu.Lock()
		ok := a.now().Before(a.progressDeadline)
		execing := a.execing
		a.mu.Unlock()
		if execing {
			// The image is being replaced: the new one beats from here.
		} else if ok {
			a.touchHeartbeat()
		} else if !withheld {
			a.log.Error("term missed its progress deadline; heartbeat withheld", "member", a.member)
			a.m.Inc("pagurus_heartbeat_withheld_total")
		}
		withheld = !ok
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ----------------------------------------------------------------- relay --

// notifyRelay is the NOTIFY_SOCKET conch is given. It records conch's
// WATCHDOG=1 pets; the agent's own petter forwards them to the real backend
// only while they are fresh. It never pets on conch's behalf.
type notifyRelay struct {
	path      string
	conn      *net.UnixConn
	freshness func() time.Duration

	mu   sync.Mutex
	last time.Time
}

func listenNotify(path string, freshness func() time.Duration) (*notifyRelay, error) {
	_ = os.Remove(path)
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		return nil, fmt.Errorf("watchdog relay: %w", err)
	}
	return newRelay(path, conn, freshness), nil
}

// newRelay serves conn (bound to path), fresh or inherited across an exec.
func newRelay(path string, conn *net.UnixConn, freshness func() time.Duration) *notifyRelay {
	r := &notifyRelay{path: path, conn: conn, freshness: freshness}
	go r.serve()
	return r
}

func (r *notifyRelay) serve() {
	buf := make([]byte, 4096)
	for {
		n, err := r.conn.Read(buf)
		if err != nil {
			return
		}
		for _, line := range strings.Split(string(buf[:n]), "\n") {
			if line == "WATCHDOG=1" {
				r.mu.Lock()
				r.last = time.Now()
				r.mu.Unlock()
			}
		}
	}
}

// Reset starts a fresh grace period (a new conch has not petted yet).
func (r *notifyRelay) Reset() {
	r.mu.Lock()
	r.last = time.Now()
	r.mu.Unlock()
}

func (r *notifyRelay) Fresh() (bool, string) {
	r.mu.Lock()
	age := time.Since(r.last)
	r.mu.Unlock()
	if age <= r.freshness() {
		return true, ""
	}
	return false, fmt.Sprintf("conch stopped petting the watchdog (last pet %s ago)", age.Round(time.Millisecond))
}

func (r *notifyRelay) Close() {
	_ = r.conn.Close()
	_ = os.Remove(r.path)
}

// ------------------------------------------------------------------ terms --

type termServer struct{ ln *net.UnixListener }

func (s *termServer) Close() { _ = s.ln.Close() }

// listenTerms accepts the `pagurus term` process conch starts on winning.
// The socket is in the root-only state dir. ln, if set, is the socket an
// old image handed over.
func (a *Agent) listenTerms(ctx context.Context, ln *net.UnixListener) (*termServer, error) {
	if ln == nil {
		_ = os.Remove(a.termSocket())
		l, err := net.Listen("unix", a.termSocket())
		if err != nil {
			return nil, fmt.Errorf("term socket: %w", err)
		}
		ln = l.(*net.UnixListener)
		if err := os.Chmod(a.termSocket(), 0o600); err != nil {
			ln.Close()
			return nil, err
		}
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go a.serveTerm(ctx, c)
		}
	}()
	a.terms = &termServer{ln: ln}
	return a.terms, nil
}

func (a *Agent) serveTerm(ctx context.Context, c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := rd.ReadBytes('\n')
	_ = c.SetReadDeadline(time.Time{})
	var h termshim.Hello
	if err != nil || json.Unmarshal(line, &h) != nil || h.Rev <= 0 {
		a.log.Error("bad term handshake", "err", err)
		return
	}
	lease, err := strconv.ParseInt(h.Lease, 16, 64)
	if err != nil {
		a.log.Error("bad term handshake: lease", "lease", h.Lease)
		return
	}

	// The whole term, up to the reply, is one step of the gate (an
	// in-place exec waits for it to park in supervise).
	a.gate.RLock()
	defer a.gate.RUnlock()
	// The term starts only between two ground-truth checks, never during one.
	a.localMu.Lock()
	a.mu.Lock()
	ok := a.conch != nil && !a.termActive
	if ok {
		a.termActive, a.termPID, a.termRan = true, h.PID, true
		a.termConn, _ = c.(*net.UnixConn)
	}
	a.mu.Unlock()
	a.localMu.Unlock()
	if !ok {
		a.log.Error("term handshake while no campaign of ours runs (or a term already does); refusing", "rev", h.Rev)
		return
	}
	a.runTerm(ctx, c, rd, h.Rev, clientv3.LeaseID(lease), nil)
}

// resumeTerm carries on a term an old image handed over (adoptTerm).
func (a *Agent) resumeTerm(ctx context.Context, c *net.UnixConn, th *termHandover) {
	defer c.Close()
	a.gate.RLock()
	defer a.gate.RUnlock()
	a.runTerm(ctx, c, bufio.NewReader(c), th.Epoch, clientv3.LeaseID(th.Lease), th)
}

// runTerm leads the term and answers the term process with its exit code.
func (a *Agent) runTerm(ctx context.Context, c net.Conn, rd io.ByteReader, epoch int64, lease clientv3.LeaseID, resumed *termHandover) {
	defer func() {
		a.mu.Lock()
		a.termActive, a.termPID, a.termConn = false, 0, nil
		a.mu.Unlock()
	}()

	// The term process going away (conch lost the office, or died) ends
	// the term at once; conch's fence stops the workload.
	lost, markLost := context.WithCancel(context.Background())
	defer markLost()
	go func() {
		_, _ = rd.ReadByte()
		markLost()
	}()

	code := a.lead(ctx, lost, epoch, lease, resumed)
	b, _ := json.Marshal(termshim.Bye{Exit: code})
	_, _ = c.Write(append(b, '\n'))
}
