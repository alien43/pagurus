package agent

// In-place upgrade: a running agent exec()s a new binary (and config) in
// its own process, keeping its term (docs/01_operations.md § In-place
// upgrade). The PID, the cgroup and the children survive exec(): conch keeps
// the lease, the term shim keeps its connection, the workload keeps running.
// The new image takes the agent's state and the open sockets from the old
// one (a handover in the environment, the sockets as inherited fds) and
// carries on supervising the same term.
//
// The exec happens only while every loop of the agent is parked between two
// steps (gate): never in the middle of a start, a fence, a release or a pull.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/alien43/pagurus/internal/config"
)

// handoverProtocol is the version of the handover: an agent re-execs only
// into a binary that speaks its protocol (the advert names it).
const handoverProtocol = 1

// HandoverEnv carries the handover from the old image to the new one.
const HandoverEnv = "PAGURUS_HANDOVER"

// Request is what `pagurus reexec` hands a running agent: the unit's command
// line and environment as they are now, and the safety hashes of the config
// that command line names.
type Request struct {
	Argv       []string `json:"argv"`
	Env        []string `json:"env"`
	SafetyHash string   `json:"safety_hash"`
	FenceHash  string   `json:"fence_hash"`
}

// Advert says that the agent with this PID takes a Request (SIGHUP); `pagurus
// reexec` restarts any agent that has not written one.
type Advert struct {
	PID      int `json:"pid"`
	Protocol int `json:"protocol"`
}

// ErrRestart: this request cannot be applied in place; the caller ends the
// agent as on SIGTERM (it releases its term) and systemd restarts it.
var ErrRestart = errors.New("not upgradable in place")

func requestPath(cfg *config.Config) string {
	return filepath.Join(cfg.StateDir, cfg.Service+".reexec")
}

func advertPath(cfg *config.Config) string {
	return filepath.Join(cfg.StateDir, cfg.Service+".inplace")
}

// WriteRequest is `pagurus reexec`'s half: leave req for the agent, then
// signal it.
func WriteRequest(cfg *config.Config, req Request) error {
	return writeJSON(requestPath(cfg), req)
}

// ReadAdvert reads the running agent's advert (ok=false: none).
func ReadAdvert(cfg *config.Config) (Advert, bool) {
	var ad Advert
	b, err := os.ReadFile(advertPath(cfg))
	if err != nil || json.Unmarshal(b, &ad) != nil || ad.Protocol != handoverProtocol {
		return Advert{}, false
	}
	return ad, true
}

// TakeRequest reads and removes the pending request.
func (a *Agent) TakeRequest() (Request, error) {
	var req Request
	b, err := os.ReadFile(requestPath(a.cfg))
	if err != nil {
		return req, err
	}
	_ = os.Remove(requestPath(a.cfg))
	if err := json.Unmarshal(b, &req); err != nil {
		return req, err
	}
	if len(req.Argv) == 0 {
		return req, errors.New("empty argv")
	}
	return req, nil
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// handover is the agent's state across the exec. Fds are inherited, by number.
type handover struct {
	Protocol         int               `json:"protocol"`
	Member           string            `json:"member"`
	Arch             string            `json:"arch"`
	AgentID          string            `json:"agent_id"`
	Types            []string          `json:"types"`
	Versions         map[string]string `json:"versions"`
	TermListener     int               `json:"term_listener"` // fd
	Relay            int               `json:"relay"`         // fd, -1: watchdog off
	LocalStopped     bool              `json:"local_stopped"`
	UnfitAfter       time.Time         `json:"unfit_after"`
	BackoffUntil     time.Time         `json:"backoff_until"`
	ProgressDeadline time.Time         `json:"progress_deadline"`
	DeferredFor      int64             `json:"deferred_for"`
	FailedTerms      int               `json:"failed_terms"`
	SkewAlerted      string            `json:"skew_alerted"`
	StartSkewAlerted string            `json:"start_skew_alerted"`
	RefreshSeen      *string           `json:"refresh_seen"` // nil: not seeded yet
	Conch            *conchHandover    `json:"conch"`
	Term             *termHandover     `json:"term"`
}

type conchHandover struct {
	PID       int           `json:"pid"`
	Delay     time.Duration `json:"delay"`
	SpawnedAt time.Time     `json:"spawned_at"`
	Failures  int           `json:"failures"`
	TermRan   bool          `json:"term_ran"`
	Exit      *conchExit    `json:"exit"` // exited, not yet handled
}

type termHandover struct {
	Conn     int             `json:"conn"` // fd
	PID      int             `json:"pid"`
	Epoch    int64           `json:"epoch"`
	Lease    int64           `json:"lease"`
	Acquired time.Time       `json:"acquired"`
	Verified map[string]bool `json:"verified"`
	Sv       superviseState  `json:"sv"`
}

// Reexec applies req in place: it waits until every loop is parked, then
// exec()s req.Argv with the agent's state handed over. It returns only if
// that did not happen: ErrRestart (the caller ends the agent; systemd
// restarts it) or the exec's error (the agent goes on as it was).
func (a *Agent) Reexec(req Request) error {
	if req.SafetyHash != a.cfg.SafetyHash() || req.FenceHash != a.cfg.FenceHash() {
		a.log.Warn("in-place upgrade: the safety config changes; releasing and restarting instead", "member", a.member)
		return ErrRestart
	}
	if a.wd != nil && a.wd.Mode() == config.WatchdogDevice {
		// The device's fd does not survive the exec un-petted reliably on
		// every driver; a restart is the known path.
		a.log.Warn("in-place upgrade: watchdog mode device; releasing and restarting instead", "member", a.member)
		return ErrRestart
	}
	began, logged := a.now(), a.now()
	for !a.gate.TryLock() {
		if a.now().Sub(logged) >= 30*time.Second {
			a.log.Info("in-place upgrade: waiting for the agent's loops to park", "member", a.member, "waited", a.now().Sub(began).Round(time.Second))
			logged = a.now()
		}
		time.Sleep(20 * time.Millisecond)
	}
	h, fds, err := a.snapshot()
	if err == nil {
		var b []byte
		if b, err = json.Marshal(h); err == nil {
			env := append(execEnv(req.Env, os.Environ()), HandoverEnv+"="+string(b))
			a.log.Info("in-place upgrade: exec", "member", a.member, "argv", req.Argv, "term", h.Term != nil, "conch", h.Conch != nil,
				"parked_after", a.now().Sub(began).Round(time.Millisecond))
			// No request may reach the new image before it handles SIGHUP.
			Unadvertise(a.cfg)
			err = a.exec(req.Argv[0], req.Argv, env)
			a.writeAdvert()
		}
	}
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
	a.mu.Lock()
	a.execing = false
	a.mu.Unlock()
	a.gate.Unlock()
	a.log.Error("in-place upgrade failed; carrying on as before", "member", a.member, "err", err)
	return err
}

// snapshot is the handover, with the fds it names dup'ed without CLOEXEC.
// The gate is held: every loop is parked. From here on conch is not reaped
// (execing): the new image adopts it.
func (a *Agent) snapshot() (*handover, []int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terms == nil {
		return nil, nil, errors.New("the agent is not running yet")
	}
	a.execing = true
	var fds []int
	dup := func(c syscall.Conn) int {
		fd, err := inheritable(c)
		if err != nil {
			return -1
		}
		fds = append(fds, fd)
		return fd
	}
	a.typesMu.Lock()
	types := slices.Clone(a.types)
	a.typesMu.Unlock()
	h := &handover{
		Protocol: handoverProtocol, Member: a.member, Arch: a.arch, AgentID: a.agentID, Types: types, Versions: a.versions,
		TermListener: dup(a.terms.ln), Relay: -1,
		LocalStopped: a.localStopped, UnfitAfter: a.unfitAfter, BackoffUntil: a.backoffUntil, ProgressDeadline: a.progressDeadline,
		DeferredFor: a.deferredFor, FailedTerms: a.failedTerms, SkewAlerted: a.skewAlerted, StartSkewAlerted: a.startSkewAlerted,
		RefreshSeen: a.refreshSeen,
	}
	if h.TermListener < 0 {
		return nil, fds, errors.New("cannot pass the term socket")
	}
	if a.relay != nil {
		if h.Relay = dup(a.relay.conn); h.Relay < 0 {
			return nil, fds, errors.New("cannot pass the watchdog relay")
		}
	}
	if a.conch != nil {
		h.Conch = &conchHandover{PID: a.conch.Process.Pid, Delay: a.fl.delay, SpawnedAt: a.fl.spawnedAt, Failures: a.fl.failures,
			TermRan: a.termRan, Exit: a.conchExit}
	}
	if a.termActive {
		t := a.term
		if t == nil || a.termConn == nil {
			return nil, fds, errors.New("a term is active but not parked")
		}
		th := &termHandover{Conn: dup(a.termConn), PID: a.termPID, Epoch: t.epoch, Lease: int64(t.lease), Acquired: t.acquired,
			Verified: t.verified, Sv: t.sv}
		if th.Conn < 0 {
			return nil, fds, errors.New("cannot pass the term's connection")
		}
		h.Term = th
	}
	return h, fds, nil
}

// inheritable dups c's fd; the dup has no CLOEXEC, so it survives exec().
func inheritable(c syscall.Conn) (int, error) {
	rc, err := c.SyscallConn()
	if err != nil {
		return -1, err
	}
	nfd, derr := -1, error(nil)
	if err := rc.Control(func(fd uintptr) { nfd, derr = unix.Dup(int(fd)) }); err != nil {
		return -1, err
	}
	return nfd, derr
}

// fromSelf are variables systemd sets for the main process alone (or with
// another value for an ExecReload= process): the new image keeps the old
// one's. MAINPID* only make sense to the reload command.
var (
	fromSelf = []string{"NOTIFY_SOCKET", "WATCHDOG_USEC", "WATCHDOG_PID", "JOURNAL_STREAM", "SYSTEMD_EXEC_PID", "INVOCATION_ID"}
	dropped  = []string{"MAINPID", "MAINPIDFDID", HandoverEnv}
)

// execEnv is the new image's environment: the unit's as `pagurus reexec`
// saw it (a new PATH, new variables), with the main process's own systemd
// variables kept.
func execEnv(unit, self []string) []string {
	var env []string
	for _, kv := range unit {
		k, _, _ := strings.Cut(kv, "=")
		if !slices.Contains(fromSelf, k) && !slices.Contains(dropped, k) {
			env = append(env, kv)
		}
	}
	for _, kv := range self {
		if k, _, _ := strings.Cut(kv, "="); slices.Contains(fromSelf, k) {
			env = append(env, kv)
		}
	}
	return env
}

// Resume reads the handover an old image left in the environment, if any,
// and removes it from the environment (conch must not inherit it).
func Resume() (*handover, error) {
	s, ok := os.LookupEnv(HandoverEnv)
	if !ok {
		return nil, nil
	}
	_ = os.Unsetenv(HandoverEnv)
	return parseHandover(s)
}

func parseHandover(s string) (*handover, error) {
	var h handover
	if err := json.Unmarshal([]byte(s), &h); err != nil {
		return nil, fmt.Errorf("in-place upgrade: bad handover: %w", err)
	}
	if h.Protocol != handoverProtocol {
		return nil, fmt.Errorf("in-place upgrade: handover protocol %d, want %d", h.Protocol, handoverProtocol)
	}
	return &h, nil
}

// Restore makes this agent the new image of an in-place exec: it takes the
// old image's identity and state instead of running Preflight (before Run;
// the sockets, conch and term are taken in Run).
func (a *Agent) Restore(h *handover) {
	a.member, a.arch, a.agentID, a.versions = h.Member, h.Arch, h.AgentID, h.Versions
	a.m.SetMember(h.Member)
	a.typesMu.Lock()
	a.types = h.Types
	a.typesMu.Unlock()
	a.localStopped, a.unfitAfter, a.backoffUntil, a.progressDeadline = h.LocalStopped, h.UnfitAfter, h.BackoffUntil, h.ProgressDeadline
	a.deferredFor, a.failedTerms, a.skewAlerted, a.startSkewAlerted = h.DeferredFor, h.FailedTerms, h.SkewAlerted, h.StartSkewAlerted
	a.refreshSeen = h.RefreshSeen
	a.resumed = h
}

// takeSockets turns the inherited fds into the agent's sockets.
func (a *Agent) takeSockets(h *handover) (*net.UnixListener, error) {
	f := os.NewFile(uintptr(h.TermListener), "term-listener")
	ln, err := net.FileListener(f)
	f.Close()
	if err != nil {
		return nil, fmt.Errorf("in-place upgrade: term socket: %w", err)
	}
	ul := ln.(*net.UnixListener)
	ul.SetUnlinkOnClose(true)
	if h.Relay >= 0 {
		f := os.NewFile(uintptr(h.Relay), "relay")
		pc, err := net.FilePacketConn(f)
		f.Close()
		if err != nil {
			ul.Close()
			return nil, fmt.Errorf("in-place upgrade: watchdog relay: %w", err)
		}
		a.relay = newRelay(filepath.Join(a.cfg.StateDir, a.cfg.Service+".notify"), pc.(*net.UnixConn), a.relayFreshness)
		a.relay.Reset() // a grace period, as for a fresh conch
	}
	return ul, nil
}

// adoptConch takes the old image's conch: still our child (unreaped), so its
// PID cannot have been reused.
func (a *Agent) adoptConch(ch *conchHandover) {
	p, _ := os.FindProcess(ch.PID) // never fails on Linux
	done := make(chan conchExit, 1)
	a.conch = &exec.Cmd{Process: p}
	a.termRan = ch.TermRan
	a.fl = followState{done: done, delay: ch.Delay, spawnedAt: ch.SpawnedAt, failures: ch.Failures}
	if ch.Exit != nil {
		a.conchExit = ch.Exit
		done <- *ch.Exit
		return
	}
	go a.waitConch(ch.PID, done)
}

// adoptTerm takes the old image's term connection.
func (a *Agent) adoptTerm(th *termHandover) (*net.UnixConn, error) {
	f := os.NewFile(uintptr(th.Conn), "term")
	c, err := net.FileConn(f)
	f.Close()
	if err != nil {
		return nil, fmt.Errorf("in-place upgrade: term connection: %w", err)
	}
	a.termActive, a.termPID, a.termConn = true, th.PID, c.(*net.UnixConn)
	return a.termConn, nil
}

// writeAdvert tells `pagurus reexec` that this agent takes requests.
func (a *Agent) writeAdvert() {
	if err := writeJSON(advertPath(a.cfg), Advert{PID: os.Getpid(), Protocol: handoverProtocol}); err != nil {
		a.log.Warn("cannot write the in-place upgrade advert; a deploy restarts this agent", "err", err)
	}
}

// ------------------------------------------------------------ wrappers ---

// writeWrappers puts conch's two commands behind scripts in the state dir,
// rewritten by every agent (fresh or resumed): a conch that outlives
// in-place upgrades runs the current binary and config, not a store path
// a garbage collection may have removed. They are run through /bin/sh,
// which works on a noexec /run too.
func (a *Agent) writeWrappers() error {
	fence := filepath.Join(a.cfg.StateDir, a.cfg.Service+".fence-self")
	term := filepath.Join(a.cfg.StateDir, a.cfg.Service+".term-cmd")
	for path, argv := range map[string][]string{fence: a.fenceCmd, term: a.termCmd} {
		body := "#!/bin/sh\nexec " + shellJoin(argv) + " \"$@\"\n"
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, []byte(body), 0o700); err != nil {
			return err
		}
		if err := os.Rename(tmp, path); err != nil {
			return err
		}
	}
	a.fenceCmd, a.termCmd = []string{"/bin/sh", fence}, []string{"/bin/sh", term}
	return nil
}

// Unadvertise removes the advert: an image that is starting (fresh or exec'd)
// takes no request until Run has set up.
func Unadvertise(cfg *config.Config) { _ = os.Remove(advertPath(cfg)) }
