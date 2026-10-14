// Package incus is the runtime driver: it runs the stack with incus-compose in
// this member's project, and stops (fences) a member's project through the
// Incus cluster API or, locally, through cgroup.kill.
//
// Incus is reached through the API interface (the Incus Go client, or a fake
// in tests); every call carries the caller's context, so a hung daemon can
// delay the agent by at most the caller's budget. Run is kept for the CLIs
// that have no library (incus-compose).
package incus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type Instance struct {
	Name     string            `json:"name"`
	Status   string            `json:"status"`
	Location string            `json:"location"`
	Type     string            `json:"type"`
	Expanded map[string]string `json:"expanded_config"`
}

const (
	StatusRunning = "Running"
	StatusStopped = "Stopped"
)

type Options struct {
	ProjectPrefix string
	// CgroupRoot is the cgroup2 mount (default /sys/fs/cgroup).
	CgroupRoot string
	// ProcRoot is procfs (default /proc), where Kill finds the proxy-device
	// processes of the containers it killed.
	ProcRoot string
	// KillPID kills one process (default SIGKILL). Tests replace it.
	KillPID func(pid int) error
	// API reaches Incus (default: NewClient over the local unix socket).
	API API
}

type Driver struct {
	o Options
}

func New(o Options) *Driver {
	if o.CgroupRoot == "" {
		o.CgroupRoot = "/sys/fs/cgroup"
	}
	if o.ProcRoot == "" {
		o.ProcRoot = "/proc"
	}
	if o.KillPID == nil {
		o.KillPID = func(pid int) error { return syscall.Kill(pid, syscall.SIGKILL) }
	}
	if o.API == nil {
		o.API = NewClient(ClientOptions{})
	}
	return &Driver{o: o}
}

func (d *Driver) Project(member string) string { return d.o.ProjectPrefix + "-" + member }

// run executes a command bounded by ctx; on cancellation the whole process
// group is killed so no grandchild outlives the budget.
// Run runs a CLI in its own process group, killing the group when ctx ends,
// and returns stdout; stderr is folded into the error.
func Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if err != nil {
		if ctx.Err() != nil {
			return out.Bytes(), fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), ctx.Err())
		}
		return out.Bytes(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.Bytes(), nil
}

// Identity returns this host's cluster member name and kernel architecture.
func (d *Driver) Identity(ctx context.Context) (member, arch string, err error) {
	name, clustered, archs, err := d.o.API.Server(ctx)
	if err != nil {
		return "", "", err
	}
	if !clustered {
		return "", "", errors.New("local Incus is not clustered")
	}
	if len(archs) == 0 {
		return "", "", errors.New("local Incus reports no architecture")
	}
	return name, archs[0], nil
}

// CheckProject verifies project(member) is pinned to exactly member: a
// restricted.cluster.groups whose groups contain member and nobody else.
// Without that pin, `incus-compose up` could place the workload on another
// member and the per-member fencing below would fence the wrong host.
func (d *Driver) CheckProject(ctx context.Context, member string) error {
	p := d.Project(member)
	cfg, err := d.o.API.ProjectConfig(ctx, p)
	if err != nil {
		return fmt.Errorf("project %s: %w", p, err)
	}
	groups := strings.Split(cfg["restricted.cluster.groups"], ",")
	if len(groups) == 0 || groups[0] == "" {
		return fmt.Errorf("project %s: restricted.cluster.groups is unset; it must pin the project to %s", p, member)
	}
	for _, g := range groups {
		members, err := d.o.API.ClusterGroupMembers(ctx, strings.TrimSpace(g))
		if err != nil {
			return fmt.Errorf("project %s: cluster group %s: %w", p, g, err)
		}
		if len(members) != 1 || members[0] != member {
			return fmt.Errorf("project %s: cluster group %s has members %v, want exactly [%s]", p, g, members, member)
		}
	}
	// incus-compose marks the project user.healthcheck.enabled=true even with
	// --no-healthd, so the project flag proves nothing. What ic-healthd acts on
	// is each instance's restart policy: any value but "no" would let it
	// restart an instance this agent fenced.
	insts, err := d.List(ctx, member)
	if err != nil {
		return err
	}
	for _, in := range insts {
		if r := in.Expanded["user.healthcheck.restart"]; r != "" && r != "no" {
			return fmt.Errorf("instance %s in %s has user.healthcheck.restart=%q — ic-healthd could restart it after a fence; use restart: \"no\"", in.Name, p, r)
		}
	}
	return nil
}

// MemberOnline reports the cluster's view of member.
func (d *Driver) MemberOnline(ctx context.Context, member string) (bool, error) {
	status, err := d.o.API.MemberStatus(ctx, member)
	if err != nil {
		return false, err
	}
	return status == "Online", nil
}

// List returns the instances of member's project, as the cluster sees them.
func (d *Driver) List(ctx context.Context, member string) ([]Instance, error) {
	// A restricted Incus client listing a project it may not see gets an
	// empty list, not an error (Incus 7.5.1, verified 2026-10-10: 200 OK,
	// 0 instances, for a project with a running instance). An empty list
	// reads as "all stopped", which a peer fence would take as a confirmed
	// fence. Reading the project itself is refused properly (403), so do
	// that first and fail instead.
	p := d.Project(member)
	if _, err := d.o.API.ProjectConfig(ctx, p); err != nil {
		return nil, fmt.Errorf("project %s not readable, so its instance list cannot be trusted: %w", p, err)
	}
	return d.o.API.Instances(ctx, p)
}

// EnforceNoAutostart sets boot.autostart=false on every instance of member's
// project. Unset means "restore the last state on boot", which would let a
// rebooting host start a second copy behind the agent's back.
func (d *Driver) EnforceNoAutostart(ctx context.Context, member string) error {
	insts, err := d.List(ctx, member)
	if err != nil {
		return err
	}
	for _, in := range insts {
		if in.Expanded["boot.autostart"] == "false" {
			continue
		}
		if err := d.o.API.SetConfig(ctx, d.Project(member), in.Name, "boot.autostart", "false"); err != nil {
			return err
		}
	}
	return nil
}

// Stop stops every non-stopped instance in member's project through the Incus
// API (forwarded by the cluster to the member that hosts it). graceful > 0
// tries a clean shutdown first; the stop is then forced.
func (d *Driver) Stop(ctx context.Context, member string, graceful time.Duration) error {
	notStopped := func() ([]string, error) {
		insts, err := d.List(ctx, member)
		if err != nil {
			return nil, err
		}
		var names []string
		for _, in := range insts {
			if in.Status != StatusStopped {
				names = append(names, in.Name)
			}
		}
		return names, nil
	}
	names, err := notStopped()
	if err != nil || len(names) == 0 {
		return err
	}
	p := d.Project(member)
	stopAll := func(ctx context.Context, timeout time.Duration, force bool) error {
		var errs []error
		for _, n := range names {
			if err := d.o.API.Stop(ctx, p, n, timeout, force); err != nil {
				errs = append(errs, fmt.Errorf("stop %s/%s: %w", p, n, err))
			}
		}
		return errors.Join(errs...)
	}
	if graceful > 0 {
		gctx, cancel := context.WithTimeout(ctx, graceful+2*time.Second)
		gerr := stopAll(gctx, graceful, false)
		cancel()
		if gerr == nil {
			return nil
		}
		// Some may have stopped; stopping a stopped instance is an error.
		if names, err = notStopped(); err != nil || len(names) == 0 {
			return err
		}
	}
	return stopAll(ctx, 0, true)
}

// AllStopped is true when every instance is Stopped (an empty project counts).
func AllStopped(insts []Instance) bool {
	for _, in := range insts {
		if in.Status != StatusStopped {
			return false
		}
	}
	return true
}

// Running returns the names of instances in a Running state.
func Running(insts []Instance) []string {
	var out []string
	for _, in := range insts {
		if in.Status == StatusRunning {
			out = append(out, in.Name)
		}
	}
	return out
}

// payloadPrefix is liblxc's payload cgroup name prefix for this member's
// project: "lxc.payload.<project>_<instance>".
func (d *Driver) payloadPrefix(member string) string {
	return "lxc.payload." + d.Project(member) + "_"
}

// LocalRunning lists this member's project containers that still have live
// processes, read straight from cgroupfs. It needs no Incus daemon and no
// cluster quorum, which makes it the ground truth for the self-fence.
func (d *Driver) LocalRunning(member string) ([]string, error) {
	ents, err := os.ReadDir(d.o.CgroupRoot)
	if err != nil {
		return nil, err
	}
	pfx := d.payloadPrefix(member)
	var out []string
	for _, e := range ents {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), pfx) {
			continue
		}
		populated, err := cgroupPopulated(filepath.Join(d.o.CgroupRoot, e.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue // removed between ReadDir and read: it is gone
		}
		if err != nil {
			return nil, err
		}
		if populated {
			out = append(out, strings.TrimPrefix(e.Name(), pfx))
		}
	}
	slices.Sort(out)
	return out, nil
}

func cgroupPopulated(dir string) (bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, "cgroup.events"))
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == "populated" {
			return f[1] == "1", nil
		}
	}
	return false, fmt.Errorf("%s/cgroup.events: no populated field", dir)
}

// Kill SIGKILLs every process of this member's project containers via
// cgroup.kill and waits until the cgroups are empty or ctx ends.
func (d *Driver) Kill(ctx context.Context, member string) error {
	names, err := d.LocalRunning(member)
	if err != nil {
		return err
	}
	pfx := d.payloadPrefix(member)
	var errs []error
	killed := map[int]bool{}
	for _, n := range names {
		dir := filepath.Join(d.o.CgroupRoot, pfx+n)
		for _, pid := range cgroupPIDs(dir) {
			killed[pid] = true
		}
		if err := os.WriteFile(filepath.Join(dir, "cgroup.kill"), []byte("1"), 0); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	for {
		left, err := d.LocalRunning(member)
		if err == nil && len(left) == 0 {
			d.killProxies(killed)
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(append(errs, fmt.Errorf("still populated after cgroup.kill: %v: %w", left, ctx.Err()))...)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// IPv4 returns the first global IPv4 address of instance name in member's
// project.
func (d *Driver) IPv4(ctx context.Context, member, name string) (string, error) {
	addrs, err := d.o.API.Addresses(ctx, d.Project(member), name)
	if err != nil {
		return "", err
	}
	ifs := make([]string, 0, len(addrs))
	for n := range addrs {
		ifs = append(ifs, n)
	}
	slices.Sort(ifs)
	for _, n := range ifs {
		if n == "lo" {
			continue
		}
		for _, a := range addrs[n] {
			if a.Family == "inet" && a.Scope == "global" {
				return a.Address, nil
			}
		}
	}
	return "", fmt.Errorf("instance %s has no global IPv4 address", name)
}

// CheckCgroup2 verifies the cgroup root is a cgroup2 mount (cgroup.kill and
// cgroup.events exist only there).
func (d *Driver) CheckCgroup2() error {
	var st unix.Statfs_t
	if err := unix.Statfs(d.o.CgroupRoot, &st); err != nil {
		return err
	}
	if st.Type != unix.CGROUP2_SUPER_MAGIC {
		return fmt.Errorf("%s is not a cgroup2 mount", d.o.CgroupRoot)
	}
	return nil
}

// StartInstances starts the named instances of member's project.
func (d *Driver) StartInstances(ctx context.Context, member string, names []string) error {
	var errs []error
	for _, n := range names {
		if err := d.o.API.Start(ctx, d.Project(member), n); err != nil {
			errs = append(errs, fmt.Errorf("start %s/%s: %w", d.Project(member), n, err))
		}
	}
	return errors.Join(errs...)
}

// ServerVersion is the Incus server's version.
func (d *Driver) ServerVersion(ctx context.Context) (string, error) {
	return d.o.API.ServerVersion(ctx)
}

// cgroupPIDs lists the processes of a cgroup and its descendants.
func cgroupPIDs(dir string) []int {
	var pids []int
	_ = filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err != nil || !e.IsDir() {
			return nil
		}
		b, err := os.ReadFile(filepath.Join(p, "cgroup.procs"))
		if err != nil {
			return nil
		}
		for _, f := range strings.Fields(string(b)) {
			if pid, err := strconv.Atoi(f); err == nil {
				pids = append(pids, pid)
			}
		}
		return nil
	})
	return pids
}

// killProxies kills the proxy devices of containers killed outside Incus.
// A proxy device is an `incusd forkproxy -- <listen pid> <fd> <addr>
// <connect pid> <fd> <addr> ...` process on the host, outside the
// container's cgroup: cgroup.kill does not reach it, and without incusd's
// stop hook (which a member cut off the cluster database cannot run) it keeps
// the published port bound, forwarding to a dead process, until killed. That
// broke routing to the port and the next start on the member ("bind: address
// already in use", 2026-10-14). Best effort: the workload is already stopped,
// so nothing here can fail the fence.
func (d *Driver) killProxies(killed map[int]bool) {
	if len(killed) == 0 {
		return
	}
	ents, err := os.ReadDir(d.o.ProcRoot)
	if err != nil {
		return
	}
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join(d.o.ProcRoot, e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		argv := strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
		if len(argv) < 7 || argv[1] != "forkproxy" || argv[2] != "--" {
			continue
		}
		listen, _ := strconv.Atoi(argv[3])
		connect, _ := strconv.Atoi(argv[6])
		if killed[listen] || killed[connect] {
			_ = d.o.KillPID(pid)
		}
	}
}
