// Package incus is the runtime driver: it runs the stack with incus-compose in
// this member's project, and stops (fences) a member's project through the
// Incus cluster API or, locally, through cgroup.kill.
//
// Everything shells out to the `incus` and `incus-compose` binaries with a
// context deadline and process-group kill, so a hung daemon can delay the
// agent by at most the caller's budget.
package incus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
	// IncusBin overrides the binary (tests).
	IncusBin string
}

type Driver struct {
	o Options
}

func New(o Options) *Driver {
	if o.CgroupRoot == "" {
		o.CgroupRoot = "/sys/fs/cgroup"
	}
	if o.IncusBin == "" {
		o.IncusBin = "incus"
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

func (d *Driver) query(ctx context.Context, path string, v any) error {
	out, err := Run(ctx, d.o.IncusBin, "query", path)
	if err != nil {
		return err
	}
	return json.Unmarshal(out, v)
}

// Identity returns this host's cluster member name and kernel architecture.
func (d *Driver) Identity(ctx context.Context) (member, arch string, err error) {
	var srv struct {
		Environment struct {
			ServerName      string   `json:"server_name"`
			ServerClustered bool     `json:"server_clustered"`
			Architectures   []string `json:"architectures"`
		} `json:"environment"`
	}
	if err := d.query(ctx, "/1.0", &srv); err != nil {
		return "", "", err
	}
	if !srv.Environment.ServerClustered {
		return "", "", errors.New("local Incus is not clustered")
	}
	if len(srv.Environment.Architectures) == 0 {
		return "", "", errors.New("local Incus reports no architecture")
	}
	return srv.Environment.ServerName, srv.Environment.Architectures[0], nil
}

// CheckProject verifies project(member) is pinned to exactly member: a
// restricted.cluster.groups whose groups contain member and nobody else.
// Without that pin, `incus-compose up` could place the workload on another
// member and the per-member fencing below would fence the wrong host.
func (d *Driver) CheckProject(ctx context.Context, member string) error {
	p := d.Project(member)
	var proj struct {
		Config map[string]string `json:"config"`
	}
	if err := d.query(ctx, "/1.0/projects/"+url.PathEscape(p), &proj); err != nil {
		return fmt.Errorf("project %s: %w", p, err)
	}
	groups := strings.Split(proj.Config["restricted.cluster.groups"], ",")
	if len(groups) == 0 || groups[0] == "" {
		return fmt.Errorf("project %s: restricted.cluster.groups is unset; it must pin the project to %s", p, member)
	}
	for _, g := range groups {
		var grp struct {
			Members []string `json:"members"`
		}
		if err := d.query(ctx, "/1.0/cluster/groups/"+url.PathEscape(strings.TrimSpace(g)), &grp); err != nil {
			return fmt.Errorf("project %s: cluster group %s: %w", p, g, err)
		}
		if len(grp.Members) != 1 || grp.Members[0] != member {
			return fmt.Errorf("project %s: cluster group %s has members %v, want exactly [%s]", p, g, grp.Members, member)
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
	var m struct {
		Status string `json:"status"`
	}
	if err := d.query(ctx, "/1.0/cluster/members/"+url.PathEscape(member), &m); err != nil {
		return false, err
	}
	return m.Status == "Online", nil
}

// List returns the instances of member's project, as the cluster sees them.
func (d *Driver) List(ctx context.Context, member string) ([]Instance, error) {
	var out []Instance
	err := d.query(ctx, "/1.0/instances?recursion=1&project="+url.QueryEscape(d.Project(member)), &out)
	return out, err
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
		if _, err := Run(ctx, d.o.IncusBin, "config", "set", in.Name, "--project", d.Project(member), "boot.autostart=false"); err != nil {
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
	base := []string{"stop", "--project", d.Project(member)}
	if graceful > 0 {
		gctx, cancel := context.WithTimeout(ctx, graceful+2*time.Second)
		_, gerr := Run(gctx, d.o.IncusBin, append(append(base, fmt.Sprintf("--timeout=%d", int(graceful.Seconds()))), names...)...)
		cancel()
		if gerr == nil {
			return nil
		}
		// Some may have stopped; `incus stop` errors on an already-stopped one.
		if names, err = notStopped(); err != nil || len(names) == 0 {
			return err
		}
	}
	_, err = Run(ctx, d.o.IncusBin, append(append(base, "--force"), names...)...)
	return err
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
	for _, n := range names {
		f := filepath.Join(d.o.CgroupRoot, pfx+n, "cgroup.kill")
		if err := os.WriteFile(f, []byte("1"), 0); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	for {
		left, err := d.LocalRunning(member)
		if err == nil && len(left) == 0 {
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
	var st struct {
		Network map[string]struct {
			Addresses []struct {
				Family  string `json:"family"`
				Address string `json:"address"`
				Scope   string `json:"scope"`
			} `json:"addresses"`
		} `json:"network"`
	}
	p := fmt.Sprintf("/1.0/instances/%s/state?project=%s", url.PathEscape(name), url.QueryEscape(d.Project(member)))
	if err := d.query(ctx, p, &st); err != nil {
		return "", err
	}
	ifs := make([]string, 0, len(st.Network))
	for n := range st.Network {
		ifs = append(ifs, n)
	}
	slices.Sort(ifs)
	for _, n := range ifs {
		if n == "lo" {
			continue
		}
		for _, a := range st.Network[n].Addresses {
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
