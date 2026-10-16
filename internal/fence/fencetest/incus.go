package fencetest

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alien43/pagurus/internal/incus"
	"github.com/lxc/incus/v7/shared/api"
)

// FakeIncus is an in-memory incus.API: members, cluster groups, projects and
// instances, with knobs for the failure modes fences care about.
type FakeIncus struct {
	mu sync.Mutex

	ServerName string
	Archs      []string
	Members    map[string]string            // member -> status ("Online", ...)
	Groups     map[string][]string          // cluster group -> members
	Projects   map[string]map[string]string // project -> config
	Insts      map[string][]incus.Instance  // project -> instances
	Addrs      map[string]map[string][]incus.Address
	// Pools that exist; Vols holds custom volumes by "<project>/<pool>/<member>/<volume>",
	// each with its snapshot names.
	Pools map[string]bool
	Vols  map[string][]string

	// Hang makes every call block until its context ends.
	Hang bool
	// StopNoop makes Stop succeed without stopping anything.
	StopNoop bool
	// Err, if set, is returned by every call (an API error, no quorum, ...).
	Err error
	// Forbidden projects behave as for an Incus client restricted to other
	// projects: ProjectConfig is refused (403) and Instances is an empty
	// list, not an error, which is what the real server does.
	Forbidden map[string]bool
	// OnStop / OnStart run after an instance changes state (e.g. to update a
	// FakeCgroupFS the way the kernel would).
	OnStop, OnStart func(project, name string)

	mutations int
}

// NewFakeIncus is an online cluster of the given members, each in its own
// single-member group host-<member>.
func NewFakeIncus(members ...string) *FakeIncus {
	f := &FakeIncus{ServerName: members[0], Archs: []string{"x86_64"}, Members: map[string]string{}, Groups: map[string][]string{},
		Projects: map[string]map[string]string{}, Insts: map[string][]incus.Instance{}, Addrs: map[string]map[string][]incus.Address{},
		Pools: map[string]bool{"default": true}, Vols: map[string][]string{}}
	for _, m := range members {
		f.Members[m] = "Online"
		f.Groups["host-"+m] = []string{m}
	}
	return f
}

// AddInstance puts an instance into project (creating the project, pinned to
// host-<location>, if needed).
func (f *FakeIncus) AddInstance(project string, in incus.Instance) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.Projects[project]; !ok {
		f.Projects[project] = map[string]string{"restricted.cluster.groups": "host-" + in.Location}
	}
	if in.Expanded == nil {
		in.Expanded = map[string]string{}
	}
	f.Insts[project] = append(f.Insts[project], in)
}

// SetHang turns hanging on or off (safe while calls are in flight).
func (f *FakeIncus) SetHang(on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Hang = on
}

// SetStatus sets an instance's status directly.
func (f *FakeIncus) SetStatus(project, name, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if in := f.find(project, name); in != nil {
		in.Status = status
	}
}

// Mutations counts the calls that change state (Stop, Start, SetConfig).
func (f *FakeIncus) Mutations() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mutations
}

func (f *FakeIncus) find(project, name string) *incus.Instance {
	for i := range f.Insts[project] {
		if f.Insts[project][i].Name == name {
			return &f.Insts[project][i]
		}
	}
	return nil
}

// gate applies Hang and Err.
func (f *FakeIncus) gate(ctx context.Context) error {
	f.mu.Lock()
	hang, err := f.Hang, f.Err
	f.mu.Unlock()
	if hang {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}

func (f *FakeIncus) Server(ctx context.Context) (string, bool, []string, error) {
	if err := f.gate(ctx); err != nil {
		return "", false, nil, err
	}
	return f.ServerName, true, f.Archs, nil
}

func (f *FakeIncus) ServerVersion(ctx context.Context) (string, error) {
	if err := f.gate(ctx); err != nil {
		return "", err
	}
	return "7.5.1", nil
}

func (f *FakeIncus) ProjectConfig(ctx context.Context, project string) (map[string]string, error) {
	if err := f.gate(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Forbidden[project] {
		return nil, api.StatusErrorf(http.StatusForbidden, "User does not have permission for project %q", project)
	}
	cfg, ok := f.Projects[project]
	if !ok {
		return nil, notFound("project " + project)
	}
	return cfg, nil
}

func (f *FakeIncus) ClusterGroupMembers(ctx context.Context, group string) ([]string, error) {
	if err := f.gate(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.Groups[group]
	if !ok {
		return nil, notFound("cluster group " + group)
	}
	return m, nil
}

func (f *FakeIncus) MemberStatus(ctx context.Context, member string) (string, error) {
	if err := f.gate(ctx); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.Members[member]
	if !ok {
		return "", fmt.Errorf("member %q not found", member)
	}
	return st, nil
}

func (f *FakeIncus) Instances(ctx context.Context, project string) ([]incus.Instance, error) {
	if err := f.gate(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Forbidden[project] {
		return nil, nil // the real server's 200 OK with an empty list
	}
	return append([]incus.Instance(nil), f.Insts[project]...), nil
}

func (f *FakeIncus) Addresses(ctx context.Context, project, name string) (map[string][]incus.Address, error) {
	if err := f.gate(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Addrs[project+"/"+name], nil
}

func (f *FakeIncus) SetConfig(ctx context.Context, project, name, key, value string) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	in := f.find(project, name)
	if in == nil {
		return fmt.Errorf("instance %s/%s not found", project, name)
	}
	f.mutations++
	in.Expanded[key] = value
	return nil
}

func (f *FakeIncus) Stop(ctx context.Context, project, name string, _ time.Duration, _ bool) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	in := f.find(project, name)
	if in == nil {
		f.mu.Unlock()
		return fmt.Errorf("instance %s/%s not found", project, name)
	}
	f.mutations++
	if in.Status == incus.StatusStopped {
		f.mu.Unlock()
		return fmt.Errorf("the instance is already stopped")
	}
	if f.StopNoop {
		f.mu.Unlock()
		return nil
	}
	in.Status = incus.StatusStopped
	hook := f.OnStop
	f.mu.Unlock()
	if hook != nil {
		hook(project, name)
	}
	return nil
}

func (f *FakeIncus) Start(ctx context.Context, project, name string) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	in := f.find(project, name)
	if in == nil {
		f.mu.Unlock()
		return fmt.Errorf("instance %s/%s not found", project, name)
	}
	f.mutations++
	in.Status = incus.StatusRunning
	hook := f.OnStart
	f.mu.Unlock()
	if hook != nil {
		hook(project, name)
	}
	return nil
}

// notFound mimics an Incus 404, so incus.IsNotFound recognises it.
func notFound(what string) error { return api.StatusErrorf(http.StatusNotFound, "%s not found", what) }

func (f *FakeIncus) CreateClusterGroup(ctx context.Context, group string, members []string) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.Groups[group]; ok {
		return fmt.Errorf("cluster group %q already exists", group)
	}
	f.mutations++
	f.Groups[group] = append([]string(nil), members...)
	return nil
}

func (f *FakeIncus) SetClusterGroupMembers(ctx context.Context, group string, members []string) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.Groups[group]; !ok {
		return notFound("cluster group " + group)
	}
	f.mutations++
	f.Groups[group] = append([]string(nil), members...)
	return nil
}

func (f *FakeIncus) SetProjectConfig(ctx context.Context, project, key, value string) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.Projects[project]
	if !ok {
		return notFound("project " + project)
	}
	f.mutations++
	if value == "" {
		delete(c, key)
	} else {
		c[key] = value
	}
	return nil
}

func (f *FakeIncus) CreateProject(ctx context.Context, project string, config map[string]string) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.Projects[project]; ok {
		return fmt.Errorf("project %q already exists", project)
	}
	f.mutations++
	cfg := map[string]string{}
	for k, v := range config {
		cfg[k] = v
	}
	f.Projects[project] = cfg
	return nil
}

func volKey(project, pool, member, vol string) string {
	return project + "/" + pool + "/" + member + "/" + vol
}

func (f *FakeIncus) Volumes(ctx context.Context, project, pool, member string) ([]string, error) {
	if err := f.gate(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.Pools[pool] {
		return nil, notFound("storage pool " + pool)
	}
	if _, ok := f.Projects[project]; !ok {
		return nil, notFound("project " + project)
	}
	pfx := volKey(project, pool, member, "")
	var out []string
	for k := range f.Vols {
		if strings.HasPrefix(k, pfx) {
			out = append(out, strings.TrimPrefix(k, pfx))
		}
	}
	slices.Sort(out)
	return out, nil
}

func (f *FakeIncus) CreateVolume(ctx context.Context, project, pool, member, name string) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.Pools[pool] {
		return notFound("storage pool " + pool)
	}
	k := volKey(project, pool, member, name)
	if _, ok := f.Vols[k]; ok {
		return fmt.Errorf("volume %s already exists", name)
	}
	f.mutations++
	f.Vols[k] = []string{}
	return nil
}

func (f *FakeIncus) volume(project, pool, member, vol string) ([]string, string, error) {
	k := volKey(project, pool, member, vol)
	snaps, ok := f.Vols[k]
	if !ok {
		return nil, k, notFound("storage volume " + vol)
	}
	return snaps, k, nil
}

func (f *FakeIncus) VolumeSnapshots(ctx context.Context, project, pool, member, vol string) ([]string, error) {
	if err := f.gate(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	snaps, _, err := f.volume(project, pool, member, vol)
	return slices.Clone(snaps), err
}

func (f *FakeIncus) CreateVolumeSnapshot(ctx context.Context, project, pool, member, vol, snap string) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	snaps, k, err := f.volume(project, pool, member, vol)
	if err != nil {
		return err
	}
	f.mutations++
	f.Vols[k] = append(snaps, snap)
	return nil
}

func (f *FakeIncus) DeleteVolumeSnapshot(ctx context.Context, project, pool, member, vol, snap string) error {
	if err := f.gate(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	snaps, k, err := f.volume(project, pool, member, vol)
	if err != nil {
		return err
	}
	if !slices.Contains(snaps, snap) {
		return notFound("snapshot " + snap)
	}
	f.mutations++
	f.Vols[k] = slices.DeleteFunc(snaps, func(s string) bool { return s == snap })
	return nil
}

var _ incus.API = (*FakeIncus)(nil)
