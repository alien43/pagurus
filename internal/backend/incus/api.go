package incus

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	incusclient "github.com/lxc/incus/v7/client"
	"github.com/lxc/incus/v7/shared/api"
)

// API is the slice of the Incus REST API pagurus uses. Client implements it
// over github.com/lxc/incus/v7/client; tests use fencetest.FakeIncus.
// Every call honours ctx: the fences' budgets depend on it.
type API interface {
	// Server returns this server's cluster member name, whether it is
	// clustered, and its architectures.
	Server(ctx context.Context) (name string, clustered bool, archs []string, err error)
	// ServerVersion is the Incus server's version (e.g. "7.5.1").
	ServerVersion(ctx context.Context) (string, error)
	ProjectConfig(ctx context.Context, project string) (map[string]string, error)
	ClusterGroupMembers(ctx context.Context, group string) ([]string, error)
	// MemberStatus is the cluster member's status ("Online", "Offline", ...).
	MemberStatus(ctx context.Context, member string) (string, error)
	Instances(ctx context.Context, project string) ([]Instance, error)
	// Addresses lists an instance's addresses per interface.
	Addresses(ctx context.Context, project, name string) (map[string][]Address, error)
	SetConfig(ctx context.Context, project, name, key, value string) error
	// Stop stops an instance; timeout > 0 tries a clean shutdown first.
	Stop(ctx context.Context, project, name string, timeout time.Duration, force bool) error
	Start(ctx context.Context, project, name string) error

	// Provisioning (pagurus provision). Missing things are reported with
	// IsNotFound.
	CreateClusterGroup(ctx context.Context, group string, members []string) error
	SetClusterGroupMembers(ctx context.Context, group string, members []string) error
	CreateProject(ctx context.Context, project string, config map[string]string) error
	// SetProjectConfig sets one key of an existing project; "" unsets it.
	SetProjectConfig(ctx context.Context, project, key, value string) error

	// Custom volumes on a member-local pool (replication): member is where
	// the volume lives. A missing pool or volume is reported with IsNotFound.
	Volumes(ctx context.Context, project, pool, member string) ([]string, error)
	CreateVolume(ctx context.Context, project, pool, member, name string) error
	VolumeSnapshots(ctx context.Context, project, pool, member, vol string) ([]string, error)
	CreateVolumeSnapshot(ctx context.Context, project, pool, member, vol, snap string) error
	DeleteVolumeSnapshot(ctx context.Context, project, pool, member, vol, snap string) error
	// VolumeConfig / SetVolumeConfig read and set a custom volume's config
	// (volatile keys included, e.g. volatile.idmap.last).
	VolumeConfig(ctx context.Context, project, pool, member, vol string) (map[string]string, error)
	SetVolumeConfig(ctx context.Context, project, pool, member, vol, key, value string) error
}

// Address is one address of an instance interface.
type Address struct {
	Family, Address, Scope string
}

// ClientOptions says how to reach Incus.
type ClientOptions struct {
	// Socket is the local unix socket; empty means the default
	// (INCUS_SOCKET, /var/lib/incus/unix.socket, /run/incus/unix.socket).
	Socket string
	// URL, if set, is a remote https:// Incus, reached with the client
	// certificate below and trusting ServerCert (PEM file; empty: system CAs).
	URL                               string
	ClientCert, ClientKey, ServerCert string
}

// Client is API over the Incus Go client. It connects on first use, so a
// driver can be built where Incus is not (yet) reachable.
type Client struct {
	o    ClientOptions
	mu   sync.Mutex
	conn incusclient.InstanceServer
}

func NewClient(o ClientOptions) *Client { return &Client{o: o} }

func (c *Client) server(ctx context.Context) (incusclient.InstanceServer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		args := &incusclient.ConnectionArgs{SkipGetServer: true, UserAgent: "pagurus"}
		var (
			conn incusclient.InstanceServer
			err  error
		)
		if c.o.URL != "" {
			read := func(p string) (string, error) {
				if p == "" {
					return "", nil
				}
				b, err := os.ReadFile(p)
				return string(b), err
			}
			if args.TLSClientCert, err = read(c.o.ClientCert); err != nil {
				return nil, err
			}
			if args.TLSClientKey, err = read(c.o.ClientKey); err != nil {
				return nil, err
			}
			if args.TLSServerCert, err = read(c.o.ServerCert); err != nil {
				return nil, err
			}
			conn, err = incusclient.ConnectIncusWithContext(ctx, c.o.URL, args)
		} else {
			conn, err = incusclient.ConnectIncusUnixWithContext(ctx, c.o.Socket, args)
		}
		if err != nil {
			return nil, fmt.Errorf("connect to Incus: %w", err)
		}
		c.conn = conn
	}
	return withContext(c.conn, ctx), nil
}

// withContext binds ctx to every request (the concrete client has
// WithContext; the interface does not declare it).
func withContext(s incusclient.InstanceServer, ctx context.Context) incusclient.InstanceServer {
	if w, ok := s.(interface {
		WithContext(context.Context) incusclient.InstanceServer
	}); ok {
		return w.WithContext(ctx)
	}
	return s
}

func (c *Client) project(ctx context.Context, project string) (incusclient.InstanceServer, error) {
	s, err := c.server(ctx)
	if err != nil {
		return nil, err
	}
	return s.UseProject(project), nil
}

func (c *Client) Server(ctx context.Context) (string, bool, []string, error) {
	s, err := c.server(ctx)
	if err != nil {
		return "", false, nil, err
	}
	srv, _, err := s.GetServer()
	if err != nil {
		return "", false, nil, err
	}
	return srv.Environment.ServerName, srv.Environment.ServerClustered, srv.Environment.Architectures, nil
}

func (c *Client) ServerVersion(ctx context.Context) (string, error) {
	s, err := c.server(ctx)
	if err != nil {
		return "", err
	}
	srv, _, err := s.GetServer()
	if err != nil {
		return "", err
	}
	return srv.Environment.ServerVersion, nil
}

func (c *Client) ProjectConfig(ctx context.Context, project string) (map[string]string, error) {
	s, err := c.server(ctx)
	if err != nil {
		return nil, err
	}
	p, _, err := s.GetProject(project)
	if err != nil {
		return nil, err
	}
	return p.Config, nil
}

func (c *Client) ClusterGroupMembers(ctx context.Context, group string) ([]string, error) {
	s, err := c.server(ctx)
	if err != nil {
		return nil, err
	}
	g, _, err := s.GetClusterGroup(group)
	if err != nil {
		return nil, err
	}
	return g.Members, nil
}

func (c *Client) MemberStatus(ctx context.Context, member string) (string, error) {
	s, err := c.server(ctx)
	if err != nil {
		return "", err
	}
	m, _, err := s.GetClusterMember(member)
	if err != nil {
		return "", err
	}
	return m.Status, nil
}

func (c *Client) Instances(ctx context.Context, project string) ([]Instance, error) {
	s, err := c.project(ctx, project)
	if err != nil {
		return nil, err
	}
	insts, err := s.GetInstances(api.InstanceTypeAny)
	if err != nil {
		return nil, err
	}
	out := make([]Instance, 0, len(insts))
	for _, in := range insts {
		out = append(out, Instance{Name: in.Name, Status: in.Status, Location: in.Location, Type: in.Type, Expanded: in.ExpandedConfig})
	}
	return out, nil
}

func (c *Client) Addresses(ctx context.Context, project, name string) (map[string][]Address, error) {
	s, err := c.project(ctx, project)
	if err != nil {
		return nil, err
	}
	st, _, err := s.GetInstanceState(name)
	if err != nil {
		return nil, err
	}
	out := map[string][]Address{}
	for ifname, n := range st.Network {
		for _, a := range n.Addresses {
			out[ifname] = append(out[ifname], Address{Family: a.Family, Address: a.Address, Scope: a.Scope})
		}
	}
	return out, nil
}

func (c *Client) SetConfig(ctx context.Context, project, name, key, value string) error {
	s, err := c.project(ctx, project)
	if err != nil {
		return err
	}
	in, etag, err := s.GetInstance(name)
	if err != nil {
		return err
	}
	if in.Config[key] == value {
		return nil
	}
	put := in.Writable()
	if put.Config == nil {
		put.Config = map[string]string{}
	}
	put.Config[key] = value
	op, err := s.UpdateInstance(name, put, etag) // ETag: refuses a concurrent edit
	if err != nil {
		return err
	}
	return op.WaitContext(ctx)
}

func (c *Client) state(ctx context.Context, project, name string, put api.InstanceStatePut) error {
	s, err := c.project(ctx, project)
	if err != nil {
		return err
	}
	op, err := s.UpdateInstanceState(name, put, "")
	if err != nil {
		return err
	}
	return op.WaitContext(ctx)
}

func (c *Client) Stop(ctx context.Context, project, name string, timeout time.Duration, force bool) error {
	return c.state(ctx, project, name, api.InstanceStatePut{Action: "stop", Timeout: int(timeout.Seconds()), Force: force})
}

func (c *Client) Start(ctx context.Context, project, name string) error {
	return c.state(ctx, project, name, api.InstanceStatePut{Action: "start"})
}

// IsNotFound reports an Incus 404.
func IsNotFound(err error) bool {
	return api.StatusErrorCheck(err, http.StatusNotFound)
}

var _ API = (*Client)(nil)

func (c *Client) CreateClusterGroup(ctx context.Context, group string, members []string) error {
	s, err := c.server(ctx)
	if err != nil {
		return err
	}
	return s.CreateClusterGroup(api.ClusterGroupsPost{Name: group, ClusterGroupPut: api.ClusterGroupPut{
		Description: "pagurus: pins projects to member " + strings.Join(members, ","), Members: members}})
}

func (c *Client) SetClusterGroupMembers(ctx context.Context, group string, members []string) error {
	s, err := c.server(ctx)
	if err != nil {
		return err
	}
	g, etag, err := s.GetClusterGroup(group)
	if err != nil {
		return err
	}
	put := g.Writable()
	put.Members = members
	return s.UpdateClusterGroup(group, put, etag)
}

func (c *Client) CreateProject(ctx context.Context, project string, config map[string]string) error {
	s, err := c.server(ctx)
	if err != nil {
		return err
	}
	return s.CreateProject(api.ProjectsPost{Name: project, ProjectPut: api.ProjectPut{Config: config, Description: "pagurus"}})
}

func (c *Client) SetProjectConfig(ctx context.Context, project, key, value string) error {
	s, err := c.server(ctx)
	if err != nil {
		return err
	}
	p, etag, err := s.GetProject(project)
	if err != nil {
		return err
	}
	put := p.Writable()
	if put.Config == nil {
		put.Config = map[string]string{}
	}
	if value == "" {
		delete(put.Config, key)
	} else {
		put.Config[key] = value
	}
	return s.UpdateProject(project, put, etag)
}

// volumes is a client for project on member (member-local pools need the
// target).
func (c *Client) volumes(ctx context.Context, project, member string) (incusclient.InstanceServer, error) {
	s, err := c.project(ctx, project)
	if err != nil {
		return nil, err
	}
	return s.UseTarget(member), nil
}

func (c *Client) Volumes(ctx context.Context, project, pool, member string) ([]string, error) {
	s, err := c.volumes(ctx, project, member)
	if err != nil {
		return nil, err
	}
	vs, err := s.GetStoragePoolVolumes(pool)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, v := range vs {
		if v.Type == "custom" && (v.Location == "" || v.Location == member) {
			out = append(out, v.Name)
		}
	}
	return out, nil
}

func (c *Client) CreateVolume(ctx context.Context, project, pool, member, name string) error {
	s, err := c.volumes(ctx, project, member)
	if err != nil {
		return err
	}
	return s.CreateStoragePoolVolume(pool, api.StorageVolumesPost{Name: name, Type: "custom",
		StorageVolumePut: api.StorageVolumePut{Description: "pagurus: replicated volume"}})
}

func (c *Client) VolumeSnapshots(ctx context.Context, project, pool, member, vol string) ([]string, error) {
	s, err := c.volumes(ctx, project, member)
	if err != nil {
		return nil, err
	}
	return s.GetStoragePoolVolumeSnapshotNames(pool, "custom", vol)
}

func (c *Client) CreateVolumeSnapshot(ctx context.Context, project, pool, member, vol, snap string) error {
	s, err := c.volumes(ctx, project, member)
	if err != nil {
		return err
	}
	op, err := s.CreateStoragePoolVolumeSnapshot(pool, "custom", vol, api.StorageVolumeSnapshotsPost{Name: snap})
	if err != nil {
		return err
	}
	return op.WaitContext(ctx)
}

func (c *Client) VolumeConfig(ctx context.Context, project, pool, member, vol string) (map[string]string, error) {
	s, err := c.volumes(ctx, project, member)
	if err != nil {
		return nil, err
	}
	v, _, err := s.GetStoragePoolVolume(pool, "custom", vol)
	if err != nil {
		return nil, err
	}
	return v.Config, nil
}

func (c *Client) SetVolumeConfig(ctx context.Context, project, pool, member, vol, key, value string) error {
	s, err := c.volumes(ctx, project, member)
	if err != nil {
		return err
	}
	v, etag, err := s.GetStoragePoolVolume(pool, "custom", vol)
	if err != nil {
		return err
	}
	if v.Config[key] == value {
		return nil
	}
	put := v.Writable()
	if put.Config == nil {
		put.Config = map[string]string{}
	}
	put.Config[key] = value
	return s.UpdateStoragePoolVolume(pool, "custom", vol, put, etag) // ETag: refuses a concurrent edit
}

func (c *Client) DeleteVolumeSnapshot(ctx context.Context, project, pool, member, vol, snap string) error {
	s, err := c.volumes(ctx, project, member)
	if err != nil {
		return err
	}
	op, err := s.DeleteStoragePoolVolumeSnapshot(pool, "custom", vol, snap)
	if err != nil {
		return err
	}
	return op.WaitContext(ctx)
}
