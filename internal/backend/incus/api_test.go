package incus_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/backend/incus"
)

// fakeIncusREST is a minimal Incus REST server on a unix socket: enough of
// /1.0 for Client (api.go) to be exercised through the real Incus Go client.
// Operations come back already finished, so the client never opens the events
// websocket.
type fakeIncusREST struct {
	mu        sync.Mutex
	projects  map[string]map[string]string
	groups    map[string][]string
	insts     map[string]map[string]*fakeInst // project -> name -> instance
	stateCall []string                        // "project/name action force"
	created   []string
	vols      map[string]*fakeVol // "project/pool/name"
	targets   []string            // ?target= of every storage call
}

type fakeVol struct {
	Location string
	Snaps    []string
}

type fakeInst struct {
	Status   string
	Location string
	Type     string
	Config   map[string]string
}

func ok(w http.ResponseWriter, md any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "sync", "status": "Success", "status_code": 200, "metadata": md})
}

func opDone(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "async", "status": "Operation created", "status_code": 100,
		"operation": "/1.0/operations/1",
		"metadata":  map[string]any{"id": "1", "class": "task", "status": "Success", "status_code": 200}})
}

func fail(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": msg, "error_code": code})
}

func (f *fakeIncusREST) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := strings.TrimPrefix(r.URL.Path, "/1.0")
	proj := r.URL.Query().Get("project")
	if proj == "" {
		proj = "default"
	}
	parts := strings.Split(strings.Trim(p, "/"), "/")
	switch {
	case p == "" || p == "/":
		ok(w, map[string]any{"api_extensions": []string{}, "environment": map[string]any{
			"server_name": "asus", "server_clustered": true, "architectures": []string{"x86_64"}, "server_version": "7.5.1"}})
	case parts[0] == "projects" && len(parts) == 1 && r.Method == http.MethodPost:
		var body struct {
			Name   string            `json:"name"`
			Config map[string]string `json:"config"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.projects[body.Name] = body.Config
		f.created = append(f.created, "project "+body.Name)
		ok(w, nil)
	case parts[0] == "projects" && len(parts) == 2:
		cfg, found := f.projects[parts[1]]
		if !found {
			fail(w, 404, "Project not found")
			return
		}
		ok(w, map[string]any{"name": parts[1], "config": cfg})
	case parts[0] == "cluster" && len(parts) == 2 && parts[1] == "groups" && r.Method == http.MethodPost:
		var body struct {
			Name    string   `json:"name"`
			Members []string `json:"members"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.groups[body.Name] = body.Members
		f.created = append(f.created, "group "+body.Name)
		ok(w, nil)
	case parts[0] == "cluster" && len(parts) == 3 && parts[1] == "groups":
		g := parts[2]
		if r.Method == http.MethodPut {
			var body struct {
				Members []string `json:"members"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.groups[g] = body.Members
			ok(w, nil)
			return
		}
		m, found := f.groups[g]
		if !found {
			fail(w, 404, "Cluster group not found")
			return
		}
		ok(w, map[string]any{"name": g, "members": m})
	case parts[0] == "cluster" && len(parts) == 3 && parts[1] == "members":
		status := "Online"
		if parts[2] == "dell" {
			status = "Offline"
		}
		ok(w, map[string]any{"server_name": parts[2], "status": status})
	case parts[0] == "instances" && len(parts) == 1:
		var list []map[string]any
		for name, in := range f.insts[proj] {
			list = append(list, map[string]any{"name": name, "status": in.Status, "location": in.Location, "type": in.Type,
				"config": in.Config, "expanded_config": in.Config, "project": proj})
		}
		ok(w, list)
	case parts[0] == "instances" && len(parts) >= 2:
		in, found := f.insts[proj][parts[1]]
		if !found {
			fail(w, 404, "Instance not found")
			return
		}
		switch {
		case len(parts) == 3 && parts[2] == "state" && r.Method == http.MethodPut:
			var body struct {
				Action string `json:"action"`
				Force  bool   `json:"force"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.stateCall = append(f.stateCall, proj+"/"+parts[1]+" "+body.Action+map[bool]string{true: " force", false: ""}[body.Force])
			if body.Action == "start" {
				in.Status = "Running"
			} else {
				in.Status = "Stopped"
			}
			opDone(w)
		case len(parts) == 3 && parts[2] == "state":
			ok(w, map[string]any{"status": in.Status, "network": map[string]any{
				"eth0": map[string]any{"addresses": []map[string]string{{"family": "inet", "address": "10.101.92.7", "netmask": "24", "scope": "global"}}}}})
		case len(parts) == 2 && r.Method == http.MethodPut:
			var body struct {
				Config map[string]string `json:"config"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			in.Config = body.Config
			opDone(w)
		case len(parts) == 2:
			w.Header().Set("ETag", "e1")
			ok(w, map[string]any{"name": parts[1], "status": in.Status, "location": in.Location, "type": in.Type, "config": in.Config})
		default:
			fail(w, 404, "not found")
		}
	case parts[0] == "storage-pools" && len(parts) >= 3 && parts[2] == "volumes":
		f.storage(w, r, proj, parts)
	default:
		fail(w, 404, "not found: "+r.URL.Path)
	}
}

// storage serves /1.0/storage-pools/<pool>/volumes[/custom[/<vol>[/snapshots[/<snap>]]]].
func (f *fakeIncusREST) storage(w http.ResponseWriter, r *http.Request, proj string, parts []string) {
	pool, target := parts[1], r.URL.Query().Get("target")
	f.targets = append(f.targets, target)
	key := func(v string) string { return proj + "/" + pool + "/" + v }
	switch {
	case len(parts) == 3 && r.Method == http.MethodGet:
		var list []map[string]any
		for k, v := range f.vols {
			if strings.HasPrefix(k, key("")) {
				list = append(list, map[string]any{"name": strings.TrimPrefix(k, key("")), "type": "custom", "location": v.Location})
			}
		}
		list = append(list, map[string]any{"name": "c1", "type": "container", "location": "asus"})
		ok(w, list)
	case len(parts) == 4 && r.Method == http.MethodPost:
		var body struct {
			Name string `json:"name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.vols[key(body.Name)] = &fakeVol{Location: target}
		ok(w, nil)
	case len(parts) >= 6 && parts[5] == "snapshots":
		v, found := f.vols[key(parts[4])]
		if !found {
			fail(w, 404, "Storage volume not found")
			return
		}
		switch {
		case len(parts) == 6 && r.Method == http.MethodPost:
			var body struct {
				Name string `json:"name"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			v.Snaps = append(v.Snaps, body.Name)
			opDone(w)
		case len(parts) == 6:
			var urls []string
			for _, n := range v.Snaps {
				urls = append(urls, "/1.0/storage-pools/"+pool+"/volumes/custom/"+parts[4]+"/snapshots/"+n)
			}
			ok(w, urls)
		case len(parts) == 7 && r.Method == http.MethodDelete:
			v.Snaps = slices.DeleteFunc(v.Snaps, func(n string) bool { return n == parts[6] })
			opDone(w)
		default:
			fail(w, 404, "not found")
		}
	default:
		fail(w, 404, "not found: "+r.URL.Path)
	}
}

func serveFakeIncus(t *testing.T) (*fakeIncusREST, *incus.Client) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "pg-incus")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "unix.socket")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeIncusREST{
		projects: map[string]map[string]string{"p-asus": {"restricted.cluster.groups": "host-asus"}},
		groups:   map[string][]string{"host-asus": {"asus"}},
		insts: map[string]map[string]*fakeInst{"p-asus": {
			"web": {Status: "Stopped", Location: "asus", Type: "container", Config: map[string]string{}},
		}},
		vols: map[string]*fakeVol{"p-asus/zp/other": {Location: "dell"}},
	}
	srv := &http.Server{Handler: f, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return f, incus.NewClient(incus.ClientOptions{Socket: sock})
}

func TestClientServer(t *testing.T) {
	_, c := serveFakeIncus(t)
	ctx := context.Background()
	name, clustered, archs, err := c.Server(ctx)
	if err != nil || name != "asus" || !clustered || len(archs) != 1 {
		t.Fatalf("Server: %q %v %v %v", name, clustered, archs, err)
	}
	if v, err := c.ServerVersion(ctx); err != nil || v != "7.5.1" {
		t.Fatalf("ServerVersion: %q %v", v, err)
	}
}

func TestClientReadsAndNotFound(t *testing.T) {
	_, c := serveFakeIncus(t)
	ctx := context.Background()
	if cfg, err := c.ProjectConfig(ctx, "p-asus"); err != nil || cfg["restricted.cluster.groups"] != "host-asus" {
		t.Fatalf("ProjectConfig: %v %v", cfg, err)
	}
	if _, err := c.ProjectConfig(ctx, "p-nope"); !incus.IsNotFound(err) {
		t.Fatalf("missing project: %v, want a 404", err)
	}
	if m, err := c.ClusterGroupMembers(ctx, "host-asus"); err != nil || len(m) != 1 || m[0] != "asus" {
		t.Fatalf("ClusterGroupMembers: %v %v", m, err)
	}
	if _, err := c.ClusterGroupMembers(ctx, "host-nope"); !incus.IsNotFound(err) {
		t.Fatalf("missing group: %v, want a 404", err)
	}
	if st, err := c.MemberStatus(ctx, "dell"); err != nil || st != "Offline" {
		t.Fatalf("MemberStatus: %q %v", st, err)
	}
	insts, err := c.Instances(ctx, "p-asus")
	if err != nil || len(insts) != 1 || insts[0].Name != "web" || insts[0].Location != "asus" || insts[0].Type != "container" {
		t.Fatalf("Instances: %+v %v", insts, err)
	}
	addrs, err := c.Addresses(ctx, "p-asus", "web")
	if err != nil || len(addrs["eth0"]) != 1 || addrs["eth0"][0].Address != "10.101.92.7" {
		t.Fatalf("Addresses: %+v %v", addrs, err)
	}
}

func TestClientMutations(t *testing.T) {
	f, c := serveFakeIncus(t)
	ctx := context.Background()
	if err := c.Start(ctx, "p-asus", "web"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Stop(ctx, "p-asus", "web", 0, true); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := c.SetConfig(ctx, "p-asus", "web", "boot.autostart", "false"); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	if err := c.SetConfig(ctx, "p-asus", "web", "boot.autostart", "false"); err != nil { // unchanged: no write
		t.Fatalf("SetConfig again: %v", err)
	}
	if err := c.CreateClusterGroup(ctx, "host-dell", []string{"dell"}); err != nil {
		t.Fatalf("CreateClusterGroup: %v", err)
	}
	if err := c.SetClusterGroupMembers(ctx, "host-dell", []string{"dell", "rpi5"}); err != nil {
		t.Fatalf("SetClusterGroupMembers: %v", err)
	}
	if err := c.CreateProject(ctx, "p-dell", map[string]string{"restricted.cluster.groups": "host-dell"}); err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if want := []string{"p-asus/web start", "p-asus/web stop force"}; strings.Join(f.stateCall, ",") != strings.Join(want, ",") {
		t.Fatalf("state calls %v, want %v", f.stateCall, want)
	}
	if f.insts["p-asus"]["web"].Config["boot.autostart"] != "false" {
		t.Fatalf("config %v", f.insts["p-asus"]["web"].Config)
	}
	if len(f.groups["host-dell"]) != 2 || f.projects["p-dell"]["restricted.cluster.groups"] != "host-dell" {
		t.Fatalf("groups %v projects %v", f.groups, f.projects)
	}
}

func TestClientUnreachable(t *testing.T) {
	c := incus.NewClient(incus.ClientOptions{Socket: filepath.Join(t.TempDir(), "absent.socket")})
	if _, err := c.ProjectConfig(context.Background(), "p"); err == nil {
		t.Fatal("no Incus, no error")
	}
}

func TestClientVolumes(t *testing.T) {
	f, c := serveFakeIncus(t)
	ctx := context.Background()
	if err := c.CreateVolume(ctx, "p-asus", "zp", "asus", "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	vs, err := c.Volumes(ctx, "p-asus", "zp", "asus")
	if err != nil || len(vs) != 1 || vs[0] != "data" {
		t.Fatalf("Volumes = %v, %v: want only asus's custom volume", vs, err)
	}
	for _, s := range []string{"pagurus-1-20261015T220000000Z", "pagurus-1-20261015T220100000Z"} {
		if err := c.CreateVolumeSnapshot(ctx, "p-asus", "zp", "asus", "data", s); err != nil {
			t.Fatalf("CreateVolumeSnapshot: %v", err)
		}
	}
	if err := c.DeleteVolumeSnapshot(ctx, "p-asus", "zp", "asus", "data", "pagurus-1-20261015T220000000Z"); err != nil {
		t.Fatalf("DeleteVolumeSnapshot: %v", err)
	}
	snaps, err := c.VolumeSnapshots(ctx, "p-asus", "zp", "asus", "data")
	if err != nil || len(snaps) != 1 || snaps[0] != "pagurus-1-20261015T220100000Z" {
		t.Fatalf("VolumeSnapshots = %v, %v", snaps, err)
	}
	if _, err := c.VolumeSnapshots(ctx, "p-asus", "zp", "asus", "absent"); !incus.IsNotFound(err) {
		t.Fatalf("a missing volume: %v, want not found", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, tg := range f.targets {
		if tg != "asus" {
			t.Fatalf("a storage call without ?target=asus (%q): member-local pools need it", tg)
		}
	}
}
