package incus_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/backend/incus"
)

// TestLivePermissionsProbe calls every incus.API method once with the
// configured credentials and logs which ones the server allows. It is a
// probe, not a pass/fail test: it answers "is this certificate enough for
// pagurus?" (singleton-incus-least-privilege). Opt-in like TestLiveClient,
// plus PAGURUS_LIVE_INCUS_MEMBER (a member to read) and
// PAGURUS_LIVE_INCUS_GROUP (a cluster group to read). The two create calls
// use throwaway names; if one is allowed, the probe reports it, and the
// caller must delete what it made.
func TestLivePermissionsProbe(t *testing.T) {
	project, name := os.Getenv("PAGURUS_LIVE_INCUS_PROJECT"), os.Getenv("PAGURUS_LIVE_INCUS_INSTANCE")
	member, group := os.Getenv("PAGURUS_LIVE_INCUS_MEMBER"), os.Getenv("PAGURUS_LIVE_INCUS_GROUP")
	if project == "" || name == "" || member == "" || group == "" {
		t.Skip("set PAGURUS_LIVE_INCUS_PROJECT, _INSTANCE, _MEMBER and _GROUP to probe a real Incus")
	}
	c := incus.NewClient(incus.ClientOptions{URL: os.Getenv("PAGURUS_LIVE_INCUS_URL"),
		ClientCert: os.Getenv("PAGURUS_LIVE_INCUS_CERT"), ClientKey: os.Getenv("PAGURUS_LIVE_INCUS_KEY"),
		ServerCert: os.Getenv("PAGURUS_LIVE_INCUS_SERVERCERT")})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var rows []string
	probe := func(call string, f func() (string, error)) {
		out, err := f()
		res := "ALLOWED " + out
		if err != nil {
			res = "REFUSED " + strings.ReplaceAll(err.Error(), "\n", " ")
		}
		rows = append(rows, fmt.Sprintf("%-44s %s", call, res))
	}
	probe("GET /1.0 (Server)", func() (string, error) {
		n, cl, a, err := c.Server(ctx)
		return fmt.Sprintf("name=%q clustered=%v archs=%v", n, cl, a), err
	})
	probe("GET /1.0 (ServerVersion)", func() (string, error) { return c.ServerVersion(ctx) })
	probe("GET /1.0/projects/"+project, func() (string, error) {
		cfg, err := c.ProjectConfig(ctx, project)
		return fmt.Sprintf("restricted.cluster.groups=%q", cfg["restricted.cluster.groups"]), err
	})
	probe("GET /1.0/projects/default", func() (string, error) { _, err := c.ProjectConfig(ctx, "default"); return "", err })
	probe("GET /1.0/cluster/groups/"+group, func() (string, error) {
		m, err := c.ClusterGroupMembers(ctx, group)
		return fmt.Sprint(m), err
	})
	probe("GET /1.0/cluster/members/"+member, func() (string, error) { return c.MemberStatus(ctx, member) })
	probe("GET /1.0/instances?project="+project, func() (string, error) {
		in, err := c.Instances(ctx, project)
		return fmt.Sprintf("%d instance(s)", len(in)), err
	})
	probe("GET /1.0/instances?project=default", func() (string, error) {
		in, err := c.Instances(ctx, "default")
		return fmt.Sprintf("%d instance(s)", len(in)), err
	})
	probe("PUT instance config ("+project+"/"+name+")", func() (string, error) {
		return "", c.SetConfig(ctx, project, name, "boot.autostart", "false")
	})
	probe("PUT state start ("+project+"/"+name+")", func() (string, error) { return "", c.Start(ctx, project, name) })
	probe("GET state ("+project+"/"+name+")", func() (string, error) {
		a, err := c.Addresses(ctx, project, name)
		return fmt.Sprintf("%d interface(s)", len(a)), err
	})
	probe("PUT state stop ("+project+"/"+name+")", func() (string, error) { return "", c.Stop(ctx, project, name, 0, true) })
	probe("POST /1.0/projects (pglp-probe-x)", func() (string, error) {
		return "MUST BE DELETED", c.CreateProject(ctx, "pglp-probe-x", map[string]string{})
	})
	probe("POST /1.0/cluster/groups (pglp-probe-g)", func() (string, error) {
		return "MUST BE DELETED", c.CreateClusterGroup(ctx, "pglp-probe-g", nil)
	})
	for _, r := range rows {
		t.Log(r)
	}
}
