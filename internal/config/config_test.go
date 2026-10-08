package config

import (
	"strings"
	"testing"
	"time"
)

const base = `{
  "service": "whoami", "compose": "/x/compose.yaml", "project_prefix": "sgl-whoami",
  "members": ["asus", "dell", "rpi5"],
  "etcd": {"endpoints": ["127.0.0.1:2379"]}
  %s
}`

func parse(extra string) (*Config, error) {
	if extra != "" {
		extra = "," + extra
	}
	return Parse([]byte(strings.Replace(base, "%s", extra, 1)))
}

func TestDefaultsAreValid(t *testing.T) {
	c, err := parse("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Fence.Unverified != UnverifiedBlock {
		t.Errorf("default unverified policy = %q, want block (fail closed)", c.Fence.Unverified)
	}
	if c.Watchdog.Mode != WatchdogDryRun || c.Watchdog.Arm {
		t.Errorf("default watchdog must be dry-run and unarmed, got %+v", c.Watchdog)
	}
	if c.Project("dell") != "sgl-whoami-dell" || c.Rank("asus") != 0 || c.Rank("rpi5") != 2 {
		t.Error("project/rank derivation wrong")
	}
	if c.EtcdPrefix != "/pagurus/v1/whoami" {
		t.Errorf("etcd prefix %q", c.EtcdPrefix)
	}
}

func TestTimingSandwichEnforced(t *testing.T) {
	cases := map[string]string{
		"ttl too short for self-fence": `"lease": {"ttl": "10s", "renew_interval": "2s"}`,
		"fractional ttl":               `"lease": {"ttl": "20500ms"}`,
		"op timeout > renew":           `"etcd": {"endpoints": ["x"], "op_timeout": "5s"}`,
		"unverified wait too short":    `"fence": {"unverified": "proceed", "unverified_wait": "1s"}`,
		"state dir not tmpfs":          `"state_dir": "/var/lib/pagurus"`,
		"bad policy":                   `"fence": {"unverified": "yolo"}`,
		"prefer not a member":          `"prefer": ["nucbox"]`,
		"unknown field":                `"ttl": "20s"`,
		"http without instance":        `"health": {"http": "http://{ip}/"}`,
		"watchdog shorter than renew":  `"watchdog": {"mode": "device", "timeout": "3s"}`,
	}
	for name, extra := range cases {
		if _, err := parse(extra); err == nil {
			t.Errorf("%s: accepted, want rejection", name)
		}
	}
}

func TestSelfFenceLead(t *testing.T) {
	c, err := parse(`"self_fence": {"stop_budget": "4s", "kill_budget": "1s"}`)
	if err != nil {
		t.Fatal(err)
	}
	if c.SelfFenceLead() != 5*time.Second {
		t.Fatal(c.SelfFenceLead())
	}
}

func TestDefaultSelfChainIsThePrototypes(t *testing.T) {
	c, err := parse(`"self_fence": {"stop_budget": "4s", "kill_budget": "1s"}`)
	if err != nil {
		t.Fatal(err)
	}
	got := c.SelfFences()
	if len(got) != 2 || got[0].Kind != "incus-stop" || got[0].Budget != 4*time.Second || got[1].Kind != "cgroup-kill" || got[1].Budget != time.Second {
		t.Fatalf("default chain %+v, want incus-stop 4s then cgroup-kill 1s", got)
	}
}

func TestExplicitSelfChain(t *testing.T) {
	c, err := parse(`"fences": {"self": [{"kind": "exec", "budget": "2s", "command": ["/bin/plug", "{member}"]}, {"kind": "cgroup-kill", "budget": "3s"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if c.SelfFenceLead() != 5*time.Second {
		t.Fatalf("lead %v, want the chain's total 5s", c.SelfFenceLead())
	}
	c, err = parse(`"fences": {"self": []}`)
	if err != nil {
		t.Fatalf("an empty self chain must be allowed: %v", err)
	}
	if len(c.SelfFences()) != 0 || c.SelfFenceLead() != 0 {
		t.Fatalf("empty chain became %+v", c.SelfFences())
	}
}

func TestSelfChainRejections(t *testing.T) {
	for name, extra := range map[string]string{
		"both forms":      `"self_fence": {"stop_budget": "4s"}, "fences": {"self": []}`,
		"unknown kind":    `"fences": {"self": [{"kind": "stonith", "budget": "1s"}]}`,
		"no budget":       `"fences": {"self": [{"kind": "cgroup-kill"}]}`,
		"exec no command": `"fences": {"self": [{"kind": "exec", "budget": "1s"}]}`,
		"chain too long":  `"fences": {"self": [{"kind": "incus-stop", "budget": "30s"}]}`,
	} {
		if _, err := parse(extra); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestDefaultPeerChainIsThePrototypes(t *testing.T) {
	c, err := parse(`"fence": {"api_timeout": "7s"}`)
	if err != nil {
		t.Fatal(err)
	}
	got := c.PeerFences()
	if len(got) != 2 || got[0].Kind != "incus-api-stop" || got[0].Budget != 7*time.Second || got[1].Kind != "watchdog-wait" {
		t.Fatalf("default peer chain %+v, want incus-api-stop 7s then watchdog-wait", got)
	}
	if _, err := parse(`"fences": {"peer": [{"kind": "exec", "budget": "1s"}]}`); err == nil {
		t.Fatal("exec peer fence without a command accepted")
	}
	if _, err := parse(`"fences": {"peer": []}`); err != nil {
		t.Fatalf("an empty peer chain (unverified policy only) must be allowed: %v", err)
	}
}

func TestStartForms(t *testing.T) {
	// The prototype's top-level compose still works, mapped onto start, with a deprecation.
	c, err := parse("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Start.Kind != "compose" || c.Start.File != "/x/compose.yaml" || strings.Join(c.Start.UpArgs, " ") != "--detach --no-healthd" || len(c.Deprecations) != 1 {
		t.Fatalf("legacy compose: start %+v deprecations %q", c.Start, c.Deprecations)
	}
	raw := func(start string) string {
		return `{"service": "s", "project_prefix": "p", "members": ["asus"], "etcd": {"endpoints": ["x"]}, "start": ` + start + `}`
	}
	c, err = Parse([]byte(raw(`{"kind": "instances", "names": ["web"]}`)))
	if err != nil {
		t.Fatal(err)
	}
	if c.Start.Kind != "instances" || len(c.Deprecations) != 0 {
		t.Fatalf("instances: %+v %q", c.Start, c.Deprecations)
	}
	for name, start := range map[string]string{
		"compose without file":  `{"kind": "compose"}`,
		"instances with a file": `{"kind": "instances", "file": "x"}`,
		"compose with names":    `{"kind": "compose", "file": "x", "names": ["a"]}`,
		"unknown kind":          `{"kind": "vm"}`,
	} {
		if _, err := Parse([]byte(raw(start))); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := Parse([]byte(`{"service": "s", "project_prefix": "p", "members": ["asus"], "etcd": {"endpoints": ["x"]}}`)); err == nil {
		t.Error("no start at all: accepted")
	}
}
