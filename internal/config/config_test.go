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
