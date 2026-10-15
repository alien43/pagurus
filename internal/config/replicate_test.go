package config

import (
	"testing"
	"time"
)

// The hash of a config without "replicate", computed before the replicate
// block existed (pagurus 521703c8). The deployed agents publish this value: a
// change here is config skew between old and new agents on every member.
const goldenNoReplicate = "6c0168c9ed1198d8"

const goldenCfg = `{"service":"whoami","start":{"kind":"instances"},"project_prefix":"pg-whoami","members":["asus","dell","rpi5"],"etcd":{"endpoints":["127.0.0.1:2379"]}}`

func TestSafetyHashWithoutReplicateIsUnchanged(t *testing.T) {
	c, err := Parse([]byte(goldenCfg))
	if err != nil {
		t.Fatal(err)
	}
	if c.Replicate != nil {
		t.Fatalf("replicate = %+v without a replicate block, want nil (opt-in)", c.Replicate)
	}
	if h := c.SafetyHash(); h != goldenNoReplicate {
		t.Fatalf("SafetyHash = %s, want %s: a config without replicate must hash as before", h, goldenNoReplicate)
	}
}

func TestReplicateDefaults(t *testing.T) {
	c, err := parse(`"replicate": {"volumes": ["data"]}`)
	if err != nil {
		t.Fatal(err)
	}
	r := c.Replicate
	if r == nil {
		t.Fatal("replicate block not parsed")
	}
	if r.Pool != "default" || r.Interval.D() != 15*time.Minute || r.MaxStaleness.D() != 2*time.Hour ||
		r.Keep != 4 || r.CatchupTimeout.D() != 10*time.Minute || r.PreHook != nil {
		t.Errorf("defaults = %+v", r)
	}
	if c.SafetyHash() == goldenNoReplicate {
		t.Error("a replicate block must change SafetyHash: members must agree on what they replicate")
	}
	d, _ := parse(`"replicate": {"volumes": ["data"], "interval": "5m"}`)
	if d.SafetyHash() == c.SafetyHash() {
		t.Error("the replicate interval must be part of SafetyHash")
	}
}

func TestReplicateValidation(t *testing.T) {
	bad := map[string]string{
		"no volumes":               `"replicate": {}`,
		"bad volume name":          `"replicate": {"volumes": ["Data/x"]}`,
		"volume twice":             `"replicate": {"volumes": ["data", "data"]}`,
		"staleness under interval": `"replicate": {"volumes": ["data"], "interval": "1h", "max_staleness": "30m"}`,
		"keep 1":                   `"replicate": {"volumes": ["data"], "keep": 1}`,
		"hook without instance":    `"replicate": {"volumes": ["data"], "pre_hook": {"command": ["sync"]}}`,
		"hook without command":     `"replicate": {"volumes": ["data"], "pre_hook": {"instance": "app-1"}}`,
		"unknown field":            `"replicate": {"volumes": ["data"], "every": "1m"}`,
	}
	for name, extra := range bad {
		if _, err := parse(extra); err == nil {
			t.Errorf("%s: accepted, want rejection", name)
		}
	}
	if _, err := parse(`"replicate": {"pool": "zfs-pool", "volumes": ["data", "media"], "interval": "1m", "max_staleness": "10m",
		"keep": 2, "catchup_timeout": "1m", "pre_hook": {"instance": "app-1", "command": ["sqlite3", "/data/app.db", ".backup /data/snap.db"]}}`); err != nil {
		t.Errorf("a full replicate block was rejected: %v", err)
	}
}
