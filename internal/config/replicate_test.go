package config

import (
	"encoding/json"
	"slices"
	"strings"
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

const instancesBase = `{"service":"vmlab","start":{"kind":"instances","names":["vm"]},"project_prefix":"pg-vmlab",
  "members":["asus","dell"],"etcd":{"endpoints":["127.0.0.1:2379"]}%s}`

// replicate.instances: whole instances as replica units, after the volumes.
// Only for the instances starter with the instance named: compose recreates
// its instances, and an empty start.names would start a kept orphan too.
func TestReplicateInstances(t *testing.T) {
	c, err := Parse([]byte(strings.Replace(instancesBase, "%s", `,"replicate":{"instances":["vm"]}`, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if u := c.Replicate.Units(); !slices.Equal(u, []string{"instance/vm"}) {
		t.Errorf("Units = %v, want [instance/vm]", u)
	}
	plain, _ := Parse([]byte(strings.Replace(instancesBase, "%s", "", 1)))
	if c.SafetyHash() == plain.SafetyHash() {
		t.Error("replicate.instances must change SafetyHash")
	}
	both, err := Parse([]byte(strings.Replace(instancesBase, "%s", `,"replicate":{"volumes":["data"],"instances":["vm"]}`, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if u := both.Replicate.Units(); !slices.Equal(u, []string{"data", "instance/vm"}) {
		t.Errorf("Units = %v, want [data instance/vm]", u)
	}
	// A volumes-only block hashes as before the field existed (omitempty).
	v, _ := parse(`"replicate": {"volumes": ["data"]}`)
	if b, _ := json.Marshal(v.Replicate); strings.Contains(string(b), "instances") {
		t.Errorf("a volumes-only replicate block serialises %s: its hash would change on every deployed agent", b)
	}
	bad := map[string]string{
		"compose starter":         `"replicate": {"instances": ["vm"]}`, // base is compose
		"not in start.names":      strings.Replace(strings.Replace(instancesBase, `"names":["vm"]`, `"names":["other"]`, 1), "%s", `,"replicate":{"instances":["vm"]}`, 1),
		"no start.names":          strings.Replace(strings.Replace(instancesBase, `,"names":["vm"]`, "", 1), "%s", `,"replicate":{"instances":["vm"]}`, 1),
		"twice":                   strings.Replace(instancesBase, "%s", `,"replicate":{"instances":["vm","vm"]}`, 1),
		"too long for its orphan": strings.Replace(strings.Replace(instancesBase, `"names":["vm"]`, `"names":["a-very-long-vm-name-xx"]`, 1), "%s", `,"replicate":{"instances":["a-very-long-vm-name-xx"]}`, 1),
	}
	for name, raw := range bad {
		var err error
		if strings.HasPrefix(raw, "{") {
			_, err = Parse([]byte(raw))
		} else {
			_, err = parse(raw)
		}
		if err == nil {
			t.Errorf("%s: accepted, want rejection", name)
		}
	}
}

func TestStopTimeout(t *testing.T) {
	c, err := parse("")
	if err != nil {
		t.Fatal(err)
	}
	if c.StopTimeout.D() != 5*time.Second {
		t.Errorf("stop_timeout default %s, want 5s (the release's graceful stop before it was configurable)", c.StopTimeout.D())
	}
	c, err = parse(`"stop_timeout": "150s"`)
	if err != nil || c.StopTimeout.D() != 150*time.Second {
		t.Errorf("stop_timeout 150s: %v, %v", c.StopTimeout.D(), err)
	}
	if h, _ := parse(""); h.SafetyHash() != c.SafetyHash() {
		t.Error("stop_timeout changed SafetyHash: it is not a fence assumption, and a change must not be config skew")
	}
}
