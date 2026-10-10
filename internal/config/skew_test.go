package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFenceBeatsExpiryUnderClockSkew: conch starts its fence at the latest at
// local ValidUntil − budget − 1s, and the fence ends within its budget, so it
// is over by local ValidUntil − 1s = (send time of the last good renewal) +
// TTL − 1s on the local clock. The server expires the lease no earlier than
// that send time + TTL on its own clock. If the local clock runs slow by a
// factor r (local = r × real), the fence ends (TTL − 1s)/r after the send in
// real time, and the 1s slack must absorb the difference.
//
// pagurus itself no longer does lease arithmetic (conch does, in a separate
// process whose monotonic clock a test cannot skew), so this checks the bound
// analytically for every shipped config, at ±1% clock rate.
func TestFenceBeatsExpiryUnderClockSkew(t *testing.T) {
	files, _ := filepath.Glob("../../examples/*/*.json")
	if len(files) == 0 {
		t.Fatal("no example configs found")
	}
	cfgs := map[string]*Config{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		c, err := Parse(b)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		cfgs[f] = c
	}
	def, err := Parse([]byte(`{"service":"s","start":{"kind":"instances"},"project_prefix":"p","members":["a","b"],"etcd":{"endpoints":["x"]}}`))
	if err != nil {
		t.Fatal(err)
	}
	cfgs["defaults"] = def

	for name, c := range cfgs {
		ttl := c.Lease.TTL.D()
		for _, r := range []float64{0.99, 1.01} {
			fenceEndReal := time.Duration(float64(ttl-time.Second) / r)
			if spare := ttl - fenceEndReal; spare <= 0 {
				t.Errorf("%s: TTL %s, clock rate %.2f: fence ends %s after the server may expire the lease", name, ttl, r, -spare)
			} else {
				t.Logf("%s: TTL %s, clock rate %.2f: fence over %s before the earliest expiry", name, ttl, r, spare.Round(time.Millisecond))
			}
		}
	}
}
