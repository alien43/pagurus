package agent

import (
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/config"
)

func TestReleaseBackoffEscalatesAndResets(t *testing.T) {
	a := &Agent{cfg: testConfig(t, func(c *config.Config) {
		c.Health.ReleaseBackoff = config.Duration(2 * time.Minute)
		c.Health.ReleaseBackoffMax = config.Duration(30 * time.Minute)
	})}
	want := []time.Duration{2, 4, 8, 16, 30, 30}
	for i, w := range want {
		if d, _ := a.releaseBackoff(false); d != w*time.Minute {
			t.Fatalf("failed term %d: backoff %s, want %s", i+1, d, w*time.Minute)
		}
	}
	// A term that got healthy before giving up resets the streak.
	if d, n := a.releaseBackoff(true); d != 2*time.Minute || n != 0 {
		t.Fatalf("after a healthy term: backoff %s streak %d, want 2m0s and 0", d, n)
	}
	if d, _ := a.releaseBackoff(false); d != 2*time.Minute {
		t.Fatalf("first failed term after a reset: backoff %s, want 2m0s", d)
	}
}

func TestReleaseBackoffMaxEqualToBaseIsFixed(t *testing.T) {
	a := &Agent{cfg: testConfig(t, func(c *config.Config) {
		c.Health.ReleaseBackoffMax = c.Health.ReleaseBackoff
	})}
	for range 5 {
		if d, _ := a.releaseBackoff(false); d != a.cfg.Health.ReleaseBackoff.D() {
			t.Fatalf("backoff %s, want the fixed %s", d, a.cfg.Health.ReleaseBackoff.D())
		}
	}
}
