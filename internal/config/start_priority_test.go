package config

import (
	"encoding/json"
	"testing"
)

// start_priority is a level; the boolean it used to be still reads (true is 1).
func TestStartPriorityReadsLevelsAndTheOldBoolean(t *testing.T) {
	for in, want := range map[string]StartPriority{`true`: 1, `false`: 0, `0`: 0, `3`: 3, `9`: 9} {
		var p StartPriority
		if err := json.Unmarshal([]byte(in), &p); err != nil || p != want {
			t.Errorf("%s: got %d, %v; want %d", in, p, err, want)
		}
	}
	var p StartPriority
	if err := json.Unmarshal([]byte(`"high"`), &p); err == nil {
		t.Error(`"high": no error`)
	}
}

func TestStartPriorityOutOfRangeIsRefused(t *testing.T) {
	cfg := func(p string) string { return goldenCfg[:len(goldenCfg)-1] + `,"start_priority":` + p + `}` }
	if c, err := Parse([]byte(cfg("2"))); err != nil || c.StartPriority != 2 {
		t.Fatalf("2: %v, %v", c, err)
	}
	for _, p := range []string{"10", "-1"} {
		if _, err := Parse([]byte(cfg(p))); err == nil {
			t.Errorf("start_priority %s: accepted", p)
		}
	}
}
