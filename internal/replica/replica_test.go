package replica

import (
	"slices"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 15, 22, 0, 0, 123e6, time.UTC)

func TestNameRoundTrip(t *testing.T) {
	n := Name(42, t0, false)
	if n != "pagurus-42-20261015T220000123Z" {
		t.Fatalf("Name = %q", n)
	}
	s, ok := Parse(n)
	if !ok || s.Epoch != 42 || !s.At.Equal(t0) || s.Final {
		t.Fatalf("Parse(%q) = %+v, %v", n, s, ok)
	}
	f := Name(42, t0, true)
	if s, ok := Parse(f); !ok || !s.Final || s.Epoch != 42 {
		t.Fatalf("Parse(%q) = %+v, %v", f, s, ok)
	}
	for _, bad := range []string{"autosync-20261015220000", "pagurus-x-20261015T220000123Z", "pagurus-42-20261015T220000Z",
		"pagurus-42-20261015T220000123Z-old", "my-pagurus-42-20261015T220000123Z", "pagurus--20261015T220000123Z"} {
		if _, ok := Parse(bad); ok {
			t.Errorf("Parse(%q) accepted a name pagurus did not make", bad)
		}
	}
}

func TestNewestAndPrune(t *testing.T) {
	a := Name(5, t0, false)
	b := Name(5, t0.Add(time.Minute), false)
	c := Name(7, t0.Add(2*time.Minute), true)
	hand := "before-upgrade"
	names := []string{c, hand, a, b}
	if n, ok := Newest(names); !ok || n.Name != c {
		t.Fatalf("Newest = %+v", n)
	}
	if _, ok := Newest([]string{hand}); ok {
		t.Fatal("Newest found a pagurus snapshot among hand-made ones")
	}
	if got := Prune(names, 2); !slices.Equal(got, []string{a}) {
		t.Fatalf("Prune(keep 2) = %v, want [%s] (never the hand-made one)", got, a)
	}
	if got := Prune(names, 3); len(got) != 0 {
		t.Fatalf("Prune(keep 3) = %v", got)
	}
}

// Diverged is rule 4: does this member's volume hold writes the source does
// not have? Snapshot epochs and the last epoch the member led decide it.
func TestDiverged(t *testing.T) {
	s := func(e int64, min int, final bool) string {
		return Name(e, t0.Add(time.Duration(min)*time.Minute), final)
	}
	cases := []struct {
		name          string
		local, source []string
		led           int64
		want          bool
	}{
		{"fresh volume, never led", nil, []string{s(10, 1, false)}, 0, false},
		{"plain follower, behind", []string{s(10, 1, false)}, []string{s(10, 1, false), s(10, 2, false)}, 0, false},
		{"follower that led long ago, base newer", []string{s(10, 1, false)}, []string{s(10, 1, false), s(10, 2, false)}, 3, false},
		{"led, crashed, base is my own non-final", []string{s(10, 1, false), s(10, 2, false)}, []string{s(10, 1, false), s(20, 3, false)}, 10, true},
		{"led after the base", []string{s(10, 1, false)}, []string{s(10, 1, false), s(20, 3, false)}, 15, true},
		{"released cleanly: base is my final", []string{s(10, 1, false), s(10, 2, true)}, []string{s(10, 2, true), s(20, 3, false)}, 10, false},
		{"own snapshot the source lacks", []string{s(10, 1, false), s(10, 2, true), s(15, 4, false)}, []string{s(10, 2, true), s(20, 5, false)}, 10, true},
		{"foreign snapshot the source lacks (two failovers)", []string{s(10, 1, false), s(12, 2, false)}, []string{s(10, 1, false), s(20, 5, false)}, 0, true},
		{"no common base, never led", []string{s(3, 1, false)}, []string{s(20, 5, false)}, 0, false},
		{"no common base, led", []string{s(3, 1, false)}, []string{s(20, 5, false)}, 3, true},
		{"led but never snapshotted", nil, []string{s(20, 5, false)}, 4, true},
		{"identical", []string{s(10, 1, false)}, []string{s(10, 1, false)}, 0, false},
	}
	for _, c := range cases {
		d := Diverged(c.local, c.source, c.led)
		if d.Diverged != c.want {
			t.Errorf("%s: Diverged = %v (%s), want %v", c.name, d.Diverged, d.Why, c.want)
		}
		if d.Diverged && d.Why == "" {
			t.Errorf("%s: diverged without a reason", c.name)
		}
	}
}
