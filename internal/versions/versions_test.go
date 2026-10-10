package versions

import "testing"

func TestParse(t *testing.T) {
	for in, want := range map[string]string{
		"incus-compose version v1.3.4\n": "1.3.4",
		"incus-compose version 1.3.4":    "1.3.4",
		"7.5.1":                          "7.5.1",
		"":                               "",
	} {
		if got := Parse(in); got != want {
			t.Errorf("Parse(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCheck(t *testing.T) {
	for _, c := range []struct {
		tool, version string
		warn          bool
	}{
		{"incus", "7.5.1", false},
		{"incus", "7.0", false},
		{"incus", "6.23.0", true},
		{"incus", "8.0.0", true},
		{"incus", "", true},
		{"incus", "seven", true},
		{"incus-compose", "1.3.4", false},
		{"incus-compose", "1.2.0", true},
		{"incus-compose", "2.0.0", true},
		{"something-else", "0.1", false},
	} {
		if got := Check(c.tool, c.version) != ""; got != c.warn {
			t.Errorf("Check(%s, %q) warns=%v, want %v (%q)", c.tool, c.version, got, c.warn, Check(c.tool, c.version))
		}
	}
}
