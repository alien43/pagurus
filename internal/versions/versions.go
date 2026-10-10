// Package versions records which Incus and incus-compose an agent drives, and
// warns when they are outside what pagurus was built and tested against. The
// assumptions that depend on them (Incus JSON shapes and status strings, the
// liblxc payload cgroup name, incus-compose's --no-healthd/--detach flags)
// fail as confusing errors, or worse, on other versions.
package versions

import (
	"fmt"
	"strconv"
	"strings"
)

// Known is what pagurus is built and tested against: per tool, the major
// version and the lowest minor of that major.
var Known = map[string]struct{ Major, MinMinor int }{
	"incus":         {7, 0},
	"incus-compose": {1, 3},
}

// Parse extracts "X.Y.Z" from tool output such as "incus-compose version
// v1.3.4" or "7.5.1": the last field, without a leading "v".
func Parse(out string) string {
	f := strings.Fields(strings.TrimSpace(out))
	if len(f) == 0 {
		return ""
	}
	return strings.TrimPrefix(f[len(f)-1], "v")
}

// Check returns a warning if version of tool is unreadable or outside Known,
// or "" if it is fine.
func Check(tool, version string) string {
	k, ok := Known[tool]
	if !ok {
		return ""
	}
	if version == "" {
		return fmt.Sprintf("%s version unreadable; pagurus is built for %d.%d+", tool, k.Major, k.MinMinor)
	}
	parts := strings.SplitN(version, ".", 3)
	major, err1 := strconv.Atoi(parts[0])
	minor := 0
	var err2 error
	if len(parts) > 1 {
		minor, err2 = strconv.Atoi(parts[1])
	}
	switch {
	case err1 != nil || err2 != nil:
		return fmt.Sprintf("%s version %q not understood; pagurus is built for %d.%d+", tool, version, k.Major, k.MinMinor)
	case major != k.Major:
		return fmt.Sprintf("%s %s: unknown major; pagurus is built and tested for %d.x (JSON shapes, flags or the payload cgroup name may differ)", tool, version, k.Major)
	case minor < k.MinMinor:
		return fmt.Sprintf("%s %s is older than %d.%d, which pagurus needs", tool, version, k.Major, k.MinMinor)
	}
	return ""
}
