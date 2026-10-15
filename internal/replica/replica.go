// Package replica holds the pure rules of volume replication (README
// "Replication"): how pagurus names its snapshots, which ones it may prune,
// and rule 4, whether a member's volume holds writes the source lacks.
//
// A snapshot name carries the epoch of the term that wrote the data and the
// time it was taken: pagurus-<epoch>-<YYYYMMDDTHHMMSSmmmZ>, with "-final"
// when it was taken after that term's workload was confirmed stopped (so no
// write can follow it). Anything else is not pagurus's: never pruned, never
// counted.
package replica

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"time"
)

const stampLayout = "20060102T150405.000Z"

var nameRE = regexp.MustCompile(`^pagurus-([1-9][0-9]*)-([0-9]{8}T[0-9]{6})([0-9]{3})Z(-final)?$`)

// Snap is a parsed pagurus snapshot name.
type Snap struct {
	Name  string
	Epoch int64
	At    time.Time
	Final bool
}

// Name is the snapshot name for data of term epoch, taken at t.
func Name(epoch int64, t time.Time, final bool) string {
	s := t.UTC().Format(stampLayout)
	n := fmt.Sprintf("pagurus-%d-%s%sZ", epoch, s[:15], s[16:19])
	if final {
		n += "-final"
	}
	return n
}

// Parse reads a name Name made; ok is false for any other name.
func Parse(name string) (Snap, bool) {
	m := nameRE.FindStringSubmatch(name)
	if m == nil {
		return Snap{}, false
	}
	e, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return Snap{}, false
	}
	at, err := time.Parse(stampLayout, m[2]+"."+m[3]+"Z")
	if err != nil {
		return Snap{}, false
	}
	return Snap{Name: name, Epoch: e, At: at, Final: m[4] != ""}, true
}

// Sorted returns the pagurus snapshots among names, oldest first (by time,
// then epoch).
func Sorted(names []string) []Snap {
	var out []Snap
	for _, n := range names {
		if s, ok := Parse(n); ok {
			out = append(out, s)
		}
	}
	slices.SortFunc(out, func(a, b Snap) int {
		if c := a.At.Compare(b.At); c != 0 {
			return c
		}
		if a.Epoch != b.Epoch {
			return int(a.Epoch - b.Epoch)
		}
		return 0
	})
	return out
}

// Newest is the newest pagurus snapshot among names.
func Newest(names []string) (Snap, bool) {
	s := Sorted(names)
	if len(s) == 0 {
		return Snap{}, false
	}
	return s[len(s)-1], true
}

// Prune returns the pagurus snapshots to delete so that keep remain, oldest
// first. Names pagurus did not make are never returned.
func Prune(names []string, keep int) []string {
	s := Sorted(names)
	var out []string
	for i := 0; i < len(s)-keep; i++ {
		out = append(out, s[i].Name)
	}
	return out
}

// Divergence is rule 4's verdict on a member's own volume.
type Divergence struct {
	Diverged bool
	Why      string
	// Base is the newest snapshot both sides have ("" if none).
	Base string
	// Key identifies this divergence, so the same one is preserved once.
	Key string
}

// Diverged decides whether the volume with snapshots local, on a member
// whose last replicated term was epoch led (0: never), holds writes the
// source (snapshots source) lacks, which a pull (`copy --refresh`, a forced
// receive) would silently destroy.
//
// Writes happen only while a member runs the workload, so only in its own
// terms, and every term writes its epoch into the snapshots it takes. The
// volume holds writes the source lacks if:
//   - it has a pagurus snapshot newer than the common base (or, with none,
//     than the source's oldest) that the source does not have: its own
//     term's, or an earlier leader's the source never got (two failovers in
//     a row);
//   - it led a term after the base was taken (led > base epoch);
//   - it took the base itself (led = base epoch) and the base is not final:
//     the term may have written after it;
//   - with no common base, the same two rules against its own newest
//     snapshot (a member finalized at a clean handover and away for longer
//     than the source keeps snapshots is not diverged), or it led and has no
//     snapshot at all.
func Diverged(local, source []string, led int64) Divergence {
	src := map[string]bool{}
	for _, n := range source {
		src[n] = true
	}
	mine := Sorted(local)
	var base *Snap
	for i := len(mine) - 1; i >= 0; i-- {
		if src[mine[i].Name] {
			base = &mine[i]
			break
		}
	}
	d := Divergence{}
	newest := ""
	if len(mine) > 0 {
		newest = mine[len(mine)-1].Name
	}
	if base != nil {
		d.Base = base.Name
	}
	d.Key = fmt.Sprintf("led=%d base=%s newest=%s", led, d.Base, newest)
	// Without a common base, the source has pruned past everything this
	// volume has; only snapshots newer than the source's oldest can be ones
	// it never had. (An older foreign one is lost here, but its own term's
	// member still preserves it: it led that epoch.)
	var floor time.Time
	if base != nil {
		floor = base.At
	} else if ss := Sorted(source); len(ss) > 0 {
		floor = ss[0].At
	}
	for _, s := range mine {
		if !src[s.Name] && s.At.After(floor) {
			d.Diverged, d.Why = true, fmt.Sprintf("has snapshot %s that the source lacks", s.Name)
			return d
		}
	}
	// The epoch rules apply to the common base or, with none, to this
	// volume's own newest snapshot: what it last agreed on with anyone.
	ref := base
	if ref == nil && len(mine) > 0 {
		ref = &mine[len(mine)-1]
	}
	switch {
	case led == 0:
	case ref == nil:
		d.Diverged, d.Why = true, fmt.Sprintf("led epoch %d and has no snapshot at all", led)
	case led > ref.Epoch:
		d.Diverged, d.Why = true, fmt.Sprintf("led epoch %d after snapshot %s", led, ref.Name)
	case led == ref.Epoch && !ref.Final:
		d.Diverged, d.Why = true, fmt.Sprintf("led epoch %d and may have written after its snapshot %s", led, ref.Name)
	}
	return d
}
