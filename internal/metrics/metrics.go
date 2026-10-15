// Package metrics is pagurus's Prometheus exposition: a few gauges and
// counters, written by hand in the text format (no client library).
//
// Every series carries svc and member labels. Label values are bounded: no
// epochs, no instance names.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
)

// Set holds one agent's metrics.
type Set struct {
	svc, member string

	mu       sync.Mutex
	gauges   map[string]float64
	counters map[string]float64 // key: name + "\x00" + extra labels
	help     map[string]string
	kinds    map[string]string
}

func New(svc, member string) *Set {
	s := &Set{svc: svc, member: member, gauges: map[string]float64{}, counters: map[string]float64{},
		help: map[string]string{}, kinds: map[string]string{}}
	for name, h := range Help {
		s.help[name] = h
		if strings.HasSuffix(name, "_total") {
			s.kinds[name] = "counter"
		} else {
			s.kinds[name] = "gauge"
		}
	}
	return s
}

// Help documents every metric pagurus exports (README § Metrics).
var Help = map[string]string{
	"pagurus_role":                    "1 while this member holds the office and runs its term, else 0",
	"pagurus_conch_running":           "1 while this member's conch elect runs (campaigning or holding)",
	"pagurus_local_running":           "containers of this member's project with live processes, read from cgroupfs",
	"pagurus_fit":                     "1 while the agent pets its watchdog backend, else 0",
	"pagurus_lease_remaining_seconds": "server-side TTL left on the holder's lease (0 when not holding)",
	"pagurus_fence_blocked":           "1 while this leader waits on a member it cannot fence (fence.unverified=block)",
	"pagurus_self_chain_verified":     "1 when some fence in the self chain can confirm a stop for this project's instance types",
	"pagurus_terms_total":             "terms this member has held",
	"pagurus_fence_runs_total":        "fences run by this agent, by chain (self|peer), kind and result",
	"pagurus_conch_exits_total":       "conch elect exits, by result (ok|fence_failed|withdrawn|other)",
	"pagurus_replica_age_seconds":     "age of this member's newest replica snapshot (replicating services only)",
	"pagurus_replica_pulls_total":     "pulls into this member's volumes, by result (ok|failed|discarded)",
	"pagurus_replica_snapshots_total": "snapshot rounds this member took as leader, by result (ok|failed)",
	"pagurus_orphaned_volumes":        "volumes this member preserved under rule 4 (<vol>-orphaned-*), never pruned automatically",
	"pagurus_replica_stale_blocked":   "1 while this member's last term refused to start on a stale replica",
}

// SetMember sets the member label (known only after the agent's preflight).
func (s *Set) SetMember(m string) {
	s.mu.Lock()
	s.member = m
	s.mu.Unlock()
}

// SetGauge sets name to v.
func (s *Set) SetGauge(name string, v float64) {
	s.mu.Lock()
	s.gauges[name] = v
	s.mu.Unlock()
}

// Bool sets a 0/1 gauge.
func (s *Set) Bool(name string, b bool) {
	v := 0.0
	if b {
		v = 1
	}
	s.SetGauge(name, v)
}

// Inc adds 1 to a counter with extra labels (pairs: key, value, ...).
func (s *Set) Inc(name string, labels ...string) {
	if len(labels)%2 != 0 {
		panic("metrics: odd label list")
	}
	var b strings.Builder
	for i := 0; i < len(labels); i += 2 {
		fmt.Fprintf(&b, ",%s=%q", labels[i], labels[i+1])
	}
	s.mu.Lock()
	s.counters[name+"\x00"+b.String()]++
	s.mu.Unlock()
}

// Get reads a gauge (tests).
func (s *Set) Get(name string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gauges[name]
}

// Counter reads a counter with exactly these extra labels (tests).
func (s *Set) Counter(name string, labels ...string) float64 {
	var b strings.Builder
	for i := 0; i+1 < len(labels); i += 2 {
		fmt.Fprintf(&b, ",%s=%q", labels[i], labels[i+1])
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counters[name+"\x00"+b.String()]
}

// Write renders the text exposition format.
func (s *Set) Write(w io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := fmt.Sprintf("svc=%q,member=%q", s.svc, s.member)
	byName := map[string][]string{}
	for name, v := range s.gauges {
		byName[name] = append(byName[name], fmt.Sprintf("%s{%s} %s", name, base, num(v)))
	}
	for key, v := range s.counters {
		name, extra, _ := strings.Cut(key, "\x00")
		byName[name] = append(byName[name], fmt.Sprintf("%s{%s%s} %s", name, base, extra, num(v)))
	}
	names := make([]string, 0, len(byName))
	for n := range byName {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		if h := s.help[n]; h != "" {
			if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", n, h, n, s.kinds[n]); err != nil {
				return err
			}
		}
		lines := byName[n]
		slices.Sort(lines)
		for _, l := range lines {
			if _, err := fmt.Fprintln(w, l); err != nil {
				return err
			}
		}
	}
	return nil
}

func num(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	return fmt.Sprintf("%g", v)
}

// Handler serves /metrics.
func (s *Set) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = s.Write(w)
	})
}
