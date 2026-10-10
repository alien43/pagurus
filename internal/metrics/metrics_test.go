package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	s := New("web", "asus")
	s.Bool("pagurus_role", true)
	s.SetGauge("pagurus_local_running", 2)
	s.Inc("pagurus_fence_runs_total", "chain", "peer", "kind", "incus-api-stop", "result", "confirmed")
	s.Inc("pagurus_fence_runs_total", "chain", "peer", "kind", "incus-api-stop", "result", "confirmed")
	s.Inc("pagurus_terms_total")

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	out := rec.Body.String()
	for _, want := range []string{
		"# TYPE pagurus_role gauge",
		`pagurus_role{svc="web",member="asus"} 1`,
		`pagurus_local_running{svc="web",member="asus"} 2`,
		"# TYPE pagurus_fence_runs_total counter",
		`pagurus_fence_runs_total{svc="web",member="asus",chain="peer",kind="incus-api-stop",result="confirmed"} 2`,
		`pagurus_terms_total{svc="web",member="asus"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if got := s.Counter("pagurus_fence_runs_total", "chain", "peer", "kind", "incus-api-stop", "result", "confirmed"); got != 2 {
		t.Fatalf("Counter = %v", got)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("content type %q", ct)
	}
	for name := range Help {
		if !strings.HasPrefix(name, "pagurus_") {
			t.Errorf("%s: not under pagurus_", name)
		}
	}
}
