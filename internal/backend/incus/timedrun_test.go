package incus

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestStampedLinesPrefixesEachLine(t *testing.T) {
	s := &stampedLines{start: time.Now()}
	_, _ = s.Write([]byte("Creating x\nStart"))
	_, _ = s.Write([]byte("ing x\n"))
	_, _ = s.Write([]byte("tail without newline"))
	got := s.String()
	for _, want := range []string{"] Creating x\n", "] Starting x\n", "] tail without newline"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	if n := strings.Count(got, "[+"); n != 3 {
		t.Errorf("%d stamps, want 3: %q", n, got)
	}
}

// TimedRun returns what Run returns: stdout, and stderr in the error.
func TestTimedRunLikeRun(t *testing.T) {
	out, err := TimedRun(context.Background(), "sh", "-c", "echo out; echo err >&2")
	if err != nil || string(out) != "out\n" {
		t.Fatalf("got %q, %v", out, err)
	}
	_, err = TimedRun(context.Background(), "sh", "-c", "echo boom >&2; exit 3")
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error %v, want the stderr in it", err)
	}
}
