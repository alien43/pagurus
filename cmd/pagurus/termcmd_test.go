package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestTermCmd(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "pagurus")
	var out bytes.Buffer
	log := slog.New(slog.NewTextHandler(&out, nil))

	if got := termCmd(exe, log); !slices.Equal(got, []string{exe, "term", "--socket"}) {
		t.Fatalf("no shim: %q", got)
	}
	if !strings.Contains(out.String(), "falling back") {
		t.Fatalf("no shim: no warning: %q", out.String())
	}

	shim := filepath.Join(dir, "pagurus-term")
	if err := os.WriteFile(shim, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if got := termCmd(exe, log); !slices.Equal(got, []string{shim, "--socket"}) {
		t.Fatalf("shim: %q", got)
	}
	if out.Len() != 0 {
		t.Fatalf("shim: unexpected log: %q", out.String())
	}
}
