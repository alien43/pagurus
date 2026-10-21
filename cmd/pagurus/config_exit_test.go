package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain lets a test re-run this binary as pagurus itself (PAGURUS_TEST_MAIN=1),
// so the exit codes below are the real ones.
func TestMain(m *testing.M) {
	if os.Getenv("PAGURUS_TEST_MAIN") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func pagurus(t *testing.T, args ...string) (int, string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "PAGURUS_TEST_MAIN=1")
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &ee):
		return ee.ExitCode(), string(out)
	default:
		t.Fatalf("running pagurus %v: %v", args, err)
		return -1, ""
	}
}

// writeConfig writes the example config with edit applied to its text.
func writeConfig(t *testing.T, edit func(string) string) string {
	t.Helper()
	b, err := os.ReadFile("../../examples/whoami/pagurus.json")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "pagurus.json")
	if err := os.WriteFile(p, []byte(edit(string(b))), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestConfigThatCannotLoadExits78(t *testing.T) {
	cases := map[string]func(string) string{
		"unknown field": func(s string) string {
			return strings.Replace(s, `"grace": "10s",`, `"grace": "10s", "no_such_field_ever": true,`, 1)
		},
		"bad duration": func(s string) string { return strings.Replace(s, `"grace": "10s"`, `"grace": "ten"`, 1) },
		"invalid value": func(s string) string {
			return strings.Replace(s, `"unverified": "proceed"`, `"unverified": "maybe"`, 1)
		},
	}
	for name, edit := range cases {
		for _, cmd := range []string{"run", "check-config", "status"} {
			code, out := pagurus(t, cmd, "-c", writeConfig(t, edit))
			if code != exitConfig || !strings.Contains(out, "not starting (exit 78") {
				t.Errorf("%s / %s: exit %d, want %d; output %q", name, cmd, code, exitConfig, out)
			}
		}
	}
	if code, _ := pagurus(t, "run", "-c", filepath.Join(t.TempDir(), "missing.json")); code != exitConfig {
		t.Errorf("missing file: exit %d, want %d", code, exitConfig)
	}
}

// check-config needs no network: the example's etcd endpoints (192.0.2.0/24,
// TEST-NET-1) never answer, and it still passes.
func TestCheckConfigNeedsNoNetwork(t *testing.T) {
	code, out := pagurus(t, "check-config", "-c", writeConfig(t, func(s string) string { return s }))
	if code != 0 || !strings.HasPrefix(out, "ok: ") {
		t.Fatalf("exit %d, output %q", code, out)
	}
}

// A loadable config whose etcd is unreachable is a failure, not exit 78: the
// reboot-on-failure path must stay armed for everything but an unloadable config.
func TestUnreachableEtcdIsNotExit78(t *testing.T) {
	cfg := writeConfig(t, func(s string) string { return strings.Replace(s, `"dial_timeout": "3s"`, `"dial_timeout": "300ms"`, 1) })
	code, out := pagurus(t, "status", "-c", cfg)
	if code == exitConfig || code == 0 {
		t.Fatalf("status with unreachable etcd: exit %d, want a plain failure; output %q", code, out)
	}
}
