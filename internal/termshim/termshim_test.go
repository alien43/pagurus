package termshim

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestRunNotUnderConch(t *testing.T) {
	t.Setenv("CONCH_REV", "")
	t.Setenv("CONCH_LEASE", "")
	if got := Run(filepath.Join(t.TempDir(), "none")); got != 64 {
		t.Fatalf("exit %d, want 64", got)
	}
}

func TestRunNoAgent(t *testing.T) {
	t.Setenv("CONCH_REV", "7")
	t.Setenv("CONCH_LEASE", "1a")
	if got := Run(filepath.Join(t.TempDir(), "none")); got != 69 {
		t.Fatalf("exit %d, want 69", got)
	}
}

// The agent's side: read Hello, answer Bye; Run exits with Bye.Exit, or 70
// when the agent hangs up without one.
func TestRunHandshake(t *testing.T) {
	t.Setenv("CONCH_REV", "7")
	t.Setenv("CONCH_LEASE", "1a")
	for _, tc := range []struct {
		reply string
		want  int
	}{{`{"exit":3}`, 3}, {`not json`, 70}, {"", 70}} {
		sock := filepath.Join(t.TempDir(), "term")
		ln, err := net.Listen("unix", sock)
		if err != nil {
			t.Fatal(err)
		}
		got := make(chan Hello, 1)
		go func() {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
			line, _ := bufio.NewReader(c).ReadBytes('\n')
			var h Hello
			_ = json.Unmarshal(line, &h)
			got <- h
			if tc.reply != "" {
				_, _ = c.Write([]byte(tc.reply + "\n"))
			}
		}()
		if exit := Run(sock); exit != tc.want {
			t.Errorf("reply %q: exit %d, want %d", tc.reply, exit, tc.want)
		}
		if h := <-got; h.Rev != 7 || h.Lease != "1a" || h.PID != os.Getpid() {
			t.Errorf("hello %+v", h)
		}
		ln.Close()
	}
}
