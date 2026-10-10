package agent

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/fence"
)

// The sim runs the real conch (the version pinned in go.mod, built here) for
// every member's office. conch's two children are this test binary in a
// helper mode:
//
//	sim-term <socket>                      `pagurus term`, unchanged
//	sim-fence <host socket> --member <m>   `pagurus fence-self`, but the self
//	                                       chain runs on the simulated host,
//	                                       which lives in the test process
var conchBin string

func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "sim-term":
			os.Exit(RunTermShim(os.Args[2]))
		case "sim-fence":
			os.Exit(simFence(os.Args[2], os.Args[len(os.Args)-1]))
		case "sim-pet": // a conch that pets n times, then stays alive, silent
			n, _ := strconv.Atoi(os.Args[2])
			addr := &net.UnixAddr{Name: os.Getenv("NOTIFY_SOCKET"), Net: "unixgram"}
			for i := 0; i < n; i++ {
				if c, err := net.DialUnix("unixgram", nil, addr); err == nil {
					_, _ = c.Write([]byte("WATCHDOG=1"))
					c.Close()
				}
				time.Sleep(100 * time.Millisecond)
			}
			time.Sleep(time.Hour)
		}
	}
	dir, err := os.MkdirTemp("", "pagurus-conch")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	conchBin = filepath.Join(dir, "conch")
	if out, err := exec.Command("go", "build", "-o", conchBin, "github.com/alien43/conch/cmd/conch").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building conch (pinned in go.mod): %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// simFence asks the simulated host to run member's self chain.
func simFence(sock, member string) int {
	c, err := net.Dial("unix", sock)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sim-fence:", err)
		return 69
	}
	defer c.Close()
	fmt.Fprintln(c, member)
	reply, _ := bufio.NewReader(c).ReadString('\n')
	if strings.TrimSpace(reply) != "ok" {
		fmt.Fprintln(os.Stderr, "sim-fence:", strings.TrimSpace(reply))
		return 1
	}
	return 0
}

// serveHost answers sim-fence: it runs the member's configured self chain
// over that member's simulated runtime, as `pagurus fence-self` would on the
// host.
func (h *harness) serveHost(t *testing.T) string {
	dir, err := os.MkdirTemp("/tmp", "pg-host")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "host.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				m, _ := bufio.NewReader(c).ReadString('\n')
				m = strings.TrimSpace(m)
				h.mu.Lock()
				n := h.nodes[m]
				h.mu.Unlock()
				if n == nil {
					fmt.Fprintln(c, "no such member")
					return
				}
				chain, err := fence.BuildSelf(n.rt, n.agent.cfg.SelfFences())
				if err != nil {
					fmt.Fprintln(c, err)
					return
				}
				res := chain.Run(context.Background(), n.agent.selfTarget(), logger(t, m+"/fence"))
				if !res.Stopped {
					fmt.Fprintln(c, "not confirmed: "+res.String())
					return
				}
				fmt.Fprintln(c, "ok")
			}()
		}
	}()
	return sock
}
