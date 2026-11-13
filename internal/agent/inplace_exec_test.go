package agent

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// execHost is the old image (test helper mode exec-host <dir>): a listening
// socket and a child that exits 7 after a moment, then exec() into exec-new.
func execHost(dir string) int {
	ln, err := net.Listen("unix", filepath.Join(dir, "sock"))
	if err != nil {
		return 1
	}
	child := exec.Command("/bin/sh", "-c", "sleep 0.5; exit 7")
	if err := child.Start(); err != nil {
		return 1
	}
	fd, err := inheritable(ln.(*net.UnixListener))
	if err != nil {
		return 1
	}
	err = syscall.Exec(os.Args[0], []string{os.Args[0], "exec-new", dir, strconv.Itoa(fd), strconv.Itoa(child.Process.Pid)}, os.Environ())
	fmt.Fprintln(os.Stderr, "exec:", err)
	return 1
}

// execNew is the new image: it takes the socket and the child by number,
// and reports on the first connection: its PID, the child's exit.
func execNew(dir, fd, pid string) int {
	n, _ := strconv.Atoi(fd)
	h := &handover{TermListener: n, Relay: -1}
	a := &Agent{} // no relay: takeSockets needs no config
	ln, err := a.takeSockets(h)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	p, _ := strconv.Atoi(pid)
	done := make(chan conchExit, 1)
	go a.waitConch(p, done)
	c, err := ln.Accept()
	if err != nil {
		return 1
	}
	ex := <-done
	fmt.Fprintf(c, "%d %d %s\n", os.Getpid(), ex.Code, ex.Err)
	c.Close()
	return 0
}

// A real exec(): the new image accepts on the socket the old one listened
// on, and reaps the child the old one started, with its exit status.
func TestExecKeepsChildAndSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "pg-exec")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	host := exec.Command(os.Args[0], "exec-host", dir)
	host.Stderr = os.Stderr
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Wait() }()
	var c net.Conn
	for end := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if c, err = net.Dial("unix", filepath.Join(dir, "sock")); err == nil {
			break
		}
		if time.Now().After(end) {
			t.Fatal(err)
		}
	}
	defer c.Close()
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Fields(line)
	if len(f) < 2 || f[0] != strconv.Itoa(host.Process.Pid) || f[1] != "7" {
		t.Fatalf("new image reported %q; want pid %d and the child's exit 7", line, host.Process.Pid)
	}
}

func TestExecEnv(t *testing.T) {
	unit := []string{"PATH=/new", "INCUS_COMPOSE_NO_DNS=1", "MAINPID=5", "MAINPIDFDID=9", "JOURNAL_STREAM=1:2", "INVOCATION_ID=x", HandoverEnv + "=stale"}
	self := []string{"PATH=/old", "NOTIFY_SOCKET=/run/systemd/notify", "WATCHDOG_USEC=30000000", "WATCHDOG_PID=5", "JOURNAL_STREAM=3:4", "INVOCATION_ID=x", "OLD_ONLY=1"}
	got := execEnv(unit, self)
	slices.Sort(got)
	want := []string{"INCUS_COMPOSE_NO_DNS=1", "INVOCATION_ID=x", "JOURNAL_STREAM=3:4", "NOTIFY_SOCKET=/run/systemd/notify", "PATH=/new", "WATCHDOG_PID=5", "WATCHDOG_USEC=30000000"}
	if !slices.Equal(got, want) {
		t.Fatalf("execEnv =\n%v\nwant\n%v", got, want)
	}
}
