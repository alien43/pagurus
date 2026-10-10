// Package testutil runs throwaway etcds (one member, or a cluster) and a TCP
// proxy that can blackhole traffic to simulate a network partition.
package testutil

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func freePort(t testing.TB) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func etcdBin(t testing.TB) string {
	t.Helper()
	bin, err := exec.LookPath("etcd")
	if err != nil {
		for _, p := range []string{"/usr/local/bin/etcd", "/usr/bin/etcd"} {
			if _, e := os.Stat(p); e == nil {
				bin, err = p, nil
			}
		}
	}
	if err != nil {
		t.Skip("etcd binary not found")
	}
	return bin
}

// Etcd starts an etcd for the test and returns its client URL host:port.
// The test is skipped if no etcd binary is available.
func Etcd(t testing.TB) string {
	t.Helper()
	bin := etcdBin(t)
	dir := t.TempDir()
	cp, pp := freePort(t), freePort(t)
	client := fmt.Sprintf("127.0.0.1:%d", cp)
	cmd := exec.Command(bin,
		"--name", "t", "--data-dir", filepath.Join(dir, "data"),
		"--listen-client-urls", "http://"+client, "--advertise-client-urls", "http://"+client,
		"--listen-peer-urls", fmt.Sprintf("http://127.0.0.1:%d", pp),
		"--initial-advertise-peer-urls", fmt.Sprintf("http://127.0.0.1:%d", pp),
		"--initial-cluster", fmt.Sprintf("t=http://127.0.0.1:%d", pp),
		"--log-level", "error")
	logf, _ := os.Create(filepath.Join(dir, "etcd.log"))
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); logf.Close() })
	cli := Client(t, client)
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := cli.Get(t.Context(), "x"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("etcd did not start")
		}
		time.Sleep(100 * time.Millisecond)
	}
	return client
}

func Client(t testing.TB, endpoint string, more ...string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: append([]string{endpoint}, more...), DialTimeout: 2 * time.Second,
		DialKeepAliveTime: time.Second, DialKeepAliveTimeout: time.Second, Logger: zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// Proxy forwards TCP to a target. While blackholed, connections stay open but
// every byte is dropped and new connections hang — a partition, as seen from
// both ends.
type Proxy struct {
	ln        net.Listener
	target    string
	blackhole atomic.Bool
	mu        sync.Mutex
	conns     []net.Conn
}

func NewProxy(t testing.TB, target string) *Proxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{ln: ln, target: target}
	go p.serve()
	t.Cleanup(p.Close)
	return p
}

func (p *Proxy) Addr() string { return p.ln.Addr().String() }

func (p *Proxy) SetBlackhole(on bool) { p.blackhole.Store(on) }

func (p *Proxy) Close() {
	p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}

func (p *Proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *Proxy) handle(c net.Conn) {
	for p.blackhole.Load() {
		time.Sleep(50 * time.Millisecond)
	}
	u, err := net.Dial("tcp", p.target)
	if err != nil {
		c.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, c, u)
	p.mu.Unlock()
	go p.pipe(u, c)
	go p.pipe(c, u)
}

func (p *Proxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.blackhole.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				dst.Close()
			}
			return
		}
	}
}

// Cluster is a throwaway 3-member (or n-member) etcd on 127.0.0.1. Each
// member's client port sits behind a Proxy, so a test can make one member
// unreachable to every client while the members still talk to each other.
type Cluster struct {
	Members []*Member
}

type Member struct {
	Name   string
	Client *Proxy // what clients dial
	cmd    *exec.Cmd
}

// Endpoints are the members' client proxies.
func (c *Cluster) Endpoints() []string {
	var eps []string
	for _, m := range c.Members {
		eps = append(eps, m.Client.Addr())
	}
	return eps
}

// Pause freezes the member's process (SIGSTOP), as a host that hangs or
// drops off the network would look to its peers; Resume continues it.
func (m *Member) Pause()  { _ = m.cmd.Process.Signal(syscall.SIGSTOP) }
func (m *Member) Resume() { _ = m.cmd.Process.Signal(syscall.SIGCONT) }

// EtcdCluster starts an n-member etcd cluster and waits until it serves.
func EtcdCluster(t testing.TB, n int) *Cluster {
	t.Helper()
	bin := etcdBin(t)
	type ports struct{ client, peer int }
	ps := make([]ports, n)
	var initial []string
	for i := range ps {
		ps[i] = ports{freePort(t), freePort(t)}
		initial = append(initial, fmt.Sprintf("m%d=http://127.0.0.1:%d", i, ps[i].peer))
	}
	c := &Cluster{}
	for i, p := range ps {
		dir := t.TempDir()
		client := fmt.Sprintf("127.0.0.1:%d", p.client)
		peer := fmt.Sprintf("http://127.0.0.1:%d", p.peer)
		cmd := exec.Command(bin,
			"--name", fmt.Sprintf("m%d", i), "--data-dir", filepath.Join(dir, "data"),
			"--listen-client-urls", "http://"+client, "--advertise-client-urls", "http://"+client,
			"--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer,
			"--initial-cluster", strings.Join(initial, ","), "--initial-cluster-state", "new",
			"--initial-cluster-token", "pagurus-test",
			"--heartbeat-interval", "100", "--election-timeout", "1000",
			"--log-level", "error")
		logf, _ := os.Create(filepath.Join(dir, "etcd.log"))
		cmd.Stdout, cmd.Stderr = logf, logf
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Signal(syscall.SIGCONT)
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			logf.Close()
		})
		c.Members = append(c.Members, &Member{Name: fmt.Sprintf("m%d", i), Client: NewProxy(t, client), cmd: cmd})
	}
	cli := Client(t, c.Endpoints()[0], c.Endpoints()[1:]...)
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err := cli.Put(ctx, "pagurus-test/ready", "1")
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("etcd cluster did not start: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return c
}
