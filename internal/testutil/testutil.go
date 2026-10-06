// Package testutil runs a throwaway single-member etcd and a TCP proxy that can
// blackhole traffic to simulate a network partition.
package testutil

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
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

// Etcd starts an etcd for the test and returns its client URL host:port.
// The test is skipped if no etcd binary is available.
func Etcd(t testing.TB) string {
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

func Client(t testing.TB, endpoint string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 2 * time.Second,
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
