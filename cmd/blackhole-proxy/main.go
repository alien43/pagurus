// Command blackhole-proxy is a drill tool, never part of a real deployment: it
// forwards local ports to etcd endpoints so one agent can be cut off from etcd
// without touching the host firewall.
//
//	blackhole-proxy 127.0.0.1:23791=192.0.2.1:2379 127.0.0.1:23792=192.0.2.2:2379 ...
//
// SIGUSR1 starts dropping every byte in both directions (connections stay
// open, new ones hang — a partition, not a refusal); SIGUSR2 heals it.
package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

var blackhole atomic.Bool

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: blackhole-proxy listen=target ...")
		os.Exit(64)
	}
	for _, spec := range os.Args[1:] {
		listen, target, ok := strings.Cut(spec, "=")
		if !ok {
			log.Fatalf("bad spec %q", spec)
		}
		ln, err := net.Listen("tcp", listen)
		if err != nil {
			log.Fatal(err)
		}
		go serve(ln, target)
	}
	sig := make(chan os.Signal, 4)
	signal.Notify(sig, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGINT)
	for s := range sig {
		switch s {
		case syscall.SIGUSR1:
			blackhole.Store(true)
			log.Print("BLACKHOLE ON")
		case syscall.SIGUSR2:
			blackhole.Store(false)
			log.Print("blackhole off")
		default:
			return
		}
	}
}

func serve(ln net.Listener, target string) {
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			for blackhole.Load() {
				time.Sleep(50 * time.Millisecond)
			}
			u, err := net.DialTimeout("tcp", target, 3*time.Second)
			if err != nil {
				c.Close()
				return
			}
			go pipe(u, c)
			go pipe(c, u)
		}()
	}
}

func pipe(dst, src net.Conn) {
	defer dst.Close()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !blackhole.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				log.Print(err)
			}
			return
		}
	}
}
