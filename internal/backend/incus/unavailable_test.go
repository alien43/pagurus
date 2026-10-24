package incus_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/backend"
	"github.com/alien43/pagurus/internal/backend/incus"
	"github.com/alien43/pagurus/internal/fence/fencetest"
)

// A read Incus does not answer is backend.ErrAPIUnavailable; one it answers
// with an error is not (the agent holds on the first and judges the second).
func TestReadsMarkAnUnavailableAPI(t *testing.T) {
	api := fencetest.NewFakeIncus("asus", "dell")
	api.AddInstance("p-asus", incus.Instance{Name: "web", Status: incus.StatusRunning, Location: "asus", Type: "container"})
	d := driver(api)
	short := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 50*time.Millisecond)
	}
	reads := map[string]func(context.Context) error{
		"List":         func(ctx context.Context) error { _, err := d.List(ctx, "asus"); return err },
		"IPv4":         func(ctx context.Context) error { _, err := d.IPv4(ctx, "asus", "web"); return err },
		"MemberOnline": func(ctx context.Context) error { _, err := d.MemberOnline(ctx, "dell"); return err },
	}
	cases := []struct {
		name  string
		setup func()
		want  bool
	}{
		{"hang", func() { api.Hang = true }, true},
		{"connection refused", func() { api.Err = fmt.Errorf("dial unix: %w", syscall.ECONNREFUSED) }, true},
		{"socket missing", func() {
			api.Err = &net.OpError{Op: "dial", Net: "unix", Err: os.NewSyscallError("connect", syscall.ENOENT)}
		}, true},
		{"answered with an error", func() { api.Err = errors.New("Failed to fetch from \"projects_config\" table") }, false},
	}
	for _, c := range cases {
		api.Hang, api.Err = false, nil
		c.setup()
		for name, read := range reads {
			ctx, cancel := short()
			err := read(ctx)
			cancel()
			if err == nil || errors.Is(err, backend.ErrAPIUnavailable) != c.want {
				t.Errorf("%s / %s: err=%v, want unavailable=%v", c.name, name, err, c.want)
			}
		}
	}
	// A project the client may not read is refused (403): an answer.
	api.Hang, api.Err = false, nil
	api.Forbidden = map[string]bool{"p-asus": true}
	if _, err := d.List(context.Background(), "asus"); err == nil || errors.Is(err, backend.ErrAPIUnavailable) {
		t.Errorf("forbidden project: %v, want an error that is not unavailable", err)
	}
}

// The real client against an incusd that accepts and never answers (the
// 2026-10-21..23 stall): the error the agents logged then,
// `Get "http://unix.socket/1.0/projects/...": context deadline exceeded`,
// is marked unavailable.
func TestRealClientTimeoutIsUnavailable(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "pg-incus")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "unix.socket")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { close(release); srv.Close() })

	d := incus.New(incus.Options{ProjectPrefix: "p", CgroupRoot: "/nonexistent", API: incus.NewClient(incus.ClientOptions{Socket: sock})})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = d.List(ctx, "asus")
	if !errors.Is(err, backend.ErrAPIUnavailable) {
		t.Fatalf("stalled incusd: %v, want backend.ErrAPIUnavailable", err)
	}
	t.Logf("stalled: %v", err)

	// incusd down: nothing listens on the socket.
	gone := incus.New(incus.Options{ProjectPrefix: "p", CgroupRoot: "/nonexistent", API: incus.NewClient(incus.ClientOptions{Socket: filepath.Join(dir, "none.socket")})})
	_, err = gone.List(context.Background(), "asus")
	if !errors.Is(err, backend.ErrAPIUnavailable) {
		t.Fatalf("no incusd: %v, want backend.ErrAPIUnavailable", err)
	}
	t.Logf("down: %v", err)
}
