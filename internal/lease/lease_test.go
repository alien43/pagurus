package lease

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/pagurus/internal/testutil"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestValidityBoundNeverExceedsServerExpiry is the property the self-fence
// rests on: the locally computed ValidUntil is never later than the moment
// etcd actually expires the lease.
func TestValidityBoundNeverExceedsServerExpiry(t *testing.T) {
	ep := testutil.Etcd(t)
	px := testutil.NewProxy(t, ep)
	cli := testutil.Client(t, px.Addr())
	observer := testutil.Client(t, ep)

	opts := Options{TTL: 5 * time.Second, RenewInterval: 500 * time.Millisecond, Margin: 500 * time.Millisecond, OpTimeout: 400 * time.Millisecond, Log: quiet()}
	k, err := Grant(t.Context(), cli, opts)
	if err != nil {
		t.Fatal(err)
	}
	// The moment that matters is when a key attached to the lease vanishes:
	// that is when a rival can create the leader key. (TimeToLive is no use
	// here: it truncates to whole seconds and reads 0 up to 1s early.)
	if _, err := observer.Put(t.Context(), "/probe", "x", clientv3.WithLease(k.ID())); err != nil {
		t.Fatal(err)
	}
	go k.Run()
	defer k.Stop()
	time.Sleep(1500 * time.Millisecond)

	px.SetBlackhole(true)
	bound := k.ValidUntil()
	var expired time.Time
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		r, err := observer.Get(context.Background(), "/probe")
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Kvs) == 0 {
			expired = time.Now()
			break
		}
	}
	if expired.IsZero() {
		t.Fatal("lease never expired")
	}
	if bound.After(expired) {
		t.Fatalf("local bound %s is AFTER server expiry %s", bound.Format(time.StampMilli), expired.Format(time.StampMilli))
	}
	t.Logf("bound precedes server expiry by %s (safe deadline by %s)", expired.Sub(bound), expired.Sub(bound.Add(-opts.Margin)))
	select {
	case <-k.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("keeper did not declare the lease lost after its bound passed")
	}
}

func TestRenewalExtendsBoundAndRevokeStops(t *testing.T) {
	ep := testutil.Etcd(t)
	cli := testutil.Client(t, ep)
	opts := Options{TTL: 5 * time.Second, RenewInterval: 200 * time.Millisecond, Margin: 500 * time.Millisecond, OpTimeout: 200 * time.Millisecond, Log: quiet()}
	k, err := Grant(t.Context(), cli, opts)
	if err != nil {
		t.Fatal(err)
	}
	first := k.ValidUntil()
	go k.Run()
	time.Sleep(700 * time.Millisecond)
	if !k.ValidUntil().After(first) {
		t.Fatal("renewals did not extend the bound")
	}
	if err := k.Revoke(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !k.IsLost() {
		t.Fatal("revoked lease not marked lost")
	}
	r, _ := cli.TimeToLive(t.Context(), k.ID())
	if r.TTL > 0 {
		t.Fatal("lease still alive after revoke")
	}
}

func TestResumeRequiresLiveLease(t *testing.T) {
	ep := testutil.Etcd(t)
	cli := testutil.Client(t, ep)
	opts := Options{TTL: 5 * time.Second, RenewInterval: time.Second, Margin: 500 * time.Millisecond, OpTimeout: time.Second, Log: quiet()}
	k, err := Grant(t.Context(), cli, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Resume(t.Context(), cli, k.ID(), opts); err != nil {
		t.Fatalf("resume of a live lease: %v", err)
	}
	_ = k.Revoke(t.Context())
	if _, err := Resume(t.Context(), cli, k.ID(), opts); err != ErrLost {
		t.Fatalf("resume of a revoked lease: got %v, want ErrLost", err)
	}
	if _, err := Resume(t.Context(), cli, clientv3.LeaseID(12345), opts); err != ErrLost {
		t.Fatalf("resume of an unknown lease: got %v, want ErrLost", err)
	}
}
