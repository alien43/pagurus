package coord_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/testutil"
)

const prefix = "/pagurus/v2/svc"

// campaign wins the office the way `conch elect` does: an etcd election on
// conch's key layout, under its own session lease.
func campaign(t *testing.T, cli *clientv3.Client, co *coord.Coord) (*concurrency.Election, int64, clientv3.LeaseID) {
	t.Helper()
	s, err := concurrency.NewSession(cli, concurrency.WithTTL(30))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	e := concurrency.NewElection(s, strings.TrimSuffix(co.ElectionPrefix(), "/"))
	if err := e.Campaign(context.Background(), "host"); err != nil {
		t.Fatal(err)
	}
	if e.Key() != co.ElectionKey(s.Lease()) {
		t.Fatalf("conch's key %q, coord expects %q", e.Key(), co.ElectionKey(s.Lease()))
	}
	return e, e.Rev(), s.Lease()
}

func setup(t *testing.T) (*clientv3.Client, *coord.Coord) {
	cli := testutil.Client(t, testutil.Etcd(t))
	return cli, coord.New(cli, prefix, 2*time.Second)
}

func TestOfficeIsConchsElection(t *testing.T) {
	_, co := setup(t)
	if co.Office() != "pagurus/v2/svc" || co.ElectionPrefix() != "/conch/v1/elect/pagurus/v2/svc/" {
		t.Fatalf("office %q prefix %q", co.Office(), co.ElectionPrefix())
	}
	if k := co.ElectionKey(0x1f); k != "/conch/v1/elect/pagurus/v2/svc/1f" {
		t.Fatalf("key %q", k)
	}
}

func TestLeaderVacantHeldIdentified(t *testing.T) {
	cli, co := setup(t)
	ctx := context.Background()
	if rec, _, _, _, err := co.Leader(ctx); err != nil || rec != nil {
		t.Fatalf("vacant: %+v %v", rec, err)
	}
	_, epoch, lease := campaign(t, cli, co)
	rec, e, l, _, err := co.Leader(ctx)
	if err != nil || rec == nil || rec.Member != "" || e != epoch || l != lease {
		t.Fatalf("held, unidentified: %+v epoch %d lease %x %v", rec, e, l, err)
	}
	// A record from an older term does not name the current holder.
	if _, err := cli.Put(ctx, co.LeaderKey(), `{"member":"old","epoch":1}`); err != nil {
		t.Fatal(err)
	}
	if rec, _, _, _, _ := co.Leader(ctx); rec.Member != "" {
		t.Fatalf("stale record named the holder: %+v", rec)
	}
	if err := co.Guarded(ctx, epoch, lease, co.OpPutLeader(coord.LeaderRecord{Member: "asus", Epoch: epoch}, lease)); err != nil {
		t.Fatal(err)
	}
	if rec, _, _, _, _ := co.Leader(ctx); rec.Member != "asus" {
		t.Fatalf("identified: %+v", rec)
	}
}

// Every leader-side write is refused once the term is over, and to anyone
// who never held it.
func TestGuardedRefusesSupersededTerms(t *testing.T) {
	cli, co := setup(t)
	ctx := context.Background()
	e, epoch, lease := campaign(t, cli, co)
	ms := co.OpPutMS("dell", coord.MemberState{State: coord.Stopped})
	if err := co.Guarded(ctx, epoch, lease, ms); err != nil {
		t.Fatalf("holder refused: %v", err)
	}
	for name, c := range map[string]struct {
		epoch int64
		lease clientv3.LeaseID
	}{"wrong epoch": {epoch + 1, lease}, "wrong lease": {epoch, lease + 1}} {
		if err := co.Guarded(ctx, c.epoch, c.lease, ms); !errors.Is(err, coord.ErrNotLeader) {
			t.Errorf("%s: %v, want ErrNotLeader", name, err)
		}
	}
	if still, err := co.StillLeader(ctx, epoch, lease); err != nil || !still {
		t.Fatalf("StillLeader while holding: %v %v", still, err)
	}
	if err := e.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	if err := co.Guarded(ctx, epoch, lease, ms); !errors.Is(err, coord.ErrNotLeader) {
		t.Fatalf("after resign: %v, want ErrNotLeader", err)
	}
	if still, err := co.StillLeader(ctx, epoch, lease); err != nil || still {
		t.Fatalf("StillLeader after resign: %v %v", still, err)
	}
	// A new term on the same lease has a new epoch: the old one stays refused.
	if err := e.Campaign(ctx, "host"); err != nil {
		t.Fatal(err)
	}
	if err := co.Guarded(ctx, epoch, lease, ms); !errors.Is(err, coord.ErrNotLeader) {
		t.Fatalf("old epoch on a new term: %v, want ErrNotLeader", err)
	}
}

func TestAckFenced(t *testing.T) {
	cli, co := setup(t)
	ctx := context.Background()
	// Vacant: allowed.
	if err := co.AckFenced(ctx, "dell", "op"); err != nil {
		t.Fatalf("vacant: %v", err)
	}
	st, _ := co.MemberStates(ctx)
	if st["dell"].State != coord.Stopped || st["dell"].Reason != "operator fence-ack" {
		t.Fatalf("record %+v", st["dell"])
	}
	_, epoch, lease := campaign(t, cli, co)
	if err := co.AckFenced(ctx, "dell", "op"); err == nil || !strings.Contains(err.Error(), "not identified") {
		t.Fatalf("unidentified holder: %v, want a refusal", err)
	}
	if err := co.Guarded(ctx, epoch, lease, co.OpPutLeader(coord.LeaderRecord{Member: "asus", Epoch: epoch}, lease)); err != nil {
		t.Fatal(err)
	}
	if err := co.AckFenced(ctx, "asus", "op"); err == nil || !strings.Contains(err.Error(), "holds the office") {
		t.Fatalf("the holder: %v, want a refusal", err)
	}
	if err := co.AckFenced(ctx, "dell", "op"); err != nil {
		t.Fatalf("another member while held: %v", err)
	}
}

func TestReportSelfStoppedIsACompareAndSwap(t *testing.T) {
	cli, co := setup(t)
	ctx := context.Background()
	_, epoch, lease := campaign(t, cli, co)
	if err := co.Guarded(ctx, epoch, lease, co.OpPutMS("rpi5", coord.MemberState{State: coord.MayRun, Epoch: epoch})); err != nil {
		t.Fatal(err)
	}
	st, _ := co.MemberStates(ctx)
	seen := st["rpi5"].ModRev
	// The leader records something newer before rpi5's report lands.
	if err := co.Guarded(ctx, epoch, lease, co.OpPutMS("rpi5", coord.MemberState{State: coord.MayRun, Epoch: epoch, Reason: "newer"})); err != nil {
		t.Fatal(err)
	}
	if err := co.ReportSelfStopped(ctx, "rpi5", seen, coord.MemberState{State: coord.Stopped}); err == nil {
		t.Fatal("a report based on a stale read overwrote a newer record")
	}
	st, _ = co.MemberStates(ctx)
	if err := co.ReportSelfStopped(ctx, "rpi5", st["rpi5"].ModRev, coord.MemberState{State: coord.Stopped}); err != nil {
		t.Fatalf("current read: %v", err)
	}
}

func TestMemberStatesRejectsGarbage(t *testing.T) {
	cli, co := setup(t)
	if _, err := cli.Put(context.Background(), co.MSKey("asus"), "{not json"); err != nil {
		t.Fatal(err)
	}
	if _, err := co.MemberStates(context.Background()); err == nil {
		t.Fatal("a corrupt member record was read as something")
	}
}

func TestRegistrationsPinEndpoint(t *testing.T) {
	cli, co := setup(t)
	ctx := context.Background()
	g, err := cli.Grant(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if err := co.Register(ctx, "dell", g.ID, coord.Registration{Version: "v", ConfigHash: "abc"}); err != nil {
		t.Fatal(err)
	}
	regs, err := co.Registrations(ctx)
	if err != nil || regs["dell"].ConfigHash != "abc" {
		t.Fatalf("%+v %v", regs, err)
	}
	if p, _ := co.Pin(ctx); p != "" {
		t.Fatalf("pin %q", p)
	}
	if err := co.SetPin(ctx, "rpi5"); err != nil {
		t.Fatal(err)
	}
	if p, _ := co.Pin(ctx); p != "rpi5" {
		t.Fatalf("pin %q", p)
	}
	if err := co.SetPin(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if p, _ := co.Pin(ctx); p != "" {
		t.Fatalf("pin %q after clearing", p)
	}
	if ep, err := co.GetEndpoint(ctx); err != nil || ep != nil {
		t.Fatalf("no endpoint: %+v %v", ep, err)
	}
	_, epoch, lease := campaign(t, cli, co)
	if err := co.Guarded(ctx, epoch, lease, co.OpPutEndpoint(coord.Endpoint{Member: "asus", Epoch: epoch, Addresses: map[string]string{"web": "192.0.2.1"}}, lease)); err != nil {
		t.Fatal(err)
	}
	if ep, err := co.GetEndpoint(ctx); err != nil || ep.Addresses["web"] != "192.0.2.1" {
		t.Fatalf("%+v %v", ep, err)
	}
}
