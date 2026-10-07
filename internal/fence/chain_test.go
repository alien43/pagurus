package fence_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alien43/pagurus/internal/fence"
)

// stub is a scripted fence for chain tests.
type stub struct {
	kind    string
	applies bool
	stops   bool
	runErr  error
	ran     *[]string
}

func (s stub) Kind() string              { return s.kind }
func (s stub) Budget() time.Duration     { return time.Second }
func (s stub) Applies(fence.Target) bool { return s.applies }
func (s stub) Run(context.Context, fence.Target) error {
	*s.ran = append(*s.ran, s.kind)
	return s.runErr
}
func (s stub) Confirm(context.Context, fence.Target) (bool, error) { return s.stops, nil }

func TestChainOrderSkipAndFirstConfirmWins(t *testing.T) {
	var ran []string
	c := fence.Chain{
		stub{kind: "a", applies: true, stops: false, runErr: errors.New("no"), ran: &ran},
		stub{kind: "b", applies: false, stops: true, ran: &ran},
		stub{kind: "c", applies: true, stops: true, ran: &ran},
		stub{kind: "d", applies: true, stops: true, ran: &ran},
	}
	res := c.Run(context.Background(), fence.Target{Member: "m"}, nil)
	if !res.Stopped || res.By != "c" {
		t.Fatalf("result %s, want stopped by c", res)
	}
	if got := len(ran); got != 2 || ran[0] != "a" || ran[1] != "c" {
		t.Fatalf("ran %v, want [a c] (b skipped, d never reached)", ran)
	}
	if !res.Steps[1].Skipped || res.Steps[0].Err == nil {
		t.Fatalf("steps %+v", res.Steps)
	}
	if c.Budget() != 4*time.Second {
		t.Fatalf("budget %v, want the sum", c.Budget())
	}
}

func TestChainAllFailIsUnfenced(t *testing.T) {
	var ran []string
	res := fence.Chain{stub{kind: "a", applies: true, ran: &ran}, stub{kind: "b", applies: true, ran: &ran}}.
		Run(context.Background(), fence.Target{}, nil)
	if res.Stopped || res.By != "" || len(ran) != 2 {
		t.Fatalf("result %s ran %v", res, ran)
	}
}

func TestEmptyOrAllSkippedChainIsUnverified(t *testing.T) {
	tg := fence.Target{Member: "m", Types: []string{"virtual-machine"}}
	if (fence.Chain{}).Verified(tg) {
		t.Fatal("an empty chain is verified")
	}
	if res := (fence.Chain{}).Run(context.Background(), tg, nil); res.Stopped {
		t.Fatal("an empty chain confirmed a stop")
	}
	vmBlind := fence.Chain{fence.IncusStop{B: time.Second}, fence.CgroupKill{B: time.Second}}
	if vmBlind.Verified(tg) {
		t.Fatal("the container chain claims to verify a VM")
	}
	if !vmBlind.Verified(fence.Target{Member: "m"}) {
		t.Fatal("the container chain must verify containers")
	}
}

func TestBuildSelfRejectsBadSpecs(t *testing.T) {
	for _, c := range []struct {
		name string
		spec fence.Spec
	}{
		{"unknown kind", fence.Spec{Kind: "stonith", Budget: time.Second}},
		{"no budget", fence.Spec{Kind: fence.KindCgroupKill}},
		{"exec without command", fence.Spec{Kind: fence.KindExec, Budget: time.Second}},
	} {
		if _, err := fence.BuildSelf(nil, []fence.Spec{c.spec}); err == nil {
			t.Errorf("%s: accepted", c.name)
		}
	}
	if c, err := fence.BuildSelf(nil, nil); err != nil || len(c) != 0 {
		t.Fatalf("empty chain: %v %v", c, err)
	}
}
