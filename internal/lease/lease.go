// Package lease keeps an etcd lease alive and tracks, on this host's monotonic
// clock, a conservative bound on how long the lease is guaranteed to be valid.
//
// The bound is the heart of the self-fence: a renewal sent at local time t that
// succeeds with TTL d means the etcd leader extended the lease to at least
// (its receipt time + d), which is no earlier than t + d in real time. So
// "validUntil = send time of the newest successful renewal + TTL" can never be
// later than the moment etcd actually expires the lease, provided the two
// clocks run at about the same rate. Margin absorbs rate differences.
//
// etcd leader elections only ever *extend* leases, so they cannot invalidate
// the bound either.
package lease

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Options struct {
	TTL           time.Duration
	RenewInterval time.Duration
	Margin        time.Duration
	OpTimeout     time.Duration
	Log           *slog.Logger
}

// ErrLost means etcd no longer has the lease.
var ErrLost = errors.New("lease lost")

type Keeper struct {
	cli  *clientv3.Client
	opts Options
	id   clientv3.LeaseID

	mu         sync.Mutex
	validUntil time.Time // carries a monotonic reading
	lastErr    error
	lost       bool
	lostCh     chan struct{}
	stopOnce   sync.Once
	stopCh     chan struct{}

	// now is replaceable in tests.
	now func() time.Time
}

func newKeeper(cli *clientv3.Client, id clientv3.LeaseID, opts Options) *Keeper {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &Keeper{cli: cli, opts: opts, id: id, lostCh: make(chan struct{}), stopCh: make(chan struct{}), now: time.Now}
}

// Grant creates a new lease.
func Grant(ctx context.Context, cli *clientv3.Client, opts Options) (*Keeper, error) {
	k := newKeeper(cli, 0, opts)
	sent := k.now()
	cctx, cancel := context.WithTimeout(ctx, opts.OpTimeout)
	defer cancel()
	resp, err := cli.Grant(cctx, int64(opts.TTL/time.Second))
	if err != nil {
		return nil, err
	}
	k.id = resp.ID
	k.validUntil = sent.Add(time.Duration(resp.TTL) * time.Second)
	return k, nil
}

// Resume adopts an existing lease (e.g. after an agent restart). Validity is
// only established by a successful renewal made now; nothing is assumed about
// the time the lease was unattended.
func Resume(ctx context.Context, cli *clientv3.Client, id clientv3.LeaseID, opts Options) (*Keeper, error) {
	k := newKeeper(cli, id, opts)
	if err := k.renew(ctx); err != nil {
		return nil, err
	}
	return k, nil
}

func (k *Keeper) ID() clientv3.LeaseID { return k.id }

// ValidUntil is the local-clock time until which the lease certainly exists.
func (k *Keeper) ValidUntil() time.Time {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.validUntil
}

// SafeDeadline is ValidUntil minus the clock-rate margin: every action that
// requires holding the lease must be finished by then.
func (k *Keeper) SafeDeadline() time.Time { return k.ValidUntil().Add(-k.opts.Margin) }

// Remaining is the time left until SafeDeadline (negative once passed).
func (k *Keeper) Remaining() time.Duration { return k.SafeDeadline().Sub(k.now()) }

// Lost is closed once etcd reports the lease gone, or ValidUntil has passed.
func (k *Keeper) Lost() <-chan struct{} { return k.lostCh }

func (k *Keeper) IsLost() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.lost
}

func (k *Keeper) LastError() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.lastErr
}

func (k *Keeper) markLost(reason string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.lost {
		return
	}
	k.lost = true
	close(k.lostCh)
	k.opts.Log.Warn("lease lost", "lease", int64(k.id), "reason", reason)
}

func (k *Keeper) renew(ctx context.Context) error {
	sent := k.now()
	cctx, cancel := context.WithTimeout(ctx, k.opts.OpTimeout)
	defer cancel()
	resp, err := k.cli.KeepAliveOnce(cctx, k.id)
	if err == nil && resp.TTL <= 0 {
		err = rpctypes.ErrLeaseNotFound
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.lastErr = err
	if err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return ErrLost
		}
		return err
	}
	if v := sent.Add(time.Duration(resp.TTL) * time.Second); v.After(k.validUntil) {
		k.validUntil = v
	}
	return nil
}

// Stop ends renewals for good; the lease then expires on its own unless
// revoked. Idempotent.
func (k *Keeper) Stop() { k.stopOnce.Do(func() { close(k.stopCh) }) }

// Run renews the lease every RenewInterval until Stop, or until the lease is
// lost. It never blocks longer than OpTimeout per attempt. It deliberately
// takes no context: a term's lease must outlive the agent's own shutdown
// signal until the release has finished (see agent.lead).
func (k *Keeper) Run() {
	t := time.NewTicker(k.opts.RenewInterval)
	defer t.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-k.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	failures := 0
	for {
		select {
		case <-k.stopCh:
			return
		case <-k.lostCh:
			return
		case <-t.C:
		}
		err := k.renew(ctx)
		switch {
		case err == nil:
			if failures > 0 {
				k.opts.Log.Info("lease renewals recovered", "after_failures", failures)
			}
			failures = 0
		case errors.Is(err, ErrLost):
			k.markLost("etcd: lease not found")
			return
		case ctx.Err() != nil:
			return
		default:
			failures++
			k.opts.Log.Warn("lease renewal failed", "err", err, "consecutive", failures, "remaining", k.Remaining().Round(time.Millisecond))
		}
		if !k.now().Before(k.ValidUntil()) {
			k.markLost("validity bound passed without a successful renewal")
			return
		}
	}
}

// Revoke stops renewals and deletes the lease (and with it every key attached
// to it). Best effort: if etcd is unreachable the lease simply expires.
func (k *Keeper) Revoke(ctx context.Context) error {
	k.Stop()
	cctx, cancel := context.WithTimeout(ctx, k.opts.OpTimeout)
	defer cancel()
	_, err := k.cli.Revoke(cctx, k.id)
	if err == nil || errors.Is(err, rpctypes.ErrLeaseNotFound) {
		k.markLost("revoked")
		return nil
	}
	return err
}
