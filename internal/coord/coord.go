// Package coord holds the etcd schema of one pagurus service.
//
//	<prefix>/leader         LeaderRecord, attached to the leader's lease. Its
//	                        create revision is the epoch (fencing token).
//	<prefix>/ms/<member>    MemberState, no lease: whether <member> may still be
//	                        running the workload. Survives every crash.
//	<prefix>/reg/<member>   Registration, attached to the agent's lease.
//	<prefix>/endpoint       Endpoint, attached to the leader's lease.
//	<prefix>/pin            plain member name: only it may campaign (planned move).
//
// The may-run/stopped record is what makes fencing complete: a new leader
// fences every member whose record says may-run, not just the last leader, so
// a chain of unclean failovers cannot leave a forgotten copy running.
package coord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	MayRun  = "may-run"
	Stopped = "stopped"
)

// ErrNotLeader means a leader-guarded write was refused: the caller's epoch is
// no longer the current one.
var ErrNotLeader = errors.New("not the leader (epoch superseded)")

type LeaderRecord struct {
	Member   string    `json:"member"`
	AgentID  string    `json:"agent_id"`
	Acquired time.Time `json:"acquired"`
}

// Caps is what a member advertises about its own ability to self-fence. A
// fencer that cannot reach a member relies on the Caps recorded in that
// member's last may-run record, never on its (lease-bound, now vanished)
// registration.
type Caps struct {
	WatchdogMode    string        `json:"watchdog_mode"`
	WatchdogTimeout time.Duration `json:"watchdog_timeout"`
	// FencesByReboot: if this member's agent stops petting, the host resets
	// within WatchdogTimeout. Only true for an armed device watchdog or a
	// systemd watchdog whose unit has a reboot FailureAction.
	FencesByReboot bool          `json:"fences_by_reboot"`
	SelfFenceLead  time.Duration `json:"self_fence_lead"`
	// NoVerifiedSelfFence: no fence in this member's self chain applies to its
	// workload, so its self-fence can never be confirmed. Peers must not
	// trust it under fence.unverified=proceed. (Zero value: the prototype's
	// chain, which is verified.)
	NoVerifiedSelfFence bool `json:"no_verified_self_fence,omitempty"`
}

type MemberState struct {
	State  string    `json:"state"`
	Epoch  int64     `json:"epoch"`
	By     string    `json:"by"`
	Reason string    `json:"reason,omitempty"`
	At     time.Time `json:"at"`
	Caps   Caps      `json:"caps"`
	// ModRev is filled on read (not stored).
	ModRev int64 `json:"-"`
}

type Registration struct {
	AgentID string    `json:"agent_id"`
	Version string    `json:"version"`
	Arch    string    `json:"arch"`
	Caps    Caps      `json:"caps"`
	Started time.Time `json:"started"`
}

type Endpoint struct {
	Member    string            `json:"member"`
	Epoch     int64             `json:"epoch"`
	Addresses map[string]string `json:"addresses"`
}

type Coord struct {
	cli       *clientv3.Client
	prefix    string
	opTimeout time.Duration
}

func New(cli *clientv3.Client, prefix string, opTimeout time.Duration) *Coord {
	return &Coord{cli: cli, prefix: prefix, opTimeout: opTimeout}
}

func (c *Coord) LeaderKey() string           { return c.prefix + "/leader" }
func (c *Coord) PinKey() string              { return c.prefix + "/pin" }
func (c *Coord) EndpointKey() string         { return c.prefix + "/endpoint" }
func (c *Coord) MSKey(member string) string  { return c.prefix + "/ms/" + member }
func (c *Coord) RegKey(member string) string { return c.prefix + "/reg/" + member }
func (c *Coord) Client() *clientv3.Client    { return c.cli }

func (c *Coord) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.opTimeout)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// Campaign tries to create the leader key atomically. On success it returns
// the epoch (the key's create revision). On failure it returns the current
// holder's epoch.
func (c *Coord) Campaign(ctx context.Context, lease clientv3.LeaseID, rec LeaderRecord) (won bool, epoch int64, err error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	k := c.LeaderKey()
	resp, err := c.cli.Txn(cctx).
		If(clientv3.Compare(clientv3.CreateRevision(k), "=", 0)).
		Then(clientv3.OpPut(k, mustJSON(rec), clientv3.WithLease(lease))).
		Else(clientv3.OpGet(k)).
		Commit()
	if err != nil {
		return false, 0, err
	}
	if resp.Succeeded {
		return true, resp.Header.Revision, nil
	}
	if kvs := resp.Responses[0].GetResponseRange().Kvs; len(kvs) > 0 {
		return false, kvs[0].CreateRevision, nil
	}
	return false, 0, nil
}

// Leader returns the current leader, or nil if the office is vacant. rev is the
// store revision of the read, to start a watch from.
func (c *Coord) Leader(ctx context.Context) (rec *LeaderRecord, epoch int64, lease clientv3.LeaseID, rev int64, err error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Get(cctx, c.LeaderKey())
	if err != nil {
		return nil, 0, 0, 0, err
	}
	rev = resp.Header.Revision
	if len(resp.Kvs) == 0 {
		return nil, 0, 0, rev, nil
	}
	kv := resp.Kvs[0]
	rec = &LeaderRecord{}
	if err := json.Unmarshal(kv.Value, rec); err != nil {
		return nil, 0, 0, rev, fmt.Errorf("decode leader record: %w", err)
	}
	return rec, kv.CreateRevision, clientv3.LeaseID(kv.Lease), rev, nil
}

func (c *Coord) leaderGuard(epoch int64, lease clientv3.LeaseID) []clientv3.Cmp {
	k := c.LeaderKey()
	return []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(k), "=", epoch),
		clientv3.Compare(clientv3.LeaseValue(k), "=", lease),
	}
}

// Guarded applies ops only if (epoch, lease) still holds the office. This is
// the linearizable "am I still the leader" check that authorises every
// leader-side mutation.
func (c *Coord) Guarded(ctx context.Context, epoch int64, lease clientv3.LeaseID, ops ...clientv3.Op) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Txn(cctx).If(c.leaderGuard(epoch, lease)...).Then(ops...).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrNotLeader
	}
	return nil
}

func (c *Coord) OpPutMS(member string, ms MemberState) clientv3.Op {
	return clientv3.OpPut(c.MSKey(member), mustJSON(ms))
}

func (c *Coord) OpPutEndpoint(ep Endpoint, lease clientv3.LeaseID) clientv3.Op {
	return clientv3.OpPut(c.EndpointKey(), mustJSON(ep), clientv3.WithLease(lease))
}

// Resign deletes the leader key if it still belongs to epoch.
func (c *Coord) Resign(ctx context.Context, epoch int64) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	k := c.LeaderKey()
	_, err := c.cli.Txn(cctx).
		If(clientv3.Compare(clientv3.CreateRevision(k), "=", epoch)).
		Then(clientv3.OpDelete(k), clientv3.OpDelete(c.EndpointKey())).
		Commit()
	return err
}

// MemberStates returns every member's may-run record.
func (c *Coord) MemberStates(ctx context.Context) (map[string]MemberState, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	pfx := c.prefix + "/ms/"
	resp, err := c.cli.Get(cctx, pfx, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := map[string]MemberState{}
	for _, kv := range resp.Kvs {
		var ms MemberState
		if err := json.Unmarshal(kv.Value, &ms); err != nil {
			return nil, fmt.Errorf("decode %s: %w", kv.Key, err)
		}
		ms.ModRev = kv.ModRevision
		out[strings.TrimPrefix(string(kv.Key), pfx)] = ms
	}
	return out, nil
}

// ReportSelfStopped records that member has verified, locally, that it runs
// nothing. Only member itself calls this, and only while not leading; the
// compare on the record's mod revision keeps it from overwriting a record it
// has not seen.
func (c *Coord) ReportSelfStopped(ctx context.Context, member string, prevModRev int64, ms MemberState) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	k := c.MSKey(member)
	resp, err := c.cli.Txn(cctx).
		If(clientv3.Compare(clientv3.ModRevision(k), "=", prevModRev)).
		Then(clientv3.OpPut(k, mustJSON(ms))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("member state of %s changed concurrently", member)
	}
	return nil
}

// AckFenced is the operator override (`pagurus fence-ack`): it records that a
// human has verified member is not running the workload. Refused while member
// holds the office.
func (c *Coord) AckFenced(ctx context.Context, member, by string) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	rec, _, _, _, err := c.Leader(cctx)
	if err != nil {
		return err
	}
	if rec != nil && rec.Member == member {
		return fmt.Errorf("%s currently holds the office; refusing", member)
	}
	ms := MemberState{State: Stopped, By: by, Reason: "operator fence-ack", At: time.Now().UTC()}
	k := c.LeaderKey()
	cmp := clientv3.Compare(clientv3.CreateRevision(k), "=", 0)
	if rec != nil {
		cmp = clientv3.Compare(clientv3.Value(k), "=", mustJSON(*rec))
	}
	resp, err := c.cli.Txn(cctx).If(cmp).Then(c.OpPutMS(member, ms)).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return errors.New("office changed while acknowledging; re-check and retry")
	}
	return nil
}

func (c *Coord) Register(ctx context.Context, member string, lease clientv3.LeaseID, reg Registration) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	_, err := c.cli.Put(cctx, c.RegKey(member), mustJSON(reg), clientv3.WithLease(lease))
	return err
}

func (c *Coord) Registrations(ctx context.Context) (map[string]Registration, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	pfx := c.prefix + "/reg/"
	resp, err := c.cli.Get(cctx, pfx, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := map[string]Registration{}
	for _, kv := range resp.Kvs {
		var r Registration
		if err := json.Unmarshal(kv.Value, &r); err == nil {
			out[strings.TrimPrefix(string(kv.Key), pfx)] = r
		}
	}
	return out, nil
}

func (c *Coord) Pin(ctx context.Context) (string, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Get(cctx, c.PinKey())
	if err != nil || len(resp.Kvs) == 0 {
		return "", err
	}
	return string(resp.Kvs[0].Value), nil
}

func (c *Coord) SetPin(ctx context.Context, member string) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	var err error
	if member == "" {
		_, err = c.cli.Delete(cctx, c.PinKey())
	} else {
		_, err = c.cli.Put(cctx, c.PinKey(), member)
	}
	return err
}

func (c *Coord) GetEndpoint(ctx context.Context) (*Endpoint, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Get(cctx, c.EndpointKey())
	if err != nil || len(resp.Kvs) == 0 {
		return nil, err
	}
	ep := &Endpoint{}
	return ep, json.Unmarshal(resp.Kvs[0].Value, ep)
}

// WatchOffice watches the whole service prefix from rev+1, so callers wake on
// leader and pin changes (and, harmlessly, on ms/reg writes) and re-read.
// WithRequireLeader makes the watch fail rather than hang on a member that
// lost the etcd quorum.
func (c *Coord) WatchOffice(ctx context.Context, rev int64) clientv3.WatchChan {
	return c.cli.Watch(clientv3.WithRequireLeader(ctx), c.prefix+"/", clientv3.WithPrefix(), clientv3.WithRev(rev+1))
}
