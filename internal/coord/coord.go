// Package coord holds the etcd schema of one pagurus service.
//
// The office itself is a conch election (`conch elect <Office()>`):
//
//	/conch/v1/elect/<office>/<lease hex>   one key per candidate, attached to
//	                        its conch session lease; the lowest create
//	                        revision holds the office. That revision is the
//	                        epoch (CONCH_REV, the fencing token).
//
// where <office> is the service prefix without its leading slash. Under the
// service prefix:
//
//	<prefix>/leader         LeaderRecord, attached to the holder's conch lease,
//	                        written by the holder: which member holds the
//	                        office (conch's own value names only the host).
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
	Epoch    int64     `json:"epoch"`
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
	// Margin is this member's lease margin: with SelfFenceLead, how long
	// after its lease's server-side expiry it may still be fencing itself.
	Margin time.Duration `json:"margin,omitempty"`
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
	// Versions of what the agent drives, e.g. {"incus": "7.5.1",
	// "incus-compose": "1.3.4"}; empty values were unreadable.
	Versions map[string]string `json:"versions,omitempty"`
	// ConfigHash is config.SafetyHash(); peers refuse to campaign while
	// theirs differs. Empty from agents that predate it (not compared).
	ConfigHash string `json:"config_hash,omitempty"`
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

func (c *Coord) LeaderKey() string { return c.prefix + "/leader" }

// Office is the conch election name for this service; ElectionPrefix is where
// conch keeps its candidates, and ElectionKey the candidate key of a lease.
func (c *Coord) Office() string         { return strings.TrimPrefix(c.prefix, "/") }
func (c *Coord) ElectionPrefix() string { return "/conch/v1/elect/" + c.Office() + "/" }
func (c *Coord) ElectionKey(lease clientv3.LeaseID) string {
	return fmt.Sprintf("%s%x", c.ElectionPrefix(), int64(lease))
}
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

// Leader returns the current office holder, or nil if the office is vacant.
// epoch and lease are the holder's election key's create revision and lease.
// rec.Member is "" while the holder has not yet written its LeaderRecord. rev
// is the store revision of the read, to start a watch from.
func (c *Coord) Leader(ctx context.Context) (rec *LeaderRecord, epoch int64, lease clientv3.LeaseID, rev int64, err error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Txn(cctx).Then(
		clientv3.OpGet(c.ElectionPrefix(), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend), clientv3.WithLimit(1)),
		clientv3.OpGet(c.LeaderKey()),
	).Commit()
	if err != nil {
		return nil, 0, 0, 0, err
	}
	rev = resp.Header.Revision
	holders := resp.Responses[0].GetResponseRange().Kvs
	if len(holders) == 0 {
		return nil, 0, 0, rev, nil
	}
	h := holders[0]
	rec = &LeaderRecord{}
	if kvs := resp.Responses[1].GetResponseRange().Kvs; len(kvs) > 0 {
		var r LeaderRecord
		if err := json.Unmarshal(kvs[0].Value, &r); err == nil && r.Epoch == h.CreateRevision {
			rec = &r
		}
	}
	return rec, h.CreateRevision, clientv3.LeaseID(h.Lease), rev, nil
}

// leaderGuard: (epoch, lease) still holds the office. A candidate key, once
// lowest, stays lowest while it exists (later keys get higher revisions), so
// its existence with that create revision is the whole check.
func (c *Coord) leaderGuard(epoch int64, lease clientv3.LeaseID) []clientv3.Cmp {
	return []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(c.ElectionKey(lease)), "=", epoch)}
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

// StillLeader reads whether (epoch, lease) still holds the office.
func (c *Coord) StillLeader(ctx context.Context, epoch int64, lease clientv3.LeaseID) (bool, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Get(cctx, c.ElectionKey(lease))
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) == 1 && resp.Kvs[0].CreateRevision == epoch, nil
}

func (c *Coord) OpPutLeader(rec LeaderRecord, lease clientv3.LeaseID) clientv3.Op {
	return clientv3.OpPut(c.LeaderKey(), mustJSON(rec), clientv3.WithLease(lease))
}

func (c *Coord) OpPutMS(member string, ms MemberState) clientv3.Op {
	return clientv3.OpPut(c.MSKey(member), mustJSON(ms))
}

func (c *Coord) OpPutEndpoint(ep Endpoint, lease clientv3.LeaseID) clientv3.Op {
	return clientv3.OpPut(c.EndpointKey(), mustJSON(ep), clientv3.WithLease(lease))
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
	rec, epoch, lease, _, err := c.Leader(cctx)
	if err != nil {
		return err
	}
	if rec != nil && rec.Member == member {
		return fmt.Errorf("%s currently holds the office; refusing", member)
	}
	if rec != nil && rec.Member == "" {
		return errors.New("the office was just taken and its holder has not identified itself yet; retry")
	}
	ms := MemberState{State: Stopped, By: by, Reason: "operator fence-ack", At: time.Now().UTC()}
	// The office must not change hands between the read and the write: the
	// same holder, or still nobody (no candidate key at all).
	cmp := clientv3.Compare(clientv3.CreateRevision(c.ElectionPrefix()), "=", 0).WithPrefix()
	if rec != nil {
		cmp = c.leaderGuard(epoch, lease)[0]
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
