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
//	<prefix>/refresh        plain token: the leader recreates its workload in
//	                        place (fresh instance from the current compose file
//	                        and the image in its project), once per new token.
//	<prefix>/refresh-done   the last token the leader applied, written once the
//	                        workload is healthy again.
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
	// FenceHash is config.FenceHash() (skew refuses to campaign) and
	// StartHash config.StartHash() (skew only warns). Empty from agents that
	// predate them: those are compared by ConfigHash.
	FenceHash string `json:"fence_hash,omitempty"`
	StartHash string `json:"start_hash,omitempty"`
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
func (c *Coord) RefreshKey() string          { return c.prefix + "/refresh" }
func (c *Coord) RefreshDoneKey() string      { return c.prefix + "/refresh-done" }
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

// PinFor pins the office to member under a lease of ttl, so a pin whose
// setter dies (a killed `pagurus move`) expires instead of leaving only that
// member allowed to campaign, forever. It returns the pin's mod revision, for
// ClearPinIf and for telling records written since the pin from older ones.
func (c *Coord) PinFor(ctx context.Context, member string, ttl time.Duration) (int64, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	lease, err := c.cli.Grant(cctx, int64(max(ttl/time.Second, 1)))
	if err != nil {
		return 0, err
	}
	resp, err := c.cli.Put(cctx, c.PinKey(), member, clientv3.WithLease(lease.ID))
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// ClearPinIf deletes the pin only while it is still the one written at rev,
// so a move never clears a pin another move wrote since. cleared is false if
// the pin had changed or was gone.
func (c *Coord) ClearPinIf(ctx context.Context, rev int64) (cleared bool, err error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Txn(cctx).
		If(clientv3.Compare(clientv3.ModRevision(c.PinKey()), "=", rev)).
		Then(clientv3.OpDelete(c.PinKey())).Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
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

// GetString reads a plain-text key ("" when absent).
func (c *Coord) GetString(ctx context.Context, key string) (string, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Get(cctx, key)
	if err != nil || len(resp.Kvs) == 0 {
		return "", err
	}
	return string(resp.Kvs[0].Value), nil
}

// PutString writes a plain-text key.
func (c *Coord) PutString(ctx context.Context, key, value string) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	_, err := c.cli.Put(cctx, key, value)
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

// ------------------------------------------------------------- replication -
//
//	<prefix>/replica/source        ReplicaSource: the leader's newest snapshot,
//	                               a guarded write after every round.
//	<prefix>/replica/led/<member>  the last epoch <member> led with replication
//	                               (decimal), a guarded write before its
//	                               workload starts. Rule 4 reads it.
//	<prefix>/replica/m/<member>    ReplicaCopy: <member>'s last complete pull.
//	<prefix>/replica/accept-stale  AcceptStale: one term may start past the
//	                               staleness gate (`pagurus accept-stale`).

// ReplicaSource is what the leader published after its last snapshot round.
type ReplicaSource struct {
	Member   string    `json:"member"`
	Epoch    int64     `json:"epoch"`
	Snapshot string    `json:"snapshot"`
	At       time.Time `json:"at"`
}

// ReplicaCopy is a follower's record of its last complete pull.
type ReplicaCopy struct {
	Snapshot    string    `json:"snapshot"`
	SourceEpoch int64     `json:"source_epoch"`
	At          time.Time `json:"at"`
	// Orphaned is, per volume, the key of the last divergence this member
	// preserved (replica.Divergence), so the same one is not preserved twice.
	Orphaned map[string]string `json:"orphaned,omitempty"`
}

// AcceptStale lets one term start past the staleness gate. Member "" means
// any member's next term.
type AcceptStale struct {
	Member string    `json:"member,omitempty"`
	By     string    `json:"by"`
	At     time.Time `json:"at"`
	// ModRev is filled on read (not stored).
	ModRev int64 `json:"-"`
}

func (c *Coord) ReplicaSourceKey() string            { return c.prefix + "/replica/source" }
func (c *Coord) ReplicaLedKey(member string) string  { return c.prefix + "/replica/led/" + member }
func (c *Coord) ReplicaCopyKey(member string) string { return c.prefix + "/replica/m/" + member }
func (c *Coord) AcceptStaleKey() string              { return c.prefix + "/replica/accept-stale" }

func (c *Coord) OpPutReplicaSource(s ReplicaSource) clientv3.Op {
	return clientv3.OpPut(c.ReplicaSourceKey(), mustJSON(s))
}

func (c *Coord) OpPutReplicaLed(member string, epoch int64) clientv3.Op {
	return clientv3.OpPut(c.ReplicaLedKey(member), fmt.Sprint(epoch))
}

// ReplicaSource returns the leader's last published round, or nil.
func (c *Coord) ReplicaSource(ctx context.Context) (*ReplicaSource, error) {
	var s ReplicaSource
	ok, err := c.getJSON(ctx, c.ReplicaSourceKey(), &s)
	if err != nil || !ok {
		return nil, err
	}
	return &s, nil
}

// ReplicaLed returns every member's last replicated term epoch.
func (c *Coord) ReplicaLed(ctx context.Context) (map[string]int64, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	pfx := c.prefix + "/replica/led/"
	resp, err := c.cli.Get(cctx, pfx, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, kv := range resp.Kvs {
		var e int64
		if _, err := fmt.Sscan(string(kv.Value), &e); err != nil {
			return nil, fmt.Errorf("decode %s: %w", kv.Key, err)
		}
		out[strings.TrimPrefix(string(kv.Key), pfx)] = e
	}
	return out, nil
}

// ReplicaCopy returns member's last complete pull, or nil.
func (c *Coord) ReplicaCopy(ctx context.Context, member string) (*ReplicaCopy, error) {
	var r ReplicaCopy
	ok, err := c.getJSON(ctx, c.ReplicaCopyKey(member), &r)
	if err != nil || !ok {
		return nil, err
	}
	return &r, nil
}

// PutReplicaCopy records member's pull. Only member writes its own record.
func (c *Coord) PutReplicaCopy(ctx context.Context, member string, r ReplicaCopy) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	_, err := c.cli.Put(cctx, c.ReplicaCopyKey(member), mustJSON(r))
	return err
}

// SetAcceptStale is the operator override of the staleness gate.
func (c *Coord) SetAcceptStale(ctx context.Context, a AcceptStale) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	_, err := c.cli.Put(cctx, c.AcceptStaleKey(), mustJSON(a))
	return err
}

// GetAcceptStale returns the pending override, or nil.
func (c *Coord) GetAcceptStale(ctx context.Context) (*AcceptStale, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Get(cctx, c.AcceptStaleKey())
	if err != nil || len(resp.Kvs) == 0 {
		return nil, err
	}
	var a AcceptStale
	if err := json.Unmarshal(resp.Kvs[0].Value, &a); err != nil {
		return nil, err
	}
	a.ModRev = resp.Kvs[0].ModRevision
	return &a, nil
}

// ConsumeAcceptStale deletes the override read at modRev, only while (epoch,
// lease) holds the office: one override starts one term.
func (c *Coord) ConsumeAcceptStale(ctx context.Context, epoch int64, lease clientv3.LeaseID, modRev int64) error {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	cmps := append(c.leaderGuard(epoch, lease), clientv3.Compare(clientv3.ModRevision(c.AcceptStaleKey()), "=", modRev))
	resp, err := c.cli.Txn(cctx).If(cmps...).Then(clientv3.OpDelete(c.AcceptStaleKey())).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return errors.New("accept-stale changed, or the office moved, while consuming it")
	}
	return nil
}

// getJSON reads key into v; ok is false when it is absent.
func (c *Coord) getJSON(ctx context.Context, key string, v any) (bool, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.cli.Get(cctx, key)
	if err != nil || len(resp.Kvs) == 0 {
		return false, err
	}
	return true, json.Unmarshal(resp.Kvs[0].Value, v)
}
