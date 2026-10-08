// Package config loads and validates a pagurus service definition.
//
// Validation is deliberately strict: every timing relation the at-most-one
// argument depends on (README.md "Timing") is checked here, so a config that
// would make the safety argument false is refused at startup rather than
// discovered during an outage.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alien43/pagurus/internal/fence"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Duration is a time.Duration that unmarshals from a Go duration string.
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"10s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// Unverified fence policies: what a new leader does when it cannot confirm that
// a member which may still run the workload has stopped.
const (
	// UnverifiedBlock never starts on an unconfirmed fence. Fail closed: the
	// service stays down until the member is reachable again or an operator
	// runs `pagurus fence-ack`.
	UnverifiedBlock = "block"
	// UnverifiedProceed trusts the old leader's software self-fence: it waits
	// fence.unverified_wait after acquiring, then starts.
	UnverifiedProceed = "proceed"
)

// Watchdog modes.
const (
	WatchdogOff     = "off"
	WatchdogDryRun  = "dry-run"
	WatchdogSystemd = "systemd"
	WatchdogDevice  = "device"
)

type Etcd struct {
	Endpoints   []string `json:"endpoints"`
	DialTimeout Duration `json:"dial_timeout"`
	// OpTimeout bounds every single etcd request the agent makes.
	OpTimeout Duration `json:"op_timeout"`
}

type Lease struct {
	TTL           Duration `json:"ttl"`
	RenewInterval Duration `json:"renew_interval"`
	// Margin is subtracted from the locally computed lease validity to absorb
	// clock-rate differences between this host and the etcd leader.
	Margin Duration `json:"margin"`
}

type SelfFence struct {
	// StopBudget bounds `incus stop --force` of the local project.
	StopBudget Duration `json:"stop_budget"`
	// KillBudget bounds the cgroup.kill fallback, used when Incus cannot stop
	// the instances (e.g. this member lost the Incus cluster quorum).
	KillBudget Duration `json:"kill_budget"`
}

// Fences configures the fence chains. Self is the ordered chain a member runs
// on itself before its lease can expire; it stops at the first fence that
// confirms. Absent, it is incus-stop (self_fence.stop_budget) then
// cgroup-kill (self_fence.kill_budget). An explicitly empty list means "no
// verified self-fence": allowed, and advertised to peers.
//
// Peer is the ordered chain a new leader runs against another member that may
// still run the workload (incus-api-stop, exec, watchdog-wait); the first
// fence that does not skip decides. Absent, it is incus-api-stop
// (fence.api_timeout) then watchdog-wait: the prototype's behaviour. When
// every fence skips, fence.unverified decides.
type Fences struct {
	Self *[]FenceStep `json:"self"`
	Peer *[]FenceStep `json:"peer"`
}

// FenceStep is one fence in a chain (see internal/fence for the kinds).
type FenceStep struct {
	Kind    string   `json:"kind"`
	Budget  Duration `json:"budget"`
	Command []string `json:"command,omitempty"`
	Confirm []string `json:"confirm,omitempty"`
	For     []string `json:"for,omitempty"`
}

// SelfFences returns the self chain's specs.
func (c *Config) SelfFences() []fence.Spec {
	if c.Fences.Self == nil {
		return nil
	}
	out := make([]fence.Spec, 0, len(*c.Fences.Self))
	for _, s := range *c.Fences.Self {
		out = append(out, fence.Spec{Kind: s.Kind, Budget: s.Budget.D(), Command: s.Command, Confirm: s.Confirm, For: s.For})
	}
	return out
}

// PeerFences returns the peer chain's specs.
func (c *Config) PeerFences() []fence.Spec {
	if c.Fences.Peer == nil {
		return nil
	}
	out := make([]fence.Spec, 0, len(*c.Fences.Peer))
	for _, s := range *c.Fences.Peer {
		out = append(out, fence.Spec{Kind: s.Kind, Budget: s.Budget.D(), Command: s.Command, Confirm: s.Confirm, For: s.For})
	}
	return out
}

type Fence struct {
	// APITimeout bounds one attempt at stopping a remote member's project via
	// the Incus cluster API.
	APITimeout Duration `json:"api_timeout"`
	// Unverified is UnverifiedBlock or UnverifiedProceed.
	Unverified string `json:"unverified"`
	// UnverifiedWait is how long after acquiring to wait before proceeding
	// under UnverifiedProceed.
	UnverifiedWait Duration `json:"unverified_wait"`
	// RetryInterval paces repeated fence attempts while blocked.
	RetryInterval Duration `json:"retry_interval"`
	// RebootMargin is added to a member's advertised watchdog timeout before
	// a watchdog-backed fence is considered complete.
	RebootMargin Duration `json:"reboot_margin"`
}

type Health struct {
	Interval Duration `json:"interval"`
	// StartPeriod: failures during this window after `up` are not counted.
	StartPeriod Duration `json:"start_period"`
	// Failures: consecutive failed checks before an in-place restart.
	Failures int `json:"failures"`
	// MaxRestarts: in-place restarts before the leader gives the office up.
	MaxRestarts int `json:"max_restarts"`
	// HTTP, if set, is fetched after the instance-state check. "{ip}" is
	// replaced with the IPv4 address of Instance's first NIC.
	HTTP     string   `json:"http"`
	Instance string   `json:"instance"`
	Timeout  Duration `json:"timeout"`
	// ReleaseBackoff: after giving the office up for being unhealthy, do not
	// campaign again for this long (lets a healthier member take over).
	ReleaseBackoff Duration `json:"release_backoff"`
}

type Watchdog struct {
	Mode string `json:"mode"`
	// Device is the watchdog character device for Mode=device.
	Device string `json:"device"`
	// Timeout is the hardware timeout set on Device (Mode=device). For
	// Mode=systemd it is read from WATCHDOG_USEC instead.
	Timeout Duration `json:"timeout"`
	// Arm must be true for Mode=device to open the device. Without it the
	// device mode runs as dry-run, because an armed watchdog reboots the host.
	Arm bool `json:"arm"`
}

// Start configures the starter.
type Start struct {
	// Kind is "compose" (incus-compose up) or "instances" (start
	// containers that already exist in the member's project).
	Kind string `json:"kind"`
	// compose
	File   string   `json:"file,omitempty"`
	Args   []string `json:"args,omitempty"`
	UpArgs []string `json:"up_args,omitempty"`
	// instances: names to start; empty means every instance in the project.
	Names []string `json:"names,omitempty"`
}

type Config struct {
	Service string `json:"service"`
	// Start says how a member brings its copy up (internal/start).
	Start Start `json:"start"`
	// Compose, ComposeArgs and UpArgs are the prototype's top-level form of
	// start.kind=compose. Still read, mapped onto Start; see Deprecations.
	Compose     string   `json:"compose,omitempty"`
	ComposeArgs []string `json:"compose_args,omitempty"`
	UpArgs      []string `json:"up_args,omitempty"`
	// Deprecations lists config forms that still work but should change.
	Deprecations []string `json:"-"`
	// ProjectPrefix: member m runs the stack in Incus project "<prefix>-<m>",
	// which must be restricted to a cluster group containing only m.
	ProjectPrefix string `json:"project_prefix"`
	// Members that may run the service. Prefer orders campaign priority.
	Members []string `json:"members"`
	Prefer  []string `json:"prefer"`
	// Grace: a member of preference rank r waits r*Grace after the office
	// falls vacant before campaigning, so a preferred member that blips back
	// wins without a failover.
	Grace Duration `json:"grace"`
	// EligibleInterval paces eligibility re-checks while not campaigning.
	EligibleInterval Duration `json:"eligible_interval"`
	// AuditInterval: how often the leader scans other members' projects for
	// running instances it did not start (and stops them).
	AuditInterval Duration `json:"audit_interval"`

	EtcdPrefix string    `json:"etcd_prefix"`
	Etcd       Etcd      `json:"etcd"`
	Lease      Lease     `json:"lease"`
	SelfFence  SelfFence `json:"self_fence"`
	Fences     Fences    `json:"fences"`
	Fence      Fence     `json:"fence"`
	Health     Health    `json:"health"`
	Watchdog   Watchdog  `json:"watchdog"`

	// StateDir holds the lease-resume file. Must be on tmpfs (/run) so that a
	// reboot never resumes a lease.
	StateDir string `json:"state_dir"`
	// AlertURL, if set, receives a POST for every event that needs a human.
	AlertURL string `json:"alert_url"`
}

var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,40}$`)

// Load reads, defaults and validates a config file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

func Parse(b []byte) (*Config, error) {
	c := &Config{}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if c.Fences.Self != nil && (c.SelfFence.StopBudget != 0 || c.SelfFence.KillBudget != 0) {
		return nil, fmt.Errorf("config: set either self_fence budgets or fences.self, not both")
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	def := func(d *Duration, v time.Duration) {
		if *d == 0 {
			*d = Duration(v)
		}
	}
	if c.EtcdPrefix == "" {
		c.EtcdPrefix = "/pagurus/v1/" + c.Service
	}
	if len(c.Prefer) == 0 {
		c.Prefer = slices.Clone(c.Members)
	}
	if c.Compose != "" || len(c.ComposeArgs) > 0 || len(c.UpArgs) > 0 {
		if c.Start.Kind == "" {
			c.Start = Start{Kind: "compose", File: c.Compose, Args: c.ComposeArgs, UpArgs: c.UpArgs}
		}
		c.Deprecations = append(c.Deprecations, `top-level "compose"/"compose_args"/"up_args": use "start": {"kind": "compose", "file": …, "args": …, "up_args": …}`)
	}
	if c.Start.Kind == "compose" && len(c.Start.UpArgs) == 0 {
		c.Start.UpArgs = []string{"--detach", "--no-healthd"}
	}
	if c.StateDir == "" {
		c.StateDir = "/run/pagurus"
	}
	def(&c.Grace, 10*time.Second)
	def(&c.EligibleInterval, 5*time.Second)
	def(&c.AuditInterval, 30*time.Second)
	def(&c.Etcd.DialTimeout, 3*time.Second)
	def(&c.Etcd.OpTimeout, 2*time.Second)
	def(&c.Lease.TTL, 20*time.Second)
	def(&c.Lease.RenewInterval, 2*time.Second)
	def(&c.Lease.Margin, 2*time.Second)
	def(&c.SelfFence.StopBudget, 6*time.Second)
	def(&c.SelfFence.KillBudget, 3*time.Second)
	if c.Fences.Self == nil {
		c.Fences.Self = &[]FenceStep{
			{Kind: fence.KindIncusStop, Budget: c.SelfFence.StopBudget},
			{Kind: fence.KindCgroupKill, Budget: c.SelfFence.KillBudget},
		}
	}
	def(&c.Fence.APITimeout, 20*time.Second)
	if c.Fences.Peer == nil {
		c.Fences.Peer = &[]FenceStep{
			{Kind: fence.KindIncusAPIStop, Budget: c.Fence.APITimeout},
			{Kind: fence.KindWatchdogWait},
		}
	}
	def(&c.Fence.RetryInterval, 10*time.Second)
	def(&c.Fence.RebootMargin, 10*time.Second)
	if c.Fence.Unverified == "" {
		c.Fence.Unverified = UnverifiedBlock
	}
	def(&c.Fence.UnverifiedWait, 2*c.Lease.TTL.D())
	def(&c.Health.Interval, 5*time.Second)
	def(&c.Health.StartPeriod, 30*time.Second)
	def(&c.Health.Timeout, 3*time.Second)
	def(&c.Health.ReleaseBackoff, 2*time.Minute)
	if c.Health.Failures == 0 {
		c.Health.Failures = 3
	}
	if c.Health.MaxRestarts == 0 {
		c.Health.MaxRestarts = 2
	}
	if c.Watchdog.Mode == "" {
		c.Watchdog.Mode = WatchdogDryRun
	}
	if c.Watchdog.Mode == WatchdogDevice && c.Watchdog.Device == "" {
		c.Watchdog.Device = "/dev/watchdog1"
	}
	if c.Watchdog.Mode == WatchdogDevice || c.Watchdog.Mode == WatchdogDryRun {
		def(&c.Watchdog.Timeout, 60*time.Second)
	}
}

// SelfFenceLead is how long before the lease's safe deadline the leader starts
// fencing itself, so that the stop has finished by the deadline.
func (c *Config) SelfFenceLead() time.Duration {
	var d time.Duration
	for _, s := range c.SelfFences() {
		d += s.Budget
	}
	return d
}

// Project returns the Incus project member m runs the stack in.
func (c *Config) Project(m string) string { return c.ProjectPrefix + "-" + m }

// Rank is m's campaign priority: its index in Prefer, or len(Prefer) if absent.
func (c *Config) Rank(m string) int {
	if i := slices.Index(c.Prefer, m); i >= 0 {
		return i
	}
	return len(c.Prefer)
}

func (c *Config) Validate() error {
	var errs []error
	bad := func(f string, a ...any) { errs = append(errs, fmt.Errorf(f, a...)) }

	if !nameRE.MatchString(c.Service) {
		bad("service %q: must match %s", c.Service, nameRE)
	}
	switch c.Start.Kind {
	case "compose":
		if c.Start.File == "" {
			bad("start.file: required for start.kind=compose")
		}
		if len(c.Start.Names) > 0 {
			bad("start.names: only for start.kind=instances")
		}
	case "instances":
		if c.Start.File != "" || len(c.Start.Args) > 0 || len(c.Start.UpArgs) > 0 {
			bad("start.file/args/up_args: only for start.kind=compose")
		}
	case "":
		bad("start: required (kind compose or instances)")
	default:
		bad("start.kind %q: must be compose or instances", c.Start.Kind)
	}
	if c.Compose != "" && c.Start.Kind != "" && c.Start.File != c.Compose {
		bad("set either the top-level compose or start, not both")
	}
	if !nameRE.MatchString(c.ProjectPrefix) {
		bad("project_prefix %q: must match %s", c.ProjectPrefix, nameRE)
	}
	if len(c.Members) == 0 {
		bad("members: at least one member required")
	}
	seen := map[string]bool{}
	for _, m := range c.Members {
		if !nameRE.MatchString(m) {
			bad("member %q: invalid name", m)
		}
		if seen[m] {
			bad("member %q listed twice", m)
		}
		seen[m] = true
	}
	for _, m := range c.Prefer {
		if !seen[m] {
			bad("prefer: %q is not in members", m)
		}
	}
	if !strings.HasPrefix(c.EtcdPrefix, "/") || strings.HasSuffix(c.EtcdPrefix, "/") {
		bad("etcd_prefix %q: must start with / and not end with /", c.EtcdPrefix)
	}
	if len(c.Etcd.Endpoints) == 0 {
		bad("etcd.endpoints: required")
	}
	if _, err := fence.BuildSelf(nil, c.SelfFences()); err != nil {
		bad("%v", err)
	}
	if _, err := fence.BuildPeer(nil, 0, c.PeerFences()); err != nil {
		bad("%v", err)
	}
	if c.Fence.Unverified != UnverifiedBlock && c.Fence.Unverified != UnverifiedProceed {
		bad("fence.unverified %q: must be %q or %q", c.Fence.Unverified, UnverifiedBlock, UnverifiedProceed)
	}
	switch c.Watchdog.Mode {
	case WatchdogOff, WatchdogDryRun, WatchdogSystemd, WatchdogDevice:
	default:
		bad("watchdog.mode %q: must be off, dry-run, systemd or device", c.Watchdog.Mode)
	}
	if c.Health.HTTP != "" && c.Health.Instance == "" {
		bad("health.instance: required when health.http is set")
	}
	if c.StateDir != "" && !strings.HasPrefix(c.StateDir, "/run/") && !strings.HasPrefix(c.StateDir, "/tmp/") {
		bad("state_dir %q: must be on tmpfs (/run/...) so a reboot can never resume a lease", c.StateDir)
	}

	// The timing argument (README.md "Timing").
	ttl, renew, margin := c.Lease.TTL.D(), c.Lease.RenewInterval.D(), c.Lease.Margin.D()
	if ttl < 5*time.Second || ttl%time.Second != 0 {
		bad("lease.ttl %s: must be a whole number of seconds, >= 5s (etcd leases are in seconds)", ttl)
	}
	if renew <= 0 || margin <= 0 {
		bad("lease.renew_interval and lease.margin must be > 0")
	}
	if c.Etcd.OpTimeout.D() > renew {
		bad("etcd.op_timeout %s must be <= lease.renew_interval %s, or one slow renewal delays the next", c.Etcd.OpTimeout.D(), renew)
	}
	// At least two renewals must be allowed to fail before self-fencing
	// starts, or ordinary jitter fences a healthy leader.
	if need := 3*renew + c.SelfFenceLead() + margin; need > ttl {
		bad("lease.ttl %s too short: 3*renew_interval + self-fence chain budget + margin = %s", ttl, need)
	}
	if c.Fence.Unverified == UnverifiedProceed && c.Fence.UnverifiedWait.D() < c.SelfFenceLead()+margin {
		bad("fence.unverified_wait %s must cover the old leader's self-fence (>= %s)", c.Fence.UnverifiedWait.D(), c.SelfFenceLead()+margin)
	}
	if c.Watchdog.Mode == WatchdogDevice || c.Watchdog.Mode == WatchdogDryRun {
		if wd := c.Watchdog.Timeout.D(); wd < 2*renew || wd%time.Second != 0 {
			bad("watchdog.timeout %s: must be whole seconds and >= 2*renew_interval", wd)
		}
	}
	if c.Health.Failures < 1 || c.Health.MaxRestarts < 0 {
		bad("health.failures must be >= 1 and health.max_restarts >= 0")
	}
	return errors.Join(errs...)
}
