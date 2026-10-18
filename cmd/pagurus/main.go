// Command pagurus runs one incus-compose stack on at most one member of an
// Incus cluster, failing it over between members. See README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/alien43/pagurus/internal/agent"
	"github.com/alien43/pagurus/internal/backend/incus"
	"github.com/alien43/pagurus/internal/backend/incus/provision"
	"github.com/alien43/pagurus/internal/backend/incus/start"
	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/fence"
	"github.com/alien43/pagurus/internal/metrics"
	"github.com/alien43/pagurus/internal/replica"
	"github.com/alien43/pagurus/internal/versions"
	"github.com/alien43/pagurus/internal/watchdog"
)

var version = "dev"

const usage = `usage: pagurus <command> -c <config.json> [flags]

commands:
  run         run the agent on this member (as root, under systemd)
  validate    check the config, this member's Incus project and the compose file
  status      show the office, member states and every member's instances
  move        planned move: pagurus move -c cfg --to <member>
  refresh     the leader recreates its workload in place from the current
              compose file and the image in its project (after a pull):
              pagurus refresh -c cfg [--timeout 5m]; waits until healthy
  fence-ack   operator override: record that a member is verified down
              pagurus fence-ack -c cfg --member <member>
  accept-stale operator override (replicate): let the next term start from
              a copy older than replicate.max_staleness, once:
              pagurus accept-stale -c cfg [--member <member>]
  fence-check read-only: run Confirm of every fence in this member's self
              chain and print what each would do (safe on production)
              pagurus fence-check -c cfg --member <member>
              Run it ON that member: incus-stop and cgroup-kill confirm
              from the local cgroupfs.
  provision   create the per-member cluster groups (host-<member>), pinned
              projects and (replicate) each member's replicated volumes:
              pagurus provision -c cfg [--dry-run] [--yes]
              (group changes need --yes or an interactive "y")
  fence-run   run ONE fence of this member's self chain for real (drills on
              scratch projects): pagurus fence-run -c cfg --member <m>
              --fence <1-based index> --i-know
  audit       watch every member's project and report any moment with more
              than one member running the workload (drill tool)

run by conch (the agent passes these to ` + "`conch elect`" + `; not for operators):
  fence-self  this member's self chain, as conch's --fence:
              pagurus fence-self -c cfg --member <member>
  term        conch's child while it holds the office; hands the term to
              the agent: pagurus term --socket <path>
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(64)
	}
	cmd, args := os.Args[1], os.Args[2:]
	if cmd == "term" { // no config: a pipe between conch and the agent
		tfs := flag.NewFlagSet(cmd, flag.ExitOnError)
		sock := tfs.String("socket", "", "the agent's term socket")
		_ = tfs.Parse(args)
		os.Exit(agent.RunTermShim(*sock))
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	cfgPath := fs.String("c", "", "config file")
	to := fs.String("to", "", "move: target member")
	member := fs.String("member", "", "fence-ack: member to acknowledge; fence-check: member whose chain to check; accept-stale: the member that may start (default any)")
	every := fs.Duration("every", time.Second, "audit: poll interval")
	dur := fs.Duration("for", 0, "audit: stop after this long (0 = until interrupted)")
	jsonLog := fs.Bool("json", false, "run: log as JSON")
	timeout := fs.Duration("timeout", 3*time.Minute, "move, refresh: how long to wait")
	self := fs.String("self", "", "run: this host's cluster member name; lets the agent fence locally even when the local Incus is down at startup")
	fenceIdx := fs.Int("fence", 0, "fence-run: 1-based index into the member's self chain")
	dryRun := fs.Bool("dry-run", false, "provision: print the incus commands, change nothing")
	yes := fs.Bool("yes", false, "provision: allow cluster group changes without asking")
	iKnow := fs.Bool("i-know", false, "fence-run: confirm that this really stops the member's workload")
	endpoints := fs.String("etcd", "", "override etcd.endpoints (comma-separated; drill harness)")
	metricsAddr := fs.String("metrics-addr", "", "run: serve Prometheus metrics on this address (e.g. :9478); off by default")
	_ = fs.Parse(args)
	if *cfgPath == "" {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(64)
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fatal(err)
	}
	if *endpoints != "" {
		cfg.Etcd.Endpoints = strings.Split(*endpoints, ",")
	}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	if *jsonLog {
		h = slog.NewJSONHandler(os.Stderr, nil)
	}
	log := slog.New(h).With("svc", cfg.Service)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for _, d := range cfg.Deprecations {
		log.Warn("deprecated config", "what", d)
	}
	api := incus.NewClient(incus.ClientOptions{Socket: cfg.Incus.Socket, URL: cfg.Incus.URL,
		ClientCert: cfg.Incus.ClientCert, ClientKey: cfg.Incus.ClientKey, ServerCert: cfg.Incus.ServerCert})
	dopts := incus.Options{ProjectPrefix: cfg.ProjectPrefix, API: api}
	if cfg.Replicate != nil {
		dopts.Pool = cfg.Replicate.Pool
	}
	drv := incus.New(dopts)
	rt := &runtime{Driver: drv, Starter: newStarter(cfg, drv)}
	be := agent.Backend{Site: drv, Starter: rt.Starter, Observer: drv, Members: drv, Replicator: drv,
		Fences: fence.Configured{Local: drv, Remote: drv, Self: cfg.SelfFences(), Peer: cfg.PeerFences(), RebootMargin: cfg.Fence.RebootMargin.D()}}

	switch cmd {
	case "validate":
		err = cmdValidate(ctx, cfg, rt, be)
	case "run":
		err = cmdRun(ctx, cfg, rt, be, log, *self, *cfgPath, *metricsAddr)
	case "fence-self":
		os.Exit(cmdFenceSelf(cfg, drv, *member, log))
	case "status":
		err = withEtcd(cfg, func(cli *clientv3.Client) error { return cmdStatus(ctx, cfg, drv, cli) })
	case "move":
		t := *timeout
		if r := cfg.Replicate; r != nil && !flagSet(fs, "timeout") {
			// The leader first copies to the target, for up to catchup_timeout.
			t = max(t, r.CatchupTimeout.D()+2*time.Minute)
		}
		err = withEtcd(cfg, func(cli *clientv3.Client) error { return cmdMove(ctx, cfg, cli, *to, t) })
	case "refresh":
		err = withEtcd(cfg, func(cli *clientv3.Client) error { return cmdRefresh(ctx, cfg, cli, *timeout) })
	case "fence-ack":
		err = withEtcd(cfg, func(cli *clientv3.Client) error { return cmdFenceAck(ctx, cfg, cli, *member) })
	case "accept-stale":
		err = withEtcd(cfg, func(cli *clientv3.Client) error { return cmdAcceptStale(ctx, cfg, cli, *member) })
	case "audit":
		err = cmdAudit(ctx, cfg, drv, *every, *dur)
	case "fence-check":
		err = cmdFenceCheck(ctx, cfg, drv, *member)
	case "provision":
		err = cmdProvision(ctx, cfg, api, *dryRun, *yes)
	case "fence-run":
		err = cmdFenceRun(ctx, cfg, drv, *member, *fenceIdx, *iKnow, log)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(64)
	}
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "pagurus:", err)
	os.Exit(1)
}

func newEtcd(cfg *config.Config) (*clientv3.Client, error) {
	cc, err := cfg.EtcdClientConfig()
	if err != nil {
		return nil, err
	}
	// The agent logs what matters itself; the client's own retry chatter
	// during a partition is noise.
	cc.Logger = zap.NewNop()
	return clientv3.New(cc)
}

func withEtcd(cfg *config.Config, f func(*clientv3.Client) error) error {
	cli, err := newEtcd(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()
	return f(cli)
}

func cmdValidate(ctx context.Context, cfg *config.Config, rt *runtime, be agent.Backend) error {
	a := agent.New(agent.Options{Config: cfg, Backend: be, Etcd: nil})
	if err := a.Preflight(ctx); err != nil {
		return err
	}
	fmt.Printf("ok: member %s, project %s pinned, %s starter check passes, timing valid (TTL %s, self-fence lead %s)\n",
		a.Member(), cfg.Project(a.Member()), rt.Starter.Kind(), cfg.Lease.TTL.D(), cfg.SelfFenceLead())
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	v := toolVersions(ctx, cfg, rt.Driver, quiet)
	fmt.Printf("versions: incus %q", v["incus"])
	if ic, ok := v["incus-compose"]; ok {
		fmt.Printf(", incus-compose %q", ic)
	}
	fmt.Println()
	for tool, ver := range v {
		if w := versions.Check(tool, ver); w != "" {
			fmt.Printf("WARNING: %s\n", w)
		}
	}
	for _, m := range cfg.Members {
		if err := rt.CheckProject(ctx, m); err != nil {
			fmt.Printf("WARNING: %v\n", err)
		}
	}
	return nil
}

func cmdRun(ctx context.Context, cfg *config.Config, rt *runtime, be agent.Backend, log *slog.Logger, self, cfgPath, metricsAddr string) error {
	if os.Geteuid() != 0 {
		return errors.New("run must be root (cgroup.kill, watchdog)")
	}
	cli, err := newEtcd(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(cfgPath)
	if err != nil {
		return err
	}
	ms := metrics.New(cfg.Service, self)
	a := agent.New(agent.Options{Config: cfg, Backend: be, Etcd: cli, Log: log, Version: version, Member: self, Versions: toolVersions(ctx, cfg, rt.Driver, log),
		FenceCmd: []string{exe, "fence-self", "-c", abs}, TermCmd: []string{exe, "term", "--socket"}, Metrics: ms,
		StartLock: filepath.Join(cfg.StateDir, "start.lock")})
	if metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", ms.Handler())
		srv := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		ln, err := net.Listen("tcp", metricsAddr)
		if err != nil {
			return fmt.Errorf("metrics: %w", err)
		}
		go func() { _ = srv.Serve(ln) }()
		defer srv.Close()
		log.Info("serving metrics", "addr", ln.Addr().String())
	}
	if err := a.Preflight(ctx); err != nil {
		// No lease is held by a starting agent, so nothing may run here. If
		// the local Incus is what failed, cgroup.kill still works: use it,
		// then exit non-zero and let systemd retry.
		if self != "" {
			if running, lerr := rt.LocalRunning(self); lerr == nil && len(running) > 0 {
				log.Error("preflight failed with local instances running and no lease: running the self-fence chain", "running", running)
				if chain, berr := fence.BuildSelf(rt, cfg.SelfFences()); berr == nil {
					res := chain.Run(context.Background(), fence.Target{Member: self, Project: cfg.Project(self)}, log)
					if !res.Stopped {
						log.Error("emergency self-fence did not confirm", "chain", res.String())
					}
				}
			}
		}
		return fmt.Errorf("preflight: %w", err)
	}
	wd, err := watchdog.New(watchdog.Options{
		Mode: cfg.Watchdog.Mode, Device: cfg.Watchdog.Device, Timeout: cfg.Watchdog.Timeout.D(), Arm: cfg.Watchdog.Arm,
		Log: log.With("member", a.Member()),
	})
	if err != nil {
		return err
	}
	err = a.Run(ctx, wd)
	// Disarm only if we exit with nothing running here; otherwise an armed
	// device must be left to fire.
	ok, _ := a.Fit()
	running, lerr := rt.LocalRunning(a.Member())
	wd.Close(ok && lerr == nil && len(running) == 0)
	return err
}

func cmdStatus(ctx context.Context, cfg *config.Config, rt *incus.Driver, cli *clientv3.Client) error {
	co := coord.New(cli, cfg.EtcdPrefix, cfg.Etcd.OpTimeout.D())
	rec, epoch, lid, _, err := co.Leader(ctx)
	if err != nil {
		return err
	}
	if rec == nil {
		fmt.Println("office:   VACANT")
	} else {
		ttl := int64(-1)
		if r, err := cli.TimeToLive(ctx, lid); err == nil {
			ttl = r.TTL
		}
		if rec.Member == "" {
			fmt.Printf("office:   held (holder not yet identified)  epoch=%d  lease_ttl=%ds\n", epoch, ttl)
		} else {
			fmt.Printf("office:   %s  epoch=%d  since=%s  lease_ttl=%ds\n", rec.Member, epoch, rec.Acquired.Local().Format(time.TimeOnly), ttl)
		}
	}
	if pin, _ := co.Pin(ctx); pin != "" {
		fmt.Printf("pin:      %s\n", pin)
	}
	if ep, _ := co.GetEndpoint(ctx); ep != nil {
		b, _ := json.Marshal(ep.Addresses)
		fmt.Printf("endpoint: %s %s\n", ep.Member, b)
	}
	states, err := co.MemberStates(ctx)
	if err != nil {
		return err
	}
	regs, _ := co.Registrations(ctx)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nMEMBER\tINCUS\tAGENT\tWATCHDOG\tRECORD\tINSTANCES")
	for _, m := range cfg.Members {
		online := "?"
		if on, err := rt.MemberOnline(ctx, m); err == nil {
			online = map[bool]string{true: "online", false: "OFFLINE"}[on]
		}
		ag, wd := "-", "-"
		if r, ok := regs[m]; ok {
			ag = r.Version
			if v := r.Versions["incus"]; v != "" {
				ag += " incus " + v
			}
			if v := r.Versions["incus-compose"]; v != "" {
				ag += " ic " + v
			}
			if r.ConfigHash != "" {
				ag += " cfg " + r.ConfigHash[:6]
				if r.ConfigHash != cfg.SafetyHash() {
					ag += "≠"
				}
			}
			wd = r.Caps.WatchdogMode
			if r.Caps.FencesByReboot {
				wd += "(reboot)"
			}
		}
		st := "-"
		if s, ok := states[m]; ok {
			st = fmt.Sprintf("%s@%d (%s)", s.State, s.Epoch, s.Reason)
		}
		var insts []string
		if l, err := rt.List(ctx, m); err == nil {
			for _, in := range l {
				insts = append(insts, in.Name+"="+in.Status)
			}
		} else {
			insts = []string{"error: " + err.Error()}
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", m, online, ag, wd, st, strings.Join(insts, ","))
	}
	if err := tw.Flush(); err != nil || cfg.Replicate == nil {
		return err
	}
	return replicaStatus(ctx, cfg, rt, co)
}

// replicaStatus prints the replication view: the leader's last round, and
// per member its last pull, its newest snapshot and its orphans.
func replicaStatus(ctx context.Context, cfg *config.Config, rt *incus.Driver, co *coord.Coord) error {
	if src, _ := co.ReplicaSource(ctx); src != nil {
		fmt.Printf("\nreplica source: %s epoch=%d %s (%s ago)\n", src.Member, src.Epoch, src.Snapshot, time.Since(src.At).Round(time.Second))
	} else {
		fmt.Println("\nreplica source: none published yet")
	}
	if a, _ := co.GetAcceptStale(ctx); a != nil {
		fmt.Printf("accept-stale pending: member=%q by %s\n", a.Member, a.By)
	}
	led, _ := co.ReplicaLed(ctx)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MEMBER\tLED\tLAST PULL\tNEWEST SNAPSHOT\tORPHANS")
	for _, m := range cfg.Members {
		pull := "-"
		if rc, _ := co.ReplicaCopy(ctx, m); rc != nil {
			pull = fmt.Sprintf("%s from epoch %d, %s ago", rc.Snapshot, rc.SourceEpoch, time.Since(rc.At).Round(time.Second))
		}
		var newest, orphans []string
		for _, v := range cfg.Replicate.Volumes {
			names, err := rt.VolumeSnapshots(ctx, m, v)
			switch n, ok := replica.Newest(names); {
			case err != nil:
				newest = append(newest, v+": error")
			case ok:
				newest = append(newest, fmt.Sprintf("%s: %s ago", v, time.Since(n.At).Round(time.Second)))
			default:
				newest = append(newest, v+": none")
			}
		}
		if vs, err := rt.ListVolumes(ctx, m); err == nil {
			for _, v := range vs {
				if strings.Contains(v, "-orphaned-") {
					orphans = append(orphans, v)
				}
			}
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\n", m, led[m], pull, strings.Join(newest, ", "), strings.Join(orphans, ","))
	}
	return tw.Flush()
}

// flagSet reports whether name was given on the command line.
func flagSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) { set = set || f.Name == name })
	return set
}

func cmdMove(ctx context.Context, cfg *config.Config, cli *clientv3.Client, to string, timeout time.Duration) error {
	if !slices.Contains(cfg.Members, to) {
		return fmt.Errorf("--to %q is not a member", to)
	}
	co := coord.New(cli, cfg.EtcdPrefix, cfg.Etcd.OpTimeout.D())
	// The pin outlives this command by at most a minute if it is killed.
	pinRev, err := co.PinFor(ctx, to, timeout+time.Minute)
	if err != nil {
		return err
	}
	// However move ends (success, give-up, timeout, Ctrl-C), it drops its own
	// pin, and only its own: one another move wrote since stays.
	defer func() {
		if cleared, err := co.ClearPinIf(context.Background(), pinRev); err != nil {
			fmt.Fprintln(os.Stderr, "WARNING: could not clear the pin:", err)
		} else if !cleared {
			fmt.Fprintln(os.Stderr, "the pin changed since this move wrote it (or expired); left as it is")
		}
	}()
	if cfg.Replicate != nil {
		fmt.Printf("pinned office to %s; the leader copies to it first (up to %s), then hands over\n", to, cfg.Replicate.CatchupTimeout.D())
	} else {
		fmt.Printf("pinned office to %s; waiting for it to take over\n", to)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// A release by the target written after the pin means it won and
		// could not host: stop holding everyone else out of the campaign.
		// Mod revisions, not timestamps: no clock skew, and an older record
		// from before this move never counts.
		if states, err := co.MemberStates(ctx); err == nil {
			if s, ok := states[to]; ok && s.State == coord.Stopped && s.ModRev > pinRev && strings.HasPrefix(s.Reason, "released: ") {
				return fmt.Errorf("%s gave up: %s; pin cleared, normal election resumes", to, strings.TrimPrefix(s.Reason, "released: "))
			}
		}
		rec, _, _, _, err := co.Leader(ctx)
		if err == nil && rec != nil && rec.Member == to {
			if ep, _ := co.GetEndpoint(ctx); ep != nil && ep.Member == to {
				fmt.Printf("%s holds the office and has published its endpoint\n", to)
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("timed out after %s; pin cleared, normal election resumes", timeout)
}

// cmdRefresh asks the leader to recreate its workload in place and waits for
// it to report the workload healthy again. With no leader, there is nothing
// to refresh: the next term recreates anyway.
func cmdRefresh(ctx context.Context, cfg *config.Config, cli *clientv3.Client, timeout time.Duration) error {
	co := coord.New(cli, cfg.EtcdPrefix, cfg.Etcd.OpTimeout.D())
	rec, _, _, _, err := co.Leader(ctx)
	if err != nil {
		return err
	}
	if rec == nil {
		fmt.Println("no leader: nothing runs; the next term starts from a fresh instance")
		return nil
	}
	host, _ := os.Hostname()
	token := fmt.Sprintf("%s/%s", time.Now().UTC().Format(time.RFC3339Nano), host)
	if err := co.PutString(ctx, co.RefreshKey(), token); err != nil {
		return err
	}
	fmt.Printf("asked %s (epoch %d) to refresh (token %s); waiting for it to be healthy\n", rec.Member, rec.Epoch, token)
	start, deadline := time.Now(), time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if done, err := co.GetString(ctx, co.RefreshDoneKey()); err == nil && done == token {
			fmt.Printf("refreshed and healthy on %s after %s\n", rec.Member, time.Since(start).Round(time.Second))
			return nil
		}
		if now, _, _, _, err := co.Leader(ctx); err == nil && (now == nil || now.Epoch != rec.Epoch) {
			who := "nobody"
			if now != nil {
				who = now.Member
			}
			return fmt.Errorf("the term ended before the refresh was healthy (now held by %s): not updated; "+
				"the new leader runs the image already in its own project", who)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("timed out after %s waiting for the refresh to be healthy (the leader may have given up the office; check pagurus status)", timeout)
}

func cmdFenceAck(ctx context.Context, cfg *config.Config, cli *clientv3.Client, member string) error {
	if !slices.Contains(cfg.Members, member) {
		return fmt.Errorf("--member %q is not a member", member)
	}
	user := os.Getenv("SUDO_USER")
	if user == "" {
		user = os.Getenv("USER")
	}
	co := coord.New(cli, cfg.EtcdPrefix, cfg.Etcd.OpTimeout.D())
	if err := co.AckFenced(ctx, member, "operator:"+user); err != nil {
		return err
	}
	fmt.Printf("recorded %s as stopped (operator acknowledgement); a blocked leader will proceed\n", member)
	return nil
}

// cmdAcceptStale lets the next term (on member, or any member) start from a
// replica older than replicate.max_staleness. One term consumes it.
func cmdAcceptStale(ctx context.Context, cfg *config.Config, cli *clientv3.Client, member string) error {
	if cfg.Replicate == nil {
		return errors.New("this service has no replicate block")
	}
	if member != "" && !slices.Contains(cfg.Members, member) {
		return fmt.Errorf("--member %q is not a member", member)
	}
	user := os.Getenv("SUDO_USER")
	if user == "" {
		user = os.Getenv("USER")
	}
	co := coord.New(cli, cfg.EtcdPrefix, cfg.Etcd.OpTimeout.D())
	if err := co.SetAcceptStale(ctx, coord.AcceptStale{Member: member, By: "operator:" + user, At: time.Now().UTC()}); err != nil {
		return err
	}
	who := member
	if who == "" {
		who = "any member"
	}
	fmt.Printf("the next term on %s may start from a stale copy (once); its data since that copy is lost there, and kept as an orphan where it was written\n", who)
	return nil
}

// cmdAudit is the drill's independent referee: it polls every member's
// project through the cluster API and flags any instant at which instances
// run in more than one member's project.
func cmdAudit(ctx context.Context, cfg *config.Config, rt *incus.Driver, every, dur time.Duration) error {
	if dur > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dur)
		defer cancel()
	}
	violations, polls := 0, 0
	last := ""
	for ctx.Err() == nil {
		qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var runningOn []string
		var notes []string
		for _, m := range cfg.Members {
			l, err := rt.List(qctx, m)
			switch {
			case err != nil:
				notes = append(notes, m+":?")
			case len(incus.Running(l)) > 0:
				runningOn = append(runningOn, m)
			default:
				if !incus.AllStopped(l) {
					notes = append(notes, m+":not-stopped")
				}
			}
		}
		cancel()
		if ctx.Err() != nil {
			break
		}
		polls++
		state := fmt.Sprintf("running_on=%v %s", runningOn, strings.Join(notes, " "))
		if len(runningOn) > 1 {
			violations++
			state = "VIOLATION " + state
		}
		if state != last {
			fmt.Printf("%s %s\n", time.Now().Format("15:04:05.000"), state)
			last = state
		}
		select {
		case <-ctx.Done():
		case <-time.After(every):
		}
	}
	fmt.Printf("audit: %d polls, %d violations\n", polls, violations)
	if violations > 0 {
		return fmt.Errorf("%d at-most-one violations observed", violations)
	}
	return nil
}

// cmdFenceCheck prints, for each fence of member's self chain, whether it
// applies and what its Confirm reports now. It runs no fence: Confirm is
// read-only by contract (internal/fence/fencetest.Contract).
func cmdFenceCheck(ctx context.Context, cfg *config.Config, rt *incus.Driver, member string) error {
	if member == "" {
		return fmt.Errorf("fence-check: --member required")
	}
	chain, err := fence.BuildSelf(rt, cfg.SelfFences())
	if err != nil {
		return err
	}
	tg := targetFor(ctx, cfg, rt, member)
	fmt.Printf("self chain of %s (project %s): %d fence(s), total budget %s, verified=%v\n",
		member, tg.Project, len(chain), chain.Budget(), chain.Verified(tg))
	for i, f := range chain {
		if !f.Applies(tg) {
			fmt.Printf("  %d. %-12s budget %-5s skipped (does not apply)\n", i+1, f.Kind(), f.Budget())
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, f.Budget())
		stopped, err := f.Confirm(cctx, tg)
		cancel()
		state := "running (a fence would act)"
		if stopped {
			state = "stopped"
		}
		if err != nil {
			state = "confirm error: " + err.Error()
		}
		fmt.Printf("  %d. %-12s budget %-5s confirm: %s\n", i+1, f.Kind(), f.Budget(), state)
	}
	return nil
}

// cmdFenceRun runs one fence of member's self chain for real, then its
// confirm. Meant for drills on scratch projects; it refuses without --i-know.
func cmdFenceRun(ctx context.Context, cfg *config.Config, rt *incus.Driver, member string, idx int, iKnow bool, log *slog.Logger) error {
	if member == "" || idx < 1 {
		return fmt.Errorf("fence-run: --member and --fence <1-based index> required")
	}
	if !iKnow {
		return fmt.Errorf("fence-run stops %s's workload in project %s for real; add --i-know", member, cfg.Project(member))
	}
	chain, err := fence.BuildSelf(rt, cfg.SelfFences())
	if err != nil {
		return err
	}
	if idx > len(chain) {
		return fmt.Errorf("fence-run: the self chain has %d fence(s)", len(chain))
	}
	res := fence.Chain{chain[idx-1]}.Run(ctx, targetFor(ctx, cfg, rt, member), log)
	fmt.Println(res.String())
	if !res.Stopped {
		return fmt.Errorf("fence %s did not confirm", chain[idx-1].Kind())
	}
	return nil
}

// cmdFenceSelf is conch's --fence: run this member's self chain within the
// budget conch gives it, and exit 0 only if it confirmed that nothing runs.
// The instance types come from what the agent recorded, so the chain is chosen
// without waiting on Incus; unknown, every type is assumed.
func cmdFenceSelf(cfg *config.Config, rt *incus.Driver, member string, log *slog.Logger) int {
	if !slices.Contains(cfg.Members, member) {
		log.Error("fence-self: --member is not a member", "member", member)
		return 64
	}
	chain, err := fence.BuildSelf(rt, cfg.SelfFences())
	if err != nil {
		log.Error("fence-self", "err", err)
		return 64
	}
	t := fence.Target{Member: member, Project: cfg.Project(member)}
	if ts, ok := agent.ReadTypes(cfg); ok {
		t.Types = ts
	} else {
		t.Types = fallbackTypes(cfg)
	}
	log = log.With("member", member, "reason", os.Getenv("CONCH_FENCE_REASON"), "rev", os.Getenv("CONCH_REV"))
	res := chain.Run(context.Background(), t, log)
	if !res.Stopped {
		log.Error("fence-self did not confirm", "chain", res.String())
		return 1
	}
	log.Info("fence-self confirmed", "by", res.By)
	return 0
}

// fallbackTypes is fence-self's target types when the agent recorded none:
// from the config. The compose starter runs containers only (nil: every
// container fence applies); a mixed container/VM target matches no single
// built-in fence, so it is only the instances starter's answer.
func fallbackTypes(cfg *config.Config) []string {
	if cfg.Start.Kind == "compose" {
		return nil
	}
	return []string{"container", "virtual-machine"}
}

// runtime is the Incus driver (projects, instances, fencing primitives) plus
// the configured starter, for the commands that use both.
type runtime struct {
	*incus.Driver
	start.Starter
}

func newStarter(cfg *config.Config, drv *incus.Driver) start.Starter {
	switch cfg.Start.Kind {
	case start.KindInstances:
		return start.Instances{Project: cfg.Project, L: drv, Names: cfg.Start.Names}
	default:
		return start.Compose{Run: incus.Run, Project: cfg.Project, File: cfg.Start.File, Args: cfg.Start.Args, UpArgs: cfg.Start.UpArgs}
	}
}

// targetFor is member's fence target with the instance types its project
// holds, read through Incus (best effort: these are operator commands, run
// while Incus is up; without types, only type-agnostic fences apply).
func targetFor(ctx context.Context, cfg *config.Config, rt *incus.Driver, member string) fence.Target {
	t := fence.Target{Member: member, Project: cfg.Project(member)}
	insts, err := rt.List(ctx, member)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: cannot list %s's instances (%v); fences that depend on the instance type will skip\n", member, err)
		return t
	}
	for _, in := range insts {
		if in.Type != "" && !slices.Contains(t.Types, in.Type) {
			t.Types = append(t.Types, in.Type)
		}
	}
	return t
}

// cmdProvision plans the per-member groups and projects, prints the plan, and
// applies it unless --dry-run. Cluster group changes need --yes or a "y" on
// an interactive terminal.
func cmdProvision(ctx context.Context, cfg *config.Config, api incus.API, dryRun, yes bool) error {
	var vols provision.Volumes
	if r := cfg.Replicate; r != nil {
		vols = provision.Volumes{Pool: r.Pool, Names: r.Volumes}
	}
	plan, err := provision.Make(ctx, api, cfg.Service, cfg.ProjectPrefix, cfg.Members, vols)
	if err != nil {
		return err
	}
	for _, ok := range plan.OK {
		fmt.Println("[ok]  " + ok)
	}
	for _, a := range plan.Actions {
		fmt.Println("[new] " + a.Command)
	}
	if len(plan.Actions) == 0 || dryRun {
		if dryRun && len(plan.Actions) > 0 {
			fmt.Println("dry run: nothing changed")
		}
		return nil
	}
	if plan.NeedsConfirmation() && !yes {
		if fi, err := os.Stdin.Stat(); err != nil || fi.Mode()&os.ModeCharDevice == 0 {
			return fmt.Errorf("cluster group changes need --yes (stdin is not a terminal)")
		}
		fmt.Print("This changes cluster groups. Apply? [y/N] ")
		var answer string
		_, _ = fmt.Scanln(&answer)
		if answer != "y" && answer != "Y" {
			return fmt.Errorf("not applied")
		}
	}
	if err := plan.Apply(ctx, api); err != nil {
		return err
	}
	fmt.Printf("applied %d change(s)\n", len(plan.Actions))
	return nil
}

// toolVersions reads the Incus server version and, with the compose
// starter, the incus-compose version, warning on any outside what pagurus is
// built for. They go into the agent's registration.
func toolVersions(ctx context.Context, cfg *config.Config, drv *incus.Driver, log *slog.Logger) map[string]string {
	vctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	v := map[string]string{}
	if s, err := drv.ServerVersion(vctx); err == nil {
		v["incus"] = s
	}
	if cfg.Start.Kind == start.KindCompose {
		if out, err := incus.Run(vctx, "incus-compose", "version"); err == nil {
			v["incus-compose"] = versions.Parse(string(out))
		} else {
			v["incus-compose"] = ""
		}
	}
	for _, tool := range []string{"incus", "incus-compose"} {
		if tool == "incus-compose" && cfg.Start.Kind != start.KindCompose {
			continue
		}
		if w := versions.Check(tool, v[tool]); w != "" {
			log.Warn("unsupported tool version", "tool", tool, "warning", w)
		}
	}
	return v
}
