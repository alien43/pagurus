// Command pagurus runs one incus-compose stack on at most one member of an
// Incus cluster, failing it over between members. See README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/alien43/pagurus/internal/agent"
	"github.com/alien43/pagurus/internal/config"
	"github.com/alien43/pagurus/internal/coord"
	"github.com/alien43/pagurus/internal/incus"
	"github.com/alien43/pagurus/internal/watchdog"
)

var version = "dev"

const usage = `usage: pagurus <command> -c <config.json> [flags]

commands:
  run         run the agent on this member (as root, under systemd)
  validate    check the config, this member's Incus project and the compose file
  status      show the office, member states and every member's instances
  move        planned move: pagurus move -c cfg --to <member>
  fence-ack   operator override: record that a member is verified down
              pagurus fence-ack -c cfg --member <member>
  audit       watch every member's project and report any moment with more
              than one member running the workload (drill tool)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(64)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	cfgPath := fs.String("c", "", "config file")
	to := fs.String("to", "", "move: target member")
	member := fs.String("member", "", "fence-ack: member to acknowledge")
	every := fs.Duration("every", time.Second, "audit: poll interval")
	dur := fs.Duration("for", 0, "audit: stop after this long (0 = until interrupted)")
	jsonLog := fs.Bool("json", false, "run: log as JSON")
	timeout := fs.Duration("timeout", 3*time.Minute, "move: how long to wait for the new holder")
	self := fs.String("self", "", "run: this host's cluster member name; lets the agent fence locally even when the local Incus is down at startup")
	endpoints := fs.String("etcd", "", "override etcd.endpoints (comma-separated; drill harness)")
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

	rt := incus.New(incus.Options{
		Compose: cfg.Compose, ComposeArgs: cfg.ComposeArgs, UpArgs: cfg.UpArgs, ProjectPrefix: cfg.ProjectPrefix,
	})

	switch cmd {
	case "validate":
		err = cmdValidate(ctx, cfg, rt)
	case "run":
		err = cmdRun(ctx, cfg, rt, log, *self)
	case "status":
		err = withEtcd(cfg, func(cli *clientv3.Client) error { return cmdStatus(ctx, cfg, rt, cli) })
	case "move":
		err = withEtcd(cfg, func(cli *clientv3.Client) error { return cmdMove(ctx, cfg, cli, *to, *timeout) })
	case "fence-ack":
		err = withEtcd(cfg, func(cli *clientv3.Client) error { return cmdFenceAck(ctx, cfg, cli, *member) })
	case "audit":
		err = cmdAudit(ctx, cfg, rt, *every, *dur)
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
	return clientv3.New(clientv3.Config{
		Endpoints:            cfg.Etcd.Endpoints,
		DialTimeout:          cfg.Etcd.DialTimeout.D(),
		DialKeepAliveTime:    cfg.Lease.RenewInterval.D(),
		DialKeepAliveTimeout: cfg.Etcd.OpTimeout.D(),
		// The agent logs what matters itself; the client's own retry chatter
		// during a partition is noise.
		Logger: zap.NewNop(),
	})
}

func withEtcd(cfg *config.Config, f func(*clientv3.Client) error) error {
	cli, err := newEtcd(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()
	return f(cli)
}

func cmdValidate(ctx context.Context, cfg *config.Config, rt *incus.Driver) error {
	a := agent.New(agent.Options{Config: cfg, Runtime: rt, Etcd: nil})
	if err := a.Preflight(ctx); err != nil {
		return err
	}
	fmt.Printf("ok: member %s, project %s pinned, compose renders, timing valid (TTL %s, self-fence lead %s)\n",
		a.Member(), cfg.Project(a.Member()), cfg.Lease.TTL.D(), cfg.SelfFenceLead())
	for _, m := range cfg.Members {
		if err := rt.CheckProject(ctx, m); err != nil {
			fmt.Printf("WARNING: %v\n", err)
		}
	}
	return nil
}

func cmdRun(ctx context.Context, cfg *config.Config, rt *incus.Driver, log *slog.Logger, self string) error {
	if os.Geteuid() != 0 {
		return errors.New("run must be root (cgroup.kill, watchdog)")
	}
	cli, err := newEtcd(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()
	a := agent.New(agent.Options{Config: cfg, Runtime: rt, Etcd: cli, Log: log, Version: version, Member: self})
	if err := a.Preflight(ctx); err != nil {
		// No lease is held by a starting agent, so nothing may run here. If
		// the local Incus is what failed, cgroup.kill still works: use it,
		// then exit non-zero and let systemd retry.
		if self != "" {
			if running, lerr := rt.LocalRunning(self); lerr == nil && len(running) > 0 {
				log.Error("preflight failed with local instances running and no lease: killing them", "running", running)
				kctx, cancel := context.WithTimeout(context.Background(), cfg.SelfFence.KillBudget.D())
				if kerr := rt.Kill(kctx, self); kerr != nil {
					log.Error("emergency cgroup.kill failed", "err", kerr)
				}
				cancel()
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
		fmt.Printf("office:   %s  epoch=%d  since=%s  lease_ttl=%ds\n", rec.Member, epoch, rec.Acquired.Local().Format(time.TimeOnly), ttl)
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
	return tw.Flush()
}

func cmdMove(ctx context.Context, cfg *config.Config, cli *clientv3.Client, to string, timeout time.Duration) error {
	if !slices.Contains(cfg.Members, to) {
		return fmt.Errorf("--to %q is not a member", to)
	}
	co := coord.New(cli, cfg.EtcdPrefix, cfg.Etcd.OpTimeout.D())
	if err := co.SetPin(ctx, to); err != nil {
		return err
	}
	defer func() {
		if err := co.SetPin(context.Background(), ""); err != nil {
			fmt.Fprintln(os.Stderr, "WARNING: could not clear the pin:", err)
		}
	}()
	fmt.Printf("pinned office to %s; waiting for it to take over\n", to)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
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
