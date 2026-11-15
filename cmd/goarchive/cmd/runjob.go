package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/replication"
	"github.com/spf13/cobra"
)

// runOptions are the flags of archive, purge and copy-only.
type runOptions struct {
	job, verification, progress  string
	skipPreflight, forceTriggers bool
}

// registerRunFlags adds the flags of the run command for typ (P4): purge copies nothing, so
// it has no --verification; copy-only deletes nothing, and its preflight has no DELETE-trigger
// check (spec §8.1), so it has no --force-triggers.
func registerRunFlags(c *cobra.Command, o *runOptions, typ jobspec.Type) {
	c.Flags().StringVarP(&o.job, "job", "j", "", "Job name from configuration file (required)")
	_ = c.MarkFlagRequired("job")
	if typ != jobspec.Purge {
		registerVerificationFlag(c, &o.verification)
	}
	registerSkipPolicyFlag(c, &o.skipPreflight)
	if typ != jobspec.CopyOnly {
		registerForceTriggersFlag(c, &o.forceTriggers)
	}
	registerProgressFlag(c, &o.progress)
}

// skipPolicyFlag is the preflight bypass (spec §8.3; destination-engine spec §5). It is a
// command-line flag only: no configuration key reaches it.
const skipPolicyFlag = "skip-dangerously-validate-preflight-allow-experimental"

// registerSkipPolicyFlag adds the preflight bypass, and its old name, hidden, which refuses
// while the command line is parsed.
func registerSkipPolicyFlag(c *cobra.Command, skipPolicy *bool) {
	c.Flags().BoolVar(skipPolicy, skipPolicyFlag, false,
		"Dangerous. Skip preflight's policy checks (storage engines, permissions, triggers, foreign-key coverage, "+
			"destination compatibility, packet size), for an experimental setup GoArchive does not test or support. "+
			"The keys are still read from MySQL; a run still checks the job's log key and binding.")
	c.Flags().Var(renamedFlag{to: skipPolicyFlag}, "skip-validate-preflight", "")
	f := c.Flags().Lookup("skip-validate-preflight")
	f.NoOptDefVal = "true"
	f.Hidden = true
}

// renamedFlag refuses a flag's old name, whatever its value, naming the new one.
type renamedFlag struct{ to string }

func (renamedFlag) String() string { return "" }
func (renamedFlag) Type() string   { return "bool" }
func (r renamedFlag) Set(string) error {
	return fmt.Errorf("--skip-validate-preflight was renamed --%s (docs/README_UPGRADING_3_0.md)", r.to)
}

// registerForceTriggersFlag adds --force-triggers, which preflight's DELETE-trigger check
// honours (archive and purge jobs).
func registerForceTriggersFlag(c *cobra.Command, forceTriggers *bool) {
	c.Flags().BoolVar(forceTriggers, "force-triggers", false,
		"Accept DELETE triggers that preflight can list on source tables of archive and purge jobs; every delete fires them.")
}

// runCommand runs one job command and turns its outcome into what Main expects (spec §3.4):
// the stop marker for a clean stop, and a reportedError when the closing entry already
// carried the error to a JSON stderr log. A stop together with an error is the error.
func runCommand(c *cobra.Command, typ jobspec.Type, o *runOptions) error {
	rep, closing, err := runJob(c, typ, o)
	switch {
	case err == nil && rep != nil && rep.Stopped:
		return errStopped
	case err != nil && closing && diagJSON != nil:
		return &reportedError{err}
	}
	return err
}

// runJob runs one job of type typ on the engine (spec §7.1): load and check the job, bring up its
// control socket if it has one, then connect, then engine.Run. The first signal, or a `stop` over the
// socket, stops at the next batch boundary; a second signal cancels the run
// (database.SetupGracefulShutdown). From the job's logger on, every return, and a panic, logs how the
// run ended (endRun); closing reports that it did. The socket ends in the order the design fixes
// (spec §4.2 End): the terminal transition and the audit drain before any final output, the close after.
func runJob(c *cobra.Command, typ jobspec.Type, o *runOptions) (rep *engine.Report, closing bool, err error) {
	started := time.Now()
	doc, spec, err := loadJob(o.job, typ)
	if err != nil {
		return nil, false, err
	}
	verification, err := verificationFor(o.verification, doc, spec)
	if err != nil {
		return nil, false, err
	}
	progress, err := parseProgressInterval(c, o.progress)
	if err != nil {
		return nil, false, usage(err)
	}
	log, err := newJobLogger(doc, spec, o.job, c, progress > 0)
	if err != nil {
		return nil, false, err
	}
	defer syncLogger(log)
	// With --progress on a JSON console log, the start, progress and closing entries stand in
	// for the lines the flag asks for, so they are written past the level filter (spec §3.2 C).
	entries := progress > 0 && log.ConsoleJSON()
	var ctl *controlRun
	defer func() {
		p := recover()
		changes := runtimeChanges(ctl.end())
		endRun(log, typ, rep, err, p, entries, changes)
		if cerr := ctl.close(); cerr != nil {
			log.Warnf("Control socket: %v", cerr)
		}
		closing = true
		if p != nil {
			// Re-raised from the deferred call, above the panicking frames: the traceback
			// still shows where the run panicked.
			panic(p)
		}
	}()
	out, stderr := c.OutOrStdout(), c.ErrOrStderr()
	emitBanners(log, stderr, banners(doc, typ, verification, o.skipPreflight, o.forceTriggers))
	switch {
	case entries:
		log.Always().Infow("Run starting", "job_type", string(typ))
	case progress > 0:
		printStart(out, o.job, typ, log.File())
	default:
		log.Infow("Run starting", "job_type", string(typ))
	}
	// failed prints the failure summary (setups A and B, spec §3.4a): on stderr without
	// --progress; with it, on stdout in place of the closing block, which needs a report. The
	// Error: line follows on stderr from Main.
	failed := func(rep *engine.Report) {
		switch {
		case log.ConsoleJSON():
		case progress == 0:
			printFailureSummary(stderr, o.job, typ, rep, log.File())
		case rep == nil:
			printFailureSummary(out, o.job, typ, nil, log.File())
		}
	}

	shutdown := database.SetupGracefulShutdown(
		func(os.Signal) {
			log.Warn("Stop requested: the current batch finishes, then the run stops. Signal again to cancel at once.")
		},
		func(os.Signal) { log.Warn("Cancelling the run now; the next run recovers what the log holds.") },
	)
	ctx, stop := shutdown.Context(), shutdown.Stop()

	// The settings store every run reads, and the tracker every run feeds: the control socket's status
	// reads the tracker, and its commands write the store. The socket binds before the run connects, so
	// that status answers during preflight and startup, and a socket in use fails the run here, exit 4.
	store := newRunStore(doc, spec, shutdown.RequestFirst)
	tracker := engine.NewProgress(started)
	ctl, err = startControl(controlParams{
		doc: doc, spec: spec, name: o.job, typ: typ, store: store, tracker: tracker,
		shutdown: shutdown, log: log, stderr: stderr, started: started,
	})
	if err != nil {
		failed(nil)
		return nil, false, err
	}
	mgr, err := connect(ctx, doc)
	if err != nil {
		failed(nil)
		return nil, false, err
	}
	defer func() { _ = mgr.Close() }()
	var lag engine.LagGate
	if doc.Shared.Replication.Enabled {
		if gerr := refuseGroupServers(doc.Shared.Replication); gerr != nil {
			failed(nil)
			return nil, false, gerr
		}
		g, gerr := replication.New(doc.Shared.Replication, mgr.Replicas, log, store)
		if gerr != nil {
			failed(nil)
			return nil, false, gerr
		}
		ctl.setGate(g)
		lag = g
	}
	rep, err = engine.Run(ctx, engine.Config{
		Name: o.job, Spec: spec,
		Source: mgr.Source, Destination: mgr.Destination,
		JobSchema:               doc.Shared.Destination.EffectiveJobSchema(),
		Processing:              spec.EffectiveProcessing(doc.Shared.Processing),
		Verification:            verification,
		DisableForeignKeyChecks: doc.Shared.Safety.DisableForeignKeyChecks,
		ForceTriggers:           o.forceTriggers, SkipPolicy: o.skipPreflight,
		Lag: lag, Stop: stop, Logger: log,
		Replication: doc.Shared.Replication, ReplicaPools: mgr.Replicas,
		Tuning: store, Tracker: tracker,
		Banner:   func(b string) { emitBanners(log, stderr, []string{b}) },
		Progress: progress, Out: out, ProgressLog: entries,
	})
	// The terminal transition and the audit drain come before any final output, so the summary the
	// block and the closing entry carry is the one the audit entries above them add up to.
	changes := runtimeChanges(ctl.end())
	switch {
	case progress > 0 && !entries && rep != nil:
		printReport(out, o.job, typ, copyOrder(spec), rep, err, changes)
	case err != nil:
		failed(rep)
	}
	return rep, false, err
}

// copyOrder is the job's tables, parents first.
func copyOrder(spec *jobspec.Spec) []string {
	var out []string
	for _, n := range jobspec.NewTree(spec).CopyOrder() {
		out = append(out, n.Table)
	}
	return out
}

// refuseGroupServers is the interim refusal of GR C5: the replication gate does not probe a `type: group` server
// until C6, so a run that would build the gate with one listed is refused, and no group server goes unjudged. It
// is called where the gate is built, so validate and dry-run, which build none, are not affected.
// C6 removes it and its test.
func refuseGroupServers(cfg config.ReplicationConfig) error {
	if !cfg.Enabled {
		return nil
	}
	for i, s := range cfg.Servers {
		if s.Type == "group" {
			return fmt.Errorf("replication.servers[%d] %s is listed with type: group, which the replication gate does not probe yet (not built yet): "+
				"a run with a group server listed is refused, so that no group server goes unjudged. Remove it from replication.servers to run; "+
				"validate and dry-run are not affected", i, s.Addr())
		}
	}
	return nil
}
