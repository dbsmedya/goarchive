package cmd

import (
	"fmt"
	"os"

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
			"The keys are still read from MySQL, and the log-key and binding checks still run.")
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

// runJob runs one job of type typ on the engine (spec §7.1): load and check the job, then
// connect, then engine.Run. The first signal stops at the next batch boundary, a second one
// cancels the run (database.SetupGracefulShutdown). From the job's logger on, every return,
// and a panic, logs how the run ended (endRun); closing reports that it did.
func runJob(c *cobra.Command, typ jobspec.Type, o *runOptions) (rep *engine.Report, closing bool, err error) {
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
	defer func() {
		p := recover()
		endRun(log, typ, rep, err, p, entries)
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
	// failed prints the failure summary on stderr (setups A and B, without --progress, spec
	// §3.4a); the Error: line follows from Main.
	failed := func(rep *engine.Report) {
		if progress == 0 && !log.ConsoleJSON() {
			printFailureSummary(stderr, o.job, typ, rep, log.File())
		}
	}

	ctx, stop := database.SetupGracefulShutdown(
		func(os.Signal) {
			log.Warn("Stop requested: the current batch finishes, then the run stops. Signal again to cancel at once.")
		},
		func(os.Signal) { log.Warn("Cancelling the run now; the next run recovers what the log holds.") },
	)
	mgr, err := connect(ctx, doc)
	if err != nil {
		failed(nil)
		return nil, false, err
	}
	defer func() { _ = mgr.Close() }()
	var lag engine.LagGate
	if doc.Shared.Replication.Enabled {
		g, gerr := replication.New(doc.Shared.Replication, mgr.Replicas, log)
		if gerr != nil {
			failed(nil)
			return nil, false, gerr
		}
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
		Progress: progress, Out: out, ProgressLog: entries,
	})
	switch {
	case progress > 0 && !entries && rep != nil:
		printReport(out, o.job, typ, copyOrder(spec), rep, err)
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
