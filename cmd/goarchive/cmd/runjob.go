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
		"Accept DELETE triggers on source tables of archive and purge jobs; every delete fires them.")
}

// runJob runs one job of type typ on the engine (spec §7.1): load and check the job, then
// connect, then engine.Run. The first signal stops at the next batch boundary, a second one
// cancels the run (database.SetupGracefulShutdown). From the job's logger on, every return
// logs how the run ended (logOutcome).
func runJob(c *cobra.Command, typ jobspec.Type, o *runOptions) (err error) {
	doc, spec, err := loadJob(o.job, typ)
	if err != nil {
		return err
	}
	verification, err := verificationFor(o.verification, doc, spec)
	if err != nil {
		return err
	}
	progress, err := parseProgressInterval(c, o.progress)
	if err != nil {
		return err
	}
	log, err := newJobLogger(doc, spec, o.job)
	if err != nil {
		return err
	}
	defer syncLogger(log)
	var rep *engine.Report
	defer func() { logOutcome(log, typ, rep, err) }()
	out := c.OutOrStdout()
	for _, b := range banners(doc, typ, verification, o.skipPreflight, o.forceTriggers) {
		_, _ = fmt.Fprintln(out, b)
	}

	ctx, stop := database.SetupGracefulShutdown(
		func(os.Signal) {
			log.Warn("Stop requested: the current batch finishes, then the run stops. Signal again to cancel at once.")
		},
		func(os.Signal) { log.Warn("Cancelling the run now; the next run recovers what the log holds.") },
	)
	mgr, err := connect(ctx, doc)
	if err != nil {
		return err
	}
	defer func() { _ = mgr.Close() }()
	var lag engine.LagGate
	if doc.Shared.Replication.Enabled {
		g, err := replication.New(doc.Shared.Replication, mgr.Replicas, log)
		if err != nil {
			return err
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
		Progress: progress, Out: out,
	})
	if rep != nil {
		printReport(out, o.job, typ, rep)
	}
	return err
}
