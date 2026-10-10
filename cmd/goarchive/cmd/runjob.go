package cmd

import (
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

// registerSkipPolicyFlag adds --skip-validate-preflight (spec §8.3).
func registerSkipPolicyFlag(c *cobra.Command, skipPolicy *bool) {
	c.Flags().BoolVar(skipPolicy, "skip-validate-preflight", false,
		"Skip preflight's policy checks (permissions, triggers, foreign-key coverage, destination compatibility). "+
			"The keys are still read from MySQL, and the log-key and binding checks still run.")
}

// registerForceTriggersFlag adds --force-triggers, which preflight's DELETE-trigger check
// honours (archive and purge jobs).
func registerForceTriggersFlag(c *cobra.Command, forceTriggers *bool) {
	c.Flags().BoolVar(forceTriggers, "force-triggers", false,
		"Accept DELETE triggers on source tables of archive and purge jobs; every delete fires them.")
}

// runJob runs one job of type typ on the engine (spec §7.1): load and check the job, then
// connect, then engine.Run. The first signal stops at the next batch boundary, a second one
// cancels the run (database.SetupGracefulShutdown).
func runJob(c *cobra.Command, typ jobspec.Type, o *runOptions) error {
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
	_, err = engine.Run(ctx, engine.Config{
		Name: o.job, Spec: spec,
		Source: mgr.Source, Destination: mgr.Destination,
		JobSchema:               doc.Shared.Destination.EffectiveJobSchema(),
		Processing:              spec.EffectiveProcessing(doc.Shared.Processing),
		Verification:            verification,
		DisableForeignKeyChecks: doc.Shared.Safety.DisableForeignKeyChecks,
		ForceTriggers:           o.forceTriggers, SkipPolicy: o.skipPreflight,
		Lag: lag, Stop: stop, Logger: log,
		Progress: progress, Out: c.OutOrStdout(),
	})
	return err
}
