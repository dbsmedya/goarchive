package cmd

import (
	"context"

	"github.com/dbsmedya/goarchive/v3/internal/engine"
	"github.com/spf13/cobra"
)

var dryrunOpts struct {
	job, verification            string
	skipPreflight, forceTriggers bool
}

var dryrunCmd = &cobra.Command{
	Use:   "dry-run",
	Short: "Show how a job would run, without changing anything",
	Long: `Dry-run runs the job's preflight under its job type's profile, reads
the first batch of root keys, and prints MySQL's plan for the root fetch and
for each child table's discovery query, bound with those keys:
EXPLAIN FORMAT=TRADITIONAL. It takes no lock, writes no tracking row and
changes no data.

Recommended operator workflow: validate -> dry-run -> archive.

Example:
  goarchive dry-run --config archiver.yaml --job archive_old_orders`,
	RunE: runDryrun,
}

func init() {
	dryrunCmd.Flags().StringVarP(&dryrunOpts.job, "job", "j", "", "Job name from configuration file (required)")
	_ = dryrunCmd.MarkFlagRequired("job")
	registerVerificationFlag(dryrunCmd, &dryrunOpts.verification)
	registerSkipPolicyFlag(dryrunCmd, &dryrunOpts.skipPreflight)
	registerForceTriggersFlag(dryrunCmd, &dryrunOpts.forceTriggers)
	rootCmd.AddCommand(dryrunCmd)
}

func runDryrun(c *cobra.Command, _ []string) error {
	o := dryrunOpts
	doc, spec, err := loadJob(o.job, "")
	if err != nil {
		return err
	}
	verification, err := verificationFor(o.verification, doc, spec)
	if err != nil {
		return err
	}
	log, err := newJobLogger(doc, spec, o.job)
	if err != nil {
		return err
	}
	defer syncLogger(log)
	ctx := context.Background()
	mgr, err := connect(ctx, doc)
	if err != nil {
		return err
	}
	defer func() { _ = mgr.Close() }()
	_, err = engine.Explain(ctx, engine.Config{
		Name: o.job, Spec: spec,
		Source: mgr.Source, Destination: mgr.Destination,
		JobSchema:     doc.Shared.Destination.EffectiveJobSchema(),
		Processing:    spec.EffectiveProcessing(doc.Shared.Processing),
		Verification:  verification,
		ForceTriggers: o.forceTriggers, SkipPolicy: o.skipPreflight,
		Logger: log,
	})
	return err
}
