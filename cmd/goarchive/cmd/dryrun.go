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
	Long: `Dry-run runs the job's preflight under its job type's profile, counts the rows
its where range matches, reads the first batch of root keys, and prints a
report: the job (root table, key, where and matching roots), the copy and
delete orders with the first batch's rows per table, the settings the run
would use, the statement caps,
and MySQL's plan (EXPLAIN FORMAT=TRADITIONAL) for the root fetch and for the
first discovery chunk of each child table.

Dry-run takes no lock, writes no tracking row and
changes no data. Its count reads the whole where range on the source: on a
large range it takes as long as that count takes.

Recommended operator workflow: validate -> dry-run -> archive.

Example:
  goarchive dry-run --config archiver.yaml --job archive_old_orders

Exit codes: see goarchive --help.`,
	RunE: runE(runDryrun),
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
	log, err := newJobLogger(doc, spec, o.job, c, false)
	if err != nil {
		return err
	}
	defer syncLogger(log)
	emitBanners(log, c.ErrOrStderr(), banners(doc, spec.Type, verification, o.skipPreflight, o.forceTriggers))
	ctx := context.Background()
	mgr, err := connect(ctx, doc)
	if err != nil {
		return err
	}
	defer func() { _ = mgr.Close() }()
	ex, err := engine.Explain(ctx, engine.Config{
		Name: o.job, Spec: spec,
		Source: mgr.Source, Destination: mgr.Destination,
		JobSchema:     doc.Shared.Destination.EffectiveJobSchema(),
		Processing:    spec.EffectiveProcessing(doc.Shared.Processing),
		Verification:  verification,
		ForceTriggers: o.forceTriggers, SkipPolicy: o.skipPreflight,
		Logger: log,
		Banner: func(b string) { emitBanners(log, c.ErrOrStderr(), []string{b}) },
	})
	if err != nil {
		return err
	}
	printExplanation(c.OutOrStdout(), o.job, spec.Type, GetConfigFile(), doc, spec, verification, ex)
	return nil
}
