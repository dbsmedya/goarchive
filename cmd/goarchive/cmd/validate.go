package cmd

import (
	"context"
	"fmt"

	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/spf13/cobra"
)

var validateOpts struct {
	job, verification string
	forceTriggers     bool
}

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate configuration and run preflight checks",
	Long: `Validate loads the configuration file, connects, and runs each job's
preflight under its job type's profile: archive gets every check, purge no
destination-table checks, copy-only no delete checks.

The checks read every table's primary key from MySQL and check the job's
documented keys and relations against it, then the permissions, triggers,
foreign-key coverage and destination compatibility, and that each statement cap fits
the server's max_allowed_packet.

Example:
  goarchive validate --config archiver.yaml
  goarchive validate --job archive_old_orders`,
	RunE: runE(runValidate),
}

func init() {
	validateCmd.Flags().StringVarP(&validateOpts.job, "job", "j", "", "Validate only this job (default: validate all jobs)")
	registerVerificationFlag(validateCmd, &validateOpts.verification)
	registerForceTriggersFlag(validateCmd, &validateOpts.forceTriggers)
	rootCmd.AddCommand(validateCmd)
}

func runValidate(c *cobra.Command, _ []string) error {
	o := validateOpts
	doc, err := jobspec.Load(GetConfigFile())
	if err != nil {
		return usage(err)
	}
	names := doc.JobNames()
	if o.job != "" {
		if _, err := doc.Job(o.job, ""); err != nil {
			return usage(err)
		}
		names = []string{o.job}
	}
	if err := checkVerificationFlag(o.verification); err != nil {
		return usage(err)
	}
	ctx := context.Background()
	mgr, err := connect(ctx, doc)
	if err != nil {
		return err
	}
	defer func() { _ = mgr.Close() }()
	src, err := database.ReadServerIdentity(ctx, mgr.Source)
	if err != nil {
		return err
	}
	dst, err := database.ReadServerIdentity(ctx, mgr.Destination)
	if err != nil {
		return err
	}

	out := c.OutOrStdout()
	var failed []string
	for i, name := range names {
		spec := doc.Jobs[name]
		_, _ = fmt.Fprintf(out, "[%d/%d] Job: %s (%s)\n", i+1, len(names), name, spec.Type)
		verification, err := verificationFor(o.verification, doc, spec)
		if err != nil {
			return err
		}
		log, err := newJobLogger(doc, spec, name, c, false)
		if err != nil {
			return err
		}
		// The R45 banners, as a run would print them (spec §3.4a; Codex MR R2).
		emitBanners(log, c.ErrOrStderr(), banners(doc, spec.Type, verification, false, o.forceTriggers))
		res, err := preflight.Run(ctx, preflight.Config{
			Source: mgr.Source, SourceDatabase: src.Schema,
			Destination: mgr.Destination, DestinationDatabase: dst.Schema,
			JobSchema: doc.Shared.Destination.EffectiveJobSchema(),
			Spec:      spec, Verification: verification, ForceTriggers: o.forceTriggers, Logger: log,
			Processing: spec.EffectiveProcessing(doc.Shared.Processing),
			Banner:     func(b string) { emitBanners(log, c.ErrOrStderr(), []string{b}) },
		})
		syncLogger(log)
		if err != nil {
			_, _ = fmt.Fprintf(out, "   FAILED: %v\n\n", err)
			failed = append(failed, name)
			continue
		}
		_, _ = fmt.Fprintf(out, "   PASSED\n")
		printCaps(out, res.Caps)
		_, _ = fmt.Fprintln(out)
	}
	if len(failed) > 0 {
		return fmt.Errorf("validation failed for %d of %d job(s): %v", len(failed), len(names), failed)
	}
	_, _ = fmt.Fprintf(out, "All %d job(s) validated.\n", len(names))
	return nil
}
