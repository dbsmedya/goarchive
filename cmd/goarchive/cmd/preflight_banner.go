package cmd

import (
	"context"
	"fmt"

	"github.com/dbsmedya/goarchive/internal/archiver"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

const skipPreflightBanner = "\n" +
	"================================================================\n" +
	"  WARNING: --skip-validate-preflight is set\n" +
	"  Preflight checks will NOT run before this destructive operation.\n" +
	"\n" +
	"  This is unsafe. Continue only if you are recovering from an\n" +
	"  incident and have manually verified schema integrity.\n" +
	"================================================================\n"

func runRuntimePreflight(
	ctx context.Context,
	cfg *config.Config,
	jobCfg *config.JobConfig,
	dbManager *database.Manager,
	log *logger.Logger,
	profile archiver.PreflightProfile,
	forceTriggers bool,
	skip bool,
) error {
	if skip {
		log.Warn(skipPreflightBanner)
		log.Warnw("preflight checks SKIPPED via --skip-validate-preflight")
		return nil
	}

	builder := graph.NewBuilder(jobCfg)
	g, err := builder.Build()
	if err != nil {
		return fmt.Errorf("preflight: failed to build graph: %w", err)
	}
	checker, err := archiver.NewPreflightChecker(dbManager.Source, cfg.Source.Database, g, log)
	if err != nil {
		return fmt.Errorf("preflight: failed to create checker: %w", err)
	}
	if dbManager.Destination != nil && cfg.Destination.Database != "" {
		if err := checker.ConfigureDestination(dbManager.Destination, cfg.Destination.Database); err != nil {
			return fmt.Errorf("preflight: failed to configure destination: %w", err)
		}
	}
	if err := checker.RunWithProfile(ctx, profile, forceTriggers); err != nil {
		return fmt.Errorf("preflight checks failed (run 'goarchive validate' for full diagnostics): %w", err)
	}
	return nil
}
