//go:build integration

package archiver

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

func newPayloadValidator(t *testing.T, setup *IntegrationTestSetup, jobCfg *config.JobConfig, batchSize int) *PayloadValidator {
	t.Helper()
	dbManager, cfg := setupRealDBManager(t, setup)

	builder := graph.NewBuilder(jobCfg)
	g, err := builder.Build()
	if err != nil {
		t.Fatalf("graph build: %v", err)
	}
	return NewPayloadValidator(dbManager.Source, dbManager.Destination, g, jobCfg,
		cfg.Safety, batchSize, logger.NewDefault())
}

func TestPayloadValidate_PassAndRollback_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	v := newPayloadValidator(t, setup, jobCfg, 1000)

	if err := v.Validate(ctx); err != nil {
		t.Fatalf("expected sane batch_size to pass, got: %v", err)
	}

	verifyDest := getVerificationDB(t, setup, "destination")
	defer func() { _ = verifyDest.Close() }()
	verifyRowCount(t, verifyDest, "customers", 0)
}

func TestPayloadValidate_RestoresFKChecksOnPool_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	dbManager, cfg := setupRealDBManager(t, setup)

	// Pin the destination pool to a single connection so the post-validation
	// query below is guaranteed to reuse the exact connection the validator used.
	// The validator disables FOREIGN_KEY_CHECKS (session-scoped) for its sample
	// INSERTs; if it failed to restore the setting before returning the connection
	// to the pool, this read would see 0.
	dbManager.Destination.SetMaxOpenConns(1)

	jobCfg := createCustomerOrderJobConfig()
	builder := graph.NewBuilder(jobCfg)
	g, err := builder.Build()
	if err != nil {
		t.Fatalf("graph build: %v", err)
	}
	v := NewPayloadValidator(dbManager.Source, dbManager.Destination, g, jobCfg,
		cfg.Safety, 1000, logger.NewDefault())

	if err := v.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	var fk int
	if err := dbManager.Destination.QueryRowContext(ctx,
		"SELECT @@SESSION.foreign_key_checks").Scan(&fk); err != nil {
		t.Fatalf("read foreign_key_checks: %v", err)
	}
	if fk != 1 {
		t.Fatalf("destination pool connection has FOREIGN_KEY_CHECKS=%d; expected 1 (session SET leaked into the pool)", fk)
	}
}

func TestPayloadValidate_OversizedBatch_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	seedTestData(t, sourceDB)

	jobCfg := createCustomerOrderJobConfig()
	v := newPayloadValidator(t, setup, jobCfg, 100000)

	err := v.Validate(ctx)
	if err == nil {
		t.Fatal("expected oversized batch_size to fail validation")
	}
	t.Logf("got expected failure: %v", err)
}
