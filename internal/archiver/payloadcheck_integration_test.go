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
	dbManager := setupRealDBManager(t, setup)
	cfg := dbManager.GetConfig()

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
