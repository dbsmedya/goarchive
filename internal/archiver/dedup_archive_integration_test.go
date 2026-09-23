//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"slices"
	"testing"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	_ "github.com/go-sql-driver/mysql"
)

// ============================================================================
// Deduplication by archiving: a table holds several rows per business key
// (base_payment_id). The job keeps the row with the minimum primary key of
// each key and archives every other row, using a correlated subquery in the
// job's `where`. The subquery reads the root table itself, while the run
// deletes from it batch by batch, so this proves the keeper is never selected
// and the dry-run counts agree with what the run actually moves.
// ============================================================================

const dedupTable = "dedup_payment"

// dedupWhere selects every row whose base_payment_id already has a row with a
// smaller id (the keeper). Rows with a NULL key are never duplicates.
const dedupWhere = dedupTable + ".base_payment_id IS NOT NULL" +
	" AND " + dedupTable + ".id > (" +
	" SELECT keeper.id FROM " + dedupTable + " AS keeper FORCE INDEX (idx_base_payment_id)" +
	" WHERE keeper.base_payment_id = " + dedupTable + ".base_payment_id" +
	" ORDER BY keeper.id LIMIT 1)"

// dedupIDs returns the ids in table, ascending.
func dedupIDs(t *testing.T, ctx context.Context, db *sql.DB) []int64 {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT id FROM "+dedupTable+" ORDER BY id")
	if err != nil {
		t.Fatalf("read ids: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ids: %v", err)
	}
	return ids
}

func TestDedupArchiveKeepsMinimumPK_Integration(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	f := newChrFixture(t, ctx)

	// 1. The test table, on both servers.
	ddl := "CREATE TABLE " + dedupTable + " (" +
		"id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
		"base_payment_id BIGINT NULL, " +
		"amount DECIMAL(10,2) NOT NULL, " +
		"KEY idx_base_payment_id (base_payment_id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)

	// 2. Duplicates. Keepers are the minimum id per base_payment_id: 1, 4, 5, 10.
	// Duplicates interleave with other keys (id 9 belongs to key 100) and span
	// batch boundaries at batch_size 2. NULL keys (7, 8) are never archived.
	f.ExecSource(t, ctx, "INSERT INTO "+dedupTable+" (id, base_payment_id, amount) VALUES "+
		"(1, 100, 10.00), (2, 100, 10.00), (3, 100, 10.00), "+
		"(4, 200, 20.00), "+
		"(5, 300, 30.00), (6, 300, 30.00), "+
		"(7, NULL, 70.00), (8, NULL, 70.00), "+
		"(9, 100, 10.00), "+
		"(10, 400, 40.00), (11, 400, 40.00), (12, 400, 40.00), (13, 400, 40.00)")
	wantKept := []int64{1, 4, 5, 7, 8, 10}
	wantArchived := []int64{2, 3, 6, 9, 11, 12, 13}

	// Independent count of the duplicates, not through goarchive's where.
	var independent int64
	if err := f.SourceDB.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(n - 1), 0) FROM (SELECT COUNT(*) AS n FROM "+dedupTable+
			" WHERE base_payment_id IS NOT NULL GROUP BY base_payment_id) AS g").Scan(&independent); err != nil {
		t.Fatalf("independent duplicate count: %v", err)
	}
	if independent != int64(len(wantArchived)) {
		t.Fatalf("fixture has %d duplicates by GROUP BY, want %d", independent, len(wantArchived))
	}

	g := graph.NewGraph(dedupTable, "id")
	cfg := &config.Config{
		Source: config.DatabaseConfig{
			Host:     getEnv("TEST_SOURCE_HOST", "127.0.0.1"),
			Port:     getEnvInt("TEST_SOURCE_PORT", 3305),
			User:     getEnv("TEST_SOURCE_USER", "root"),
			Password: getEnv("TEST_SOURCE_PASSWORD", "qazokm"),
			Database: f.SourceSchema,
			TLS:      "disable",
		},
		Destination: config.DatabaseConfig{
			Host:     getEnv("TEST_DEST_HOST", "127.0.0.1"),
			Port:     getEnvInt("TEST_DEST_PORT", 3307),
			User:     getEnv("TEST_DEST_USER", "root"),
			Password: getEnv("TEST_DEST_PASSWORD", "qazokm"),
			Database: f.DestSchema,
			TLS:      "disable",
		},
		Processing:   config.ProcessingConfig{BatchSize: 2, BatchDeleteSize: 1},
		Verification: config.VerificationConfig{Method: "sha256"},
		Logging:      config.LoggingConfig{Level: "error", Format: "json"},
	}
	jobCfg := &config.JobConfig{RootTable: dedupTable, PrimaryKey: "id", Where: dedupWhere}

	dbManager := database.NewManager(cfg)
	if err := dbManager.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = dbManager.Close() })

	// Preflight with the archive command's profile must accept the job.
	if err := chrRun(t, ctx, f.Checker(t, g), chrCommands[0], false); err != nil {
		t.Fatalf("archive preflight rejected the dedup job: %v", err)
	}

	// 3a. The dry-run's steps, as the dry-run command runs them: the estimate,
	// then the payload validation (which samples the root through the where).
	result, err := NewEstimator(dbManager.Source, cfg, jobCfg, g, nil).Estimate(ctx)
	if err != nil {
		t.Fatalf("dry-run estimate: %v", err)
	}
	t.Logf("dry-run estimate: root_count=%d estimated_batches=%d", result.RootCount, result.EstimatedBatches)
	if result.RootCount != int64(len(wantArchived)) {
		t.Errorf("dry-run root count = %d, want %d (the duplicates)", result.RootCount, len(wantArchived))
	}
	if want := int64((len(wantArchived) + 1) / 2); result.EstimatedBatches != want {
		t.Errorf("dry-run estimated batches = %d, want %d", result.EstimatedBatches, want)
	}
	validator := NewPayloadValidator(dbManager.Source, dbManager.Destination, g, f.SourceSchema, jobCfg,
		cfg.Safety, cfg.Processing.BatchSize, cfg.Verification, nil)
	if err := validator.Validate(ctx); err != nil {
		t.Fatalf("dry-run payload validation: %v", err)
	}
	if got := dedupIDs(t, ctx, f.DestDB); len(got) != 0 {
		t.Fatalf("dry-run left rows in the destination: %v", got)
	}

	// 3b. The archive run.
	const jobName = "dedup_archive_min_pk_job"
	testsupport.CleanupArchiverState(t, dbManager.Destination, jobName)
	orch, err := NewOrchestrator(cfg, jobName, jobCfg, dbManager)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	run, err := orch.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("archive execute: %v", err)
	}
	if !run.Success {
		t.Fatalf("archive did not succeed: %+v", run)
	}

	// The source keeps exactly the minimum id of each key, plus the NULL keys;
	// the destination holds exactly the duplicates.
	if got := dedupIDs(t, ctx, f.SourceDB); !slices.Equal(got, wantKept) {
		t.Errorf("source ids after archive = %v, want %v", got, wantKept)
	}
	if got := dedupIDs(t, ctx, f.DestDB); !slices.Equal(got, wantArchived) {
		t.Errorf("destination ids after archive = %v, want %v", got, wantArchived)
	}
	var dupKeys int
	if err := f.SourceDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM (SELECT base_payment_id FROM "+dedupTable+
			" WHERE base_payment_id IS NOT NULL GROUP BY base_payment_id HAVING COUNT(*) > 1) AS d").Scan(&dupKeys); err != nil {
		t.Fatalf("count remaining duplicate keys: %v", err)
	}
	if dupKeys != 0 {
		t.Errorf("source still has %d duplicated base_payment_id values", dupKeys)
	}

	// A second dry-run after the run finds nothing left to archive.
	again, err := NewEstimator(dbManager.Source, cfg, jobCfg, g, nil).Estimate(ctx)
	if err != nil {
		t.Fatalf("dry-run estimate after archive: %v", err)
	}
	t.Logf("dry-run estimate after archive: root_count=%d", again.RootCount)
	if again.RootCount != 0 {
		t.Errorf("dry-run root count after archive = %d, want 0", again.RootCount)
	}
}
