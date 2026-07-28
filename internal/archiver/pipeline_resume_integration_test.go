//go:build integration

// Deterministic resume-scenario tests for the shared batch pipeline.
//
// The pre-existing crash-recovery integration tests (TestCopyOnly_CrashRecovery,
// TestPurge_CrashRecovery, TestPurge_ResumeFromCheckpoint) are timing-based:
// they sleep, cancel, and assert final row counts, so WHICH resume path they
// exercise varies per run. These tests seed the per-job log table to an exact
// status set via SQL and then run Execute, proving the gates and replay paths of
// batchPipeline.recover() end-to-end (issues #1, #8, #18).
//
// # Why the seeded state puts non-terminal rows BELOW the checkpoint
//
// Scenarios 1-4 seed non-terminal rows below an already-advanced checkpoint.
// That is deliberate and load-bearing: it is the only shape in which the
// forward scan CANNOT reach those roots, so passing proves recovery did the
// work. Seeding non-terminal rows with an EMPTY checkpoint (the naive shape)
// produces tests that still pass with recover() stubbed out, because the
// forward scan re-fetches every root above the empty checkpoint and converges
// on the same end state — such a test asserts a true fact while proving nothing
// about recovery. It is also the realistic shape: a checkpoint that ran ahead of
// non-terminal rows is exactly the permanent hole issue #18 describes, and it is
// what the operator guidance in recover()'s legacy-failed-row gate produces when
// a failed row below the checkpoint is re-queued to 'pending'.
//
// Scenario 5 is the deliberate exception: its checkpoint MUST be empty, because
// the bypass it guards is ShouldResume returning false for a failed-only log
// table with no checkpoint.

package archiver

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
)

// resumeScenarioTables is the customer/orders graph in parent-first order.
var resumeScenarioTables = []string{"customers", "orders", "order_items", "order_payments"}

// Per-root row counts produced by seedResumeScenarioData: one customer, two
// orders, four order_items, two order_payments.
const (
	rowsPerRootOrders   = 2
	rowsPerRootItems    = 4
	rowsPerRootPayments = 2
)

// resumeScenarioRoots is the source root set every scenario seeds. Roots 1-2 (or
// 1-3) carry the seeded non-terminal state; the rest are already 'completed' and
// sit at/below the checkpoint.
var resumeScenarioRoots = []int64{1, 2, 3, 4, 5}

// resumeScenarioCheckpoint is the job checkpoint seeded for scenarios 1-4: the
// max root PK, i.e. the forward scan is already finished and can never re-reach
// the seeded non-terminal roots.
const resumeScenarioCheckpoint = "5"

// resumeScenarioWhere selects the whole seeded root set.
const resumeScenarioWhere = "id <= 5"

// ============================================================================
// Fixtures
// ============================================================================

// truncateResumeScenarioTables empties the data tables (not the tracking
// tables) so a scenario starts from a known row set.
func truncateResumeScenarioTables(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatalf("disable FK checks: %v", err)
	}
	for _, table := range []string{"order_payments", "order_items", "orders", "customers"} {
		if _, err := db.Exec("TRUNCATE TABLE " + sqlutil.QuoteIdentifier(table)); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatalf("re-enable FK checks: %v", err)
	}
}

// seedResumeScenarioData inserts a fully deterministic subgraph for each root
// id: customer <id>, orders <id>01/<id>02, two order_items and one
// order_payment per order. All timestamps are literal so a source row and its
// destination copy hash identically under sha256 verification.
func seedResumeScenarioData(t *testing.T, db *sql.DB, customerIDs ...int64) {
	t.Helper()
	for _, cid := range customerIDs {
		if _, err := db.Exec(
			"INSERT INTO customers (id, name, email, created_at) VALUES (?, ?, ?, '2023-01-01 00:00:00')",
			cid, fmt.Sprintf("Customer %d", cid), fmt.Sprintf("customer%d@example.com", cid),
		); err != nil {
			t.Fatalf("seed customer %d: %v", cid, err)
		}
		for o := int64(1); o <= rowsPerRootOrders; o++ {
			oid := cid*100 + o
			if _, err := db.Exec(
				"INSERT INTO orders (id, customer_id, total, status, created_at) VALUES (?, ?, ?, 'completed', '2023-01-02 00:00:00')",
				oid, cid, 100+o,
			); err != nil {
				t.Fatalf("seed order %d: %v", oid, err)
			}
			for i := int64(1); i <= rowsPerRootItems/rowsPerRootOrders; i++ {
				iid := oid*10 + i
				if _, err := db.Exec(
					"INSERT INTO order_items (id, order_id, product, quantity, price) VALUES (?, ?, ?, ?, ?)",
					iid, oid, fmt.Sprintf("Product %d", iid), i, 10*i,
				); err != nil {
					t.Fatalf("seed order_item %d: %v", iid, err)
				}
			}
			if _, err := db.Exec(
				"INSERT INTO order_payments (id, order_id, amount, method, paid_at) VALUES (?, ?, ?, 'credit_card', '2023-01-03 00:00:00')",
				oid, oid, 100+o,
			); err != nil {
				t.Fatalf("seed order_payment %d: %v", oid, err)
			}
		}
	}
}

// resumeScenarioDBs pulls the two connections out of a setup.
func resumeScenarioDBs(t *testing.T, setup *IntegrationTestSetup) (*sql.DB, *sql.DB) {
	t.Helper()
	sourceDB, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found in setup")
	}
	destDB, ok := setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in setup")
	}
	return sourceDB, destDB
}

// resetResumeScenarioData returns both sides to the scenario's starting row set
// without re-running the (DDL-heavy) database setup, so a test with subtests can
// share one SetupIntegrationTest.
func resetResumeScenarioData(t *testing.T, sourceDB, destDB *sql.DB) {
	t.Helper()
	truncateResumeScenarioTables(t, sourceDB)
	truncateResumeScenarioTables(t, destDB)
	seedResumeScenarioData(t, sourceDB, resumeScenarioRoots...)
}

// resumeScenarioDBManager builds a database manager and config with an explicit
// verification method and batch size. A small batch size makes recovery chunk
// (proving the per-chunk behaviour) instead of running as one chunk.
func resumeScenarioDBManager(t *testing.T, setup *IntegrationTestSetup, method string, batchSize int) (*database.Manager, *config.Config) {
	t.Helper()
	var sourceCfg, destCfg DatabaseConfig
	found := 0
	for _, db := range setup.Config.Databases {
		switch db.Name {
		case "source":
			sourceCfg = db
			found++
		case "destination":
			destCfg = db
			found++
		}
	}
	if found != 2 {
		t.Fatal("source and/or destination database config not found")
	}

	cfg := &config.Config{
		Source: config.DatabaseConfig{
			Host: sourceCfg.Host, Port: sourceCfg.Port, User: sourceCfg.User,
			Password: sourceCfg.Password, Database: sourceCfg.Database, TLS: "disable",
		},
		Destination: config.DatabaseConfig{
			Host: destCfg.Host, Port: destCfg.Port, User: destCfg.User,
			Password: destCfg.Password, Database: destCfg.Database, TLS: "disable",
		},
		Processing: config.ProcessingConfig{
			BatchSize:       batchSize,
			BatchDeleteSize: 10,
			SleepSeconds:    0,
		},
		Verification: config.VerificationConfig{Method: method},
		Safety:       config.SafetyConfig{DisableForeignKeyChecks: true},
		Logging:      config.LoggingConfig{Level: "info", Format: "json"},
	}

	dbManager := database.NewManager(cfg)
	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := dbManager.Connect(connectCtx); err != nil {
		t.Fatalf("connect database manager: %v", err)
	}
	// Close the pool when the scenario ends. Leaving pooled connections open
	// against a schema a later test DROPs is a needless way to provoke
	// server-side trouble.
	t.Cleanup(func() {
		if err := dbManager.Close(); err != nil {
			t.Logf("close database manager: %v", err)
		}
	})
	return dbManager, cfg
}

// resumeScenarioJobConfig is the customer/orders graph with an explicit,
// id-based WHERE so the forward scan's reach is exactly controllable.
func resumeScenarioJobConfig() *config.JobConfig {
	jobCfg := createCustomerOrderJobConfig()
	jobCfg.Where = resumeScenarioWhere
	return jobCfg
}

// dropDestinationSecondaryUniqueIndexes removes non-PRIMARY unique indexes from
// the destination graph tables (customers.email in the fixture).
//
// Why: a destination secondary unique index forces strict INSERT even under
// sha256 (shouldUseStrictInsert), and recover()'s strict-INSERT gate then
// REFUSES to replay 'pending' rows by design. Dropping the index is not a test
// hack — DEST_SCHEMA_COMPATIBILITY_CHECK documents a looser destination
// (dropped secondary indexes) as a supported write-performance optimization.
// The strict-refusal path itself is covered by the unit-level gate tests.
func dropDestinationSecondaryUniqueIndexes(t *testing.T, destDB *sql.DB, schema string) {
	t.Helper()
	ctx := context.Background()
	found, err := destinationSecondaryUniqueIndexes(ctx, destDB, schema, resumeScenarioTables)
	if err != nil {
		t.Fatalf("inspect destination unique indexes: %v", err)
	}
	for _, tableIndex := range found {
		parts := strings.SplitN(tableIndex, ".", 2)
		if len(parts) != 2 {
			t.Fatalf("unexpected table.index form %q", tableIndex)
		}
		stmt := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s",
			sqlutil.QuoteIdentifier(parts[0]), sqlutil.QuoteIdentifier(parts[1]))
		if _, err := destDB.Exec(stmt); err != nil {
			t.Fatalf("drop destination index %s: %v", tableIndex, err)
		}
	}
	// Re-assert the precondition: if a future fixture change adds another
	// unique index, fail loudly here rather than silently turning every
	// pending-replay scenario into a strict-INSERT refusal.
	found, err = destinationSecondaryUniqueIndexes(ctx, destDB, schema, resumeScenarioTables)
	if err != nil {
		t.Fatalf("re-inspect destination unique indexes: %v", err)
	}
	if len(found) > 0 {
		t.Fatalf("destination still has secondary unique indexes after drop: %v", found)
	}
}

// ============================================================================
// Tracking-state bootstrap
// ============================================================================

// bootstrapJobTracking materializes archiver_job and the per-job
// archiver_job_log_<id> table for jobName WITHOUT touching any data, so the
// scenario can then seed an exact status set via SQL. It uses the same
// ResumeManager calls beginJobStartup makes (InitializeTables +
// GetOrCreateJobWithType), so the created state is identical to what a real run
// produces — with no lock, heartbeat or prompt side effects.
//
// Returns the fully-qualified, quoted per-job log table name.
func bootstrapJobTracking(t *testing.T, destDB *sql.DB, destSchema, jobName, rootTable, jobType string) string {
	t.Helper()
	ctx := context.Background()

	// "Before" cleanup: drop any leftover tracking state for this job name.
	var staleID int64
	if err := destDB.QueryRowContext(ctx, "SELECT id FROM archiver_job WHERE job_name = ?", jobName).Scan(&staleID); err == nil {
		if _, err := destDB.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `archiver_job_log_%d`", staleID)); err != nil {
			t.Fatalf("drop stale log table for %q: %v", jobName, err)
		}
		if _, err := destDB.ExecContext(ctx, "DELETE FROM archiver_job WHERE job_name = ?", jobName); err != nil {
			t.Fatalf("delete stale job row for %q: %v", jobName, err)
		}
	}

	resumeMgr, err := NewResumeManager(destDB, nil, destSchema)
	if err != nil {
		t.Fatalf("new resume manager: %v", err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		t.Fatalf("initialize tracking tables: %v", err)
	}
	if _, err := resumeMgr.GetOrCreateJobWithType(ctx, jobName, rootTable, jobType); err != nil {
		t.Fatalf("create job %q: %v", jobName, err)
	}

	// "After" cleanup.
	testsupport.CleanupArchiverState(t, destDB, jobName)

	logTable := resumeMgr.LogTableName()
	if logTable == "" {
		t.Fatal("per-job log table not resolved after bootstrap")
	}
	return logTable
}

// seedLogStatus writes exact per-root-PK statuses into the per-job log table.
func seedLogStatus(t *testing.T, destDB *sql.DB, logTable string, status LogStatus, pks ...string) {
	t.Helper()
	for _, pk := range pks {
		if _, err := destDB.Exec(
			fmt.Sprintf("INSERT INTO %s (root_pk_id, log_status) VALUES (?, ?)", logTable), pk, status,
		); err != nil {
			t.Fatalf("seed log row %s=%s: %v", pk, status, err)
		}
	}
}

// setJobCheckpoint seeds last_processed_root_pk_id for the job.
func setJobCheckpoint(t *testing.T, destDB *sql.DB, jobName, checkpoint string) {
	t.Helper()
	res, err := destDB.Exec(
		"UPDATE archiver_job SET last_processed_root_pk_id = ? WHERE job_name = ?", checkpoint, jobName)
	if err != nil {
		t.Fatalf("seed checkpoint for %q: %v", jobName, err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("seed checkpoint for %q: %d rows affected (err %v), want 1", jobName, n, err)
	}
}

// readLogStatuses returns the full per-job log table as pk -> status.
func readLogStatuses(t *testing.T, destDB *sql.DB, logTable string) map[string]LogStatus {
	t.Helper()
	rows, err := destDB.Query(fmt.Sprintf("SELECT root_pk_id, log_status FROM %s", logTable))
	if err != nil {
		t.Fatalf("read log statuses: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]LogStatus)
	for rows.Next() {
		var pk string
		var status int8
		if err := rows.Scan(&pk, &status); err != nil {
			t.Fatalf("scan log status: %v", err)
		}
		out[pk] = LogStatus(status)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate log statuses: %v", err)
	}
	return out
}

// assertAllCompleted fails unless the log table holds exactly wantPKs, all
// LogStatusCompleted.
func assertAllCompleted(t *testing.T, destDB *sql.DB, logTable string, wantPKs ...string) {
	t.Helper()
	got := readLogStatuses(t, destDB, logTable)
	if len(got) != len(wantPKs) {
		t.Errorf("log table has %d rows (%v), want %d (%v)", len(got), got, len(wantPKs), wantPKs)
	}
	for _, pk := range wantPKs {
		status, ok := got[pk]
		if !ok {
			t.Errorf("log row for PK %s missing", pk)
			continue
		}
		if status != LogStatusCompleted {
			t.Errorf("PK %s has status %s, want completed", pk, status)
		}
	}
}

// readJobCheckpoint returns last_processed_root_pk_id ("" when NULL/unset).
func readJobCheckpoint(t *testing.T, destDB *sql.DB, jobName string) string {
	t.Helper()
	var checkpoint sql.NullString
	if err := destDB.QueryRow(
		"SELECT last_processed_root_pk_id FROM archiver_job WHERE job_name = ?", jobName,
	).Scan(&checkpoint); err != nil {
		t.Fatalf("read checkpoint for %q: %v", jobName, err)
	}
	if !checkpoint.Valid {
		return ""
	}
	return checkpoint.String
}

// assertCheckpointUnchanged asserts the seeded checkpoint neither advanced nor
// regressed. A regression would re-open every completed root above it to the
// forward scan (the checkpointFloor guarantee).
func assertCheckpointUnchanged(t *testing.T, destDB *sql.DB, jobName string) {
	t.Helper()
	if got := readJobCheckpoint(t, destDB, jobName); got != resumeScenarioCheckpoint {
		t.Errorf("checkpoint = %q, want %q (recovery must not move a checkpoint at/above the recovered PKs)",
			got, resumeScenarioCheckpoint)
	}
}

// countRows runs a scalar COUNT(*) query.
func countRows(t *testing.T, db *sql.DB, query string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// assertRootSet asserts db holds exactly the subgraphs of wantRoots: the exact
// root id list, the exact per-table row totals those roots imply, and no child
// row belonging to any other root.
func assertRootSet(t *testing.T, db *sql.DB, label string, wantRoots ...int64) {
	t.Helper()

	rows, err := db.Query("SELECT id FROM customers ORDER BY id")
	if err != nil {
		t.Fatalf("%s: list customers: %v", label, err)
	}
	defer func() { _ = rows.Close() }()
	var got []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("%s: scan customer id: %v", label, err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: iterate customer ids: %v", label, err)
	}
	if fmt.Sprint(got) != fmt.Sprint(wantRoots) {
		t.Errorf("%s: customer ids = %v, want %v", label, got, wantRoots)
	}

	n := len(wantRoots)
	for table, want := range map[string]int{
		"customers":      n,
		"orders":         n * rowsPerRootOrders,
		"order_items":    n * rowsPerRootItems,
		"order_payments": n * rowsPerRootPayments,
	} {
		if have := countRows(t, db, "SELECT COUNT(*) FROM "+sqlutil.QuoteIdentifier(table)); have != want {
			t.Errorf("%s: %s has %d rows, want %d", label, table, have, want)
		}
	}

	// No child row may reference a root outside the expected set.
	if orphans := countRows(t, db,
		"SELECT COUNT(*) FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE c.id IS NULL",
	); orphans != 0 {
		t.Errorf("%s: %d orders reference a root outside the expected set", label, orphans)
	}
}

// ============================================================================
// The scenarios
// ============================================================================

// TestPipelineResume_Integration runs the five deterministic resume scenarios
// against one set of databases.
//
// They share a single SetupIntegrationTest deliberately: each scenario uses a
// distinct job name and resets the data tables itself, so a per-scenario
// re-setup would only repeat the (DDL-heavy) drop/create/fixture cycle for no
// coverage. Sharing it also keeps this file's schema churn to a single cycle.
func TestPipelineResume_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	sourceDB, destDB := resumeScenarioDBs(t, setup)
	destSchema := getDestSchema(setup)

	// ------------------------------------------------------------------
	// Scenario 1 — copy-only pending replay below the checkpoint (issue #18)
	// ------------------------------------------------------------------
	//
	// Roots {1,2,3} are 'pending' UNDER a checkpoint of 5 — the permanent hole
	// issue #18 describes, where a prior release advanced the checkpoint past
	// rows it never finished. The forward scan starts after PK 5 and can never
	// re-reach them, so recovery is the only thing that can close the hole.
	//
	// It must replay all three, mark them completed, and leave the checkpoint at
	// 5: copy-only recovery advances the checkpoint per chunk, but never below
	// the startup floor, or every completed root above it would re-open.
	t.Run("copyonly-pending-replay", func(t *testing.T) {
		const jobName = "pipeline_resume_copyonly_pending"
		resetResumeScenarioData(t, sourceDB, destDB)
		dropDestinationSecondaryUniqueIndexes(t, destDB, destSchema)

		// batch_size 2 with three pending roots forces recovery to run TWO chunks.
		dbManager, cfg := resumeScenarioDBManager(t, setup, "sha256", 2)
		jobCfg := resumeScenarioJobConfig()

		logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, jobCfg.RootTable, JobTypeCopyOnly)
		seedLogStatus(t, destDB, logTable, LogStatusPending, "1", "2", "3")
		seedLogStatus(t, destDB, logTable, LogStatusCompleted, "4", "5")
		setJobCheckpoint(t, destDB, jobName, resumeScenarioCheckpoint)

		orch, err := NewCopyOnlyOrchestrator(cfg, jobName, jobCfg, dbManager)
		if err != nil {
			t.Fatalf("NewCopyOnlyOrchestrator: %v", err)
		}
		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize: %v", err)
		}

		result, err := orch.Execute(ctx, false)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if !result.Success {
			t.Fatalf("expected success, got errors: %v", result.Errors)
		}

		assertAllCompleted(t, destDB, logTable, "1", "2", "3", "4", "5")

		// The hole is closed: every row of all three subgraphs is present,
		// exactly once. Roots 4 and 5 were completed by the (simulated) prior
		// run and are correctly not re-copied.
		assertRootSet(t, destDB, "destination", 1, 2, 3)
		assertCheckpointUnchanged(t, destDB, jobName)

		// copy-only never deletes: the source is untouched.
		assertRootSet(t, sourceDB, "source", resumeScenarioRoots...)
	})

	// ------------------------------------------------------------------
	// Scenario 2 — copy-only copied promotion (issue #1: no re-copy on resume)
	// ------------------------------------------------------------------
	//
	// Roots {1,2} are 'copied' — copy+verify already succeeded and their rows
	// are already committed to the destination — again below the checkpoint, so
	// only recovery can reach them. Recovery must run the completion tail ONLY
	// (batchPromote): no discovery, no re-copy, no re-verify.
	t.Run("copyonly-copied-promote", func(t *testing.T) {
		const jobName = "pipeline_resume_copyonly_copied"
		resetResumeScenarioData(t, sourceDB, destDB)
		dropDestinationSecondaryUniqueIndexes(t, destDB, destSchema)

		// The prior run's copy is already durable on the destination.
		seedResumeScenarioData(t, destDB, 1, 2)
		assertRootSet(t, destDB, "destination (pre-run)", 1, 2)

		dbManager, cfg := resumeScenarioDBManager(t, setup, "sha256", 2)
		jobCfg := resumeScenarioJobConfig()

		logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, jobCfg.RootTable, JobTypeCopyOnly)
		seedLogStatus(t, destDB, logTable, LogStatusCopied, "1", "2")
		seedLogStatus(t, destDB, logTable, LogStatusCompleted, "3", "4", "5")
		setJobCheckpoint(t, destDB, jobName, resumeScenarioCheckpoint)

		orch, err := NewCopyOnlyOrchestrator(cfg, jobName, jobCfg, dbManager)
		if err != nil {
			t.Fatalf("NewCopyOnlyOrchestrator: %v", err)
		}
		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		// The destination is intentionally non-empty (the prior run's copy), so
		// the empty-destination preflight must be bypassed with force + confirm.
		orch.promptReader = bytes.NewReader([]byte("y\n"))

		result, err := orch.Execute(ctx, true)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if !result.Success {
			t.Fatalf("expected success, got errors: %v", result.Errors)
		}

		// The point of batchPromote: neither the copy nor the verify phase runs.
		// The verification counters are the decisive signal — RecordsCopied alone
		// would also read 0 for a wrongly re-run copy, because INSERT IGNORE
		// reports zero rows affected when every row is already present.
		if result.RecordsCopied != 0 {
			t.Errorf("RecordsCopied = %d, want 0 (copied rows must not be re-copied)", result.RecordsCopied)
		}
		if result.TablesVerified != 0 || result.RecordsVerified != 0 {
			t.Errorf("TablesVerified/RecordsVerified = %d/%d, want 0/0 (promotion must not re-verify)",
				result.TablesVerified, result.RecordsVerified)
		}

		assertAllCompleted(t, destDB, logTable, "1", "2", "3", "4", "5")
		assertRootSet(t, destDB, "destination (post-run)", 1, 2)
		assertCheckpointUnchanged(t, destDB, jobName)
		assertRootSet(t, sourceDB, "source", resumeScenarioRoots...)
	})

	// ------------------------------------------------------------------
	// Scenario 3 — purge pending replay
	// ------------------------------------------------------------------
	//
	// Roots {1,2} are 'pending' below the checkpoint for a purge job. Recovery
	// must delete those subgraphs from source and mark them completed, and must
	// NOT touch the checkpoint — purge recovery passes a nil checkpoint, because
	// recovered rows are deleted and so can never be re-fetched.
	t.Run("purge-pending-replay", func(t *testing.T) {
		const jobName = "pipeline_resume_purge_pending"
		resetResumeScenarioData(t, sourceDB, destDB)

		dbManager, cfg := resumeScenarioDBManager(t, setup, "sha256", 2)
		jobCfg := resumeScenarioJobConfig()

		logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, jobCfg.RootTable, JobTypePurge)
		seedLogStatus(t, destDB, logTable, LogStatusPending, "1", "2")
		seedLogStatus(t, destDB, logTable, LogStatusCompleted, "3", "4", "5")
		setJobCheckpoint(t, destDB, jobName, resumeScenarioCheckpoint)

		orch, err := NewPurgeOrchestrator(cfg, jobName, jobCfg, dbManager)
		if err != nil {
			t.Fatalf("NewPurgeOrchestrator: %v", err)
		}
		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize: %v", err)
		}

		result, err := orch.Execute(ctx)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if !result.Success {
			t.Fatal("expected successful purge")
		}

		// Roots 1 and 2 and their whole subgraphs are gone; 3-5 are untouched.
		assertRootSet(t, sourceDB, "source", 3, 4, 5)
		assertAllCompleted(t, destDB, logTable, "1", "2", "3", "4", "5")
		assertCheckpointUnchanged(t, destDB, jobName)
	})

	// ------------------------------------------------------------------
	// Scenario 4 — archive pending replay
	// ------------------------------------------------------------------
	//
	// Roots {1,2} are 'pending' below the checkpoint for an archive job with an
	// empty destination. Recovery must run the full pipeline for them — copy +
	// verify to destination AND delete from source — and mark them completed.
	t.Run("archive-pending-replay", func(t *testing.T) {
		const jobName = "pipeline_resume_archive_pending"
		resetResumeScenarioData(t, sourceDB, destDB)
		dropDestinationSecondaryUniqueIndexes(t, destDB, destSchema)

		// sha256 (not count): recover()'s count-mode gate refuses to resume ANY
		// non-terminal archive row, because a pre-existing destination row cannot
		// be proven equal to source by a row count alone.
		dbManager, cfg := resumeScenarioDBManager(t, setup, "sha256", 2)
		jobCfg := resumeScenarioJobConfig()

		logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, jobCfg.RootTable, JobTypeArchive)
		seedLogStatus(t, destDB, logTable, LogStatusPending, "1", "2")
		seedLogStatus(t, destDB, logTable, LogStatusCompleted, "3", "4", "5")
		setJobCheckpoint(t, destDB, jobName, resumeScenarioCheckpoint)

		orch, err := NewOrchestrator(cfg, jobName, jobCfg, dbManager)
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}
		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize: %v", err)
		}

		result, err := orch.Execute(ctx, nil)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if !result.Success {
			t.Fatalf("expected success, got errors: %v", result.Errors)
		}

		// Copied to destination, and deleted from source.
		assertRootSet(t, destDB, "destination", 1, 2)
		assertRootSet(t, sourceDB, "source", 3, 4, 5)
		assertAllCompleted(t, destDB, logTable, "1", "2", "3", "4", "5")
		assertCheckpointUnchanged(t, destDB, jobName)
	})

	// ------------------------------------------------------------------
	// Scenario 5 — legacy failed-row gate, at Execute level, for all three modes
	// ------------------------------------------------------------------
	//
	// A single legacy 'failed' (log_status=3) row with an EMPTY checkpoint: every
	// mode must refuse to run, with actionable guidance, before touching data.
	//
	// The empty checkpoint is the load-bearing part. ShouldResume used to count
	// only pending/copied, so a failed-only log table with no checkpoint returned
	// false, recover() never ran, its failed-row gate never fired, and the
	// forward scan silently re-processed the row. A unit test on recover() cannot
	// catch that: recover() was never reached. Only an Execute-level test proves
	// the ShouldResume extension is actually wired in.
	t.Run("failed-row-gate", func(t *testing.T) {
		// assertFailedGateError checks the refusal carries the marker and the
		// requeue SQL an operator needs.
		assertFailedGateError := func(t *testing.T, err error) {
			t.Helper()
			if err == nil {
				t.Fatal("expected refusal for legacy failed rows, got nil error")
			}
			for _, want := range []string{"log_status=3", "SET log_status=0 WHERE log_status=3"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error missing %q\ngot: %v", want, err)
				}
			}
		}

		// seedFailedOnlyJob bootstraps tracking with a single failed row and an
		// explicitly empty checkpoint.
		seedFailedOnlyJob := func(t *testing.T, jobName, rootTable, jobType string) string {
			t.Helper()
			logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, rootTable, jobType)
			seedLogStatus(t, destDB, logTable, LogStatusFailed, "1")
			if got := readJobCheckpoint(t, destDB, jobName); got != "" {
				t.Fatalf("precondition: checkpoint must be empty, got %q", got)
			}
			return logTable
		}

		t.Run("copy-only", func(t *testing.T) {
			const jobName = "pipeline_resume_failed_gate_copyonly"
			resetResumeScenarioData(t, sourceDB, destDB)
			dropDestinationSecondaryUniqueIndexes(t, destDB, destSchema)

			dbManager, cfg := resumeScenarioDBManager(t, setup, "sha256", 2)
			jobCfg := resumeScenarioJobConfig()
			logTable := seedFailedOnlyJob(t, jobName, jobCfg.RootTable, JobTypeCopyOnly)

			orch, err := NewCopyOnlyOrchestrator(cfg, jobName, jobCfg, dbManager)
			if err != nil {
				t.Fatalf("NewCopyOnlyOrchestrator: %v", err)
			}
			if err := orch.Initialize(); err != nil {
				t.Fatalf("Initialize: %v", err)
			}

			_, err = orch.Execute(ctx, false)
			assertFailedGateError(t, err)

			// Nothing was processed: the destination is still empty, the source
			// is intact, and the failed row kept its status.
			assertRootSet(t, destDB, "destination")
			assertRootSet(t, sourceDB, "source", resumeScenarioRoots...)
			if got := readLogStatuses(t, destDB, logTable)["1"]; got != LogStatusFailed {
				t.Errorf("PK 1 status = %s, want failed (untouched)", got)
			}
		})

		t.Run("purge", func(t *testing.T) {
			const jobName = "pipeline_resume_failed_gate_purge"
			resetResumeScenarioData(t, sourceDB, destDB)

			dbManager, cfg := resumeScenarioDBManager(t, setup, "sha256", 2)
			jobCfg := resumeScenarioJobConfig()
			logTable := seedFailedOnlyJob(t, jobName, jobCfg.RootTable, JobTypePurge)

			orch, err := NewPurgeOrchestrator(cfg, jobName, jobCfg, dbManager)
			if err != nil {
				t.Fatalf("NewPurgeOrchestrator: %v", err)
			}
			if err := orch.Initialize(); err != nil {
				t.Fatalf("Initialize: %v", err)
			}

			_, err = orch.Execute(ctx)
			assertFailedGateError(t, err)

			// Nothing was deleted.
			assertRootSet(t, sourceDB, "source", resumeScenarioRoots...)
			if got := readLogStatuses(t, destDB, logTable)["1"]; got != LogStatusFailed {
				t.Errorf("PK 1 status = %s, want failed (untouched)", got)
			}
		})

		t.Run("archive", func(t *testing.T) {
			const jobName = "pipeline_resume_failed_gate_archive"
			resetResumeScenarioData(t, sourceDB, destDB)
			dropDestinationSecondaryUniqueIndexes(t, destDB, destSchema)

			dbManager, cfg := resumeScenarioDBManager(t, setup, "sha256", 2)
			jobCfg := resumeScenarioJobConfig()
			logTable := seedFailedOnlyJob(t, jobName, jobCfg.RootTable, JobTypeArchive)

			orch, err := NewOrchestrator(cfg, jobName, jobCfg, dbManager)
			if err != nil {
				t.Fatalf("NewOrchestrator: %v", err)
			}
			if err := orch.Initialize(); err != nil {
				t.Fatalf("Initialize: %v", err)
			}

			_, err = orch.Execute(ctx, nil)
			assertFailedGateError(t, err)

			// Nothing was copied and nothing was deleted.
			assertRootSet(t, destDB, "destination")
			assertRootSet(t, sourceDB, "source", resumeScenarioRoots...)
			if got := readLogStatuses(t, destDB, logTable)["1"]; got != LogStatusFailed {
				t.Errorf("PK 1 status = %s, want failed (untouched)", got)
			}
		})
	})
}
