//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/lock"
)

func TestArchiveForceBlockedByFreshHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	destDB, _ := setup.GetDB("destination")
	seedTestData(t, sourceDB)

	jobName := "force_lock_test_fresh"
	testsupport.CleanupArchiverState(t, destDB, jobName)

	destSchema := getDestSchema(setup)
	resumeMgr, err := NewResumeManager(destDB, nil, destSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		t.Fatal(err)
	}
	clearArchiverStateNow(t, destDB)
	if _, err := destDB.ExecContext(ctx, `
		INSERT INTO archiver_job (job_name, root_table, job_type, job_status, last_heartbeat_at)
		VALUES (?, 'customers', 'archive', ?, NOW())
	`, jobName, JobStatusRunning); err != nil {
		t.Fatal(err)
	}

	holderLock := lock.NewJobLock(destDB, jobName)
	if err := holderLock.AcquireOrFail(ctx); err != nil {
		t.Fatalf("setup acquire holder lock: %v", err)
	}
	defer func() { _, _ = holderLock.ReleaseLock(context.Background()) }()

	dbManager := setupRealDBManager(t, setup)
	defer func() { _ = dbManager.Close() }()
	cfg := dbManager.GetConfig()
	cfg.Verification.Method = "sha256"
	jobCfg := createCustomerOrderJobConfig()

	orch, err := NewOrchestrator(cfg, jobName, jobCfg, dbManager)
	if err != nil {
		t.Fatal(err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	orch.SetForce(true)

	_, execErr := orch.Execute(ctx, nil)
	if execErr == nil {
		t.Fatal("expected --force to be rejected against fresh heartbeat")
	}
	msg := execErr.Error()
	if !strings.Contains(msg, "live") || !strings.Contains(msg, "cannot bypass") {
		t.Fatalf("expected live-lock rejection, got: %v", execErr)
	}
}

func TestArchiveForceAllowedOnStaleHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	destDB, _ := setup.GetDB("destination")
	seedTestData(t, sourceDB)

	jobName := "force_lock_test_stale"
	testsupport.CleanupArchiverState(t, destDB, jobName)

	destSchema := getDestSchema(setup)
	resumeMgr, err := NewResumeManager(destDB, nil, destSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		t.Fatal(err)
	}
	clearArchiverStateNow(t, destDB)
	if _, err := destDB.ExecContext(ctx, `
		INSERT INTO archiver_job (job_name, root_table, job_type, job_status, last_heartbeat_at)
		VALUES (?, 'customers', 'archive', ?, NOW() - INTERVAL 120 SECOND)
	`, jobName, JobStatusRunning); err != nil {
		t.Fatal(err)
	}

	holderLock := lock.NewJobLock(destDB, jobName)
	if err := holderLock.AcquireOrFail(ctx); err != nil {
		t.Fatalf("setup acquire holder lock: %v", err)
	}
	defer func() { _, _ = holderLock.ReleaseLock(context.Background()) }()

	dbManager := setupRealDBManager(t, setup)
	defer func() { _ = dbManager.Close() }()
	cfg := dbManager.GetConfig()
	cfg.Verification.Method = "sha256"
	jobCfg := createCustomerOrderJobConfig()

	orch, err := NewOrchestrator(cfg, jobName, jobCfg, dbManager)
	if err != nil {
		t.Fatal(err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	orch.SetForce(true)

	execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, execErr := orch.Execute(execCtx, nil)
	if execErr != nil {
		msg := execErr.Error()
		if strings.Contains(msg, "cannot bypass") || strings.Contains(msg, "live instance") {
			t.Fatalf("--force should proceed past stale heartbeat, got: %v", execErr)
		}
		t.Logf("Execute returned downstream error after stale-lock bypass: %v", execErr)
	}
	if !orch.staleAtStartup {
		t.Fatal("expected staleAtStartup=true for 120s-old heartbeat")
	}
}

// TestArchivePlainRunBlockedByLockHolder proves that a plain archive run
// (no --force) is rejected when the job-name lock is held by another process,
// regardless of heartbeat staleness. Only --force can bypass a held lock.
func TestArchivePlainRunBlockedByLockHolder(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	destDB, _ := setup.GetDB("destination")
	seedTestData(t, sourceDB)

	jobName := "force_lock_test_plain_blocked"
	testsupport.CleanupArchiverState(t, destDB, jobName)

	destSchema := getDestSchema(setup)
	resumeMgr, err := NewResumeManager(destDB, nil, destSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		t.Fatal(err)
	}
	clearArchiverStateNow(t, destDB)
	// Stale heartbeat — proves plain run rejects regardless of staleness.
	if _, err := destDB.ExecContext(ctx, `
		INSERT INTO archiver_job (job_name, root_table, job_type, job_status, last_heartbeat_at)
		VALUES (?, 'customers', 'archive', ?, NOW() - INTERVAL 120 SECOND)
	`, jobName, JobStatusRunning); err != nil {
		t.Fatal(err)
	}

	holderLock := lock.NewJobLock(destDB, jobName)
	if err := holderLock.AcquireOrFail(ctx); err != nil {
		t.Fatalf("setup acquire holder lock: %v", err)
	}
	defer func() { _, _ = holderLock.ReleaseLock(context.Background()) }()

	dbManager := setupRealDBManager(t, setup)
	defer func() { _ = dbManager.Close() }()
	cfg := dbManager.GetConfig()
	cfg.Verification.Method = "sha256"
	jobCfg := createCustomerOrderJobConfig()

	orch, err := NewOrchestrator(cfg, jobName, jobCfg, dbManager)
	if err != nil {
		t.Fatal(err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	// SetForce intentionally NOT called — this is a plain run.

	_, execErr := orch.Execute(ctx, nil)
	if execErr == nil {
		t.Fatal("expected plain run to be rejected when lock is held")
	}
	if !strings.Contains(execErr.Error(), "already running") {
		t.Fatalf("expected 'already running' rejection, got: %v", execErr)
	}
}

// TestArchiveForceDoesNotBypassSameRoot proves that --force only bypasses the
// job-name lock — it does NOT bypass the cross-job same-root concurrency check.
// Two distinct job names targeting the same root table must still serialize.
func TestArchiveForceDoesNotBypassSameRoot(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	clearDestination(t, setup)
	sourceDB, _ := setup.GetDB("source")
	destDB, _ := setup.GetDB("destination")
	seedTestData(t, sourceDB)

	incumbentJob := "force_lock_test_incumbent"
	contenderJob := "force_lock_test_contender"
	testsupport.CleanupArchiverState(t, destDB, incumbentJob)
	testsupport.CleanupArchiverState(t, destDB, contenderJob)

	destSchema := getDestSchema(setup)
	resumeMgr, err := NewResumeManager(destDB, nil, destSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumeMgr.InitializeTables(ctx); err != nil {
		t.Fatal(err)
	}
	clearArchiverStateNow(t, destDB)

	// Incumbent: different job name, same root table, fresh heartbeat → live.
	if _, err := destDB.ExecContext(ctx, `
		INSERT INTO archiver_job (job_name, root_table, job_type, job_status, last_heartbeat_at)
		VALUES (?, 'customers', 'archive', ?, NOW())
	`, incumbentJob, JobStatusRunning); err != nil {
		t.Fatal(err)
	}

	dbManager := setupRealDBManager(t, setup)
	defer func() { _ = dbManager.Close() }()
	cfg := dbManager.GetConfig()
	cfg.Verification.Method = "sha256"
	jobCfg := createCustomerOrderJobConfig()

	orch, err := NewOrchestrator(cfg, contenderJob, jobCfg, dbManager)
	if err != nil {
		t.Fatal(err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatal(err)
	}
	orch.SetForce(true)

	_, execErr := orch.Execute(ctx, nil)
	if execErr == nil {
		t.Fatal("expected --force to be rejected by same-root concurrency check")
	}
	msg := execErr.Error()
	// concurrency_check.go:79 — "live job(s) running on root_table"
	if !strings.Contains(msg, "live") || !strings.Contains(msg, "running on root_table") {
		t.Fatalf("expected same-root live-conflict rejection, got: %v", execErr)
	}
}

// getDestSchema returns the destination database name from the integration test setup.
// Since JobSchema is not set in these tests, EffectiveJobSchema() returns Database.
func getDestSchema(setup *IntegrationTestSetup) string {
	for _, db := range setup.Config.Databases {
		if db.Name == "destination" {
			return db.Database
		}
	}
	return "goarchive_test"
}

func clearArchiverStateNow(t *testing.T, destDB *sql.DB) {
	t.Helper()
	dropAllJobLogTables(t, destDB)
	if _, err := destDB.Exec("DELETE FROM archiver_job"); err != nil {
		t.Logf("archiver_job cleanup skipped: %v", err)
	}
}
