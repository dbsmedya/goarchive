package archiver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/database"
)

func TestNewCopyOnlyOrchestrator_Success(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	if orch == nil {
		t.Fatal("NewCopyOnlyOrchestrator returned nil")
	}
}

func TestNewCopyOnlyOrchestrator_NilValidation(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	if _, err := NewCopyOnlyOrchestrator(nil, "test_job", jobCfg, dbManager); err == nil {
		t.Fatal("expected error for nil config")
	}
	if _, err := NewCopyOnlyOrchestrator(cfg, "test_job", nil, dbManager); err == nil {
		t.Fatal("expected error for nil job config")
	}
	if _, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, nil); err == nil {
		t.Fatal("expected error for nil database manager")
	}
}

func TestCopyOnlyOrchestrator_Initialize(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	order, err := orch.GetCopyOrder()
	if err != nil {
		t.Fatalf("GetCopyOrder failed: %v", err)
	}
	if len(order) == 0 {
		t.Fatal("expected non-empty copy order")
	}
}

func TestCopyOnlyOrchestrator_ExecutePreconditions(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}

	if _, err := orch.Execute(context.Background(), false); err == nil {
		t.Fatal("expected error when orchestrator is not initialized")
	}

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if _, err := orch.Execute(context.TODO(), false); err == nil {
		t.Fatal("expected error for invalid context")
	}
}

func TestCopyOnlyOrchestrator_CheckConcurrentJobs(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock new failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, &database.Manager{Destination: db})
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}

	rows := sqlmock.NewRows([]string{"job_name"}).AddRow("other_job")
	mock.ExpectQuery("SELECT job_name FROM archiver_job").WillReturnRows(rows)

	err = orch.checkConcurrentJobs(context.Background())
	if err == nil {
		t.Fatal("expected concurrent job error")
	}
}

func TestCopyOnlyOrchestrator_CheckDestinationEmpty(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock new failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, &database.Manager{Destination: db})
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	mock.ExpectQuery("SELECT 1 FROM `users` LIMIT 1").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

	err = orch.checkDestinationEmpty(context.Background())
	if err == nil {
		t.Fatal("expected destination not empty error")
	}
}

// TestCopyOnlyOrchestrator_Execute_ResetsStatusOnLockTimeout exercises the
// defer-Idle guarantee: if anything fails after UpdateJobStatus(Running) but
// before completion, job_status must end up Idle so later runs are not blocked.
func TestCopyOnlyOrchestrator_Execute_ResetsStatusOnLockTimeout(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock new failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, &database.Manager{Destination: db, Source: db})
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// InitializeTables: CREATE + column checks + alters
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS archiver_job").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT DATA_TYPE FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"data_type"}).AddRow("varchar"))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS archiver_job_log").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT DATA_TYPE FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"data_type"}).AddRow("varchar"))

	// checkConcurrentJobs: no conflicts
	mock.ExpectQuery("SELECT job_name FROM archiver_job").
		WillReturnRows(sqlmock.NewRows([]string{"job_name"}))

	// GetOrCreateJobWithType: existing job
	mock.ExpectQuery("SELECT job_name, root_table").
		WithArgs("test_job").
		WillReturnRows(sqlmock.NewRows([]string{
			"job_name", "root_table", "job_type", "last_processed_root_pk_id",
			"job_status", "created_at", "updated_at",
		}).AddRow("test_job", "users", JobTypeCopyOnly, "", JobStatusIdle,
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))

	// UpdateJobStatus -> Running
	mock.ExpectExec("UPDATE archiver_job SET job_status").
		WithArgs(JobStatusRunning, "test_job").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// GET_LOCK returns 0 (timeout) — forces lock acquisition to fail
	mock.ExpectQuery("SELECT GET_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK"}).AddRow(int64(0)))

	// Deferred UpdateJobStatus -> Idle MUST be called
	mock.ExpectExec("UPDATE archiver_job SET job_status").
		WithArgs(JobStatusIdle, "test_job").
		WillReturnResult(sqlmock.NewResult(0, 1))

	_, execErr := orch.Execute(context.Background(), false)
	if execErr == nil {
		t.Fatal("expected Execute to fail on lock timeout")
	}
	if !strings.Contains(execErr.Error(), "already running") {
		t.Fatalf("expected lock-timeout error, got: %v", execErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("mock expectations not met (deferred Idle update likely missing): %v", err)
	}
}

func TestCopyOnlyOrchestrator_DisplayInfoOrPrompt_ForceDeclined(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	orch.promptReader = strings.NewReader("n\n")

	err = orch.displayInfoOrPrompt(true)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}

func TestCopyOnlyOrchestrator_DisplayInfoOrPrompt_ForceAccepted(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
	}
	orch.promptReader = strings.NewReader("y\n")

	if err := orch.displayInfoOrPrompt(true); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}
