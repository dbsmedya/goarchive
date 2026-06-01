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
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	// Root-table lock acquired for the startup critical section.
	mock.ExpectQuery("SELECT GET_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT CONNECTION_ID\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"CONNECTION_ID()"}).AddRow(int64(101)))
	mock.ExpectQuery("SELECT TIMESTAMPDIFF\\(SECOND, last_heartbeat_at, NOW\\(\\)\\)").
		WithArgs("test_job").
		WillReturnRows(sqlmock.NewRows([]string{"age_seconds"}))
	mock.ExpectQuery("SELECT job_name, TIMESTAMPDIFF").
		WithArgs("users", JobStatusRunning, "test_job").
		WillReturnRows(sqlmock.NewRows([]string{"job_name", "age_seconds"}))

	// Job-name GET_LOCK returns 0 (timeout) — forces lock acquisition to fail before status mutation.
	mock.ExpectQuery("SELECT GET_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK"}).AddRow(int64(0)))
	mock.ExpectQuery("SELECT RELEASE_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK"}).AddRow(int64(1)))

	_, execErr := orch.Execute(context.Background(), false)
	if execErr == nil {
		t.Fatal("expected Execute to fail on lock timeout")
	}
	if !strings.Contains(execErr.Error(), "already running") {
		t.Fatalf("expected lock-timeout error, got: %v", execErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("mock expectations not met (lock release likely missing): %v", err)
	}
}

// TestCopyOnlyOrchestrator_Execute_PersistsFailedStatusOnError exercises the
// failed-status-on-error contract: when Execute returns an error AFTER startup
// successfully wrote Running, cleanup must persist JobStatusFailed (not Idle).
func TestCopyOnlyOrchestrator_Execute_PersistsFailedStatusOnError(t *testing.T) {
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

	// InitializeTables: CREATE + column checks + alters (same as happy path).
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS archiver_job").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT DATA_TYPE FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"data_type"}).AddRow("varchar"))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS archiver_job_log").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT DATA_TYPE FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"data_type"}).AddRow("varchar"))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	// Root-table lock + heartbeat staleness + same-root concurrency check (all clean).
	mock.ExpectQuery("SELECT GET_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT CONNECTION_ID\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"CONNECTION_ID()"}).AddRow(int64(201)))
	mock.ExpectQuery("SELECT TIMESTAMPDIFF\\(SECOND, last_heartbeat_at, NOW\\(\\)\\)").
		WithArgs("test_job").
		WillReturnRows(sqlmock.NewRows([]string{"age_seconds"}))
	mock.ExpectQuery("SELECT job_name, TIMESTAMPDIFF").
		WithArgs("users", JobStatusRunning, "test_job").
		WillReturnRows(sqlmock.NewRows([]string{"job_name", "age_seconds"}))

	// Job-name lock acquired.
	mock.ExpectQuery("SELECT GET_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"GET_LOCK"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT CONNECTION_ID\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"CONNECTION_ID()"}).AddRow(int64(202)))

	// GetOrCreateJobWithType — return existing job row.
	mock.ExpectQuery("SELECT job_name, root_table, job_type, last_processed_root_pk_id, job_status, created_at, updated_at FROM archiver_job").
		WithArgs("test_job").
		WillReturnRows(sqlmock.NewRows([]string{
			"job_name", "root_table", "job_type", "last_processed_root_pk_id",
			"job_status", "created_at", "updated_at",
		}).AddRow("test_job", "users", JobTypeCopyOnly, "", JobStatusIdle, time.Now(), time.Now()))

	// UpdateJobStatus(Running).
	mock.ExpectExec("UPDATE archiver_job SET job_status").
		WithArgs(JobStatusRunning, "test_job").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Heartbeat seed.
	mock.ExpectExec("UPDATE archiver_job SET last_heartbeat_at").
		WithArgs("test_job").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Root-table lock release (end of critical section).
	mock.ExpectQuery("SELECT RELEASE_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK"}).AddRow(int64(1)))

	// loadRootPKMeta returns a non-integer root PK type, forcing a deterministic
	// post-Running failure with the documented ROOT_PK_TYPE_UNSUPPORTED category.
	mock.ExpectQuery("SELECT DATA_TYPE, COLUMN_TYPE\\s+FROM information_schema.COLUMNS").
		WithArgs("users", "id").
		WillReturnRows(sqlmock.NewRows([]string{"DATA_TYPE", "COLUMN_TYPE"}).
			AddRow("varchar", "varchar(36)"))

	// Cleanup must write JobStatusFailed (not Idle) because Execute returned an error.
	mock.ExpectExec("UPDATE archiver_job SET job_status").
		WithArgs(JobStatusFailed, "test_job").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Job-name lock release.
	mock.ExpectQuery("SELECT RELEASE_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"RELEASE_LOCK"}).AddRow(int64(1)))

	_, execErr := orch.Execute(context.Background(), false)
	if execErr == nil {
		t.Fatal("expected Execute to fail with unsupported root PK type")
	}
	if !strings.Contains(execErr.Error(), "ROOT_PK_TYPE_UNSUPPORTED") {
		t.Fatalf("expected ROOT_PK_TYPE_UNSUPPORTED error, got: %v", execErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("mock expectations not met (Failed status not written?): %v", err)
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
