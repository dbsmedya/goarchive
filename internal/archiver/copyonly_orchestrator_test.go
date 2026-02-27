package archiver

import (
	"context"
	"strings"
	"testing"

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
