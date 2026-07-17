package archiver

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/verifier"
	"github.com/stretchr/testify/require"
)

// stubLagWaiter is a test fake for the lagWaiter interface, letting tests
// assert both that the pre-delete re-check ran (calls) and gate the delete
// on a returned lag error.
type stubLagWaiter struct {
	calls int
	err   error
}

func (s *stubLagWaiter) WaitForLag(context.Context) error {
	s.calls++
	return s.err
}

// ============================================================================
// Test Helpers
// ============================================================================

func createTestConfig() *config.Config {
	return &config.Config{
		Source: config.DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "password",
			Database: "test",
		},
		Destination: config.DatabaseConfig{
			Host:     "localhost",
			Port:     3307,
			User:     "root",
			Password: "password",
			// Matches the real connection used by realDBManager (TEST_DEST_DB),
			// so EffectiveJobSchema() resolves to an existing schema when the
			// integration-flavored Execute tests run against a live MySQL.
			Database: getEnv("TEST_DEST_DB", "sakila_archive"),
		},
		Processing: config.ProcessingConfig{
			BatchSize:       1000,
			BatchDeleteSize: 500,
			SleepSeconds:    1,
		},
		Safety: config.SafetyConfig{
			DisableForeignKeyChecks: true, // Required for tests with partial data
		},
		Logging: config.LoggingConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

func createTestJobConfig() *config.JobConfig {
	return &config.JobConfig{
		RootTable:  "users",
		PrimaryKey: "id",
		Where:      "created_at < DATE_SUB(NOW(), INTERVAL 1 YEAR)",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-N",
				Relations: []config.Relation{
					{
						Table:          "order_items",
						PrimaryKey:     "id",
						ForeignKey:     "order_id",
						DependencyType: "1-N",
					},
				},
			},
			{
				Table:          "profiles",
				PrimaryKey:     "id",
				ForeignKey:     "user_id",
				DependencyType: "1-1",
			},
		},
	}
}

// mockDBManager creates a minimal database manager for testing
func mockDBManager(cfg *config.Config) *database.Manager {
	return database.NewManager(cfg)
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// ============================================================================
// NewOrchestrator Tests
// ============================================================================

func TestNewOrchestrator_Success(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}

	if orch == nil {
		t.Fatal("NewOrchestrator returned nil")
	}

	if orch.config != cfg {
		t.Error("Orchestrator config mismatch")
	}
	if orch.jobConfig != jobCfg {
		t.Error("Orchestrator jobConfig mismatch")
	}
	if orch.dbManager != dbManager {
		t.Error("Orchestrator dbManager mismatch")
	}
	if orch.jobName != "test_job" {
		t.Errorf("Expected job name 'test_job', got %s", orch.jobName)
	}
	if orch.initialized {
		t.Error("New orchestrator should not be initialized")
	}
}

func TestNewOrchestrator_NilConfig(t *testing.T) {
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(createTestConfig())

	_, err := NewOrchestrator(nil, "test_job", jobCfg, dbManager)
	if err == nil {
		t.Error("Expected error for nil config")
	}
}

func TestNewOrchestrator_NilJobConfig(t *testing.T) {
	cfg := createTestConfig()
	dbManager := mockDBManager(cfg)

	_, err := NewOrchestrator(cfg, "test_job", nil, dbManager)
	if err == nil {
		t.Error("Expected error for nil job config")
	}
}

func TestNewOrchestrator_NilDBManager(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()

	_, err := NewOrchestrator(cfg, "test_job", jobCfg, nil)
	if err == nil {
		t.Error("Expected error for nil db manager")
	}
}

// ============================================================================
// Initialize Tests
// ============================================================================

func TestInitialize_Success(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	err := orch.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if !orch.initialized {
		t.Error("Orchestrator should be initialized")
	}

	if orch.graph == nil {
		t.Error("Graph should be built after Initialize")
	}

	if len(orch.copyOrder) == 0 {
		t.Error("Copy order should be computed")
	}

	if len(orch.deleteOrder) == 0 {
		t.Error("Delete order should be computed")
	}
}

func TestInitialize_Idempotent(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	// Initialize twice
	if err := orch.Initialize(); err != nil {
		t.Fatalf("First Initialize failed: %v", err)
	}

	copyOrder := orch.copyOrder
	deleteOrder := orch.deleteOrder

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Second Initialize failed: %v", err)
	}

	// Orders should remain the same
	if len(orch.copyOrder) != len(copyOrder) {
		t.Error("Copy order changed on second Initialize")
	}
	if len(orch.deleteOrder) != len(deleteOrder) {
		t.Error("Delete order changed on second Initialize")
	}
}

func TestInitialize_InvalidJobConfig(t *testing.T) {
	cfg := createTestConfig()
	// Empty root table should fail
	jobCfg := &config.JobConfig{
		RootTable:  "", // Invalid
		PrimaryKey: "id",
	}
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "invalid_job", jobCfg, dbManager)

	err := orch.Initialize()
	if err == nil {
		t.Error("Expected error for invalid job config")
	}
}

// ============================================================================
// ValidateGraph Tests
// ============================================================================

func TestValidateGraph_Success(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	// Must initialize first
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Now validate
	err := orch.ValidateGraph()
	if err != nil {
		t.Errorf("ValidateGraph failed for valid DAG: %v", err)
	}
}

func TestValidateGraph_NoGraph(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	// Don't initialize - graph is nil
	err := orch.ValidateGraph()
	if err == nil {
		t.Error("Expected error when graph is nil")
	}
}

func TestValidateGraph_Cycle(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	// Create orchestrator normally first to get proper initialization
	orch, _ := NewOrchestrator(cfg, "cycle_job", jobCfg, dbManager)

	// Manually set a cyclic graph (bypassing normal initialization)
	orch.graph = &graph.Graph{
		Nodes: map[string]*graph.Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
		Parents: map[string][]string{
			"A": {"B"},
			"B": {"A"},
		},
	}

	err := orch.ValidateGraph()
	if err == nil {
		t.Error("Expected error for cycle graph")
	}
}

// ============================================================================
// Execute Tests
// ============================================================================

func TestExecute_NotInitialized(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	ctx := context.Background()
	_, err := orch.Execute(ctx, nil)
	if err == nil {
		t.Error("Expected error when not initialized")
	}
}

func TestExecute_NilContext(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "test_job", jobCfg, dbManager)

	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Test error handling when nil context is passed
	_, err := orch.Execute(context.TODO(), nil)
	if err == nil {
		t.Error("Expected error for nil context")
	}
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestOrchestrator_CycleDetection(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbManager := mockDBManager(cfg)

	orch, _ := NewOrchestrator(cfg, "cycle_test", jobCfg, dbManager)

	// Manually set a cyclic graph
	orch.graph = &graph.Graph{
		Nodes: map[string]*graph.Node{
			"A": {Name: "A"},
			"B": {Name: "B"},
			"C": {Name: "C"},
		},
		Children: map[string][]string{
			"A": {"B"},
			"B": {"C"},
			"C": {"A"},
		},
		Parents: map[string][]string{
			"A": {"C"},
			"B": {"A"},
			"C": {"B"},
		},
	}

	// ValidateGraph should detect cycle
	err := orch.ValidateGraph()
	if err == nil {
		t.Fatal("Expected cycle detection error")
	}
}

func TestSortPendingPKsNumeric(t *testing.T) {
	signed := []string{"10", "100", "9"}
	sortPendingPKsNumeric(signed, false)
	wantSigned := []string{"9", "10", "100"}
	for i := range wantSigned {
		if signed[i] != wantSigned[i] {
			t.Fatalf("signed sort = %v, want %v", signed, wantSigned)
		}
	}
	unsigned := []string{"18446744073709551615", "2", "100"}
	sortPendingPKsNumeric(unsigned, true)
	wantUnsigned := []string{"2", "100", "18446744073709551615"}
	for i := range wantUnsigned {
		if unsigned[i] != wantUnsigned[i] {
			t.Fatalf("unsigned sort = %v, want %v", unsigned, wantUnsigned)
		}
	}
}

func newReplayTestResumeManager(t *testing.T) (*ResumeManager, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rm, err := NewResumeManager(db, logger.NewDefault(), "goarchive")
	if err != nil {
		t.Fatalf("NewResumeManager failed: %v", err)
	}
	rm.logTable = "`goarchive`.`archiver_job_log_1`"
	return rm, mock
}

func TestPendingReplayPKs_NumericOrder_Signed(t *testing.T) {
	rm, mock := newReplayTestResumeManager(t)
	mock.ExpectQuery("SELECT root_pk_id FROM").WillReturnRows(
		sqlmock.NewRows([]string{"root_pk_id"}).AddRow("10").AddRow("9").AddRow("-2"))

	g := graph.NewGraph("payment", "payment_id")
	g.SetRootPKMeta("int", false)

	pending, dataType, unsigned, err := pendingReplayPKs(context.Background(), rm, "job", g)
	if err != nil {
		t.Fatalf("pendingReplayPKs failed: %v", err)
	}
	if dataType != "int" || unsigned {
		t.Fatalf("meta = (%q, %v), want (\"int\", false)", dataType, unsigned)
	}
	want := []string{"-2", "9", "10"}
	if len(pending) != len(want) {
		t.Fatalf("pending = %v, want %v", pending, want)
	}
	for i := range want {
		if pending[i] != want[i] {
			t.Fatalf("pending = %v, want %v (lexicographic leak?)", pending, want)
		}
	}
}

func TestPendingReplayPKs_NumericOrder_Unsigned(t *testing.T) {
	rm, mock := newReplayTestResumeManager(t)
	mock.ExpectQuery("SELECT root_pk_id FROM").WillReturnRows(
		sqlmock.NewRows([]string{"root_pk_id"}).AddRow("18446744073709551615").AddRow("10").AddRow("9"))

	g := graph.NewGraph("payment", "payment_id")
	g.SetRootPKMeta("bigint", true)

	pending, _, unsigned, err := pendingReplayPKs(context.Background(), rm, "job", g)
	if err != nil {
		t.Fatalf("pendingReplayPKs failed: %v", err)
	}
	if !unsigned {
		t.Fatal("expected unsigned=true")
	}
	want := []string{"9", "10", "18446744073709551615"}
	for i := range want {
		if pending[i] != want[i] {
			t.Fatalf("pending = %v, want %v", pending, want)
		}
	}
}

func TestPendingReplayPKs_EmptyAndMissingMeta(t *testing.T) {
	rm, mock := newReplayTestResumeManager(t)
	mock.ExpectQuery("SELECT root_pk_id FROM").WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	g := graph.NewGraph("payment", "payment_id")
	pending, _, _, err := pendingReplayPKs(context.Background(), rm, "job", g)
	if err != nil || pending != nil {
		t.Fatalf("empty pending: got (%v, %v), want (nil, nil)", pending, err)
	}

	rm2, mock2 := newReplayTestResumeManager(t)
	mock2.ExpectQuery("SELECT root_pk_id FROM").WillReturnRows(
		sqlmock.NewRows([]string{"root_pk_id"}).AddRow("1"))
	if _, _, _, err := pendingReplayPKs(context.Background(), rm2, "job", g); err == nil {
		t.Fatal("expected error when root PK metadata is not loaded")
	}
}

func TestProcessBatchDeleteOnlySkipsCopyVerify(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New() // MUST remain untouched
	defer func() { _ = destDB.Close() }()
	archDB, archMock, _ := sqlmock.New()
	defer func() { _ = archDB.Close() }()

	g := createSimpleGraph() // root "customers", PK "id", leaf (no children)
	log := logger.NewDefault()

	discovery, _ := NewRecordDiscovery(g, sourceDB, 1000)
	copyPhase, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)
	dataVerifier, _ := verifier.NewVerifier(sourceDB, destDB, g, verifier.MethodSHA256, log)
	deletePhase, _ := NewDeletePhase(sourceDB, g, 1000, log)
	fetcher := NewRootIDFetcher(sourceDB, "customers", "id", "", 1000, nil)
	resumeMgr, _ := NewResumeManager(archDB, log, "testdb")
	resumeMgr.setJobID(7)

	o := &ArchiveOrchestrator{
		jobName:         "job1",
		logger:          log,
		graph:           g,
		processingCfg:   config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 1000},
		verificationCfg: config.VerificationConfig{},
	}

	sourceMock.ExpectExec("DELETE FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()

	stub := &stubLagWaiter{}
	_, err := o.processBatch(context.Background(), []interface{}{int64(1)},
		batchDeleteOnly, false, nil,
		discovery, copyPhase, dataVerifier, deletePhase, fetcher, resumeMgr, stub)
	require.NoError(t, err)
	require.Equal(t, 1, stub.calls)

	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}

// TestProcessBatchDeleteOnlyLagErrorGatesDelete proves the pre-delete lag
// re-check (issue #2) gates the delete phase in batchDeleteOnly mode: when
// WaitForLag errors, neither the source DELETE nor the T3 CompleteBatch
// bookkeeping should fire.
func TestProcessBatchDeleteOnlyLagErrorGatesDelete(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New() // MUST remain untouched
	defer func() { _ = destDB.Close() }()
	archDB, archMock, _ := sqlmock.New()
	defer func() { _ = archDB.Close() }()

	g := createSimpleGraph() // root "customers", PK "id", leaf (no children)
	log := logger.NewDefault()

	discovery, _ := NewRecordDiscovery(g, sourceDB, 1000)
	copyPhase, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)
	dataVerifier, _ := verifier.NewVerifier(sourceDB, destDB, g, verifier.MethodSHA256, log)
	deletePhase, _ := NewDeletePhase(sourceDB, g, 1000, log)
	fetcher := NewRootIDFetcher(sourceDB, "customers", "id", "", 1000, nil)
	resumeMgr, _ := NewResumeManager(archDB, log, "testdb")
	resumeMgr.setJobID(7)

	o := &ArchiveOrchestrator{
		jobName:         "job1",
		logger:          log,
		graph:           g,
		processingCfg:   config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 1000},
		verificationCfg: config.VerificationConfig{},
	}

	// No source DELETE and no arch CompleteBatch (BEGIN/UPDATE completed/COMMIT)
	// expectations: the lag error must gate the delete before either fires.
	stub := &stubLagWaiter{err: errors.New("lag too high")}
	_, err := o.processBatch(context.Background(), []interface{}{int64(1)},
		batchDeleteOnly, false, nil,
		discovery, copyPhase, dataVerifier, deletePhase, fetcher, resumeMgr, stub)
	require.Error(t, err)
	require.Contains(t, err.Error(), "lag")
	require.Equal(t, 1, stub.calls)

	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}

// TestProcessBatchFullLagErrorGatesDeleteAfterMarkCopied proves the pre-delete
// lag re-check (issue #2) also fires in batchFull mode, and specifically that
// it sits AFTER copy+verify+MarkBatchCopied have durably succeeded but BEFORE
// the delete: copy and MarkBatchCopied expectations must fire, while the
// source DELETE and T3 CompleteBatch must not. Modeled on the "pending=20"
// phase of TestResumePendingRecoversCopiedBeforePending.
func TestProcessBatchFullLagErrorGatesDeleteAfterMarkCopied(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()
	archDB, archMock, _ := sqlmock.New()
	defer func() { _ = archDB.Close() }()

	g := createSimpleGraph()
	g.SetRootPKMeta("bigint", false)
	log := logger.NewDefault()

	discovery, _ := NewRecordDiscovery(g, sourceDB, 1000)
	copyPhase, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)
	dataVerifier, _ := verifier.NewVerifier(sourceDB, destDB, g, verifier.MethodSHA256, log)
	deletePhase, _ := NewDeletePhase(sourceDB, g, 1000, log)
	fetcher := NewRootIDFetcher(sourceDB, "customers", "id", "", 1000, nil)
	resumeMgr, _ := NewResumeManager(archDB, log, "testdb")
	resumeMgr.setJobID(7)

	o := &ArchiveOrchestrator{
		jobName:         "job1",
		logger:          log,
		graph:           g,
		processingCfg:   config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 1000},
		verificationCfg: config.VerificationConfig{Method: "sha256", SkipVerification: true},
	}

	// Copy phase: must fire (proves the re-check runs AFTER copy/verify).
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(20)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(20, "p"))
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").WillReturnResult(sqlmock.NewResult(0, 1))
	destMock.ExpectCommit()

	// MarkBatchCopied: must fire (proves the re-check runs AFTER it).
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCopied, "20").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// No source DELETE and no arch CompleteBatch (BEGIN/UPDATE completed/COMMIT)
	// expectations: the lag error must gate the delete before either fires.
	stub := &stubLagWaiter{err: errors.New("lag too high")}
	_, err := o.processBatch(context.Background(), []interface{}{int64(20)},
		batchFull, false, nil,
		discovery, copyPhase, dataVerifier, deletePhase, fetcher, resumeMgr, stub)
	require.Error(t, err)
	require.Contains(t, err.Error(), "lag")
	require.Equal(t, 1, stub.calls)

	require.NoError(t, destMock.ExpectationsWereMet())
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, archMock.ExpectationsWereMet())
}

func TestResumePendingRecoversCopiedBeforePending(t *testing.T) {
	sourceDB, sourceMock, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, destMock, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()
	archDB, archMock, _ := sqlmock.New()
	defer func() { _ = archDB.Close() }()

	g := createSimpleGraph()
	g.SetRootPKMeta("bigint", false)
	log := logger.NewDefault()

	discovery, _ := NewRecordDiscovery(g, sourceDB, 1000)
	copyPhase, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)
	dataVerifier, _ := verifier.NewVerifier(sourceDB, destDB, g, verifier.MethodSHA256, log)
	deletePhase, _ := NewDeletePhase(sourceDB, g, 1000, log)
	fetcher := NewRootIDFetcher(sourceDB, "customers", "id", "", 1000, nil)
	resumeMgr, _ := NewResumeManager(archDB, log, "testdb")
	resumeMgr.setJobID(7)

	o := &ArchiveOrchestrator{
		jobName:         "job1",
		logger:          log,
		graph:           g,
		processingCfg:   config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 1000},
		verificationCfg: config.VerificationConfig{Method: "sha256", SkipVerification: true},
	}
	result := &ArchiveResult{}

	// arch DB: status fetches first (copied, then pending)
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusCopied).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("10"))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusPending).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("20"))
	// Phase A (copied=10): CompleteBatch only.
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "10").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()
	// Phase B (pending=20): MarkBatchCopied, then CompleteBatch.
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCopied, "20").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectBegin()
	archMock.ExpectExec("UPDATE .*archiver_job_log_\\d+. SET log_status").
		WithArgs(LogStatusCompleted, "20").
		WillReturnResult(sqlmock.NewResult(0, 1))
	archMock.ExpectCommit()

	// source DB: copied delete (10) before pending copy SELECT (20), then pending delete (20)
	sourceMock.ExpectExec("DELETE FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(10)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	sourceMock.ExpectQuery("SELECT \\* FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(20)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(20, "p"))
	sourceMock.ExpectExec("DELETE FROM `customers` WHERE `id` IN \\(\\?\\)").
		WithArgs(int64(20)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// dest DB: only the pending copy writes
	destMock.ExpectBegin()
	destMock.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	destMock.ExpectExec("INSERT IGNORE INTO `customers`").WillReturnResult(sqlmock.NewResult(0, 1))
	destMock.ExpectCommit()

	err := o.resumePending(context.Background(), resumeMgr,
		discovery, copyPhase, dataVerifier, deletePhase, fetcher, nil, nil, result)
	require.NoError(t, err)
	require.NoError(t, archMock.ExpectationsWereMet())
	require.NoError(t, sourceMock.ExpectationsWereMet())
	require.NoError(t, destMock.ExpectationsWereMet())
}

// TestResumePendingRefusesStrictInsertWithPending guards review-003 Claim 1: when
// strict INSERT is forced (here via skip-verify) and a prior run left 'pending'
// rows, resume must REFUSE rather than re-copy them under a strict INSERT that
// would abort on the already-committed destination rows (self-block on every run).
func TestResumePendingRefusesStrictInsertWithPending(t *testing.T) {
	sourceDB, _, _ := sqlmock.New()
	defer func() { _ = sourceDB.Close() }()
	destDB, _, _ := sqlmock.New()
	defer func() { _ = destDB.Close() }()
	archDB, archMock, _ := sqlmock.New()
	defer func() { _ = archDB.Close() }()

	g := createSimpleGraph()
	g.SetRootPKMeta("bigint", false)
	log := logger.NewDefault()

	discovery, _ := NewRecordDiscovery(g, sourceDB, 1000)
	copyPhase, _ := NewCopyPhase(sourceDB, destDB, g, config.SafetyConfig{}, log)
	copyPhase.SetStrictInsert(true) // forced by --skip-verify or a dest unique index
	dataVerifier, _ := verifier.NewVerifier(sourceDB, destDB, g, verifier.MethodSHA256, log)
	deletePhase, _ := NewDeletePhase(sourceDB, g, 1000, log)
	fetcher := NewRootIDFetcher(sourceDB, "customers", "id", "", 1000, nil)
	resumeMgr, _ := NewResumeManager(archDB, log, "testdb")
	resumeMgr.setJobID(7)

	o := &ArchiveOrchestrator{
		jobName:         "job1",
		logger:          log,
		graph:           g,
		processingCfg:   config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 1000},
		verificationCfg: config.VerificationConfig{Method: "sha256", SkipVerification: true},
	}
	result := &ArchiveResult{}

	// Only the two status fetches run; the strict-insert guard then refuses, so
	// NO copy/verify/delete is attempted (no source/dest expectations).
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusCopied).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}))
	archMock.ExpectQuery("SELECT root_pk_id FROM .*archiver_job_log_\\d+. WHERE log_status = \\?").
		WithArgs(LogStatusPending).
		WillReturnRows(sqlmock.NewRows([]string{"root_pk_id"}).AddRow("20"))

	err := o.resumePending(context.Background(), resumeMgr,
		discovery, copyPhase, dataVerifier, deletePhase, fetcher, nil, nil, result)
	if err == nil {
		t.Fatal("expected refusal when strict insert + pending rows, got nil")
	}
	if !strings.Contains(err.Error(), "strict INSERT") {
		t.Fatalf("expected strict-insert refusal message, got: %v", err)
	}
	require.NoError(t, archMock.ExpectationsWereMet())
}
