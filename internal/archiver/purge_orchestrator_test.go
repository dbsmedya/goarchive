package archiver

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/logger"
)

func newPurgeReplicationUnitOrchestrator(t *testing.T, replicas int) (*PurgeOrchestrator, sqlmock.Sqlmock, sqlmock.Sqlmock) {
	t.Helper()

	sourceDB, sourceMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("source sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sourceDB.Close() })
	destDB, destMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("destination sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = destDB.Close() })

	cfg := createTestConfig()
	cfg.Replication = config.ReplicationConfig{
		Enabled: true,
		Servers: make([]config.ReplicationServerConfig, replicas),
	}
	dbm := database.NewManager(cfg)
	dbm.Source = sourceDB
	dbm.Destination = destDB
	for range replicas {
		replicaDB, _, replicaErr := sqlmock.New()
		if replicaErr != nil {
			t.Fatalf("replica sqlmock: %v", replicaErr)
		}
		t.Cleanup(func() { _ = replicaDB.Close() })
		dbm.Replicas = append(dbm.Replicas, replicaDB)
	}

	orch, err := NewPurgeOrchestrator(cfg, "purge_replication_unit", createTestJobConfig(), dbm)
	if err != nil {
		t.Fatalf("NewPurgeOrchestrator: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return orch, sourceMock, destMock
}

func TestPurgeReplicationNoFleet(t *testing.T) {
	orch, sourceMock, destMock := newPurgeReplicationUnitOrchestrator(t, 0)
	factoryCalled := false
	orch.lagFactory = func([]*sql.DB, config.ReplicationConfig, *logger.Logger) (lagWaiter, error) {
		factoryCalled = true
		return &stubLagWaiter{}, nil
	}

	_, err := orch.Execute(context.Background())
	if err == nil || err.Error() != "replication monitoring enabled but no replica connections" {
		t.Fatalf("Execute error = %v, want exact no-replica refusal", err)
	}
	if factoryCalled {
		t.Error("lag factory called for an empty fleet")
	}
	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("source SQL occurred before empty-fleet refusal: %v", err)
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("destination SQL occurred before empty-fleet refusal: %v", err)
	}
}

func TestPurgeReplicationFactoryFailure(t *testing.T) {
	orch, sourceMock, destMock := newPurgeReplicationUnitOrchestrator(t, 1)
	sentinel := errors.New("factory sentinel")
	orch.lagFactory = func(dbs []*sql.DB, _ config.ReplicationConfig, _ *logger.Logger) (lagWaiter, error) {
		if len(dbs) != 1 {
			t.Fatalf("factory received %d replica handles, want 1", len(dbs))
		}
		return nil, sentinel
	}

	_, err := orch.Execute(context.Background())
	if !errors.Is(err, sentinel) || err == nil || !strings.Contains(err.Error(), "failed to create replication gate") {
		t.Fatalf("Execute error = %v, want factory wrapper around sentinel", err)
	}
	if err := sourceMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("source SQL occurred before factory refusal: %v", err)
	}
	if err := destMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("destination SQL occurred before factory refusal: %v", err)
	}
}

// TestPurgeOrchestrator_WiresBatchSizeIntoResumeChunking guards issue #8
// Problem 2 for purge: purge is delete-only (no copy/verify phase, and discovery
// and delete already receive batch_size / batch_delete_size directly), but its
// resume-bookkeeping chunk size must still honor processing.batch_size instead of
// silently pinning at the 1000 default, mirroring archive and copy-only.
func TestPurgeOrchestrator_WiresBatchSizeIntoResumeChunking(t *testing.T) {
	archDB, _, _ := sqlmock.New()
	defer func() { _ = archDB.Close() }()

	log := logger.NewDefault()
	resumeMgr, _ := NewResumeManager(archDB, log, "testdb")

	const batchSize = 42
	o := &PurgeOrchestrator{
		logger:        log,
		processingCfg: config.ProcessingConfig{BatchSize: batchSize},
	}

	if resumeMgr.effectiveChunkSize() == batchSize {
		t.Fatal("precondition failed: resume chunk already at batch_size before wiring")
	}

	o.applyResumeChunkSizing(resumeMgr)

	if got := resumeMgr.effectiveChunkSize(); got != batchSize {
		t.Errorf("resume chunk size = %d, want %d (batch_size ignored)", got, batchSize)
	}
}
