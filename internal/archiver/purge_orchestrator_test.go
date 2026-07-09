package archiver

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
)

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
