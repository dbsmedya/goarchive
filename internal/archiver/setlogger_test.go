package archiver

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/logger"
)

func TestOrchestratorsSetLogger(t *testing.T) {
	cfg := createTestConfig()
	jobCfg := createTestJobConfig()
	dbm := database.NewManager(cfg)
	custom := logger.NewDefault()

	t.Run("archive orchestrator", func(t *testing.T) {
		orch, err := NewOrchestrator(cfg, "test_job", jobCfg, dbm)
		if err != nil {
			t.Fatalf("NewOrchestrator failed: %v", err)
		}
		orch.SetLogger(custom)
		if orch.logger != custom {
			t.Error("SetLogger should replace the archive orchestrator logger")
		}
	})

	t.Run("purge orchestrator", func(t *testing.T) {
		orch, err := NewPurgeOrchestrator(cfg, "test_job", jobCfg, dbm)
		if err != nil {
			t.Fatalf("NewPurgeOrchestrator failed: %v", err)
		}
		orch.SetLogger(custom)
		if orch.logger != custom {
			t.Error("SetLogger should replace the purge orchestrator logger")
		}
	})

	t.Run("copy-only orchestrator", func(t *testing.T) {
		orch, err := NewCopyOnlyOrchestrator(cfg, "test_job", jobCfg, dbm)
		if err != nil {
			t.Fatalf("NewCopyOnlyOrchestrator failed: %v", err)
		}
		orch.SetLogger(custom)
		if orch.logger != custom {
			t.Error("SetLogger should replace the copy-only orchestrator logger")
		}
	})
}
