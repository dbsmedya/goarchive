//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
)

// sameDatabaseConfig is issue #13's configuration shape: destination is a copy
// of source, sha256 verification, and (below) a root table with no secondary
// unique index — the combination that selects non-strict INSERT IGNORE, lets
// the copy no-op against itself, lets verification match the table against
// itself, and then deletes the only copy.
func sameDatabaseConfig() *config.Config {
	src := config.DatabaseConfig{
		Host:     getEnv("TEST_SOURCE_HOST", "127.0.0.1"),
		Port:     getEnvInt("TEST_SOURCE_PORT", 3305),
		User:     getEnv("TEST_SOURCE_USER", "root"),
		Password: getEnv("TEST_SOURCE_PASSWORD", "qazokm"),
		Database: getEnv("TEST_SOURCE_DB", "sakila"),
		TLS:      "disable",
	}
	return &config.Config{
		Source:       src,
		Destination:  src,
		Verification: config.VerificationConfig{Method: "sha256"},
		Processing:   config.ProcessingConfig{BatchSize: 100, BatchDeleteSize: 50},
		Logging:      config.LoggingConfig{Level: "error", Format: "json"},
	}
}

func execOrFatal(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// trackingRowsForJob counts archiver_job rows for jobName in db's schema. An
// absent archiver_job table (nothing ever stamped this schema) counts as zero.
func trackingRowsForJob(t *testing.T, db *sql.DB, jobName string) int {
	t.Helper()
	var n int
	err := db.QueryRow("SELECT COUNT(*) FROM archiver_job WHERE job_name = ?", jobName).Scan(&n)
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1146 { // ER_NO_SUCH_TABLE
		return 0
	}
	if err != nil {
		t.Fatalf("count tracking rows for %q: %v", jobName, err)
	}
	return n
}

// TestArchiveAgainstItselfIsRefused_Integration runs issue #13's failure chain
// in the order cmd/goarchive/cmd/archive.go runs it: NewManager → Connect →
// NewOrchestrator → Execute. The guard must stop it at Connect.
//
// If Connect ever returns nil for identical endpoints, the test does NOT stop
// there: it continues into Execute against a throwaway three-row table, so the
// row-count assertion witnesses the only copy being deleted — the P1 itself,
// not a proxy for it. That is what makes this test red on a guard-less binary.
func TestArchiveAgainstItselfIsRefused_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()

	// An independent handle on the source server, opened through a normal
	// distinct-endpoint manager so the guard under test is not in its path.
	probe := realDBManager(t)
	t.Cleanup(func() { _ = probe.Close() })
	src := probe.Source

	const (
		table   = "identity_guard_probe"
		jobName = "identity_guard_self_archive"
	)
	execOrFatal(t, src, "DROP TABLE IF EXISTS "+table)
	execOrFatal(t, src, "CREATE TABLE "+table+" (id INT NOT NULL PRIMARY KEY, note VARCHAR(20) NOT NULL) ENGINE=InnoDB")
	execOrFatal(t, src, "INSERT INTO "+table+" (id, note) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		// Only a guard-less binary gets far enough to write tracking state into
		// the SOURCE schema; remove it so a red run does not pollute the estate.
		var id int64
		if err := src.QueryRow("SELECT id FROM archiver_job WHERE job_name = ?", jobName).Scan(&id); err == nil {
			_, _ = src.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `archiver_job_log_%d`", id))
			_, _ = src.Exec("DELETE FROM archiver_job WHERE job_name = ?", jobName)
		}
	})

	before := countRows(t, src, "SELECT COUNT(*) FROM "+table)
	if before != 3 {
		t.Fatalf("fixture: %s has %d rows, want 3", table, before)
	}

	cfg := sameDatabaseConfig()
	jobCfg := &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id <= 3"}

	mgr := database.NewManager(cfg)
	t.Cleanup(func() { _ = mgr.Close() })
	connectErr := mgr.Connect(ctx)
	if connectErr == nil {
		// No guard: keep going exactly as the archive command would.
		orch, err := NewOrchestrator(cfg, jobName, jobCfg, mgr)
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}
		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		_, _ = orch.Execute(ctx, nil)
	}

	after := countRows(t, src, "SELECT COUNT(*) FROM "+table)
	if after != before {
		t.Fatalf("archive against itself deleted %d of %d rows: the only copy is gone (issue #13)", before-after, before)
	}
	if connectErr == nil {
		t.Fatal("Connect() accepted source == destination; SRC_DEST_IDENTITY_CHECK is missing")
	}
	var same *database.SameDatabaseError
	if !errors.As(connectErr, &same) || !strings.Contains(connectErr.Error(), "SRC_DEST_IDENTITY_CHECK") {
		t.Fatalf("Connect() error = %v, want *database.SameDatabaseError naming SRC_DEST_IDENTITY_CHECK", connectErr)
	}
	if n := trackingRowsForJob(t, src, jobName); n != 0 {
		t.Fatalf("%d tracking row(s) for %q were written into the source schema before the refusal", n, jobName)
	}
}
