//go:build integration

package archiver

import (
	"context"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
)

// ============================================================================
// One statement per database call. A `where` that ends the statement and
// appends a second one is refused by MySQL with a syntax error (1064) on the
// paths that send text without bound arguments: the first --progress count
// (nil checkpoint) and the dry-run root count. Nothing of the appended
// statement runs.
// ============================================================================

// estateManager connects a database.Manager to the fixture's schemas, the way
// the archive commands connect.
func estateManager(t *testing.T, ctx context.Context, f *chrFixture) (*config.Config, *database.Manager) {
	t.Helper()
	cfg := &config.Config{
		Source: config.DatabaseConfig{
			Host:     getEnv("TEST_SOURCE_HOST", "127.0.0.1"),
			Port:     getEnvInt("TEST_SOURCE_PORT", 3305),
			User:     getEnv("TEST_SOURCE_USER", "root"),
			Password: getEnv("TEST_SOURCE_PASSWORD", "qazokm"),
			Database: f.SourceSchema,
			TLS:      "disable",
		},
		Destination: config.DatabaseConfig{
			Host:     getEnv("TEST_DEST_HOST", "127.0.0.1"),
			Port:     getEnvInt("TEST_DEST_PORT", 3307),
			User:     getEnv("TEST_DEST_USER", "root"),
			Password: getEnv("TEST_DEST_PASSWORD", "qazokm"),
			Database: f.DestSchema,
			TLS:      "disable",
		},
		Processing:   config.ProcessingConfig{BatchSize: 100, BatchDeleteSize: 100},
		Verification: config.VerificationConfig{Method: "count"},
		Logging:      config.LoggingConfig{Level: "error", Format: "json"},
	}
	mgr := database.NewManager(cfg)
	if err := mgr.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return cfg, mgr
}

// msFixture creates ms_root and ms_victim, three rows each, in the source schema.
func msFixture(t *testing.T, ctx context.Context) (*chrFixture, *config.Config, *database.Manager) {
	t.Helper()
	f := newChrFixture(t, ctx)
	for _, table := range []string{"ms_root", "ms_victim"} {
		f.ExecSource(t, ctx, "CREATE TABLE "+table+" (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB")
		f.ExecSource(t, ctx, "INSERT INTO "+table+" (id) VALUES (1), (2), (3)")
	}
	cfg, mgr := estateManager(t, ctx, f)
	return f, cfg, mgr
}

func msVictimRows(t *testing.T, ctx context.Context, f *chrFixture) int {
	t.Helper()
	var n int
	if err := f.SourceDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM ms_victim").Scan(&n); err != nil {
		t.Fatalf("count ms_victim: %v", err)
	}
	return n
}

func isMySQLError(err error, number uint16) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && myErr.Number == number
}

func TestProgressCountRefusesAppendedStatement_Integration(t *testing.T) {
	ctx := context.Background()
	f, _, mgr := msFixture(t, ctx)

	// The parenthesis-closing form: inside the count's `WHERE (<where>)` it
	// closes the predicate, ends the statement and opens a DELETE. The nil
	// checkpoint is explicit: with a checkpoint the count binds an argument and
	// is prepared, which never runs a second statement.
	fetcher := NewRootIDFetcher(mgr.Source, "ms_root", "id", "1=1) ; DELETE FROM ms_victim WHERE (1=1", 100, nil)
	count, err := fetcher.CountRemaining(ctx)

	victims := msVictimRows(t, ctx, f)
	if err == nil {
		t.Fatalf("CountRemaining returned %d with no error and ms_victim has %d rows; want MySQL error 1064 and 3 rows", count, victims)
	}
	if !isMySQLError(err, 1064) {
		t.Errorf("CountRemaining error = %v; want MySQL error 1064", err)
	}
	if victims != 3 {
		t.Errorf("ms_victim has %d rows after the refused count; want 3", victims)
	}
}

func TestDryRunEstimateRefusesAppendedStatement_Integration(t *testing.T) {
	ctx := context.Background()
	f, cfg, mgr := msFixture(t, ctx)

	jobCfg := &config.JobConfig{RootTable: "ms_root", PrimaryKey: "id", Where: "id < 100; DELETE FROM ms_victim"}
	result, err := NewEstimator(mgr.Source, cfg, jobCfg, graph.NewGraph("ms_root", "id"), nil).Estimate(ctx)

	victims := msVictimRows(t, ctx, f)
	if err == nil {
		t.Fatalf("Estimate returned root count %d with no error and ms_victim has %d rows; want MySQL error 1064 and 3 rows", result.RootCount, victims)
	}
	if !isMySQLError(err, 1064) {
		t.Errorf("Estimate error = %v; want MySQL error 1064", err)
	}
	if victims != 3 {
		t.Errorf("ms_victim has %d rows after the refused estimate; want 3", victims)
	}
}
