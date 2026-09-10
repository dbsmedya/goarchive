//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"os"
	"strconv"
	"testing"
)

func vpEnvDB(t testing.TB, prefix string) config.DatabaseConfig {
	t.Helper()
	port, err := strconv.Atoi(os.Getenv(prefix + "_PORT"))
	if err != nil || port < 1 || os.Getenv(prefix+"_HOST") == "" ||
		os.Getenv(prefix+"_USER") == "" || os.Getenv(prefix+"_PASSWORD") == "" ||
		os.Getenv(prefix+"_DB") == "" {
		t.Fatalf("VP fixture: load %s_HOST/PORT/USER/PASSWORD/DB", prefix)
	}
	return config.DatabaseConfig{Host: os.Getenv(prefix + "_HOST"), Port: port,
		User: os.Getenv(prefix + "_USER"), Password: os.Getenv(prefix + "_PASSWORD"),
		Database: os.Getenv(prefix + "_DB"), TLS: "disable"}
}

func vpFixture(t testing.TB) (*config.Config, *database.Manager) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Source, cfg.Destination = vpEnvDB(t, "TEST_SOURCE"), vpEnvDB(t, "TEST_DEST")
	cfg.Processing.BatchSize, cfg.Processing.BatchDeleteSize = 100, 100
	cfg.Processing.SleepSeconds = 0
	cfg.Processing.SentinelFile = ""
	cfg.Verification.Method = "count"
	cfg.Logging.Level = "info"
	cfg.Replication.Enabled = false
	mgr := database.NewManager(cfg)
	if err := mgr.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Error(err)
		}
	})
	return cfg, mgr
}

func vpExec(t testing.TB, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("VP fixture SQL failed: %v", err)
	}
}

func vpSeed(t testing.TB, cfg config.DatabaseConfig, mode string, statements ...string) {
	t.Helper()
	setup, err := sql.Open("mysql", database.BuildDSN(&cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	conn, err := setup.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(context.Background(), "SET SESSION sql_mode = ?", mode); err != nil {
		t.Fatal(err)
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
	}
	// This is a disposable setup pool, never mgr.Source or mgr.Destination.
}

func vpTable(t testing.TB, mgr *database.Manager, table, definition string) {
	t.Helper()
	name := sqlutil.QuoteIdentifier(table)
	for _, db := range []*sql.DB{mgr.Source, mgr.Destination} {
		vpExec(t, db, "DROP TABLE IF EXISTS "+name)
		vpExec(t, db, "CREATE TABLE "+name+" "+definition)
		db := db
		t.Cleanup(func() { vpExec(t, db, "DROP TABLE IF EXISTS "+name) })
	}
}
