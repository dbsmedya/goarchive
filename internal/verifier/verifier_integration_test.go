//go:build integration

package verifier

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// Mirror the env-driven DSN construction used by the archiver package's
// real-DB integration tests (internal/archiver/orchestrator_realdb_integration_test.go):
// TEST_SOURCE_*/TEST_DEST_* env vars, sourced from tests/.env (see
// tests/README.md). Source: 127.0.0.1:3305/sakila, destination:
// 127.0.0.1:3307/sakila_archive.

// openIntegrationDB opens a connection to the source (port 3305) or
// destination (port 3307) integration MySQL instance using the TEST_SOURCE_*
// / TEST_DEST_* environment variables. It skips the test if the relevant
// env vars are not set (tests/.env not sourced).
func openIntegrationDB(t *testing.T, port int) *sql.DB {
	t.Helper()

	var prefix string
	switch port {
	case 3305:
		prefix = "TEST_SOURCE"
	case 3307:
		prefix = "TEST_DEST"
	default:
		t.Fatalf("openIntegrationDB: unsupported port %d", port)
	}

	host := os.Getenv(prefix + "_HOST")
	user := os.Getenv(prefix + "_USER")
	password := os.Getenv(prefix + "_PASSWORD")
	dbName := os.Getenv(prefix + "_DB")

	if host == "" || user == "" || password == "" || dbName == "" {
		t.Skipf("skipping: %s_HOST/_USER/_PASSWORD/_DB not set (source tests/.env; see tests/README.md)", prefix)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=false&parseTime=true", user, password, host, port, dbName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open(%s): %v", prefix, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping %s db: %v", prefix, err)
	}

	return db
}

// mustExec runs a statement against db, failing the test on error.
func mustExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func TestVerifier_SHA256_RealMySQLTypes_Integration(t *testing.T) {
	sourceDB := openIntegrationDB(t, 3305)
	destDB := openIntegrationDB(t, 3307)

	const ddl = `CREATE TABLE IF NOT EXISTS ga9_verify_types (
		id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		d  DECIMAL(10,4) NULL,
		ts DATETIME NULL,
		vb VARBINARY(16) NULL,
		s  VARCHAR(64) NULL,
		f  DOUBLE NULL
	) ENGINE=InnoDB`
	for _, db := range []*sql.DB{sourceDB, destDB} {
		mustExec(t, db, "DROP TABLE IF EXISTS ga9_verify_types")
		mustExec(t, db, ddl)
	}
	t.Cleanup(func() {
		mustExec(t, sourceDB, "DROP TABLE IF EXISTS ga9_verify_types")
		mustExec(t, destDB, "DROP TABLE IF EXISTS ga9_verify_types")
	})

	const insert = `INSERT INTO ga9_verify_types VALUES
		(1, 1234.5678, '2026-07-17 12:00:00', X'00FF7A', 'plain', 3.141592653589793),
		(2, NULL, NULL, NULL, NULL, NULL),
		(18446744073709551615, -0.0001, '1970-01-01 00:00:01', X'', '', 1e300)`
	mustExec(t, sourceDB, insert)
	mustExec(t, destDB, insert)

	g := graph.NewGraph("ga9_verify_types", "id")
	v, err := NewVerifier(sourceDB, destDB, g, MethodSHA256, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	pks := []interface{}{uint64(1), uint64(2), uint64(18446744073709551615)}

	result, err := v.verifyBySHA256(context.Background(), "ga9_verify_types", pks)
	if err != nil {
		t.Fatalf("verifyBySHA256: %v", err)
	}
	if !result.Match {
		t.Fatalf("identical rows must hash equal: %+v", result)
	}

	// A single-column mutation on one row must flip the hash.
	mustExec(t, destDB, "UPDATE ga9_verify_types SET s = 'tampered' WHERE id = 1")
	result, err = v.verifyBySHA256(context.Background(), "ga9_verify_types", pks)
	if err != nil {
		t.Fatalf("verifyBySHA256 after mutation: %v", err)
	}
	if result.Match {
		t.Fatal("mutated destination row must produce a hash mismatch")
	}
}
