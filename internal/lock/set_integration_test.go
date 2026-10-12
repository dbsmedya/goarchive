//go:build integration

package lock

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// These tests run on TEST_SOURCE_* (db1). Each pool is the test's own, so its connection
// counts are the set's alone.
func pool(t *testing.T) *sql.DB {
	t.Helper()
	host, user, pass := os.Getenv("TEST_SOURCE_HOST"), os.Getenv("TEST_SOURCE_USER"), os.Getenv("TEST_SOURCE_PASSWORD")
	port, _ := strconv.Atoi(os.Getenv("TEST_SOURCE_PORT"))
	if host == "" || user == "" || port == 0 {
		t.Fatal("integration environment not loaded: set -a; source tests/.env; set +a")
	}
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = user, pass, "tcp", fmt.Sprintf("%s:%d", host, port)
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestMySQLLockNameFormula (probe L8): MySQL computes the same names, so an operator can
// find a job's lock from SQL (README_JOBS_SCHEMA.md, README_OPERATIONS.md).
func TestMySQLLockNameFormula(t *testing.T) {
	db := pool(t)
	for _, tc := range []struct{ prefix, a, b, want string }{
		{"goarchive:j:", "GoArchive", "Archive_Old_Orders", JobLockName("GoArchive", "Archive_Old_Orders")},
		{"goarchive:t:", "Sakila", "Payment", TableLockName("Sakila", "Payment")},
	} {
		var got string
		if err := db.QueryRowContext(context.Background(),
			"SELECT CONCAT(?, LEFT(SHA2(CONCAT(LOWER(?), CHAR(0), LOWER(?)), 256), 32))", tc.prefix, tc.a, tc.b).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("SQL formula(%s, %s) = %s, want %s", tc.a, tc.b, got, tc.want)
		}
	}
}
