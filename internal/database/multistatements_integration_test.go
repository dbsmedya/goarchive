//go:build integration

package database

import (
	"context"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
)

// TestProductionConnectionRefusesMultiStatementText_Integration pins that a
// connection opened the way production opens it carries exactly one statement
// per call: text holding two statements fails with MySQL's syntax error (1064)
// and neither statement is applied.
func TestProductionConnectionRefusesMultiStatementText_Integration(t *testing.T) {
	ctx := context.Background()
	m := NewManager(&config.Config{
		Source:      estateDatabaseConfig(t, "TEST_SOURCE"),
		Destination: estateDatabaseConfig(t, "TEST_DEST"),
	})
	t.Cleanup(func() { _ = m.Close() })
	if err := m.Connect(ctx); err != nil {
		t.Fatalf("Connect(): %v", err)
	}

	// A temporary table lives on one session, so every call uses one pinned
	// connection.
	conn, err := m.Source.Conn(ctx)
	if err != nil {
		t.Fatalf("pin a source connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(ctx, "CREATE TEMPORARY TABLE ms_probe (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create ms_probe: %v", err)
	}

	_, execErr := conn.ExecContext(ctx, "INSERT INTO ms_probe VALUES (1); INSERT INTO ms_probe VALUES (2)")

	var rows int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM ms_probe").Scan(&rows); err != nil {
		t.Fatalf("count ms_probe: %v", err)
	}
	if execErr == nil {
		t.Fatalf("multi-statement Exec succeeded (rows in ms_probe = %d); want MySQL error 1064 and 0 rows", rows)
	}
	var myErr *mysql.MySQLError
	if !errors.As(execErr, &myErr) || myErr.Number != 1064 {
		t.Errorf("multi-statement Exec error = %v; want MySQL error 1064", execErr)
	}
	if rows != 0 {
		t.Errorf("rows in ms_probe = %d after the refused text; want 0", rows)
	}
}
