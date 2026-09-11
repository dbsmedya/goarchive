//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestIntegrationHarnessTemporalDecode_Integration(t *testing.T) {
	setup, _ := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)

	for _, name := range []string{"source", "destination"} {
		db, ok := setup.GetDB(name)
		if !ok { t.Fatalf("%s database missing from integration setup", name) }
		var got any
		if err := db.QueryRow("SELECT CAST('2024-01-02 03:04:05' AS DATETIME)").Scan(&got); err != nil {
			t.Fatalf("%s datetime query: %v", name, err)
		}
		value, ok := got.(time.Time)
		if !ok {
			t.Fatalf("HARNESS_PARSE_TIME_DISABLED: %s DATETIME decoded as %T, want time.Time", name, got)
		}
		want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
		if !value.Equal(want) { t.Fatalf("%s DATETIME = %s, want %s", name, value, want) }
	}
}

func TestIntegrationHarnessEndpointControl_Integration(t *testing.T) {
	setup, _ := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	for _, name := range []string{"source", "destination"} {
		db, ok := setup.GetDB(name); if !ok { t.Fatalf("%s database missing", name) }
		var databaseName string; var seven int
		if err := db.QueryRow("SELECT DATABASE(), 7").Scan(&databaseName, &seven); err != nil { t.Fatalf("%s endpoint query: %v", name, err) }
		if databaseName != "goarchive_test" || seven != 7 { t.Fatalf("%s endpoint = %q/%d, want goarchive_test/7", name, databaseName, seven) }
	}
}

func TestIntegrationHarnessSessionReplacement_Integration(t *testing.T) {
	setup, _ := SetupIntegrationTest(t); t.Cleanup(setup.Close)
	var source DatabaseConfig
	for _, cfg := range setup.Config.Databases { if cfg.Name == "source" { source = cfg } }
	dsn, err := integrationDSN(source, source.Database, 5*time.Second); if err != nil { t.Fatal(err) }
	db, err := sql.Open("mysql", dsn); if err != nil { t.Fatal(err) }; t.Cleanup(func() { _ = db.Close() })
	db.SetMaxIdleConns(0)
	ctx := context.Background()
	var firstID int64
	t.Run("session-settings-only", func(t *testing.T) {
		conn, err := db.Conn(ctx); if err != nil { t.Fatal(err) }
		defer conn.Close()
		var zone, mode string; var notes int
		if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID(), @@time_zone, @@sql_notes, @@sql_mode").Scan(&firstID, &zone, &notes, &mode); err != nil { t.Fatal(err) }
		if zone != "+00:00" || notes != 1 || !strings.Contains(mode, "NO_AUTO_VALUE_ON_ZERO") { t.Fatalf("session = zone %q notes %d mode %q", zone, notes, mode) }
		if _, err := conn.ExecContext(ctx, "CREATE TEMPORARY TABLE integration_dsn_session (id INT PRIMARY KEY, seq INT AUTO_INCREMENT, KEY(seq))"); err != nil { t.Fatal(err) }
		if _, err := conn.ExecContext(ctx, "INSERT INTO integration_dsn_session (id, seq) VALUES (1, 0)"); err != nil { t.Fatal(err) }
		var zero int; if err := conn.QueryRowContext(ctx, "SELECT seq FROM integration_dsn_session WHERE id=1").Scan(&zero); err != nil || zero != 0 { t.Fatalf("explicit zero = %d, %v", zero, err) }
	})
	t.Run("temporal-decode", func(t *testing.T) {
		conn, err := db.Conn(ctx); if err != nil { t.Fatal(err) }; defer conn.Close()
		var secondID int64; var value any
		if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID(), CAST('2024-01-02 03:04:05' AS DATETIME)").Scan(&secondID, &value); err != nil { t.Fatal(err) }
		if secondID == firstID { t.Fatalf("connection was reused: %d", secondID) }
		if _, ok := value.(time.Time); !ok { t.Fatalf("temporal decode = %T, want time.Time", value) }
	})
}
