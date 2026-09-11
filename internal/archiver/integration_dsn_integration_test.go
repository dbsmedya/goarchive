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
		if !ok {
			t.Fatalf("%s database missing from integration setup", name)
		}
		var got any
		if err := db.QueryRow("SELECT CAST('2024-01-02 03:04:05' AS DATETIME)").Scan(&got); err != nil {
			t.Fatalf("%s datetime query: %v", name, err)
		}
		value, ok := got.(time.Time)
		if !ok {
			t.Fatalf("HARNESS_PARSE_TIME_DISABLED: %s DATETIME decoded as %T, want time.Time", name, got)
		}
		want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
		if !value.Equal(want) {
			t.Fatalf("%s DATETIME = %s, want %s", name, value, want)
		}
	}
}

func TestIntegrationHarnessEndpointControl_Integration(t *testing.T) {
	setup, _ := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	for _, cfg := range setup.Config.Databases {
		db, ok := setup.GetDB(cfg.Name)
		if !ok {
			t.Fatalf("%s database missing", cfg.Name)
		}
		var name string
		var seven int
		if err := db.QueryRow("SELECT DATABASE(), 7").Scan(&name, &seven); err != nil {
			t.Fatalf("%s endpoint query: %v", cfg.Name, err)
		}
		if name != cfg.Database || seven != 7 {
			t.Fatalf("%s endpoint=%q/%d, want %q/7", cfg.Name, name, seven, cfg.Database)
		}
	}
}

func TestIntegrationHarnessSessionReplacement_Integration(t *testing.T) {
	setup, _ := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	for _, subcase := range []string{"session-settings-only", "temporal-decode"} {
		t.Run(subcase, func(t *testing.T) {
			for _, cfg := range setup.Config.Databases {
				t.Run(cfg.Name, func(t *testing.T) {
					dsn, err := integrationDSN(cfg, cfg.Database, 30*time.Second)
					if err != nil {
						t.Fatal(err)
					}
					db, err := sql.Open("mysql", dsn)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = db.Close() })
					db.SetMaxIdleConns(0)
					db.SetMaxOpenConns(1)
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					var previousID int64
					for session := 0; session < 2; session++ {
						conn, err := db.Conn(ctx)
						if err != nil {
							t.Fatal(err)
						}
						// Close even on a fatal assertion, then explicitly close before reconnect.
						t.Cleanup(func() { _ = conn.Close() })
						var id int64
						if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil {
							t.Fatal(err)
						}
						if id == 0 || id == previousID {
							t.Fatalf("physical connection not replaced: previous=%d current=%d", previousID, id)
						}
						previousID = id
						if _, err := conn.ExecContext(ctx, "CREATE TEMPORARY TABLE integration_dsn_session (id INT PRIMARY KEY, seq INT AUTO_INCREMENT, stamp DATETIME, KEY(seq))"); err != nil {
							t.Fatal(err)
						}
						if _, err := conn.ExecContext(ctx, "INSERT INTO integration_dsn_session (id, seq, stamp) VALUES (1, 0, '2024-01-02 03:04:05')"); err != nil {
							t.Fatal(err)
						}
						if subcase == "session-settings-only" {
							var zone, mode string
							var notes, zero int
							if err := conn.QueryRowContext(ctx, "SELECT @@time_zone, @@sql_notes, @@sql_mode, seq FROM integration_dsn_session WHERE id=1").Scan(&zone, &notes, &mode, &zero); err != nil {
								t.Fatal(err)
							}
							hasZeroMode := false
							for _, item := range strings.Split(mode, ",") {
								if item == "NO_AUTO_VALUE_ON_ZERO" {
									hasZeroMode = true
								}
							}
							if zone != "+00:00" || notes != 1 || !hasZeroMode || zero != 0 {
								t.Fatalf("session%d: zone=%q notes=%d mode=%q zero=%d", session, zone, notes, mode, zero)
							}
						} else {
							var value any
							if err := conn.QueryRowContext(ctx, "SELECT stamp FROM integration_dsn_session WHERE id=1").Scan(&value); err != nil {
								t.Fatal(err)
							}
							got, ok := value.(time.Time)
							want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
							if !ok || !got.Equal(want) || got.Location() != time.UTC {
								t.Fatalf("session%d DATETIME=%v (%T), want literal UTC %s", session, value, value, want)
							}
						}
						t.Logf("session%d connection_id=%d", session, id)
						if err := conn.Close(); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		})
	}
}
