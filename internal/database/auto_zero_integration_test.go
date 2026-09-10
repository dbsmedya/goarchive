//go:build integration

package database

import (
	"context"
	"database/sql"
	"testing"
)

func TestBuildDSNAutoZeroRoundTrip_Integration(t *testing.T) {
	cfg := estateDatabaseConfig(t, "TEST_DEST")
	db, err := sql.Open("mysql", BuildDSN(&cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, q := range []string{
		"CREATE TEMPORARY TABLE vp_auto_driver(id BIGINT PRIMARY KEY, seq BIGINT NOT NULL AUTO_INCREMENT, KEY(seq)) ENGINE=InnoDB",
		"INSERT INTO vp_auto_driver(id,seq) VALUES(1,0)",
	} {
		if _, err := conn.ExecContext(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	var warnings, got int64
	if err := conn.QueryRowContext(context.Background(), "SHOW COUNT(*) WARNINGS").Scan(&warnings); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRowContext(context.Background(), "SELECT seq FROM vp_auto_driver WHERE id=1").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Errorf("AUTO_ZERO_LOST: seq=%d warnings=%d, want zero", got, warnings)
	}
	if warnings != 0 {
		t.Errorf("unexpected warnings=%d", warnings)
	}
}
