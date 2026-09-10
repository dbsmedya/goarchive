//go:build integration

package database

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
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

func vpAutoDB(t *testing.T) *sql.DB {
	t.Helper()
	cfg := estateDatabaseConfig(t, "TEST_DEST")
	db, err := sql.Open("mysql", BuildDSN(&cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}
func vpAutoFacts(t *testing.T, conn *sql.Conn) (int64, string) {
	t.Helper()
	var id int64
	var mode string
	if err := conn.QueryRowContext(context.Background(), "SELECT CONNECTION_ID(),@@SESSION.sql_mode").Scan(&id, &mode); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, token := range strings.Split(mode, ",") {
		if strings.EqualFold(token, "NO_AUTO_VALUE_ON_ZERO") {
			found = true
		}
	}
	if !found {
		t.Fatalf("physical connection %d lacks preserving mode: %s", id, mode)
	}
	t.Logf("connection_id=%d mode=%s", id, mode)
	return id, mode
}
func TestBuildDSNAutoZeroModeSets_Integration(t *testing.T) {
	db := vpAutoDB(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, session := vpAutoFacts(t, conn)
	var global string
	if err := conn.QueryRowContext(context.Background(), "SELECT @@GLOBAL.sql_mode").Scan(&global); err != nil {
		t.Fatal(err)
	}
	set := func(s string) map[string]bool {
		out := map[string]bool{}
		for _, token := range strings.Split(s, ",") {
			if token != "" {
				out[strings.ToUpper(token)] = true
			}
		}
		return out
	}
	want := set(global)
	want["NO_AUTO_VALUE_ON_ZERO"] = true
	if got := set(session); !reflect.DeepEqual(got, want) {
		t.Fatalf("mode set=%v want=%v", got, want)
	}
}
func TestBuildDSNAutoZeroReplacementConnections_Integration(t *testing.T) {
	db := vpAutoDB(t)
	db.SetMaxOpenConns(3)
	a, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	id1, _ := vpAutoFacts(t, a)
	id2, _ := vpAutoFacts(t, b)
	if id1 == id2 {
		t.Fatal("expected two physical sessions")
	}
	db.SetMaxIdleConns(0)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id3, _ := vpAutoFacts(t, c)
	if id3 == id1 || id3 == id2 {
		t.Fatal("replacement reused closed physical session")
	}
}
func TestBuildDSNAutoZeroPrimaryAndOmittedID_Integration(t *testing.T) {
	db := vpAutoDB(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, q := range []string{
		"CREATE TEMPORARY TABLE vp_auto_primary(id BIGINT AUTO_INCREMENT PRIMARY KEY,payload INT)",
		"INSERT INTO vp_auto_primary(id,payload) VALUES(0,10)",
		"INSERT INTO vp_auto_primary(payload) VALUES(20)",
		"INSERT INTO vp_auto_primary(id,payload) VALUES(NULL,30)",
	} {
		if _, err := conn.ExecContext(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	var zero, positive int
	if err := conn.QueryRowContext(context.Background(), "SELECT SUM(id=0 AND payload=10),SUM(id>0 AND payload IN(20,30)) FROM vp_auto_primary").Scan(&zero, &positive); err != nil {
		t.Fatal(err)
	}
	if zero != 1 || positive != 2 {
		t.Fatalf("zero=%d allocated=%d, want 1/2", zero, positive)
	}
}
