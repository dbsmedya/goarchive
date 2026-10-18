//go:build integration

package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestMySQLInitConnectCharset (CHS-1, Sonnet's F1 and F2): every value is read and bound
// through the session's character set. A server that narrows an account's session (an
// init_connect such as SET NAMES latin1, 8.0's --skip-character-set-client-handshake under a
// latin1 server default, or a proxy) used to read utf8mb4 text outside that set as '?': the
// copy stored 'x?' for 'x😀', and a key 'x😀' bound back matched a look-alike row 'x?'.
// The job runs as an account with exactly the grant recipe's privileges, without
// CONNECTION_ADMIN or SUPER, whom init_connect skips. It runs only where that account's
// unpinned session is narrowed, the environment matrix row init_connect_set_names; the test
// never sets init_connect itself. Every result is read back as HEX() on the root pools, which
// a narrowed session cannot change.
func TestMySQLInitConnectCharset(t *testing.T) {
	t.Run("text", func(t *testing.T) {
		e := newEnv(t, "chsinit")
		e.both(t, "CREATE TABLE notes (id INT PRIMARY KEY, body VARCHAR(20) CHARACTER SET utf8mb4) ENGINE=InnoDB")
		execAll(t, e.src, "INSERT INTO notes VALUES (1, CONVERT(X'78F09F9880' USING utf8mb4))")
		src, dst := narrowedPools(t, e)
		cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: notes\n    primary_key: id\n    where: 'id = 1'\n"))
		cfg.Source, cfg.Destination = src, dst
		if _, err := e.run(t, cfg); err != nil {
			t.Fatalf("archive under a narrowed session: %v", err)
		}
		if got := hexes(t, e.dst, "SELECT HEX(body) FROM notes"); !equal(got, []string{"78F09F9880"}) {
			t.Errorf("destination body = %v, want [78F09F9880] ('x😀' byte for byte)", got)
		}
	})

	t.Run("key", func(t *testing.T) {
		e := newEnv(t, "chsinit")
		e.both(t, "CREATE TABLE tags (code VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY, created DATE NOT NULL) ENGINE=InnoDB")
		// The eligible key 'x😀', and a look-alike 'x?' outside the where range.
		execAll(t, e.src, "INSERT INTO tags VALUES (CONVERT(X'78F09F9880' USING utf8mb4), '2024-01-01'), "+
			"(CONVERT(X'783F' USING utf8mb4), '2026-01-01')")
		src, dst := narrowedPools(t, e)
		cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: tags\n    primary_key: code\n    where: \"created < '2025-01-01'\"\n"))
		cfg.Source, cfg.Destination = src, dst
		_, err := e.run(t, cfg)
		left, moved := hexes(t, e.src, "SELECT HEX(code) FROM tags ORDER BY code"), hexes(t, e.dst, "SELECT HEX(code) FROM tags ORDER BY code")
		t.Logf("run error %v; source %v, destination %v", err, left, moved)
		if err != nil {
			t.Fatalf("archive under a narrowed session: %v", err)
		}
		if !equal(left, []string{"783F"}) || !equal(moved, []string{"78F09F9880"}) {
			t.Errorf("source %v, destination %v; want the look-alike [783F] left and the eligible [78F09F9880] moved", left, moved)
		}
	})
}

// narrowedPools creates an account with the grant recipes on both servers
// (docs/README_PERMISSIONS.md), skips the test unless that account's unpinned session is
// narrowed, and returns its source and destination pools as GoArchive builds them.
func narrowedPools(t *testing.T, e env) (src, dst *sql.DB) {
	t.Helper()
	user, pass := fmt.Sprintf("ga_chs_%d", os.Getpid()), "charset_only"
	for _, s := range []struct {
		admin          *sql.DB
		schema, grants string
	}{{e.src, e.srcSchema, "SELECT, DELETE, TRIGGER"}, {e.dst, e.dstSchema, "SELECT, INSERT, CREATE, UPDATE, TRIGGER"}} {
		execAll(t, s.admin, "CREATE USER IF NOT EXISTS '"+user+"'@'%' IDENTIFIED BY '"+pass+"'",
			"GRANT "+s.grants+" ON `"+s.schema+"`.* TO '"+user+"'@'%'", "GRANT PROCESS ON *.* TO '"+user+"'@'%'")
		admin := s.admin
		t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'") })
	}
	account := func(prefix, schema string) *config.DatabaseConfig {
		port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
		return &config.DatabaseConfig{Host: os.Getenv(prefix + "_HOST"), Port: port, User: user, Password: pass, Database: schema, TLS: "disable"}
	}

	// The gate: the account's session as the server gives it, without GoArchive's pin.
	raw := mysql.NewConfig()
	srcCfg := account("TEST_SOURCE", e.srcSchema)
	raw.User, raw.Passwd, raw.Net, raw.Addr, raw.DBName = user, pass, "tcp", fmt.Sprintf("%s:%d", srcCfg.Host, srcCfg.Port), e.srcSchema
	probe, err := sql.Open("mysql", raw.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	var results sql.NullString
	err = probe.QueryRow("SELECT @@character_set_results").Scan(&results)
	_ = probe.Close()
	if err != nil {
		t.Fatal(err)
	}
	if results.Valid && results.String == "utf8mb4" {
		t.Skip("the account's session is utf8mb4; the pair needs a narrowed session, the environment matrix row init_connect_set_names")
	}
	t.Logf("the account's unpinned session reads character_set_results = %q (valid %t)", results.String, results.Valid)

	open := func(cfg *config.DatabaseConfig) *sql.DB {
		db, err := sql.Open("mysql", database.BuildDSN(cfg))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	return open(srcCfg), open(account("TEST_DEST", e.dstSchema))
}

// hexes returns the single string column of every row of q.
func hexes(t *testing.T, db *sql.DB, q string) []string {
	t.Helper()
	rs, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rs.Close() }()
	var out []string
	for rs.Next() {
		var s string
		if err := rs.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
