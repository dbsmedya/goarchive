//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// These tests run the 3.0 preflight against MySQL: the source on TEST_SOURCE_* (db1) and
// the destination on TEST_DEST_* (db2). Each test creates its own schemas and drops them.
// They prove that the library's real facts for composite keys, collations and declared
// foreign keys reach the checks in the shape the unit tests assume.

// openServer opens a pool on one test server with the session pins the engine uses.
func openServer(t *testing.T, prefix, schema string) *sql.DB {
	t.Helper()
	host, user, pass := os.Getenv(prefix+"_HOST"), os.Getenv(prefix+"_USER"), os.Getenv(prefix+"_PASSWORD")
	port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
	if host == "" || user == "" || port == 0 {
		t.Fatal("integration environment not loaded: set -a; source tests/.env; set +a")
	}
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = user, pass, "tcp", fmt.Sprintf("%s:%d", host, port), schema
	cfg.Params = map[string]string{"time_zone": "'+00:00'"}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// scratch creates an empty schema on a server, drops it after the test, and returns a pool
// whose default schema it is.
func scratch(t *testing.T, prefix, name string) *sql.DB {
	t.Helper()
	admin := openServer(t, prefix, "")
	schema := fmt.Sprintf("ga3m3_%s_%d", name, os.Getpid())
	ctx := context.Background()
	for _, q := range []string{"DROP DATABASE IF EXISTS `" + schema + "`", "CREATE DATABASE `" + schema + "`"} {
		if _, err := admin.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+schema+"`") })
	return openServer(t, prefix, schema)
}

func execAll(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, q := range stmts {
		if _, err := db.ExecContext(context.Background(), q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

func schemaOf(t *testing.T, db *sql.DB) string {
	t.Helper()
	var s string
	if err := db.QueryRowContext(context.Background(), "SELECT DATABASE()").Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// orderTables is a root on a CHAR(36) key with a unique code, lines on a composite key, and
// notes on a composite foreign key. cs is the charset and collation of every character
// column.
func orderTables(cs string) []string {
	return []string{
		"CREATE TABLE orders (id CHAR(36) " + cs + " NOT NULL, placed DATE, code VARCHAR(20) " + cs + ", " +
			"PRIMARY KEY (id), UNIQUE KEY uk_code (code)) ENGINE=InnoDB",
		"CREATE TABLE order_lines (order_id CHAR(36) " + cs + " NOT NULL, line_no SMALLINT NOT NULL, sku VARCHAR(32), " +
			"PRIMARY KEY (order_id, line_no), CONSTRAINT fk_lines_order FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB",
		"CREATE TABLE line_notes (note_id INT NOT NULL, order_id CHAR(36) " + cs + ", line_no SMALLINT, body TEXT, " +
			"PRIMARY KEY (note_id), KEY ix_line (order_id, line_no), CONSTRAINT fk_notes_line FOREIGN KEY (order_id, line_no) " +
			"REFERENCES order_lines (order_id, line_no)) ENGINE=InnoDB",
	}
}

const orderJob = `
    root_table: orders
    primary_key: id
    where: "1=1"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
        relations:
          - table: line_notes
            primary_key: note_id
            foreign_key: [order_id, line_no]
            references: [order_id, line_no]
`

func job(t *testing.T, typ jobspec.Type, body string) *jobspec.Spec {
	t.Helper()
	doc, err := jobspec.Parse([]byte("source: {host: s, user: u, database: src}\ndestination: {host: d, user: u, database: dst}\n" +
		"jobs:\n  j:\n    job_type: " + string(typ) + body))
	if err != nil {
		t.Fatalf("jobspec.Parse: %v", err)
	}
	s, err := doc.Job("j", "")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func runOn(t *testing.T, src, dst *sql.DB, spec *jobspec.Spec) (*Result, error) {
	t.Helper()
	return runVerified(t, src, dst, spec, "count")
}

func runVerified(t *testing.T, src, dst *sql.DB, spec *jobspec.Spec, method string) (*Result, error) {
	t.Helper()
	return Run(context.Background(), Config{
		Source: src, SourceDatabase: schemaOf(t, src),
		Destination: dst, DestinationDatabase: schemaOf(t, dst), JobSchema: schemaOf(t, dst),
		Spec: spec, Verification: jobspec.Verification(method),
	})
}

func refusedWith(t *testing.T, err error, check string) *Error {
	t.Helper()
	var pe *Error
	if !errors.As(err, &pe) || pe.Check != check {
		t.Fatalf("Run() = %v, want %s", err, check)
	}
	return pe
}

// TestMySQLCompositeKeysAndTheTwinCollation: a utf8mb3_general_ci composite job copied into
// utf8mb4_general_ci passes under sha256 verification (R32, spec §10), with MySQL's keys in
// key order and the caps reported. Under count verification it is refused: the twin
// exception covers key columns, and orders.code and line_notes' foreign_key columns are not
// their table's key, so they keep the 2.x non-key rule (spec §8.3). Both tables are named,
// each once, and nothing else (M3-F2). The same job into utf8mb4_0900_ai_ci is refused for
// the key itself.
func TestMySQLCompositeKeysAndTheTwinCollation(t *testing.T) {
	src := scratch(t, "TEST_SOURCE", "src")
	execAll(t, src, orderTables("CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci")...)

	twin := scratch(t, "TEST_DEST", "dst")
	execAll(t, twin, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")...)
	res, err := runVerified(t, src, twin, job(t, jobspec.Archive, orderJob), "sha256")
	if err != nil {
		t.Fatalf("Run() = %v, want success", err)
	}
	_, err = runOn(t, src, twin, job(t, jobspec.Archive, orderJob))
	const charset = "character set mismatch (source=utf8mb3, destination=utf8mb4): copying can silently " +
		"transliterate or truncate text and count verification cannot detect it; align charsets or use sha256 " +
		"verification; source=utf8mb3, destination=utf8mb4)"
	if e := refusedWith(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK"); len(e.Tables) != 2 ||
		e.Tables[0] != "orders(column code: "+charset || e.Tables[1] != "line_notes(column order_id: "+charset {
		t.Errorf("tables = %q, want exactly the two non-key columns: orders.code and line_notes' foreign_key order_id", e.Tables)
	}
	lines := res.Keys["order_lines"]
	if !slices.Equal(lines.Names(), []string{"order_id", "line_no"}) {
		t.Errorf("order_lines key = %v", lines.Names())
	}
	if c := lines.Columns[0]; c.Family != key.FamilyString || c.Charset != "utf8mb3" || c.Collation != "utf8mb3_general_ci" || c.Length != 36 {
		t.Errorf("order_lines.order_id = %+v", c)
	}
	if len(res.Caps) != 8 { // 3 source by key, 2 discovery, 3 destination by key
		t.Errorf("caps = %+v", res.Caps)
	}

	other := scratch(t, "TEST_DEST", "dst0900")
	execAll(t, other, orderTables("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")...)
	_, err = runOn(t, src, other, job(t, jobspec.Archive, orderJob))
	if e := refusedWith(t, err, "DEST_KEY_IDENTITY_CHECK"); !hasTable(e, "orders(key column id: source utf8mb3 utf8mb3_general_ci, destination utf8mb4 utf8mb4_0900_ai_ci)") {
		t.Errorf("tables = %v", e.Tables)
	}
}

// TestMySQLSwappedPairingIsRefused is spec §10's witness: a composite relation whose
// references swap the declared foreign key's pairing is refused.
func TestMySQLSwappedPairingIsRefused(t *testing.T) {
	src := scratch(t, "TEST_SOURCE", "pair")
	execAll(t, src,
		"CREATE TABLE parent (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) ENGINE=InnoDB",
		"CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pa INT, pb INT, KEY ix_p (pa, pb), "+
			"CONSTRAINT fk_child FOREIGN KEY (pa, pb) REFERENCES parent (a, b)) ENGINE=InnoDB")
	dst := scratch(t, "TEST_DEST", "pairdst")
	const body = `
    root_table: parent
    primary_key: [a, b]
    where: "1=1"
    relations:
      - table: child
        primary_key: id
        foreign_key: [pa, pb]
        references: %s
`
	_, err := runOn(t, src, dst, job(t, jobspec.Purge, fmt.Sprintf(body, "[b, a]")))
	refusedWith(t, err, "FK_PAIRING_CHECK")

	// The declared pairing, written in another order, passes, and purge needs no
	// destination tables (R5).
	if _, err := runOn(t, src, dst, job(t, jobspec.Purge, fmt.Sprintf(body, "[a, b]"))); err != nil {
		t.Fatalf("Run() = %v, want success", err)
	}
}

// TestMySQLRefusalsFromRealFacts: an approximate key, a prefix key part, a documented key
// that differs only in case, and a foreign_key indexed only by a prefix are each refused
// from the library's real facts.
func TestMySQLRefusalsFromRealFacts(t *testing.T) {
	src := scratch(t, "TEST_SOURCE", "refuse")
	execAll(t, src,
		"CREATE TABLE f (k DOUBLE NOT NULL PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE p (k VARCHAR(64) NOT NULL, PRIMARY KEY (k(10))) ENGINE=InnoDB",
		"CREATE TABLE c (Order_ID INT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE q (id VARCHAR(64) NOT NULL PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE r (id INT NOT NULL PRIMARY KEY, q_id VARCHAR(64), KEY ix_q (q_id(10))) ENGINE=InnoDB")
	dst := scratch(t, "TEST_DEST", "refusedst")
	for _, tc := range []struct{ body, check, shows string }{
		{"\n    root_table: f\n    primary_key: k\n    where: \"1=1\"\n", "KEY_TYPE_UNSUPPORTED", "f.k (double): approximate"},
		{"\n    root_table: p\n    primary_key: k\n    where: \"1=1\"\n", "KEY_TYPE_UNSUPPORTED", "prefix key part (10)"},
		{"\n    root_table: c\n    primary_key: order_id\n    where: \"1=1\"\n", "PK_COLUMN_CASE_CHECK", "c(primary_key (order_id) but the PRIMARY KEY is (Order_ID))"},
		{"\n    root_table: q\n    primary_key: id\n    where: \"1=1\"\n    relations:\n      - {table: r, primary_key: id, foreign_key: q_id}\n",
			"FK_INDEX_CHECK", "r(q_id)"},
	} {
		_, err := runOn(t, src, dst, job(t, jobspec.Purge, tc.body))
		if e := refusedWith(t, err, tc.check); !hasTable(e, tc.shows) {
			t.Errorf("%s: tables = %v, want %q", tc.check, e.Tables, tc.shows)
		}
	}
}
