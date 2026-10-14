//go:build integration

package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// These tests run the commands end to end: the source on TEST_SOURCE_* (db1) and the
// destination on TEST_DEST_* (db2). Each creates its own ga3m6_<name>_<pid> schemas on both
// servers, writes a config file naming them, and drops them afterwards.

type server struct {
	host, user, pass string
	port             int
}

func serverFor(t *testing.T, prefix string) server {
	t.Helper()
	port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
	s := server{os.Getenv(prefix + "_HOST"), os.Getenv(prefix + "_USER"), os.Getenv(prefix + "_PASSWORD"), port}
	if s.host == "" || s.user == "" || s.port == 0 {
		t.Fatal("integration environment not loaded: set -a; source tests/.env; set +a")
	}
	return s
}

func (s server) open(t *testing.T, schema string) *sql.DB {
	t.Helper()
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = s.user, s.pass, "tcp", fmt.Sprintf("%s:%d", s.host, s.port), schema
	// The test pool pins sql_select_limit as BuildDSN does (beta audit A1), so the test's own
	// reads are whole under the matrix row sql_select_limit_20.
	cfg.Params = map[string]string{"time_zone": "'+00:00'", "autocommit": "1", "sql_select_limit": "18446744073709551615"}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// fixture is one test's pair of scratch schemas and its config file.
type fixture struct {
	src, dst         *sql.DB
	srcName, dstName string
	config           string
}

func newFixture(t *testing.T, name string, jobs string) fixture {
	t.Helper()
	ss, ds := serverFor(t, "TEST_SOURCE"), serverFor(t, "TEST_DEST")
	f := fixture{srcName: fmt.Sprintf("ga3m6_%s_%d", name, os.Getpid()), dstName: fmt.Sprintf("ga3m6_%s_%d", name, os.Getpid())}
	for _, p := range []struct {
		s      server
		schema string
	}{{ss, f.srcName}, {ds, f.dstName}} {
		admin := p.s.open(t, "")
		mustExec(t, admin, "DROP DATABASE IF EXISTS `"+p.schema+"`", "CREATE DATABASE `"+p.schema+"`")
		schema := p.schema
		t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+schema+"`") })
	}
	f.src, f.dst = ss.open(t, f.srcName), ds.open(t, f.dstName)
	f.config = writeConfig(t, fmt.Sprintf(`
source:
  host: %s
  port: %d
  user: %s
  password: %q
  database: %s
  tls: disable
destination:
  host: %s
  port: %d
  user: %s
  password: %q
  database: %s
  tls: disable
processing:
  batch_size: 2
  batch_delete_size: 2
logging:
  level: error
  output: stderr
jobs:
%s`, ss.host, ss.port, ss.user, ss.pass, f.srcName, ds.host, ds.port, ds.user, ds.pass, f.dstName, jobs))
	return f
}

func mustExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.ExecContext(context.Background(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func scalar(t *testing.T, db *sql.DB, q string) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

var orderDDL = []string{
	"CREATE TABLE orders (id CHAR(36) NOT NULL, placed DATE NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
	"CREATE TABLE order_lines (order_id CHAR(36) NOT NULL, line_no SMALLINT NOT NULL, sku VARCHAR(32), " +
		"PRIMARY KEY (order_id, line_no), CONSTRAINT fk_lines_order FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB",
}

// orderJobs are an archive, a purge and a copy-only job over the same tree.
const orderJobs = `
  archive_orders:
    job_type: archive
    root_table: orders
    primary_key: id
    where: "placed < '2020-01-01'"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
  purge_orders:
    job_type: purge
    root_table: orders
    primary_key: id
    where: "placed < '2020-01-01'"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
  copy_orders:
    job_type: copy-only
    root_table: orders
    primary_key: id
    where: "placed < '2020-01-01'"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
`

// seed puts three old orders with two lines each, and one recent order, in the source.
func seed(t *testing.T, f fixture) {
	t.Helper()
	for i, placed := range []string{"2019-01-01", "2019-02-01", "2019-03-01", "2024-01-01"} {
		id := fmt.Sprintf("00000000-0000-0000-0000-00000000000%d", i)
		mustExec(t, f.src, fmt.Sprintf("INSERT INTO orders VALUES ('%s', '%s')", id, placed),
			fmt.Sprintf("INSERT INTO order_lines VALUES ('%s', 1, 'a'), ('%s', 2, 'b')", id, id))
	}
}

func TestMySQLArchiveCommand(t *testing.T) {
	f := newFixture(t, "archive", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)
	if out, err := execute(t, "archive", "-c", f.config, "--job", "archive_orders"); err != nil {
		t.Fatalf("archive: %v\n%s", err, out)
	}
	if got := scalar(t, f.src, "SELECT COUNT(*) FROM order_lines"); got != 2 {
		t.Errorf("source lines = %d, want the recent order's 2", got)
	}
	if got := scalar(t, f.dst, "SELECT COUNT(*) FROM order_lines"); got != 6 {
		t.Errorf("destination lines = %d, want 6", got)
	}
	if got := scalar(t, f.dst, "SELECT job_status FROM archiver_job WHERE job_name = 'archive_orders'"); got != 0 {
		t.Errorf("job_status = %d, want idle", got)
	}
}

// TestMySQLPurgeCommand: a purge needs no destination tables, only the tracking schema.
func TestMySQLPurgeCommand(t *testing.T) {
	f := newFixture(t, "purge", orderJobs)
	mustExec(t, f.src, orderDDL...)
	seed(t, f)
	if out, err := execute(t, "purge", "-c", f.config, "--job", "purge_orders"); err != nil {
		t.Fatalf("purge: %v\n%s", err, out)
	}
	if got := scalar(t, f.src, "SELECT COUNT(*) FROM orders"); got != 1 {
		t.Errorf("source orders = %d, want the recent one", got)
	}
}

// TestMySQLCopyOnlyCommand (spec §6.6): a copy-only rerun without resume fails on the
// duplicate and suggests resume: true; with it, the rerun skips what was copied.
func TestMySQLCopyOnlyCommand(t *testing.T) {
	f := newFixture(t, "copyonly", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)
	if out, err := execute(t, "copy-only", "-c", f.config, "--job", "copy_orders", "--verification=count"); err != nil {
		t.Fatalf("copy-only: %v\n%s", err, out)
	}
	if got := scalar(t, f.src, "SELECT COUNT(*) FROM order_lines"); got != 8 {
		t.Errorf("source lines = %d, want all 8 kept", got)
	}
	if _, err := execute(t, "copy-only", "-c", f.config, "--job", "copy_orders"); err == nil || !strings.Contains(err.Error(), "resume: true") {
		t.Errorf("a rerun without resume = %v, want the duplicate refusal suggesting resume: true", err)
	}
	resumed := writeConfig(t, strings.Replace(mustRead(t, f.config), "    job_type: copy-only\n", "    job_type: copy-only\n    resume: true\n", 1))
	if out, err := execute(t, "copy-only", "-c", resumed, "--job", "copy_orders"); err != nil {
		t.Fatalf("a rerun with resume: %v\n%s", err, out)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestMySQLValidateCommand (spec §8.1, §10, R5): validate runs each job's own profile, so a
// purge job passes without destination tables, while the archive job over the same tables
// fails on them.
func TestMySQLValidateCommand(t *testing.T) {
	f := newFixture(t, "validate", orderJobs)
	mustExec(t, f.src, orderDDL...)
	if out, err := execute(t, "validate", "-c", f.config, "--job", "purge_orders"); err != nil {
		t.Errorf("validate purge_orders: %v\n%s", err, out)
	}
	out, err := execute(t, "validate", "-c", f.config, "--job", "archive_orders")
	if err == nil || !strings.Contains(err.Error(), "archive_orders") {
		t.Errorf("validate archive_orders without destination tables = %v\n%s", err, out)
	}
}

// TestMySQLDryRunCommand (spec §7.2): dry-run changes nothing and creates no tracking table.
func TestMySQLDryRunCommand(t *testing.T) {
	f := newFixture(t, "dryrun", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)
	if out, err := execute(t, "dry-run", "-c", f.config, "--job", "archive_orders"); err != nil {
		t.Fatalf("dry-run: %v\n%s", err, out)
	}
	if got := scalar(t, f.src, "SELECT COUNT(*) FROM order_lines"); got != 8 {
		t.Errorf("dry-run changed the source: %d lines", got)
	}
	var name string
	if err := f.dst.QueryRowContext(context.Background(), "SHOW TABLES LIKE 'archiver_job'").Scan(&name); err != sql.ErrNoRows {
		t.Errorf("dry-run created the tracking tables (%q, %v)", name, err)
	}
}
