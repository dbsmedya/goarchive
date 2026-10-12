//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// These tests run the engine end to end: the source on TEST_SOURCE_* (db1) and the
// destination on TEST_DEST_* (db2, whose global time_zone is +03:00). Each test creates its
// own ga3m5_<name>_<pid> schemas on both servers and drops them afterwards.

// openServer opens a pool pinned to UTC and autocommit, as internal/database's DSN pins every session
// (spec §4.5). params adds or overrides session variables.
func openServer(t *testing.T, prefix, schema string, params map[string]string) *sql.DB {
	t.Helper()
	host, user, pass := os.Getenv(prefix+"_HOST"), os.Getenv(prefix+"_USER"), os.Getenv(prefix+"_PASSWORD")
	port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
	if host == "" || user == "" || port == 0 {
		t.Fatal("integration environment not loaded: set -a; source tests/.env; set +a")
	}
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = user, pass, "tcp", fmt.Sprintf("%s:%d", host, port), schema
	cfg.Params = map[string]string{"time_zone": "'+00:00'", "sql_notes": "1", "autocommit": "1"}
	for k, v := range params {
		cfg.Params[k] = v
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// scratch creates an empty schema on a server, drops it after the test, and returns its
// name and a pool whose default schema it is.
func scratch(t *testing.T, prefix, name string) (string, *sql.DB) {
	t.Helper()
	admin := openServer(t, prefix, "", nil)
	schema := fmt.Sprintf("ga3m5_%s_%d", name, os.Getpid())
	execAll(t, admin, "DROP DATABASE IF EXISTS `"+schema+"`", "CREATE DATABASE `"+schema+"`")
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+schema+"`") })
	return schema, openServer(t, prefix, schema, nil)
}

func execAll(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, q := range stmts {
		if _, err := db.ExecContext(context.Background(), q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// env is one test's pair of schemas. The destination schema is also the job_schema unless
// a test says otherwise.
type env struct {
	src, dst             *sql.DB
	srcSchema, dstSchema string
	rec                  *recorder // nil in the helper child
}

// recorder holds the lock names of every run the env started, for the leak guard
// (source-table-locks spec §8a rule 3). The names are hashes, so they are computed from the
// Config that reached Run, never searched for.
type recorder struct {
	mu                  sync.Mutex
	source, destination map[string]bool
}

func newEnv(t *testing.T, name string) env {
	t.Helper()
	ss, src := scratch(t, "TEST_SOURCE", name)
	ds, dst := scratch(t, "TEST_DEST", name)
	e := env{src: src, dst: dst, srcSchema: ss, dstSchema: ds,
		rec: &recorder{source: map[string]bool{}, destination: map[string]bool{}}}
	// Registered after scratch's cleanups, so it runs before the schemas are dropped.
	t.Cleanup(func() {
		for _, name := range e.leaked(t) {
			t.Errorf("lock %s is still held after the test: a run left its lock session alive", name)
		}
	})
	return e
}

// record notes the lock names a run of cfg takes: its job lock, and a table lock for every
// table of its tree in the env's source schema.
func (e env) record(cfg Config) {
	if e.rec == nil || cfg.Spec == nil {
		return
	}
	e.rec.mu.Lock()
	defer e.rec.mu.Unlock()
	e.rec.destination[lock.JobLockName(cfg.JobSchema, cfg.Name)] = true
	for _, n := range jobspec.NewTree(cfg.Spec).CopyOrder() {
		e.rec.source[lock.TableLockName(e.srcSchema, n.Table)] = true
	}
}

// leaked returns the recorded lock names some session still holds.
func (e env) leaked(t *testing.T) []string {
	t.Helper()
	if e.rec == nil {
		return nil
	}
	e.rec.mu.Lock()
	defer e.rec.mu.Unlock()
	var out []string
	for _, side := range []struct {
		db    *sql.DB
		names map[string]bool
	}{{e.src, e.rec.source}, {e.dst, e.rec.destination}} {
		for name := range side.names {
			id, err := lock.Holder(context.Background(), side.db, name)
			if err != nil {
				t.Errorf("leak guard: %v", err)
				continue
			}
			if id != 0 {
				out = append(out, name)
			}
		}
	}
	return out
}

// both creates the tables on the source and, for archive and copy-only, the destination.
func (e env) both(t *testing.T, ddl ...string) {
	t.Helper()
	execAll(t, e.src, ddl...)
	execAll(t, e.dst, ddl...)
}

// job parses one job of type typ; body is its YAML after job_type, indented four spaces.
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

func quietLogger(t *testing.T) *logger.Logger {
	t.Helper()
	l, err := logger.New(&config.LoggingConfig{Level: "error", Format: "text", Output: "stderr"})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// config is a run of spec as job "j", two roots per batch and two keys per delete chunk.
func (e env) config(t *testing.T, spec *jobspec.Spec) Config {
	t.Helper()
	v := jobspec.VerifySHA256
	if spec.Type == jobspec.Purge {
		v = ""
	}
	return Config{
		Name: "j", Spec: spec, Source: e.src, Destination: e.dst, JobSchema: e.dstSchema,
		Processing:   config.ProcessingConfig{BatchSize: 2, BatchDeleteSize: 2},
		Verification: v, Logger: quietLogger(t),
	}
}

func (e env) run(t *testing.T, cfg Config) (*Report, error) {
	t.Helper()
	return e.runContext(t, context.Background(), cfg)
}

// runContext is the one way an engine test runs a job: it records the run's lock names for
// the leak guard first.
func (e env) runContext(t *testing.T, ctx context.Context, cfg Config) (*Report, error) {
	t.Helper()
	e.record(cfg)
	return Run(ctx, cfg)
}

func (e env) mustRun(t *testing.T, cfg Config) *Report {
	t.Helper()
	rep, err := e.run(t, cfg)
	if err != nil {
		t.Fatalf("Run(%s): %v", cfg.Spec.Type, err)
	}
	return rep
}

// rows returns a table's rows as text, each column as HEX(), in primary-key order, so a
// comparison is byte for byte.
func rows(t *testing.T, db *sql.DB, table, orderBy string) []string {
	t.Helper()
	ctx := context.Background()
	cols, err := db.QueryContext(ctx, "SELECT * FROM `"+table+"` LIMIT 0")
	if err != nil {
		t.Fatal(err)
	}
	names, _ := cols.Columns()
	_ = cols.Close()
	hexed := make([]string, len(names))
	for i, n := range names {
		hexed[i] = "IFNULL(HEX(`" + n + "`), 'NULL')"
	}
	r, err := db.QueryContext(ctx, "SELECT CONCAT_WS('|', "+strings.Join(hexed, ", ")+") FROM `"+table+"` ORDER BY "+orderBy)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var out []string
	for r.Next() {
		var s string
		if err := r.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func count(t *testing.T, db *sql.DB, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRowContext(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

// statuses returns job j's log, as status → count, from jobSchema on db.
func statuses(t *testing.T, db *sql.DB, jobSchema string) map[int]int64 {
	t.Helper()
	ctx := context.Background()
	var id int64
	err := db.QueryRowContext(ctx, "SELECT id FROM `"+jobSchema+"`.archiver_job WHERE job_name = 'j'").Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.QueryContext(ctx, fmt.Sprintf("SELECT ga_status, COUNT(*) FROM `%s`.archiver_job_log_%d GROUP BY ga_status", jobSchema, id))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	out := map[int]int64{}
	for r.Next() {
		var st int
		var n int64
		if err := r.Scan(&st, &n); err != nil {
			t.Fatal(err)
		}
		out[st] = n
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func wantErr[T error](t *testing.T, err error) T {
	t.Helper()
	var target T
	if !errors.As(err, &target) {
		t.Fatalf("err = %v, want %T", err, target)
	}
	return target
}
