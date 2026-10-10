//go:build integration

package tracking

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// These tests run the tracking store on TEST_DEST_* (db2), whose global time_zone is not
// UTC. Each test creates and drops its own ga3m4_<name>_<pid> schema, which serves as both
// the job_schema and the schema of the root table whose key the log mirrors.

func openDest(t *testing.T, schema string) *sql.DB {
	t.Helper()
	return openAs(t, schema, os.Getenv("TEST_DEST_USER"), os.Getenv("TEST_DEST_PASSWORD"))
}

func openAs(t *testing.T, schema, user, pass string) *sql.DB {
	t.Helper()
	host := os.Getenv("TEST_DEST_HOST")
	port, _ := strconv.Atoi(os.Getenv("TEST_DEST_PORT"))
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

// scratch creates an empty schema, drops it after the test, and returns its name and a
// pool whose default schema it is.
func scratch(t *testing.T, name string) (string, *sql.DB) {
	t.Helper()
	admin := openDest(t, "")
	schema := fmt.Sprintf("ga3m4_%s_%d", name, os.Getpid())
	exec(t, admin, "DROP DATABASE IF EXISTS `"+schema+"`", "CREATE DATABASE `"+schema+"`")
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+schema+"`") })
	return schema, openDest(t, schema)
}

// leastPrivilege returns a pool on schema for a new account holding only the tracking
// grants docs/README_PERMISSIONS.md documents: CREATE, SELECT, INSERT and UPDATE, and no
// DELETE.
func leastPrivilege(t *testing.T, admin *sql.DB, schema string) *sql.DB {
	t.Helper()
	user := fmt.Sprintf("ga3m4_lp_%d", os.Getpid())
	pass := fmt.Sprintf("lp-%d-%d", os.Getpid(), time.Now().UnixNano())
	exec(t, admin, "DROP USER IF EXISTS '"+user+"'@'%'", "CREATE USER '"+user+"'@'%' IDENTIFIED BY '"+pass+"'",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+schema+"`.* TO '"+user+"'@'%'")
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'") })
	return openAs(t, schema, user, pass)
}

func exec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, q := range stmts {
		if _, err := db.ExecContext(context.Background(), q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// rootKey reads table's key the way preflight does.
func rootKey(t *testing.T, db *sql.DB, schema, table string) key.Def {
	t.Helper()
	ctx := context.Background()
	insp := validations.NewInspector(db, schema)
	pks, err := insp.PrimaryKeys(ctx, []string{table})
	if err != nil || len(pks) != 1 {
		t.Fatalf("PrimaryKeys(%s) = %v, %v", table, pks, err)
	}
	spec, err := insp.TableSpec(ctx, validations.Ref(schema, table), validations.WithIndexes())
	if err != nil {
		t.Fatal(err)
	}
	d, err := key.FromFacts(pks[0], spec)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

const ordersDDL = "CREATE TABLE orders (region VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, " +
	"placed TIMESTAMP(3) NOT NULL, id INT UNSIGNED NOT NULL, amount DECIMAL(10,2), PRIMARY KEY (region, placed, id)) ENGINE=InnoDB"

func countTables(t *testing.T, db *sql.DB) int {
	t.Helper()
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}

func binding(where string) Binding {
	return Binding{SourceServerUUID: "11111111-1111-1111-1111-111111111111", SourceDatabase: "src",
		DestinationDatabase: "dst", Fingerprint: Fingerprint{Relations: []Edge{}, Where: where}}
}

func mustOpen(t *testing.T, db *sql.DB, schema string) *Store {
	t.Helper()
	s, err := Open(context.Background(), db, schema)
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	return s
}

func inTx(t *testing.T, db *sql.DB, commit bool, f func(tx *sql.Tx) error) error {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if !commit {
		return tx.Rollback()
	}
	return tx.Commit()
}

// TestMySQLSchemaLabel: a new job_schema is created and stamped 3.0, a second Open and
// eight concurrent ones accept it, and nothing else is written.
func TestMySQLSchemaLabel(t *testing.T) {
	schema, db := scratch(t, "label")
	mustOpen(t, db, schema)
	var label string
	if err := db.QueryRow("SELECT schema_version FROM goarchive_meta WHERE id = 1").Scan(&label); err != nil || label != "3.0" {
		t.Fatalf("label = %q, %v", label, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := Open(context.Background(), db, schema); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Open() = %v", err)
		}
	}
	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM goarchive_meta").Scan(&rows); err != nil || rows != 1 {
		t.Errorf("goarchive_meta rows = %d, %v", rows, err)
	}

	// Eight runs stamping one new schema at once.
	schema2, db2 := scratch(t, "race")
	var wg2 sync.WaitGroup
	errs2 := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg2.Add(1)
		go func() { defer wg2.Done(); _, err := Open(context.Background(), db2, schema2); errs2 <- err }()
	}
	wg2.Wait()
	close(errs2)
	for err := range errs2 {
		if err != nil {
			t.Errorf("racing Open() = %v", err)
		}
	}
}

// TestMySQLRefusesOtherSchemas is R37: a 2.2 label, a marker-less 2.x archiver_job with or
// without rows, an unknown label, and a 3.0 label whose archiver_job was dropped are each
// refused before anything is written.
func TestMySQLRefusesOtherSchemas(t *testing.T) {
	const job2x = "CREATE TABLE archiver_job (id BIGINT AUTO_INCREMENT PRIMARY KEY, job_name VARCHAR(255) NOT NULL, " +
		"root_table VARCHAR(255) NOT NULL, job_type VARCHAR(32) NOT NULL DEFAULT 'archive', last_processed_root_pk_id VARCHAR(255), " +
		"job_status TINYINT NOT NULL DEFAULT 0, UNIQUE KEY uk_job_name (job_name)) ENGINE=InnoDB"
	const meta = "CREATE TABLE goarchive_meta (id TINYINT NOT NULL PRIMARY KEY, schema_version VARCHAR(16) NOT NULL) ENGINE=InnoDB"
	for _, tc := range []struct {
		name  string
		setup []string
		found string
	}{
		{"label22", []string{job2x, meta, "INSERT INTO goarchive_meta VALUES (1, '2.2')"}, `report schema_version "2.2"`},
		{"unknown", []string{meta, "INSERT INTO goarchive_meta VALUES (1, '3.1')"}, `report schema_version "3.1"`},
		{"bare2x", []string{job2x}, "hold a 2.x archiver_job table without a schema_version label"},
		{"rows2x", []string{job2x, "INSERT INTO archiver_job (job_name, root_table) VALUES ('j', 't')"},
			"hold a 2.x archiver_job table without a schema_version label"},
		{"nojob", []string{meta, "INSERT INTO goarchive_meta VALUES (1, '3.0')"}, "are labelled 3.0 but have no archiver_job table"},
		// The DBA's shortcut: relabel a 2.x schema instead of finishing and dropping it.
		{"label30on2x", []string{job2x, meta, "INSERT INTO goarchive_meta VALUES (1, '3.0')"},
			`hold a 2.x archiver_job table under schema_version "3.0"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema, db := scratch(t, tc.name)
			exec(t, db, tc.setup...)
			_, err := Open(context.Background(), db, schema)
			var se *SchemaError
			if !errors.As(err, &se) || !strings.Contains(err.Error(), tc.found) {
				t.Fatalf("Open() = %v, want a SchemaError saying %q", err, tc.found)
			}
			if n := countTables(t, db); n != strings.Count(strings.Join(tc.setup, ";"), "CREATE TABLE") {
				t.Errorf("tables after the refusal = %d: Open wrote to a refused schema", n)
			}
		})
	}

	// A 3.0 archiver_job without a label and without rows is the crash between the
	// two CREATEs of a first run, and is stamped.
	schema, db := scratch(t, "half")
	s := mustOpen(t, db, schema)
	exec(t, db, "DELETE FROM goarchive_meta")
	if _, err := Open(context.Background(), db, s.schema); err != nil {
		t.Fatalf("Open() on an unlabelled empty 3.0 schema = %v", err)
	}
	exec(t, db, "DELETE FROM goarchive_meta", "INSERT INTO archiver_job (job_name, root_table, job_type, source_server_uuid, "+
		"source_database, destination_database, config_fingerprint, root_key_columns) VALUES ('j','t','archive','u','s','d','{}','[]')")
	if _, err := Open(context.Background(), db, schema); err == nil || !strings.Contains(err.Error(), "hold job rows but no schema_version label") {
		t.Fatalf("Open() on an unlabelled 3.0 schema with rows = %v", err)
	}
}

// TestMySQLTypedLog: the log mirrors a composite root key of a utf8mb4_0900_ai_ci
// VARCHAR, a TIMESTAMP(3) and an INT UNSIGNED as k1…k3, the TIMESTAMP stored as
// DATETIME(3), with each source name in the column COMMENT; root_key_columns holds the
// names; and statuses are written only when the caller's transaction commits.
func TestMySQLTypedLog(t *testing.T) {
	ctx := context.Background()
	schema, db := scratch(t, "typed")
	exec(t, db, ordersDDL)
	root := rootKey(t, db, schema, "orders")
	s := mustOpen(t, db, schema)
	j, err := s.Attach(ctx, JobInput{Name: "orders_archive", Type: jobspec.Archive, RootKey: root, Binding: binding("1=1")})
	if err != nil {
		t.Fatalf("Attach() = %v", err)
	}
	var ddl, ignored string
	if err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE archiver_job_log_%d", j.ID)).Scan(&ignored, &ddl); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"`k1` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT 'region'",
		"`k2` datetime(3) NOT NULL COMMENT 'placed'",
		"`k3` int unsigned NOT NULL COMMENT 'id'",
		"PRIMARY KEY (`k1`,`k2`,`k3`)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("log DDL lacks %q:\n%s", want, ddl)
		}
	}
	var cols string
	if err := db.QueryRow("SELECT root_key_columns FROM archiver_job WHERE id = ?", j.ID).Scan(&cols); err != nil || cols != `["region","placed","id"]` {
		t.Errorf("root_key_columns = %q, %v", cols, err)
	}

	keys := []key.Key{
		{[]byte("eu"), "2024-03-31 01:30:00.125", uint64(1)},
		{[]byte("eu"), "2024-03-31 02:30:00.000", uint64(2)},
		{[]byte("us"), "2024-10-27 01:30:00.999", uint64(3)},
	}
	// Rolled back: nothing is logged.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := inTx(t, db, false, func(tx *sql.Tx) error { return j.Insert(ctx, tx, StatusCopied, keys) }); err != nil {
		t.Fatal(err)
	}
	if got, err := j.Roots(ctx, conn, StatusCopied, nil, 10); err != nil || len(got) != 0 {
		t.Fatalf("after a rollback, Roots = %v, %v", got, err)
	}
	if err := inTx(t, db, true, func(tx *sql.Tx) error { return j.Insert(ctx, tx, StatusCopied, keys) }); err != nil {
		t.Fatal(err)
	}
	// Read back exactly, in key order, across pages, on a server whose global zone is not UTC.
	first, err := j.Roots(ctx, conn, StatusCopied, nil, 2)
	if err != nil || len(first) != 2 {
		t.Fatalf("Roots page 1 = %v, %v", first, err)
	}
	rest, err := j.Roots(ctx, conn, StatusCopied, first[1], 2)
	if err != nil || len(rest) != 1 {
		t.Fatalf("Roots page 2 = %v, %v", rest, err)
	}
	for i, got := range append(first, rest...) {
		if key.Encode(got) != key.Encode(keys[i]) {
			t.Errorf("root %d = %v, want %v", i, got, keys[i])
		}
	}
	// The log stores DATETIME, so a session in another zone reads the same text; a
	// TIMESTAMP column would shift by the session's offset.
	other := openDest(t, schema)
	other.SetMaxOpenConns(1)
	exec(t, other, "SET time_zone = '+03:00'")
	var text string
	if err := other.QueryRow(fmt.Sprintf("SELECT CAST(k2 AS CHAR) FROM archiver_job_log_%d WHERE k3 = 1", j.ID)).Scan(&text); err != nil ||
		text != "2024-03-31 01:30:00.125" {
		t.Errorf("k2 read at +03:00 = %q, %v", text, err)
	}
	// A duplicate fails loudly (R25).
	if err := inTx(t, db, true, func(tx *sql.Tx) error { return j.Insert(ctx, tx, StatusCopied, keys[:1]) }); err == nil {
		t.Error("a duplicate key was logged twice")
	}
	// Transition needs every key in the from status; one in another status fails it.
	if err := inTx(t, db, true, func(tx *sql.Tx) error {
		return j.Transition(ctx, tx, StatusCopied, StatusCompleted, keys[:2])
	}); err != nil {
		t.Fatalf("Transition() = %v", err)
	}
	err = inTx(t, db, true, func(tx *sql.Tx) error { return j.Transition(ctx, tx, StatusCopied, StatusCompleted, keys) })
	if err == nil || !strings.Contains(err.Error(), "1 of 3 root(s) were in status copied") {
		t.Fatalf("Transition() over a mixed set = %v", err)
	}
	got, err := j.Logged(ctx, conn, StatusCompleted, []key.Key{keys[0], keys[2], {[]byte("EU"), "2024-03-31 02:30:00.000", uint64(2)}})
	if err != nil {
		t.Fatal(err)
	}
	// keys[2] is still copied; "EU" equals "eu" under the root's _ai_ci collation.
	enc := make([]string, len(got))
	for i, k := range got {
		enc[i] = key.Encode(k)
	}
	slices.Sort(enc)
	want := []string{key.Encode(keys[0]), key.Encode(keys[1])}
	slices.Sort(want)
	if !slices.Equal(enc, want) {
		t.Errorf("Logged = %v, want keys 0 and 1", got)
	}
}

// TestMySQLLogKeyCheck: a reattach with the same key passes; a reordered key, a widened
// key column and a changed collation are each refused by the log-key check (spec §6.2).
func TestMySQLLogKeyCheck(t *testing.T) {
	ctx := context.Background()
	schema, db := scratch(t, "logkey")
	exec(t, db, ordersDDL)
	root := rootKey(t, db, schema, "orders")
	s := mustOpen(t, db, schema)
	in := JobInput{Name: "j", Type: jobspec.Archive, RootKey: root, Binding: binding("1=1")}
	if _, err := s.Attach(ctx, in); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Attach(ctx, in); err != nil {
		t.Fatalf("reattach = %v", err)
	}
	for _, tc := range []struct {
		name, alter, reason string
	}{
		{"reordered", "ALTER TABLE orders DROP PRIMARY KEY, ADD PRIMARY KEY (placed, region, id)", "it was (region, placed, id) and is now (placed, region, id)"},
		{"widened", "ALTER TABLE orders MODIFY id BIGINT UNSIGNED NOT NULL", "key column id is bigint unsigned, and the log stores int unsigned"},
		{"collation", "ALTER TABLE orders MODIFY region VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
			"key column region is utf8mb4 utf8mb4_bin, and the log stores utf8mb4 utf8mb4_0900_ai_ci"},
		{"timestamp precision", "ALTER TABLE orders MODIFY placed TIMESTAMP(6) NOT NULL", "key column placed is timestamp(6), and the log stores datetime(3)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec(t, db, "DROP TABLE orders", ordersDDL, tc.alter)
			in := in
			in.RootKey = rootKey(t, db, schema, "orders")
			_, err := s.Attach(ctx, in)
			var le *LogKeyError
			if !errors.As(err, &le) || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("Attach() = %v, want a LogKeyError saying %q", err, tc.reason)
			}
		})
	}
}

// TestMySQLBinding: with pending work, a changed compared part is refused with the count
// of pending roots; with none, the binding is overwritten. Copy-only's completed roots
// pin it whatever the resume setting (spec §6.2, R25, R27 B1).
func TestMySQLBinding(t *testing.T) {
	ctx := context.Background()
	schema, db := scratch(t, "bind")
	exec(t, db, ordersDDL)
	root := rootKey(t, db, schema, "orders")
	s := mustOpen(t, db, schema)
	k := key.Key{[]byte("eu"), "2024-01-01 00:00:00.000", uint64(1)}
	stored := func(name string) Binding {
		var b Binding
		var fp string
		if err := db.QueryRow("SELECT source_server_uuid, source_database, destination_database, config_fingerprint FROM archiver_job WHERE job_name = ?", name).
			Scan(&b.SourceServerUUID, &b.SourceDatabase, &b.DestinationDatabase, &fp); err != nil {
			t.Fatal(err)
		}
		f, err := parseFingerprint(fp)
		if err != nil {
			t.Fatal(err)
		}
		b.Fingerprint = f
		return b
	}
	for _, tc := range []struct {
		typ     jobspec.Type
		status  Status
		change  func(b *Binding)
		refused bool
	}{
		{jobspec.Archive, StatusCopied, func(b *Binding) { b.SourceServerUUID = "22222222-2222-2222-2222-222222222222" }, true},
		{jobspec.Archive, StatusCopied, func(b *Binding) { b.Fingerprint.Where = "id > 5" }, false},
		{jobspec.Archive, StatusCompleted, func(b *Binding) { b.SourceDatabase = "other" }, false},
		{jobspec.Purge, StatusDeleting, func(b *Binding) { b.Fingerprint.Where = "id > 5" }, true},
		{jobspec.Purge, StatusDeleting, func(b *Binding) { b.DestinationDatabase = "other" }, false},
		{jobspec.CopyOnly, StatusCompleted, func(b *Binding) { b.DestinationDatabase = "other" }, true},
		{jobspec.CopyOnly, StatusCompleted, func(b *Binding) {
			b.Fingerprint.Relations = []Edge{{Parent: "orders", Table: "lines", Pairs: [][2]string{{"order_id", "id"}}}}
		}, true},
		{jobspec.CopyOnly, StatusCompleted, func(b *Binding) { b.Fingerprint.Where = "id > 5" }, false},
	} {
		name := fmt.Sprintf("%s_%s_%v", tc.typ, tc.status, tc.refused)
		t.Run(name, func(t *testing.T) {
			name := strings.NewReplacer("-", "_").Replace(name) + strconv.Itoa(len(t.Name()))
			in := JobInput{Name: name, Type: tc.typ, RootKey: root, Binding: binding("1=1")}
			j, err := s.Attach(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			if err := inTx(t, db, true, func(tx *sql.Tx) error { return j.Insert(ctx, tx, tc.status, []key.Key{k}) }); err != nil {
				t.Fatal(err)
			}
			tc.change(&in.Binding)
			_, err = s.Attach(ctx, in)
			var be *BindingError
			switch {
			case tc.refused && (!errors.As(err, &be) || be.Pending != 1):
				t.Fatalf("Attach() = %v, want a BindingError with 1 pending root", err)
			case tc.refused && !sameBinding(stored(name), binding("1=1")):
				t.Error("a refused run rebound the job")
			case !tc.refused && err != nil:
				t.Fatalf("Attach() = %v, want a rebind", err)
			case !tc.refused && !sameBinding(stored(name), in.Binding):
				t.Errorf("binding = %+v, want %+v", stored(name), in.Binding)
			}
		})
	}
}

// TestMySQLJobIsFixed: a job's type and root table are fixed, and a name differing only
// in letter case is refused; status and heartbeat are written.
func TestMySQLJobIsFixed(t *testing.T) {
	ctx := context.Background()
	schema, db := scratch(t, "fixed")
	exec(t, db, ordersDDL, "CREATE TABLE other (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB")
	root := rootKey(t, db, schema, "orders")
	s := mustOpen(t, db, schema)
	in := JobInput{Name: "Orders", Type: jobspec.Archive, RootKey: root, Binding: binding("1=1")}
	j, err := s.Attach(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		edit func(in *JobInput)
		want string
	}{
		{func(in *JobInput) { in.Type = jobspec.Purge }, "exists with job_type archive"},
		{func(in *JobInput) { in.RootKey = rootKey(t, db, schema, "other") }, `exists for root table "orders"`},
		{func(in *JobInput) { in.Name = "orders" }, "differs only in letter case"},
	} {
		in := in
		tc.edit(&in)
		if _, err := s.Attach(ctx, in); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("Attach() = %v, want %q", err, tc.want)
		}
	}
	if _, ok, err := s.HeartbeatAge(ctx, "Orders"); err != nil || ok {
		t.Errorf("HeartbeatAge before a beat = %v, %v", ok, err)
	}
	if err := j.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if age, ok, err := s.HeartbeatAge(ctx, "Orders"); err != nil || !ok || age > 5e9 || age < 0 {
		t.Errorf("HeartbeatAge after a beat = %v, %v, %v", age, ok, err)
	}
	if err := j.SetStatus(ctx, JobRunning); err != nil {
		t.Fatal(err)
	}
	var st int
	if err := db.QueryRow("SELECT job_status FROM archiver_job WHERE id = ?", j.ID).Scan(&st); err != nil || st != int(JobRunning) {
		t.Errorf("job_status = %d, %v", st, err)
	}
}

// TestMySQLLeftoverLog: a log left by a dropped or truncated registry is never adopted,
// by the run that meets it or by any retry, under the documented grants alone (no
// DELETE). A registration that a crash interrupted completes on the next run.
func TestMySQLLeftoverLog(t *testing.T) {
	ctx := context.Background()
	schema, admin := scratch(t, "leftover")
	exec(t, admin, ordersDDL)
	root := rootKey(t, admin, schema, "orders")
	db := leastPrivilege(t, admin, schema)
	s := mustOpen(t, db, schema)
	in := JobInput{Name: "orders", Type: jobspec.Archive, RootKey: root, Binding: binding("1=1")}
	logOf := func(id int64) string { _, q := s.logTable(id); return q }
	typedLog := func(id int64) string {
		ddl, err := createLogSQL(logOf(id), root)
		if err != nil {
			t.Fatal(err)
		}
		return ddl
	}
	const remedy = "drop it after confirming no job uses it"

	// The next job's id is 1. Its log is already there, typed as this job's would be,
	// holding a copied root.
	exec(t, admin, typedLog(1), fmt.Sprintf("INSERT INTO %s (k1, k2, k3, ga_status) VALUES ('eu', '2026-01-01 00:00:00.000', 42, %d)",
		logOf(1), StatusCopied))
	for attempt := 1; attempt <= 3; attempt++ {
		if j, err := s.Attach(ctx, in); err == nil || j != nil || !strings.Contains(err.Error(), remedy) {
			t.Fatalf("attempt %d: Attach() = %v, %v; want the leftover-log refusal", attempt, j, err)
		}
	}
	keyColumns := func(name string) string {
		var text string
		if err := admin.QueryRow("SELECT root_key_columns FROM archiver_job WHERE job_name = ?", name).Scan(&text); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return text
	}
	if got := keyColumns("orders"); got != "" {
		t.Errorf("a refused registration stored root_key_columns %q", got)
	}
	// The DBA drops it; the next run completes the registration with a new, empty log.
	exec(t, admin, "DROP TABLE "+logOf(1))
	j, err := s.Attach(ctx, in)
	if err != nil {
		t.Fatalf("Attach() after the leftover log was dropped = %v", err)
	}
	if roots, err := j.Roots(ctx, db, StatusCopied, nil, 10); err != nil || len(roots) != 0 || j.ID != 1 {
		t.Errorf("job %d: Roots() = %v, %v; want none", j.ID, roots, err)
	}
	if got, want := keyColumns("orders"), encodeKeyColumns(root.Names()); got != want {
		t.Errorf("root_key_columns = %q, want %q", got, want)
	}

	// A crash after the registry INSERT leaves a row with no key names, and either no log
	// (id 2) or the empty log it had just created (id 3). Both complete on the next run.
	exec(t, admin, typedLog(3))
	for _, name := range []string{"crashed_before_create", "crashed_after_create"} {
		exec(t, admin, fmt.Sprintf("INSERT INTO archiver_job (job_name, root_table, job_type, job_status, source_server_uuid, "+
			"source_database, destination_database, config_fingerprint, root_key_columns) VALUES ('%s', 'orders', 'archive', 0, "+
			"'%s', 'src', 'dst', '%s', '')", name, in.Binding.SourceServerUUID, in.Binding.Fingerprint.Text()))
		in := in
		in.Name = name
		if _, err := s.Attach(ctx, in); err != nil {
			t.Errorf("%s: Attach() = %v", name, err)
		} else if got := keyColumns(name); got != encodeKeyColumns(root.Names()) {
			t.Errorf("%s: root_key_columns = %q", name, got)
		}
	}

	// An empty leftover log of another shape is refused the same way, not as a changed key.
	exec(t, admin, "CREATE TABLE "+logOf(4)+" (k1 INT NOT NULL PRIMARY KEY)")
	in.Name = "other_shape"
	var lk *LogKeyError
	if _, err := s.Attach(ctx, in); err == nil || errors.As(err, &lk) || !strings.Contains(err.Error(), remedy) {
		t.Errorf("a leftover log of another shape: %v", err)
	}
}

// TestMySQLChunkedStatements: a batch larger than one statement's cap is split, every
// chunk lands, and a key missing from a later chunk fails the transition.
func TestMySQLChunkedStatements(t *testing.T) {
	ctx := context.Background()
	schema, db := scratch(t, "chunks")
	exec(t, db, "CREATE TABLE t (a INT NOT NULL, b VARCHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, PRIMARY KEY (a, b)) ENGINE=InnoDB")
	s := mustOpen(t, db, schema)
	j, err := s.Attach(ctx, JobInput{Name: "chunks", Type: jobspec.Purge, RootKey: rootKey(t, db, schema, "t"), Binding: binding("1=1")})
	if err != nil {
		t.Fatal(err)
	}
	j.maxTuples, j.insertMax = 2, 2
	var keys []key.Key
	for i := 1; i <= 5; i++ {
		keys = append(keys, key.Key{int64(i), []byte("x")})
	}
	if err := inTx(t, db, true, func(tx *sql.Tx) error { return j.Insert(ctx, tx, StatusDeleting, keys) }); err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if got, err := j.Logged(ctx, conn, StatusDeleting, keys); err != nil || len(got) != 5 {
		t.Fatalf("Logged = %d keys, %v", len(got), err)
	}
	missing := append(slices.Clone(keys[:4]), key.Key{int64(9), []byte("x")})
	err = inTx(t, db, true, func(tx *sql.Tx) error { return j.Transition(ctx, tx, StatusDeleting, StatusCompleted, missing) })
	if err == nil || !strings.Contains(err.Error(), "0 of 1 root(s)") {
		t.Fatalf("Transition() with a missing key in the last chunk = %v", err)
	}
	if got, err := j.Logged(ctx, conn, StatusDeleting, keys); err != nil || len(got) != 5 {
		t.Errorf("after the rolled-back transition, deleting = %d keys, %v", len(got), err)
	}
}

// TestMySQLRangeWarning: a membership statement over the log that exceeds the session's
// range budget falls back to a full scan, and Transition and Logged report warning 3170
// (spec §4.3, §7.1a) instead of returning as if nothing happened.
func TestMySQLRangeWarning(t *testing.T) {
	ctx := context.Background()
	schema, db := scratch(t, "range")
	exec(t, db, "CREATE TABLE r (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) ENGINE=InnoDB")
	s := mustOpen(t, db, schema)
	j, err := s.Attach(ctx, JobInput{Name: "range", Type: jobspec.Purge, RootKey: rootKey(t, db, schema, "r"), Binding: binding("1=1")})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]key.Key, 10000)
	for i := range keys {
		keys[i] = key.Key{int64(i / 100), int64(i % 100)}
	}
	if err := inTx(t, db, true, func(tx *sql.Tx) error { return j.Insert(ctx, tx, StatusDeleting, keys) }); err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET SESSION range_optimizer_max_mem_size = 1048576"); err != nil {
		t.Fatal(err)
	}
	j.maxTuples = len(keys) // one statement far over the 1 MiB budget
	if _, err := j.Logged(ctx, conn, StatusDeleting, keys); !errors.Is(err, key.ErrRangeScan) {
		t.Errorf("Logged over the budget = %v, want ErrRangeScan", err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := j.Transition(ctx, tx, StatusDeleting, StatusCompleted, keys); !errors.Is(err, key.ErrRangeScan) {
		t.Errorf("Transition over the budget = %v, want ErrRangeScan", err)
	}
}
