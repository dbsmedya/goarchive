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

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
)

// These tests run the tracking store on TEST_DEST_* (db2), whose global time_zone is not
// UTC. Each test creates and drops its own ga3m4_<name>_<pid> schema, which serves as both
// the job_schema and the schema of the root table whose key the log mirrors.

// backtickQuoted returns server text (SHOW CREATE TABLE, CURRENT_ROLE()) with identifiers
// quoted as the default sql_mode quotes them: under ANSI_QUOTES MySQL writes "name", which
// the environment matrix's sql_mode_ansi_quotes row sets globally (M7-8). goarchive's own
// SQL runs under it unchanged; only these assertions compare the server's text.
func backtickQuoted(t *testing.T, db *sql.DB, s string) string {
	t.Helper()
	var mode string
	if err := db.QueryRowContext(context.Background(), "SELECT @@SESSION.sql_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(mode, "ANSI_QUOTES") {
		return s
	}
	return strings.ReplaceAll(s, `"`, "`")
}

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
	cfg := config.DatabaseConfig{Host: host, Port: port, User: user, Password: pass, Database: schema, TLS: "disable"}
	// goarchive's own pool (database.OpenPool); the cap is lifted, as the suites race more
	// sessions than a run.
	db, err := database.OpenPool(t.Context(), &cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(0)
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

// schemaLockName is the schema lock's name by the SQL formula the docs publish
// (README_OPERATIONS.md), so these tests also pin the formula against Open's lock.
func schemaLockName(t *testing.T, db *sql.DB, schema string) string {
	t.Helper()
	var name string
	if err := db.QueryRowContext(context.Background(),
		"SELECT CONCAT('goarchive:s:', LEFT(SHA2(CONCAT(LOWER(?), CHAR(0), LOWER('')), 256), 32))", schema).Scan(&name); err != nil {
		t.Fatal(err)
	}
	return name
}

// holdLock takes name on a session of its own and returns that session's connection id and
// a release, which the test's cleanup also runs.
func holdLock(t *testing.T, db *sql.DB, name string) (int64, func()) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil || got.Int64 != 1 {
		t.Fatalf("GET_LOCK(%s) = %v, %v", name, got, err)
	}
	var id int64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	release := sync.OnceFunc(func() {
		_, _ = conn.ExecContext(ctx, "DO RELEASE_LOCK(?)", name)
		_ = conn.Close()
	})
	t.Cleanup(release)
	return id, release
}

// lockHolder returns the connection id holding name, or 0 when it is free.
func lockHolder(t *testing.T, db *sql.DB, name string) int64 {
	t.Helper()
	var id sql.NullInt64
	if err := db.QueryRowContext(context.Background(), "SELECT IS_USED_LOCK(?)", name).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id.Int64
}

// lockPending reports whether a session waits for name (Fable's probe B: the waiter's
// USER LEVEL LOCK row is PENDING while it waits, and gone when it is gone).
func lockPending(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM performance_schema.metadata_locks "+
		"WHERE OBJECT_TYPE = 'USER LEVEL LOCK' AND OBJECT_NAME = ? AND LOCK_STATUS = 'PENDING'", name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

// finishingFacts is the library's facts with one interleaving forced: the first Tables
// call returns the list, then lets another run finish creating the schema before it
// returns (Fable F1).
type finishingFacts struct {
	facts
	finish func()
	once   sync.Once
}

func (f *finishingFacts) Tables(ctx context.Context, tables []string) ([]validations.TableInfo, error) {
	infos, err := f.facts.Tables(ctx, tables)
	f.once.Do(f.finish)
	return infos, err
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

	// Sixteen runs stamping one new schema at once.
	schema2, db2 := scratch(t, "race")
	var wg2 sync.WaitGroup
	errs2 := make(chan error, 16)
	for i := 0; i < 16; i++ {
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

// TestMySQLSchemaCreationWaitsForTheLock (TRK-1): while another session holds the schema
// lock, Open on a new schema waits for it and creates nothing; once it is released, Open
// creates and labels the schema and leaves the lock free. Open runs on a pool of two
// sessions: the lock's and one working session are all it takes (spec §3.3).
func TestMySQLSchemaCreationWaitsForTheLock(t *testing.T) {
	schema, db := scratch(t, "lockwait")
	name := schemaLockName(t, db, schema)
	_, release := holdLock(t, db, name)
	two := openDest(t, schema)
	two.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := Open(ctx, two, schema); done <- err }()

	deadline := time.Now().Add(5 * time.Second)
	for !lockPending(t, db, name) {
		select {
		case err := <-done:
			t.Fatalf("Open() = %v while another session held the schema lock; want it to wait", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Open did not wait for the schema lock within 5 s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := countTables(t, db); n != 0 {
		t.Fatalf("tables while waiting = %d: Open created the schema without the lock", n)
	}

	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Open() after the release = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Open did not return within 5 s of the release")
	}
	var label string
	if err := db.QueryRow("SELECT schema_version FROM goarchive_meta WHERE id = 1").Scan(&label); err != nil || label != "3.0" {
		t.Fatalf("label = %q, %v", label, err)
	}
	if h := lockHolder(t, db, name); h != 0 {
		t.Errorf("the schema lock is held by connection %d after Open returned", h)
	}
}

// TestMySQLSchemaCreationLockTimeout (TRK-1): a schema lock held for the whole wait ends
// Open with SchemaLockedError naming the holder, and nothing is created. initLockWait is
// shortened to 1 s and restored; this package's tests do not run in parallel.
func TestMySQLSchemaCreationLockTimeout(t *testing.T) {
	defer func(w time.Duration) { initLockWait = w }(initLockWait)
	initLockWait = time.Second
	schema, db := scratch(t, "locktimeout")
	name := schemaLockName(t, db, schema)
	id, _ := holdLock(t, db, name)
	start := time.Now()
	_, err := Open(context.Background(), db, schema)
	var locked *SchemaLockedError
	var held *lock.HeldError
	if !errors.As(err, &locked) || !errors.As(err, &held) || locked.Schema != schema || held.Holder != id || held.Name != name {
		t.Fatalf("Open() = %v, want a SchemaLockedError on %s held by connection %d", err, name, id)
	}
	if d := time.Since(start); d < time.Second {
		t.Errorf("Open() refused after %s, before the 1 s wait ran out", d)
	}
	if n := countTables(t, db); n != 0 {
		t.Errorf("tables after the refusal = %d", n)
	}
	if h := lockHolder(t, db, name); h != id {
		t.Errorf("the schema lock is held by %d, want the test's %d", h, id)
	}
}

// TestMySQLSchemaCreationMeetsAFinishingRun (TRK-1, Fable F1): a run that reads a new
// schema while another run finishes creating it starts. Its first table list shows
// archiver_job alone; the other run then creates goarchive_meta, stamps 3.0 and registers a
// job. Judged before the lock, that read was "job rows but no schema_version label".
func TestMySQLSchemaCreationMeetsAFinishingRun(t *testing.T) {
	schema, db := scratch(t, "finishing")
	s := mustOpen(t, db, schema)
	exec(t, db, "DROP TABLE goarchive_meta")
	f := &finishingFacts{facts: inspectorFacts{schema: schema, insp: validations.NewInspector(db, schema)}, finish: func() {
		exec(t, db, s.createMetaTableSQL(), "INSERT INTO goarchive_meta (id, schema_version) VALUES (1, '3.0')",
			"INSERT INTO archiver_job (job_name, root_table, job_type, source_server_uuid, source_database, "+
				"destination_database, config_fingerprint, root_key_columns) VALUES ('j','t','archive','u','s','d','{}','[]')")
	}}
	if _, err := open(context.Background(), db, schema, f); err != nil {
		t.Fatalf("Open() while another run finished the schema = %v", err)
	}
	var label string
	if err := db.QueryRow("SELECT schema_version FROM goarchive_meta WHERE id = 1").Scan(&label); err != nil || label != "3.0" {
		t.Fatalf("label = %q, %v", label, err)
	}
}

// TestMySQLLabelledSchemaTakesNoLock (TRK-1, goal 2): a labelled schema is opened at once
// while another session holds its schema lock. A guard: it passes before the change.
func TestMySQLLabelledSchemaTakesNoLock(t *testing.T) {
	schema, db := scratch(t, "labelled")
	mustOpen(t, db, schema)
	holdLock(t, db, schemaLockName(t, db, schema))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := Open(ctx, db, schema); err != nil {
		t.Fatalf("Open() on a labelled schema = %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("Open() on a labelled schema took %s: it waited for the schema lock", d)
	}
}

// TestMySQLDestinationServerUUIDColumn (pre-alpha minors item 3): archiver_job carries
// destination_server_uuid, reserved for GH #14's destination binding so the 3.0 label needs
// no bump when that lands. alpha.0 neither writes nor compares it: a registered job holds
// its default, the empty string.
func TestMySQLDestinationServerUUIDColumn(t *testing.T) {
	schema, db := scratch(t, "dstuuid")
	mustOpen(t, db, schema)
	spec, err := validations.NewInspector(db, schema).TableSpec(context.Background(), validations.Ref(schema, "archiver_job"))
	if err != nil {
		t.Fatal(err)
	}
	var col *validations.ColumnSpec
	var after string
	for i := range spec.Columns {
		if spec.Columns[i].Name == "destination_server_uuid" {
			col = &spec.Columns[i]
			if i > 0 {
				after = spec.Columns[i-1].Name
			}
		}
	}
	if col == nil {
		t.Fatal("archiver_job has no destination_server_uuid column")
	}
	if col.Type != "varchar(64)" || col.Charset != "ascii" || col.Collation != "ascii_bin" || col.Nullable ||
		col.Default == nil || *col.Default != "" || after != "destination_database" {
		t.Errorf("destination_server_uuid = %s %s %s nullable=%v default=%v after %q; want varchar(64) ascii ascii_bin NOT NULL DEFAULT '' after destination_database",
			col.Type, col.Charset, col.Collation, col.Nullable, col.Default, after)
	}
	exec(t, db, "INSERT INTO archiver_job (job_name, root_table, job_type, source_server_uuid, source_database, "+
		"destination_database, config_fingerprint, root_key_columns) VALUES ('j','t','archive','u','s','d','{}','[]')")
	var got string
	if err := db.QueryRow("SELECT destination_server_uuid FROM archiver_job WHERE job_name = 'j'").Scan(&got); err != nil || got != "" {
		t.Errorf("a row inserted without it holds %q, %v; want ''", got, err)
	}
	if _, err := Open(context.Background(), db, schema); err != nil {
		t.Errorf("Open() on its own schema = %v", err)
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
	// A 3.0 archiver_job as built before destination_server_uuid (pre-alpha minors item 3).
	const job30pre = "CREATE TABLE archiver_job (id BIGINT AUTO_INCREMENT PRIMARY KEY, job_name VARCHAR(255) NOT NULL, " +
		"root_table VARCHAR(64) NOT NULL, job_type VARCHAR(16) NOT NULL, job_status TINYINT NOT NULL DEFAULT 0, " +
		"source_server_uuid VARCHAR(64) NOT NULL, source_database VARCHAR(64) NOT NULL, destination_database VARCHAR(64) NOT NULL, " +
		"config_fingerprint TEXT NOT NULL, root_key_columns TEXT NOT NULL, UNIQUE KEY uk_job_name (job_name)) ENGINE=InnoDB"
	for _, tc := range []struct {
		name   string
		setup  []string
		found  string
		remedy string // "" checks no remedy
	}{
		{"label22", []string{job2x, meta, "INSERT INTO goarchive_meta VALUES (1, '2.2')"}, `report schema_version "2.2"`,
			"Finish every 2.x job with the 2.x binary"},
		// SLR-1: a label 2.x never wrote is not given the 2.x drop.
		{"unknown", []string{meta, "INSERT INTO goarchive_meta VALUES (1, '3.1')"}, `report schema_version "3.1"`,
			"an earlier or a later GoArchive release"},
		{"bare2x", []string{job2x}, "hold a 2.x archiver_job table without a schema_version label", ""},
		{"rows2x", []string{job2x, "INSERT INTO archiver_job (job_name, root_table) VALUES ('j', 't')"},
			"hold a 2.x archiver_job table without a schema_version label", ""},
		{"nojob", []string{meta, "INSERT INTO goarchive_meta VALUES (1, '3.0')"}, "are labelled 3.0 but have no archiver_job table", ""},
		// The DBA's shortcut: relabel a 2.x schema instead of finishing and dropping it.
		{"label30on2x", []string{job2x, meta, "INSERT INTO goarchive_meta VALUES (1, '3.0')"},
			`hold a 2.x archiver_job table under schema_version "3.0"`, ""},
		// A schema a pre-alpha v3 build created: labelled 3.0, no destination_server_uuid.
		{"label30pre", []string{job30pre, meta, "INSERT INTO goarchive_meta VALUES (1, '3.0')"},
			`under schema_version "3.0"`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema, db := scratch(t, tc.name)
			exec(t, db, tc.setup...)
			_, err := Open(context.Background(), db, schema)
			var se *SchemaError
			if !errors.As(err, &se) || !strings.Contains(err.Error(), tc.found) {
				t.Fatalf("Open() = %v, want a SchemaError saying %q", err, tc.found)
			}
			if !strings.Contains(err.Error(), tc.remedy) {
				t.Errorf("Open() = %v, want the remedy %q", err, tc.remedy)
			}
			if n := countTables(t, db); n != strings.Count(strings.Join(tc.setup, ";"), "CREATE TABLE") {
				t.Errorf("tables after the refusal = %d: Open wrote to a refused schema", n)
			}
			if h := lockHolder(t, db, schemaLockName(t, db, schema)); h != 0 {
				t.Errorf("the schema lock is held by connection %d after the refusal", h)
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
	ddl = backtickQuoted(t, db, ddl)
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
	// A registered job's log that is no longer the table GoArchive created is refused.
	exec(t, db, "ALTER TABLE archiver_job_log_1 ENGINE=MyISAM")
	if _, err := s.Attach(ctx, in); err == nil || !strings.Contains(err.Error(), "is not the table GoArchive created: its engine is MyISAM, not InnoDB") {
		t.Errorf("a MyISAM log: %v", err)
	}
	exec(t, db, "ALTER TABLE archiver_job_log_1 ENGINE=InnoDB")
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
	if err := j.Heartbeat(ctx); err != nil {
		t.Fatal(err)
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

	// An empty leftover log of another shape is refused the same way, not as a changed
	// key, and so is one whose writes would not roll back or whose key would not be
	// strict. The row stays unregistered across retries.
	exec(t, admin, "CREATE TABLE "+logOf(4)+" (k1 INT NOT NULL PRIMARY KEY)",
		strings.Replace(typedLog(5), "ENGINE=InnoDB", "ENGINE=MyISAM", 1),
		typedLog(7), "ALTER TABLE "+logOf(7)+" DROP COLUMN ga_updated_at")
	// The keyless leftover is built on one session that allows it: a server running with
	// sql_require_primary_key or sql_generate_invisible_primary_key refuses the DROP
	// (M7c environment matrix), and this table is what such a server would never hold.
	keyless, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"SET SESSION sql_require_primary_key = 0", "SET SESSION sql_generate_invisible_primary_key = 0",
		typedLog(6), "ALTER TABLE " + logOf(6) + " DROP PRIMARY KEY"} {
		if _, err := keyless.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	_ = keyless.Close()
	for _, tc := range []struct{ name, reason string }{
		{"other_shape", "has 1 key columns and the root key has 3"},
		{"myisam", "its engine is MyISAM, not InnoDB"},
		{"no_primary_key", "its primary key is not (k1, k2, k3)"},
		{"no_updated_at", "column ga_updated_at is missing"},
	} {
		in := in
		in.Name = tc.name
		var lk *LogKeyError
		for attempt := 1; attempt <= 2; attempt++ {
			j, err := s.Attach(ctx, in)
			if err == nil || j != nil || errors.As(err, &lk) || !strings.Contains(err.Error(), remedy) || !strings.Contains(err.Error(), tc.reason) {
				t.Errorf("%s, attempt %d: Attach() = %v, %v; want the leftover-log refusal naming %q", tc.name, attempt, j, err, tc.reason)
			}
		}
		if got := keyColumns(tc.name); got != "" {
			t.Errorf("%s: a refused registration stored root_key_columns %q", tc.name, got)
		}
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
	root := rootKey(t, db, schema, "r")
	j, err := s.Attach(ctx, JobInput{Name: "range", Type: jobspec.Purge, RootKey: root, Binding: binding("1=1")})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]key.Key, 10000)
	for i := range keys {
		keys[i] = key.Key{int64(i / 100), int64(i % 100)}
	}
	// The test sends these keys straight to MySQL, past preflight: the log INSERT and one
	// membership statement over all 10,000. Under the environment matrix's
	// max_allowed_packet_64k row MySQL cannot carry them, and preflight would refuse such a
	// run (M7c-2), so the premise cannot hold there.
	need := max(key.InsertBytes(root.Columns, len(keys), 1, 1024),
		key.StatementBytes([]string{"k1", "k2"}, root.Columns, len(keys), 2, 1024))
	var packet uint64
	if err := db.QueryRowContext(ctx, "SELECT @@SESSION.max_allowed_packet").Scan(&packet); err != nil {
		t.Fatal(err)
	}
	if need > packet {
		t.Skipf("the test's statements need about %d bytes and max_allowed_packet is %d (the environment matrix row max_allowed_packet_64k)", need, packet)
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
