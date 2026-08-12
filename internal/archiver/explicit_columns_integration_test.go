//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dbsmedya/goarchive/internal/archiver/testsupport"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/dbsmedya/goarchive/internal/verifier"
	_ "github.com/go-sql-driver/mysql"
)

// ============================================================================
// Explicit-column-list round-trip tests (issue #23). Every case creates its
// own table on BOTH servers, copies with CopyPhase, asserts destination
// values, and proves SHA256 verification with the shared source-derived
// column list. These are the tests the issue mandates: they fail on any
// regression back to SELECT * because the seeded values live in columns
// SELECT * cannot see.
// ============================================================================

// explicitColsDBs resolves the harness's source/destination pools and the
// source schema name.
func explicitColsDBs(t *testing.T, setup *IntegrationTestSetup) (srcDB, dstDB *sql.DB, srcSchema string) {
	t.Helper()
	srcDB, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found in integration setup")
	}
	dstDB, ok = setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in integration setup")
	}
	for _, c := range setup.Config.Databases {
		if c.Name == "source" {
			srcSchema = c.Database
		}
	}
	if srcSchema == "" {
		t.Fatal("could not resolve source schema name")
	}
	return srcDB, dstDB, srcSchema
}

// explicitColsCreate drops/creates the table on both servers and registers cleanup.
func explicitColsCreate(t *testing.T, ctx context.Context, srcDB, dstDB *sql.DB, table, srcDDL, dstDDL string) {
	t.Helper()
	for db, ddl := range map[*sql.DB]string{srcDB: srcDDL, dstDB: dstDDL} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
	}
	t.Cleanup(func() {
		cctx := context.Background()
		_, _ = srcDB.ExecContext(cctx, "DROP TABLE IF EXISTS "+table)
		_, _ = dstDB.ExecContext(cctx, "DROP TABLE IF EXISTS "+table)
	})
}

// explicitColsCopyVerify builds the column lists, copies pks with CopyPhase,
// runs SHA256 verification with the same lists, and returns the dest DB for
// asserts. The table must already exist on both servers and be seeded on source.
func explicitColsCopyVerify(t *testing.T, ctx context.Context, srcDB, dstDB *sql.DB,
	srcSchema, table, pkColumn string, pks []interface{}) *sql.DB {
	t.Helper()
	g := graph.NewGraph(table, pkColumn)
	lists, err := sourceColumnLists(ctx, srcDB, srcSchema, g.AllNodes())
	if err != nil {
		t.Fatalf("sourceColumnLists: %v", err)
	}
	cp, err := NewCopyPhase(srcDB, dstDB, g, config.SafetyConfig{}, logger.NewDefault())
	if err != nil {
		t.Fatalf("new copy phase: %v", err)
	}
	cp.SetColumnLists(lists)
	rs := &RecordSet{RootPKs: pks, Records: map[string][]interface{}{table: pks}}
	if _, err := cp.Copy(ctx, rs); err != nil {
		t.Fatalf("copy: %v", err)
	}
	v, err := verifier.NewVerifier(srcDB, dstDB, g, verifier.MethodSHA256, logger.NewDefault())
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	v.SetColumnLists(lists)
	stats, err := v.Verify(ctx, rs)
	if err != nil {
		t.Fatalf("sha256 verification: %v", err)
	}
	if stats.TablesFailed != 0 {
		t.Fatalf("sha256 verification failed: %+v", stats)
	}
	return dstDB
}

// explicitColsRoundTrip composes create + seed + copy + verify for the
// plain-DDL cases.
func explicitColsRoundTrip(t *testing.T, ctx context.Context, setup *IntegrationTestSetup,
	table, srcDDL, dstDDL, seedSQL, pkColumn string, pks []interface{}) *sql.DB {
	t.Helper()
	srcDB, dstDB, srcSchema := explicitColsDBs(t, setup)
	explicitColsCreate(t, ctx, srcDB, dstDB, table, srcDDL, dstDDL)
	if _, err := srcDB.ExecContext(ctx, seedSQL); err != nil {
		t.Fatalf("seed %s: %v", table, err)
	}
	return explicitColsCopyVerify(t, ctx, srcDB, dstDB, srcSchema, table, pkColumn, pks)
}

// Spec case 1: an invisible non-PK column with a non-default value survives
// the copy and the hash. This is issue #23's exact reproduction scenario.
func TestExplicitColumnsInvisibleValueCopiedAndVerified(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	const table = "exp_inv_value"
	ddl := "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, payload VARCHAR(255) INVISIBLE DEFAULT 'default-value') ENGINE=InnoDB"
	dstDB := explicitColsRoundTrip(t, ctx, setup, table, ddl, ddl,
		"INSERT INTO "+table+" (id, payload) VALUES (1, 'original-secret')",
		"id", []interface{}{int64(1)})

	var got string
	if err := dstDB.QueryRowContext(ctx, "SELECT payload FROM "+table+" WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("read dest payload: %v", err)
	}
	if got != "original-secret" {
		t.Fatalf("invisible column value = %q on destination, want %q", got, "original-secret")
	}
}

// Spec case 2: a REAL generated invisible primary key
// (sql_generate_invisible_primary_key=ON). The session variable and the
// CREATE TABLE must execute on the SAME session, so both run on one dedicated
// *sql.Conn — pool round-robin would silently split them. Requires MySQL
// 8.0.30+ (the estate is 8.4) and a privileged account (tests connect as
// root). MySQL generates `my_row_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT
// INVISIBLE PRIMARY KEY`; the copied values must survive.
func TestExplicitColumnsGeneratedInvisiblePrimaryKey(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	srcDB, dstDB, srcSchema := explicitColsDBs(t, setup)
	const table = "exp_gipk"

	createGIPK := func(db *sql.DB) {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("dedicated conn: %v", err)
		}
		defer func() { _ = conn.Close() }()
		if _, err := conn.ExecContext(ctx, "SET SESSION sql_generate_invisible_primary_key = ON"); err != nil {
			t.Fatalf("SET SESSION sql_generate_invisible_primary_key: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "CREATE TABLE "+table+" (data VARCHAR(50) NOT NULL) ENGINE=InnoDB"); err != nil {
			t.Fatalf("create GIPK table: %v", err)
		}
	}
	for _, db := range []*sql.DB{srcDB, dstDB} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop: %v", err)
		}
		createGIPK(db)
	}
	t.Cleanup(func() {
		cctx := context.Background()
		_, _ = srcDB.ExecContext(cctx, "DROP TABLE IF EXISTS "+table)
		_, _ = dstDB.ExecContext(cctx, "DROP TABLE IF EXISTS "+table)
	})

	if _, err := srcDB.ExecContext(ctx, "INSERT INTO "+table+" (data) VALUES ('alpha'), ('beta')"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Prove MySQL actually generated the invisible PK before relying on it.
	var firstID int64
	if err := srcDB.QueryRowContext(ctx, "SELECT my_row_id FROM "+table+" ORDER BY my_row_id LIMIT 1").Scan(&firstID); err != nil {
		t.Fatalf("GIPK column my_row_id was not generated: %v", err)
	}
	if firstID != 1 {
		t.Fatalf("GIPK my_row_id = %d, want 1", firstID)
	}

	explicitColsCopyVerify(t, ctx, srcDB, dstDB, srcSchema, table, "my_row_id",
		[]interface{}{int64(1), int64(2)})

	rows, err := dstDB.QueryContext(ctx, "SELECT my_row_id, data FROM "+table+" ORDER BY my_row_id")
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	defer func() { _ = rows.Close() }()
	want := map[int64]string{1: "alpha", 2: "beta"}
	seen := 0
	for rows.Next() {
		var id int64
		var data string
		if err := rows.Scan(&id, &data); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if want[id] != data {
			t.Fatalf("dest row my_row_id=%d data=%q, want %q", id, data, want[id])
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if seen != 2 {
		t.Fatalf("dest has %d rows, want 2 (invisible PK values not preserved)", seen)
	}
}

// Spec case 3: an invisible STORED GENERATED source column materializes into a
// plain destination column (destination generated columns are fatal in
// preflight, so the destination side is always plain).
func TestExplicitColumnsInvisibleStoredGenerated(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	const table = "exp_inv_gen"
	srcDDL := "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, amount INT NOT NULL, doubled INT GENERATED ALWAYS AS (amount * 2) STORED INVISIBLE) ENGINE=InnoDB"
	dstDDL := "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, amount INT NOT NULL, doubled INT) ENGINE=InnoDB"
	dstDB := explicitColsRoundTrip(t, ctx, setup, table, srcDDL, dstDDL,
		"INSERT INTO "+table+" (id, amount) VALUES (1, 21)",
		"id", []interface{}{int64(1)})

	var doubled int
	if err := dstDB.QueryRowContext(ctx, "SELECT doubled FROM "+table+" WHERE id = 1").Scan(&doubled); err != nil {
		t.Fatalf("read dest doubled: %v", err)
	}
	if doubled != 42 {
		t.Fatalf("generated invisible value = %d on destination, want 42", doubled)
	}
}

// Spec case 4: a column visible on source but INVISIBLE on destination.
// Before issue #23 the destination's SELECT * omitted it and verification
// failed spuriously; the shared explicit list reads it on both sides.
func TestExplicitColumnsDestinationInvisible(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	const table = "exp_dst_inv"
	srcDDL := "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB"
	dstDDL := "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, note VARCHAR(50) INVISIBLE) ENGINE=InnoDB"
	dstDB := explicitColsRoundTrip(t, ctx, setup, table, srcDDL, dstDDL,
		"INSERT INTO "+table+" (id, note) VALUES (1, 'kept')",
		"id", []interface{}{int64(1)})

	var note string
	if err := dstDB.QueryRowContext(ctx, "SELECT note FROM "+table+" WHERE id = 1").Scan(&note); err != nil {
		t.Fatalf("read dest note: %v", err)
	}
	if note != "kept" {
		t.Fatalf("dest note = %q, want %q", note, "kept")
	}
}

// Spec case 1 at FULL depth (operator review, blocking finding 1): the
// complete archive pipeline — preflight acceptance, orchestrated copy, sha256
// verification, source deletion — over an invisible column. This test fails
// if the guard is still wired anywhere or if the orchestrator's column-list
// injection is broken, even when the phase-level round trips above pass. It
// uses the characterization fixture (throwaway schemas on both servers) for
// preflight+data and the realdb env helpers for the orchestrator's config.
func TestExplicitColumnsFullArchive(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	f := newChrFixture(t, ctx)

	const table = "exp_full_archive"
	ddl := "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, payload VARCHAR(255) INVISIBLE DEFAULT 'default-value') ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)
	f.ExecSource(t, ctx, "INSERT INTO "+table+" (id, payload) VALUES (1, 'original-secret'), (2, 'second-secret')")

	g := graph.NewGraph(table, "id")

	// 1. Preflight with the archive command's profile must ACCEPT the table.
	if err := chrRun(t, ctx, f.Checker(t, g), chrCommands[0], false); err != nil {
		t.Fatalf("archive preflight rejected a table with an invisible column: %v", err)
	}

	// 2. Full archive with sha256 verification through the orchestrator.
	cfg := &config.Config{
		Source: config.DatabaseConfig{
			Host:     getEnv("TEST_SOURCE_HOST", "127.0.0.1"),
			Port:     getEnvInt("TEST_SOURCE_PORT", 3305),
			User:     getEnv("TEST_SOURCE_USER", "root"),
			Password: getEnv("TEST_SOURCE_PASSWORD", "qazokm"),
			Database: f.SourceSchema,
			TLS:      "disable",
		},
		Destination: config.DatabaseConfig{
			Host:     getEnv("TEST_DEST_HOST", "127.0.0.1"),
			Port:     getEnvInt("TEST_DEST_PORT", 3307),
			User:     getEnv("TEST_DEST_USER", "root"),
			Password: getEnv("TEST_DEST_PASSWORD", "qazokm"),
			Database: f.DestSchema,
			TLS:      "disable",
		},
		Processing:   config.ProcessingConfig{BatchSize: 100, BatchDeleteSize: 50},
		Verification: config.VerificationConfig{Method: "sha256"},
		Logging:      config.LoggingConfig{Level: "error", Format: "json"},
	}
	dbManager := database.NewManager(cfg)
	if err := dbManager.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = dbManager.Close() })

	const jobName = "exp_full_archive_job"
	testsupport.CleanupArchiverState(t, dbManager.Destination, jobName)

	jobCfg := &config.JobConfig{RootTable: table, PrimaryKey: "id"}
	orch, err := NewOrchestrator(cfg, jobName, jobCfg, dbManager)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	result, err := orch.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("archive execute: %v", err)
	}
	if !result.Success {
		t.Fatalf("archive did not succeed: %+v", result)
	}

	// 3. Destination retains the invisible values.
	for id, want := range map[int]string{1: "original-secret", 2: "second-secret"} {
		var got string
		if err := f.DestDB.QueryRowContext(ctx, "SELECT payload FROM "+table+" WHERE id = ?", id).Scan(&got); err != nil {
			t.Fatalf("read dest payload id=%d: %v", id, err)
		}
		if got != want {
			t.Fatalf("dest payload id=%d = %q, want %q", id, got, want)
		}
	}

	// 4. Source rows are deleted.
	var remaining int
	if err := f.SourceDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&remaining); err != nil {
		t.Fatalf("count source rows: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("source still holds %d rows after archive; want 0 (deletion did not run)", remaining)
	}
}
