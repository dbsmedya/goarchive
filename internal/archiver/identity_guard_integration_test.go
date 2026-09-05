//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/database"
)

// sameDatabaseConfig is issue #13's configuration shape: destination is a copy
// of source, sha256 verification, and (below) a root table with no secondary
// unique index — the combination that selects non-strict INSERT IGNORE, lets
// the copy no-op against itself, lets verification match the table against
// itself, and then deletes the only copy.
func sameDatabaseConfig() *config.Config {
	src := config.DatabaseConfig{
		Host:     getEnv("TEST_SOURCE_HOST", "127.0.0.1"),
		Port:     getEnvInt("TEST_SOURCE_PORT", 3305),
		User:     getEnv("TEST_SOURCE_USER", "root"),
		Password: getEnv("TEST_SOURCE_PASSWORD", "qazokm"),
		Database: getEnv("TEST_SOURCE_DB", "sakila"),
		TLS:      "disable",
	}
	return &config.Config{
		Source:       src,
		Destination:  src,
		Verification: config.VerificationConfig{Method: "sha256"},
		Processing:   config.ProcessingConfig{BatchSize: 100, BatchDeleteSize: 50},
		Logging:      config.LoggingConfig{Level: "error", Format: "json"},
	}
}

func execOrFatal(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// trackingRowsForJob counts archiver_job rows for jobName in db's schema. An
// absent archiver_job table (nothing ever stamped this schema) counts as zero.
func trackingRowsForJob(t *testing.T, db *sql.DB, jobName string) int {
	t.Helper()
	var n int
	err := db.QueryRow("SELECT COUNT(*) FROM archiver_job WHERE job_name = ?", jobName).Scan(&n)
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1146 { // ER_NO_SUCH_TABLE
		return 0
	}
	if err != nil {
		t.Fatalf("count tracking rows for %q: %v", jobName, err)
	}
	return n
}

// TestArchiveAgainstItselfIsRefused_Integration runs issue #13's failure chain
// in the order cmd/goarchive/cmd/archive.go runs it: NewManager → Connect →
// NewOrchestrator → Execute. The guard must stop it at Connect.
//
// If Connect ever returns nil for identical endpoints, the test does NOT stop
// there: it continues into Execute against a throwaway three-row table, so the
// row-count assertion witnesses the only copy being deleted — the P1 itself,
// not a proxy for it. That is what makes this test red on a guard-less binary.
func TestArchiveAgainstItselfIsRefused_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()

	// An independent handle on the source server, opened through a normal
	// distinct-endpoint manager so the guard under test is not in its path.
	probe := realDBManager(t)
	t.Cleanup(func() { _ = probe.Close() })
	src := probe.Source

	const (
		table   = "identity_guard_probe"
		jobName = "identity_guard_self_archive"
	)
	execOrFatal(t, src, "DROP TABLE IF EXISTS "+table)
	execOrFatal(t, src, "CREATE TABLE "+table+" (id INT NOT NULL PRIMARY KEY, note VARCHAR(20) NOT NULL) ENGINE=InnoDB")
	execOrFatal(t, src, "INSERT INTO "+table+" (id, note) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		// Only a guard-less binary gets far enough to write tracking state into
		// the SOURCE schema; remove it so a red run does not pollute the estate.
		var id int64
		if err := src.QueryRow("SELECT id FROM archiver_job WHERE job_name = ?", jobName).Scan(&id); err == nil {
			_, _ = src.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `archiver_job_log_%d`", id))
			_, _ = src.Exec("DELETE FROM archiver_job WHERE job_name = ?", jobName)
		}
	})

	before := countRows(t, src, "SELECT COUNT(*) FROM "+table)
	if before != 3 {
		t.Fatalf("fixture: %s has %d rows, want 3", table, before)
	}

	cfg := sameDatabaseConfig()
	jobCfg := &config.JobConfig{RootTable: table, PrimaryKey: "id", Where: "id <= 3"}

	mgr := database.NewManager(cfg)
	t.Cleanup(func() { _ = mgr.Close() })
	connectErr := mgr.Connect(ctx)
	if connectErr == nil {
		// No guard: keep going exactly as the archive command would.
		orch, err := NewOrchestrator(cfg, jobName, jobCfg, mgr)
		if err != nil {
			t.Fatalf("NewOrchestrator: %v", err)
		}
		if err := orch.Initialize(); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		_, _ = orch.Execute(ctx, nil)
	}

	after := countRows(t, src, "SELECT COUNT(*) FROM "+table)
	if after != before {
		t.Fatalf("archive against itself deleted %d of %d rows: the only copy is gone (issue #13)", before-after, before)
	}
	if connectErr == nil {
		t.Fatal("Connect() accepted source == destination; SRC_DEST_IDENTITY_CHECK is missing")
	}
	var same *database.SameDatabaseError
	if !errors.As(connectErr, &same) || !strings.Contains(connectErr.Error(), "SRC_DEST_IDENTITY_CHECK") {
		t.Fatalf("Connect() error = %v, want *database.SameDatabaseError naming SRC_DEST_IDENTITY_CHECK", connectErr)
	}
	if n := trackingRowsForJob(t, src, jobName); n != 0 {
		t.Fatalf("%d tracking row(s) for %q were written into the source schema before the refusal", n, jobName)
	}
}

// ordersRootJobConfig re-points a job at the `orders` table of the resume
// scenario fixture. Orders ids are customer*100+n (101, 102, 201, …, 502).
func ordersRootJobConfig() *config.JobConfig {
	return &config.JobConfig{
		RootTable:  "orders",
		PrimaryKey: "id",
		Where:      "id <= 502",
		Relations: []config.Relation{
			{Table: "order_items", PrimaryKey: "id", ForeignKey: "order_id", DependencyType: "1-N"},
			{Table: "order_payments", PrimaryKey: "id", ForeignKey: "order_id", DependencyType: "1-N"},
		},
	}
}

// TestGetOrCreateJobRootTableIsSticky_Integration pins both directions of the
// #14 rule at the job row: the unchanged root resumes the SAME job, a changed
// root is refused with the documented text, and neither call moves anything.
func TestGetOrCreateJobRootTableIsSticky_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	_, destDB := resumeScenarioDBs(t, setup)
	destSchema := getDestSchema(setup)

	const jobName = "identity_guard_root_sticky"
	logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, "customers", JobTypeArchive)
	seedLogStatus(t, destDB, logTable, LogStatusCompleted, "1")

	// The ResumeManager reads the job row back (created_at → time.Time), which
	// needs a parseTime=true pool: production pools come from database.BuildDSN
	// and have it; the raw harness handles from setup.GetDB do not (they are
	// fine for the counts below, which scan no time column). Use the pool shape
	// production uses.
	dbManager, _ := resumeScenarioDBManager(t, setup, "sha256", 2)
	rm, err := NewResumeManager(dbManager.Destination, nil, destSchema)
	if err != nil {
		t.Fatalf("NewResumeManager: %v", err)
	}

	// Unchanged root: accepted, and it is the same job (same log table).
	state, err := rm.GetOrCreateJobWithType(ctx, jobName, "customers", JobTypeArchive)
	if err != nil {
		t.Fatalf("unchanged root was refused: %v", err)
	}
	if state.RootTable != "customers" {
		t.Fatalf("state.RootTable = %q, want customers", state.RootTable)
	}
	if got := rm.LogTableName(); got != logTable {
		t.Fatalf("log table = %s, want %s (a new job was created instead of resuming the existing one)", got, logTable)
	}

	// Changed root: refused with the documented text; nothing changes.
	_, err = rm.GetOrCreateJobWithType(ctx, jobName, "orders", JobTypeArchive)
	const want = `job "identity_guard_root_sticky" exists for root table "customers", expected "orders"`
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("GetOrCreateJobWithType(root=orders) = %v, want an error containing %q", err, want)
	}
	if n := countRows(t, destDB, "SELECT COUNT(*) FROM "+logTable); n != 1 {
		t.Fatalf("log table has %d rows after the refusal, want the 1 seeded row", n)
	}
	if n := countRows(t, destDB, "SELECT COUNT(*) FROM archiver_job WHERE job_name = ?", jobName); n != 1 {
		t.Fatalf("%d job rows for %q after the refusal, want 1", n, jobName)
	}
}

// ordersScenarioCheckpoint sits at the last selected order id, so the forward
// scan (`id > 502`) can reach nothing. On a guard-less binary the ONLY path
// that can touch order 101 is the delete-only replay of the copied marker —
// the #14 mechanism itself, not an ordinary copy-and-delete. (The customers
// checkpoint "5" would be below every order id and let the forward scan
// archive 101 normally, which proves nothing about marker replay.)
const ordersScenarioCheckpoint = "502"

// TestCopiedMarkerIsNotReplayedAgainstAnotherRoot_Integration is issue #14's
// reproduction: a `copied` marker produced for root `customers` whose PK value
// (101) is also a live `orders` id. Re-pointing the same job name at root
// `orders` must be refused before recovery reads the marker.
//
// Post-state evidence is collected BEFORE any verdict, so a guard-less run
// reports what it destroyed — order 101 gone from the source with no copy on
// the destination, marker promoted — instead of stopping at the missing
// refusal.
func TestCopiedMarkerIsNotReplayedAgainstAnotherRoot_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	setup, ctx := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)
	sourceDB, destDB := resumeScenarioDBs(t, setup)
	destSchema := getDestSchema(setup)

	const jobName = "identity_guard_marker_replay"
	resetResumeScenarioData(t, sourceDB, destDB)
	dropDestinationSecondaryUniqueIndexes(t, destDB, destSchema)
	dbManager, cfg := resumeScenarioDBManager(t, setup, "sha256", 2)

	logTable := bootstrapJobTracking(t, destDB, destSchema, jobName, "customers", JobTypeArchive)
	seedLogStatus(t, destDB, logTable, LogStatusCopied, "101")
	setJobCheckpoint(t, destDB, jobName, ordersScenarioCheckpoint)

	const (
		orderQuery    = "SELECT COUNT(*) FROM orders WHERE id = 101"
		itemsQuery    = "SELECT COUNT(*) FROM order_items WHERE order_id = 101"
		paymentsQuery = "SELECT COUNT(*) FROM order_payments WHERE order_id = 101"
	)
	markersQuery := "SELECT COUNT(*) FROM " + logTable + " WHERE root_pk_id = '101' AND log_status = ?"

	// Preconditions the evidence depends on, proven rather than assumed: the
	// marker exists with status copied, order 101 and its two items and one
	// payment are on the source, and nothing for 101 is on the destination.
	if n := countRows(t, destDB, markersQuery, LogStatusCopied); n != 1 {
		t.Fatalf("fixture: %d copied marker(s) for 101, want 1", n)
	}
	if o, i, p := countRows(t, sourceDB, orderQuery), countRows(t, sourceDB, itemsQuery), countRows(t, sourceDB, paymentsQuery); o != 1 || i != 2 || p != 1 {
		t.Fatalf("fixture: order 101 subgraph on the source is orders=%d items=%d payments=%d, want 1/2/1", o, i, p)
	}
	if n := countRows(t, destDB, orderQuery); n != 0 {
		t.Fatalf("fixture: order 101 is already on the destination (%d row)", n)
	}
	assertRootSet(t, sourceDB, "source (pre-run)", resumeScenarioRoots...)

	orch, err := NewOrchestrator(cfg, jobName, ordersRootJobConfig(), dbManager)
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.Initialize(); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	_, execErr := orch.Execute(ctx, nil)

	// Evidence first, verdicts second.
	gotOrder := countRows(t, sourceDB, orderQuery)
	gotItems := countRows(t, sourceDB, itemsQuery)
	gotPayments := countRows(t, sourceDB, paymentsQuery)
	gotDestOrder := countRows(t, destDB, orderQuery)
	gotMarkers := countRows(t, destDB, markersQuery, LogStatusCopied)

	// The specific refusal, not just "some error", so an unrelated startup
	// failure cannot satisfy this test.
	const want = `exists for root table "customers", expected "orders"`
	if execErr == nil || !strings.Contains(execErr.Error(), want) {
		t.Errorf("Execute() = %v, want the root-table refusal containing %q", execErr, want)
	}
	if gotOrder != 1 || gotItems != 2 || gotPayments != 1 {
		t.Errorf("order 101 lost from the source WITHOUT a destination copy: source orders=%d items=%d payments=%d, destination orders=%d — a copied marker from root customers was replayed delete-only against root orders (issue #14)",
			gotOrder, gotItems, gotPayments, gotDestOrder)
	}
	if gotDestOrder != 0 {
		t.Errorf("order 101 appeared on the destination (%d row): the run copied instead of refusing", gotDestOrder)
	}
	if gotMarkers != 1 {
		t.Errorf("copied marker for 101 changed: %d row(s), want 1 (a replay would have promoted it)", gotMarkers)
	}
	if t.Failed() {
		t.FailNow()
	}
	assertRootSet(t, sourceDB, "source (post-run)", resumeScenarioRoots...)
}
