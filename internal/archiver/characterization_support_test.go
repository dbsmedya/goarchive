//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	_ "github.com/go-sql-driver/mysql"
)

// ============================================================================
// Characterization harness
//
// Pins the behaviour of the 1.8 preflight engine BEFORE any validator is
// re-platformed onto dbsgomysql. Every assertion here is a stable property:
// the check ID, the error type, and the sorted set of table names. Raw message
// text is never asserted — ValidateForeignKeyCoverage builds its message by
// ranging over a map (preflight.go:928-940), so the text is nondeterministic.
// ============================================================================

// chrCommand pins the exact (profile, enforceFKVisibility) tuple one CLI command
// passes to RunWithProfile. The applicability matrix's unit is the COMMAND, not the
// profile: dry-run and copy-only share PreflightProfileNonDestructive but differ on
// FK visibility enforcement.
//
// Source of truth for each tuple:
//
//	archive   → cmd/goarchive/cmd/archive.go:123
//	purge     → cmd/goarchive/cmd/purge.go:121
//	copy-only → cmd/goarchive/cmd/copyonly.go:99
//	dry-run   → cmd/goarchive/cmd/dryrun.go:112
//	validate  → cmd/goarchive/cmd/validate.go:189 (RunAllChecks → Full, true)
type chrCommand struct {
	Name                string
	Profile             PreflightProfile
	EnforceFKVisibility bool
}

var chrCommands = []chrCommand{
	{Name: "archive", Profile: PreflightProfileFull, EnforceFKVisibility: true},
	{Name: "purge", Profile: PreflightProfileSourceOnly, EnforceFKVisibility: true},
	{Name: "copy-only", Profile: PreflightProfileNonDestructive, EnforceFKVisibility: false},
	{Name: "dry-run", Profile: PreflightProfileNonDestructive, EnforceFKVisibility: true},
	{Name: "validate", Profile: PreflightProfileFull, EnforceFKVisibility: true},
}

// chrFixture owns one throwaway source schema and one throwaway destination schema
// so broken-schema fixtures never mutate Sakila. Both schemas are dropped by
// t.Cleanup, including on failure.
//
// SourceDB and DestDB are fixture-OWNED pools whose DSN default schema is the
// throwaway schema — they are NOT the shared pools from SetupIntegrationTest.
// This is load-bearing, for two reasons — though only the second is live today:
//
//  1. HISTORICAL. Through 1.8, ValidateRootPKNumeric was the one validator keyed on
//     the session's default schema (`WHERE TABLE_SCHEMA = DATABASE()`) instead of
//     p.sourceDBName, so a pool bound to some other schema failed with
//     ROOT_PK_TYPE_LOOKUP. Phase 017 moved it onto the configured source schema via
//     the run's memoized PrimaryKeys fact (Inspector.PrimaryKeys), so this reason no
//     longer holds — but the fixture design survives on reason 2 alone.
//  2. Issuing `USE <schema>` on a borrowed connection mutates session state that
//     database/sql hands back to the shared pool, so a shared pool would leak the
//     throwaway schema into unrelated tests in this package — and those schemas
//     are dropped at cleanup.
type chrFixture struct {
	SourceDB     *sql.DB
	DestDB       *sql.DB
	SourceSchema string
	DestSchema   string
	Logs         *observer.ObservedLogs

	log *logger.Logger
}

// chrSchemaSeq makes schema names unique within a package test run.
var chrSchemaSeq int

// chrServerConfig returns the integration DatabaseConfig registered under name
// ("source" or "destination").
func chrServerConfig(t *testing.T, setup *IntegrationTestSetup, name string) DatabaseConfig {
	t.Helper()
	for _, dbCfg := range setup.Config.Databases {
		if dbCfg.Name == name {
			return dbCfg
		}
	}
	t.Fatalf("%s database not found in integration setup", name)
	return DatabaseConfig{}
}

// chrOpenSchema opens a pool bound to schema on the server described by dbCfg.
func chrOpenSchema(t *testing.T, ctx context.Context, dbCfg DatabaseConfig, schema string) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=30s&multiStatements=true",
		dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port, schema)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", schema, err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("ping %s: %v", schema, err)
	}
	return db
}

// chrExternalSchemaName is the single definition of the naming convention for a
// schema OUTSIDE a fixture's own source/destination pair that holds a foreign key
// pointing INTO the fixture's source schema (see chrCreateExternalChild). Both the
// startup recovery in newChrFixture and every caller that creates such a schema
// must agree on this name, so it lives in one place.
func chrExternalSchemaName(srcSchema string) string {
	return srcSchema + "_ext"
}

// newChrFixture creates chr_src_<n> on the source server and chr_dst_<n> on the
// destination server, and registers cleanup that closes the fixture pools and drops
// both schemas.
func newChrFixture(t *testing.T, ctx context.Context) *chrFixture {
	t.Helper()

	setup, _ := SetupIntegrationTest(t)
	t.Cleanup(setup.Close)

	adminSource, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found in integration setup")
	}
	adminDest, ok := setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in integration setup")
	}

	chrSchemaSeq++
	srcSchema := fmt.Sprintf("chr_src_%d", chrSchemaSeq)
	dstSchema := fmt.Sprintf("chr_dst_%d", chrSchemaSeq)

	// Startup recovery: a killed run (this project's documented failure mode) can
	// leave srcSchema's external-child schema (chrCreateExternalChild) behind with
	// a live foreign key pointing INTO srcSchema. t.Cleanup never runs for a killed
	// process, so that residue survives the process exit. Drop it BEFORE dropping
	// srcSchema below — the FK lives in the external schema, so removing that
	// schema first is what unblocks srcSchema's own DROP DATABASE, which would
	// otherwise fail with Error 3730 ("Cannot drop table ... referenced by a
	// foreign key constraint") and brick every characterization test in the
	// package. See TestCharacterizationFixtureRecoversExternalSchemaResidue for the proof this
	// actually works.
	if _, err := adminSource.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+chrExternalSchemaName(srcSchema)+"`"); err != nil {
		t.Fatalf("startup recovery drop %s: %v", chrExternalSchemaName(srcSchema), err)
	}

	// Create the schemas over the shared pools. CREATE/DROP DATABASE does not
	// mutate the session's default schema, so the shared pools stay uncontaminated.
	for _, spec := range []struct {
		db     *sql.DB
		schema string
	}{{adminSource, srcSchema}, {adminDest, dstSchema}} {
		if _, err := spec.db.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+spec.schema+"`"); err != nil {
			t.Fatalf("drop %s: %v", spec.schema, err)
		}
		if _, err := spec.db.ExecContext(ctx, "CREATE DATABASE `"+spec.schema+"`"); err != nil {
			t.Fatalf("create %s: %v", spec.schema, err)
		}
	}

	sourceDB := chrOpenSchema(t, ctx, chrServerConfig(t, setup, "source"), srcSchema)
	destDB := chrOpenSchema(t, ctx, chrServerConfig(t, setup, "destination"), dstSchema)

	core, recorded := observer.New(zapcore.DebugLevel)
	f := &chrFixture{
		SourceDB:     sourceDB,
		DestDB:       destDB,
		SourceSchema: srcSchema,
		DestSchema:   dstSchema,
		Logs:         recorded,
		log:          &logger.Logger{SugaredLogger: zap.New(core).Sugar()},
	}

	// Registered after t.Cleanup(setup.Close), so it runs BEFORE it (LIFO): the
	// fixture pools are closed and the schemas dropped while the shared admin
	// pools are still open.
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		if err := sourceDB.Close(); err != nil {
			t.Logf("cleanup close %s: %v", srcSchema, err)
		}
		if err := destDB.Close(); err != nil {
			t.Logf("cleanup close %s: %v", dstSchema, err)
		}
		if _, err := adminSource.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+srcSchema+"`"); err != nil {
			t.Logf("cleanup drop %s: %v", srcSchema, err)
		}
		if _, err := adminDest.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+dstSchema+"`"); err != nil {
			t.Logf("cleanup drop %s: %v", dstSchema, err)
		}
	})

	return f
}

// ExecSource runs DDL inside the fixture's source schema.
func (f *chrFixture) ExecSource(t *testing.T, ctx context.Context, ddl string) {
	t.Helper()
	f.exec(t, ctx, f.SourceDB, f.SourceSchema, ddl)
}

// ExecDest runs DDL inside the fixture's destination schema.
func (f *chrFixture) ExecDest(t *testing.T, ctx context.Context, ddl string) {
	t.Helper()
	f.exec(t, ctx, f.DestDB, f.DestSchema, ddl)
}

func (f *chrFixture) exec(t *testing.T, ctx context.Context, db *sql.DB, schema, ddl string) {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire conn for %s: %v", schema, err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("close conn: %v", cerr)
		}
	}()
	if _, err := conn.ExecContext(ctx, "USE `"+schema+"`"); err != nil {
		t.Fatalf("USE %s: %v", schema, err)
	}
	if _, err := conn.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("DDL in %s failed: %v\nDDL: %s", schema, err, ddl)
	}
}

// Checker builds a PreflightChecker bound to the fixture's two throwaway schemas,
// with the tracking schema equal to the destination schema.
func (f *chrFixture) Checker(t *testing.T, g *graph.Graph) *PreflightChecker {
	t.Helper()
	c, err := NewPreflightChecker(f.SourceDB, f.SourceSchema, g, f.log)
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}
	if err := c.ConfigureDestination(f.DestDB, f.DestSchema, f.DestSchema); err != nil {
		t.Fatalf("ConfigureDestination: %v", err)
	}
	return c
}

// WarnMessages returns every WARN-level message emitted so far, in order.
func (f *chrFixture) WarnMessages() []string {
	var out []string
	for _, entry := range f.Logs.FilterLevelExact(zapcore.WarnLevel).All() {
		out = append(out, entry.Message)
	}
	return out
}

// chrAddChild registers a child table on g exactly the way graph.Builder does
// (builder.go:78-99): a Node, an edge carrying the FK metadata, and the child's PK.
//
// Use this rather than calling AddEdgeWithMeta directly. AddEdgeWithMeta only
// records the edge — it does NOT create the node — and a table absent from g.Nodes
// is invisible to AllNodes(). Such a table reads as OUT-of-graph to
// FK_COVERAGE_CHECK (so a correct config fails with an uncovered FK) and is
// silently skipped by INTERNAL_FK_COVERAGE.
func chrAddChild(g *graph.Graph, parent, parentPK, child, foreignKey, childPK string) {
	g.AddNode(child, &graph.Node{
		Name:           child,
		ForeignKey:     foreignKey,
		ReferenceKey:   parentPK,
		DependencyType: "1-N",
	})
	g.AddEdgeWithMeta(parent, child, foreignKey, parentPK, "1-N")
	g.SetPK(child, childPK)
}

// chrRun invokes RunWithProfile with the tuple the named command uses.
func chrRun(t *testing.T, ctx context.Context, c *PreflightChecker, cmd chrCommand, forceTriggers bool) error {
	t.Helper()
	return c.RunWithProfile(ctx, cmd.Profile, forceTriggers, cmd.EnforceFKVisibility)
}

// chrTableNames extracts the bare table names from a *PreflightError's Tables slice.
// Entries are decorated by the producing validator, e.g.
//
//	"orders(3-column PRIMARY KEY)"  → "orders"
//	"orders.customer_id"            → "orders.customer_id"
//	"orders(position 2: ...)"       → "orders"
//
// Only the decoration inside parentheses is stripped, so dotted table.column entries
// survive intact. The result is sorted, because several validators build their list
// from map iteration.
func chrTableNames(err error) []string {
	var pe *PreflightError
	if !errors.As(err, &pe) {
		return nil
	}
	out := make([]string, 0, len(pe.Tables))
	for _, entry := range pe.Tables {
		if idx := strings.Index(entry, "("); idx >= 0 {
			entry = entry[:idx]
		}
		out = append(out, strings.TrimSpace(entry))
	}
	sort.Strings(out)
	return out
}

// chrAssertCheck asserts the stable properties of a failing preflight run: the error
// is a *PreflightError, its Check equals wantID, and its table-name set equals
// wantTables. Pass a nil wantTables to skip the table assertion (used for checks whose
// detail lives only in Message, e.g. FK_COVERAGE_CHECK and INTERNAL_FK_COVERAGE).
func chrAssertCheck(t *testing.T, err error, wantID string, wantTables []string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil error", wantID)
	}
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError %s, got %T: %v", wantID, err, err)
	}
	if pe.Check != wantID {
		t.Fatalf("expected check %s, got %s (message: %s)", wantID, pe.Check, pe.Message)
	}
	if wantTables == nil {
		return
	}
	want := append([]string(nil), wantTables...)
	sort.Strings(want)
	got := chrTableNames(err)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("check %s tables = %v, want %v (raw: %v)", wantID, got, want, pe.Tables)
	}
}

// chrAssertPasses asserts a preflight run returned no error.
func chrAssertPasses(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("expected preflight to pass, got: %v", err)
	}
}

// chrAssertPlainErrorPrefix asserts the error is a PLAIN error (not a
// *PreflightError) whose text starts with wantPrefix. ROOT_PK_TYPE_UNSUPPORTED and
// ROOT_PK_TYPE_LOOKUP are string-prefixed errors today (preflight.go:267,270); that
// shape is deliberately preserved by the re-platforming, so it is characterized here.
func chrAssertPlainErrorPrefix(t *testing.T, err error, wantPrefix string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected plain error %q, got nil", wantPrefix)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("expected a PLAIN error %q, got *PreflightError %s", wantPrefix, pe.Check)
	}
	if !strings.HasPrefix(err.Error(), wantPrefix) {
		t.Fatalf("error %q does not start with %q", err.Error(), wantPrefix)
	}
}
