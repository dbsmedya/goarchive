//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/v3/internal/logger"
	_ "github.com/go-sql-driver/mysql"
)

// The schema fixture once shared by the 2.x preflight characterization suites, which
// 3.0 M3 deleted with the 2.x preflight (rulings R38, R39). multistatements_integration_test.go
// still uses it, until M5 replaces the 2.x estimator and progress count it tests.

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

// WarnMessages returns every WARN-level message emitted so far, in order.
func (f *chrFixture) WarnMessages() []string {
	var out []string
	for _, entry := range f.Logs.FilterLevelExact(zapcore.WarnLevel).All() {
		out = append(out, entry.Message)
	}
	return out
}
