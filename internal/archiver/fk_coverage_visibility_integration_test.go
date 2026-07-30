//go:build integration

package archiver

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	_ "github.com/go-sql-driver/mysql"
)

// sourceDBConfig returns the resolved source DatabaseConfig.
func sourceDBConfig(setup *IntegrationTestSetup) (DatabaseConfig, bool) {
	for _, c := range setup.Config.Databases {
		if c.Name == "source" {
			return c, true
		}
	}
	return DatabaseConfig{}, false
}

// TestIntegrationFKCoverageVisibility_HiddenCrossSchemaFK_FailsClosed first shows
// the blind spot — an account that cannot see the referencing schema gets a false
// "coverage passed" from ValidateForeignKeyCoverage — then proves
// ValidateForeignKeyMetadataVisibility fails closed so RunWithProfile never
// reaches copy/delete.
//
// Under deviation D2 the blind spot survives for a different reason than 1.8's: the
// limited account holds no PROCESS, so the library's PROCESS-gated InnoDB registry read
// fails and it falls back to information_schema.KEY_COLUMN_USAGE, which simply cannot
// see the row in the schema the account has no privilege on — the fallback succeeds
// (VisibilityUnconfirmed) with zero matching keys, so ValidateForeignKeyCoverage, which
// only judges the external-edge flavour, finds nothing and falsely passes exactly as
// before. Both calls below share ONE *preflightRun, so the coverage facts and the
// completeness verdict come from a single cached fkIncoming fetch rather than two
// independent ones — proving they cannot disagree about what was seen (spec §3.5).
func TestIntegrationFKCoverageVisibility_HiddenCrossSchemaFK_FailsClosed(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	rootDB, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found")
	}
	srcCfg, ok := sourceDBConfig(setup)
	if !ok {
		t.Fatal("source config not found")
	}

	// As root: in-graph parent + a cross-schema child with ON DELETE CASCADE.
	setupCrossSchemaFK(t, ctx, rootDB, srcCfg.Database, "CASCADE")
	defer dropCrossSchemaFK(ctx, rootDB, srcCfg.Database)

	// Limited user: SELECT only on the source schema — cannot see the child schema.
	const limUser, limPass = "fkvis_lim", "fkvispw"
	_, _ = rootDB.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", limUser))
	if _, err := rootDB.ExecContext(ctx, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", limUser, limPass)); err != nil {
		t.Fatalf("create limited user: %v", err)
	}
	defer func() { _, _ = rootDB.ExecContext(ctx, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", limUser)) }()
	if _, err := rootDB.ExecContext(ctx, fmt.Sprintf("GRANT SELECT ON `%s`.* TO '%s'@'%%'", srcCfg.Database, limUser)); err != nil {
		t.Fatalf("grant schema select: %v", err)
	}

	limDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=10s", limUser, limPass, srcCfg.Host, srcCfg.Port, srcCfg.Database)
	limDB, err := sql.Open("mysql", limDSN)
	if err != nil {
		t.Fatalf("open limited conn: %v", err)
	}
	defer func() { _ = limDB.Close() }()

	g := graph.NewGraph(xschemaParentTable, "id")
	checker, err := NewPreflightChecker(limDB, srcCfg.Database, g, logger.NewDefault())
	if err != nil {
		t.Fatalf("new checker: %v", err)
	}

	// Blind-spot precondition: coverage alone CANNOT see the cross-schema child,
	// so it falsely passes for the limited account. This is exactly the metadata
	// hole the visibility guard exists to close. Both calls share the SAME run so
	// the second reuses the first's memoized fkIncoming fetch.
	run := newPreflightRun(checker)
	if err := checker.ValidateForeignKeyCoverage(ctx, run); err != nil {
		t.Fatalf("blind-spot precondition: expected coverage to falsely pass for the limited account "+
			"(it cannot see the cross-schema FK), got: %v", err)
	}

	// The guard: fail closed.
	err = checker.ValidateForeignKeyMetadataVisibility(ctx, run)
	if err == nil {
		t.Fatal("expected FK_COVERAGE_VISIBILITY_CHECK to fail closed for an account without PROCESS, got nil")
	}
	if pfErr, ok := err.(*PreflightError); !ok || pfErr.Check != "FK_COVERAGE_VISIBILITY_CHECK" {
		t.Fatalf("expected FK_COVERAGE_VISIBILITY_CHECK, got: %v", err)
	}
}

// TestIntegrationFKCoverageVisibility_PROCESSPrivilegedAccount_Passes is the positive
// control. It still passes — but under deviation D2 that is because the suite's root
// account holds PROCESS, letting the library's PROCESS-gated InnoDB registry read
// succeed, NOT because of global SELECT as the pre-025 name claimed. Renamed because
// under D2 the old name (TestIntegrationFKCoverageVisibility_GlobalSelect_Passes) is
// actively false: global SELECT no longer has anything to do with why this passes.
func TestIntegrationFKCoverageVisibility_PROCESSPrivilegedAccount_Passes(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	rootDB, ok := setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found")
	}
	srcSchema := xschemaSourceName(setup)

	g := graph.NewGraph(xschemaParentTable, "id")
	checker, err := NewPreflightChecker(rootDB, srcSchema, g, logger.NewDefault())
	if err != nil {
		t.Fatalf("new checker: %v", err)
	}
	if err := checker.ValidateForeignKeyMetadataVisibility(ctx, newPreflightRun(checker)); err != nil {
		t.Fatalf("expected PROCESS-privileged account to pass, got: %v", err)
	}
}
