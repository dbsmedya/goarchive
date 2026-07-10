//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	_ "github.com/go-sql-driver/mysql"
)

// ============================================================================
// Valid-graph + cross-schema FK integration tests (issue #15 follow-up).
//
// These build a genuine multi-table archive graph (root + one relation, both in
// the source schema — the shape an operator writes in archiver.yaml) via the
// same graph.NewBuilder path the `validate` CLI uses, and run it through the
// FULL preflight profile (RunAllChecks — exactly what `validate` runs). They
// then observe how validation reacts to a cross-schema FK in each direction:
//
//   - baseline:  valid in-source-schema graph            -> RunAllChecks passes
//   - incoming:  out-of-schema child -> in-graph parent  -> FK_COVERAGE_CHECK
//   - outgoing:  in-graph child -> out-of-schema parent  -> RunAllChecks passes
//
// Incoming is fatal because the external child cannot be represented in the
// graph (identifiers forbid schema.table) and a source DELETE could cascade
// into rows GoArchive never copied. Outgoing is safe: deleting a child never
// cascades UP into its parent, so a reference out to another schema is allowed.
//
// The tables are all-integer (no varchar) to sidestep the charset branch of
// DEST_SCHEMA_COMPATIBILITY_CHECK, and are dedicated `vg_*` names so the seeded
// customer_orders fixture's own FKs never interfere. NewPreflightChecker caches
// FK metadata per instance, so every RunAllChecks below uses a fresh checker.
// ============================================================================

const (
	vgRoot      = "vg_orders"      // in-graph root, source schema
	vgChild     = "vg_order_items" // in-graph child, source schema
	vgIncSchema = "goarchive_vg_incoming"
	vgIncChild  = "vg_audit_log" // out-of-graph child referencing in-graph root
	vgOutSchema = "goarchive_vg_outgoing"
	vgOutParent = "vg_customers"          // out-of-graph parent referenced by in-graph root
	vgOutFKName = "fk_vg_orders_customer" // outgoing FK constraint on vg_orders.customer_id
)

// vgSourceDest resolves the source/destination *sql.DB handles and their schema
// (database) names from the integration setup.
func vgSourceDest(t *testing.T, setup *IntegrationTestSetup) (srcDB, destDB *sql.DB, srcSchema, destSchema string) {
	t.Helper()
	var ok bool
	srcDB, ok = setup.GetDB("source")
	if !ok {
		t.Fatal("source database not found in integration setup")
	}
	destDB, ok = setup.GetDB("destination")
	if !ok {
		t.Fatal("destination database not found in integration setup")
	}
	for _, dbCfg := range setup.Config.Databases {
		switch dbCfg.Name {
		case "source":
			srcSchema = dbCfg.Database
		case "destination":
			destSchema = dbCfg.Database
		}
	}
	if srcSchema == "" || destSchema == "" {
		t.Fatalf("could not resolve source/destination schema names (src=%q dest=%q)", srcSchema, destSchema)
	}
	return srcDB, destDB, srcSchema, destSchema
}

// vgCreateBaseTables creates the in-graph root + child in `db`, identically on
// source and destination. All columns are integer. `customer_id` is present
// (but unconstrained) so the outgoing case can add an FK on it without changing
// the column set the destination-compatibility check compares.
func vgCreateBaseTables(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	vgDropBaseTables(ctx, db)
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s` (id BIGINT NOT NULL, customer_id BIGINT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		vgRoot)); err != nil {
		t.Fatalf("create %s: %v", vgRoot, err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s` (id BIGINT NOT NULL, order_id BIGINT NOT NULL, PRIMARY KEY (id), "+
			"CONSTRAINT fk_vg_oi_order FOREIGN KEY (order_id) REFERENCES `%s`(id)) ENGINE=InnoDB",
		vgChild, vgRoot)); err != nil {
		t.Fatalf("create %s: %v", vgChild, err)
	}
}

// vgDropBaseTables drops the in-graph tables child-first (best-effort).
func vgDropBaseTables(ctx context.Context, db *sql.DB) {
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", vgChild))
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", vgRoot))
}

// vgSetupIncoming creates an out-of-graph schema whose child table has an
// ON DELETE CASCADE FK referencing the in-graph root (the dangerous direction).
func vgSetupIncoming(t *testing.T, ctx context.Context, srcDB *sql.DB, srcSchema string) {
	t.Helper()
	vgDropIncoming(ctx, srcDB)
	if _, err := srcDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", vgIncSchema)); err != nil {
		t.Fatalf("create incoming schema %s: %v", vgIncSchema, err)
	}
	if _, err := srcDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`%s` (id BIGINT NOT NULL, order_id BIGINT NULL, PRIMARY KEY (id), "+
			"CONSTRAINT fk_vg_audit_order FOREIGN KEY (order_id) REFERENCES `%s`.`%s`(id) ON DELETE CASCADE) ENGINE=InnoDB",
		vgIncSchema, vgIncChild, srcSchema, vgRoot)); err != nil {
		t.Fatalf("create incoming cross-schema child %s.%s: %v", vgIncSchema, vgIncChild, err)
	}
}

// vgDropIncoming removes the incoming out-of-graph schema (best-effort).
func vgDropIncoming(ctx context.Context, srcDB *sql.DB) {
	_, _ = srcDB.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", vgIncSchema, vgIncChild))
	_, _ = srcDB.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", vgIncSchema))
}

// vgSetupOutgoing creates an out-of-graph schema holding a parent table, then
// adds an FK from the in-graph root (vg_orders.customer_id) out to it.
func vgSetupOutgoing(t *testing.T, ctx context.Context, srcDB *sql.DB, srcSchema string) {
	t.Helper()
	vgDropOutgoing(ctx, srcDB)
	if _, err := srcDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", vgOutSchema)); err != nil {
		t.Fatalf("create outgoing schema %s: %v", vgOutSchema, err)
	}
	if _, err := srcDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s`.`%s` (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB",
		vgOutSchema, vgOutParent)); err != nil {
		t.Fatalf("create outgoing cross-schema parent %s.%s: %v", vgOutSchema, vgOutParent, err)
	}
	if _, err := srcDB.ExecContext(ctx, fmt.Sprintf(
		"ALTER TABLE `%s`.`%s` ADD CONSTRAINT `%s` FOREIGN KEY (customer_id) REFERENCES `%s`.`%s`(id)",
		srcSchema, vgRoot, vgOutFKName, vgOutSchema, vgOutParent)); err != nil {
		t.Fatalf("add outgoing FK on %s.%s: %v", srcSchema, vgRoot, err)
	}
}

// vgDropOutgoing drops the outgoing FK first (so the parent can be removed),
// then the out-of-graph schema (best-effort).
func vgDropOutgoing(ctx context.Context, srcDB *sql.DB) {
	_, _ = srcDB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `%s` DROP FOREIGN KEY `%s`", vgRoot, vgOutFKName))
	_, _ = srcDB.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", vgOutSchema))
}

// vgBuildChecker builds a job config + graph via the same graph.NewBuilder path
// the `validate` command uses, then a FRESH PreflightChecker configured for
// source+destination (matching validate.go). Fresh each call so the per-instance
// FK metadata cache never hides a constraint added between runs.
func vgBuildChecker(t *testing.T, srcDB, destDB *sql.DB, srcSchema, destSchema string) *PreflightChecker {
	t.Helper()
	jobCfg := &config.JobConfig{
		RootTable:  vgRoot,
		PrimaryKey: "id",
		Where:      "1=1",
		Relations: []config.Relation{{
			Table:          vgChild,
			PrimaryKey:     "id",
			ForeignKey:     "order_id",
			DependencyType: "1-N",
		}},
	}
	g, err := graph.NewBuilder(jobCfg).Build()
	if err != nil {
		t.Fatalf("build graph from job config: %v", err)
	}
	checker, err := NewPreflightChecker(srcDB, srcSchema, g, logger.NewDefault())
	if err != nil {
		t.Fatalf("new preflight checker: %v", err)
	}
	// tracking schema == destination DB (JobSchema unset -> EffectiveJobSchema()).
	if err := checker.ConfigureDestination(destDB, destSchema, destSchema); err != nil {
		t.Fatalf("configure destination: %v", err)
	}
	checker.SetVerification(config.VerificationConfig{Method: "count"})
	return checker
}

// TestIntegrationFKCoverage_ValidGraph_CrossSchemaDirections exercises the full
// `validate` preflight (RunAllChecks) against a valid multi-table graph and both
// cross-schema FK directions. It proves the fix does not over-reject a clean
// graph or a safe outgoing reference, while still catching the dangerous
// incoming direction — and that the incoming failure is specifically
// FK_COVERAGE_CHECK, i.e. every earlier check in the full profile still passed.
func TestIntegrationFKCoverage_ValidGraph_CrossSchemaDirections(t *testing.T) {
	setup, ctx := SetupIntegrationTest(t)
	defer setup.Close()

	srcDB, destDB, srcSchema, destSchema := vgSourceDest(t, setup)

	// Clean slate, then the valid in-graph tables on BOTH source and destination
	// (destination needs them for DEST_SCHEMA_COMPATIBILITY_CHECK).
	vgDropIncoming(ctx, srcDB)
	vgDropOutgoing(ctx, srcDB)
	vgCreateBaseTables(t, ctx, srcDB)
	vgCreateBaseTables(t, ctx, destDB)
	defer func() {
		vgDropIncoming(ctx, srcDB)
		vgDropOutgoing(ctx, srcDB)
		vgDropBaseTables(ctx, srcDB)
		vgDropBaseTables(ctx, destDB)
	}()

	// Baseline: a valid root -> child graph, both in the source schema, fully
	// covered by the relation, clears the entire `validate` profile.
	t.Run("baseline_valid_graph_passes", func(t *testing.T) {
		vgDropIncoming(ctx, srcDB)
		vgDropOutgoing(ctx, srcDB)
		checker := vgBuildChecker(t, srcDB, destDB, srcSchema, destSchema)
		if err := checker.RunAllChecks(ctx, false); err != nil {
			t.Fatalf("expected the valid in-source-schema graph to pass full preflight, got: %v", err)
		}
	})

	// Incoming: an out-of-graph child in another schema references the in-graph
	// root with ON DELETE CASCADE. Validation must fail, specifically with
	// FK_COVERAGE_CHECK (not an earlier check), naming the offending child.
	t.Run("incoming_cross_schema_rejected", func(t *testing.T) {
		vgSetupIncoming(t, ctx, srcDB, srcSchema)
		defer vgDropIncoming(ctx, srcDB)

		checker := vgBuildChecker(t, srcDB, destDB, srcSchema, destSchema)
		err := checker.RunAllChecks(ctx, false)
		if err == nil {
			t.Fatal("expected FK_COVERAGE_CHECK for the incoming cross-schema FK, got nil — " +
				"an external ON DELETE CASCADE would delete uncopied rows")
		}
		var pfErr *PreflightError
		if !errors.As(err, &pfErr) {
			t.Fatalf("expected *PreflightError, got %T: %v", err, err)
		}
		if pfErr.Check != "FK_COVERAGE_CHECK" {
			t.Fatalf("expected FK_COVERAGE_CHECK (all earlier checks should pass), got %q: %v", pfErr.Check, err)
		}
		if !strings.Contains(err.Error(), vgIncChild) {
			t.Fatalf("expected error to name the offending cross-schema child %q, got: %v", vgIncChild, err)
		}
	})

	// Outgoing: the in-graph root references a parent in another schema. Deleting
	// the archived subgraph never cascades up into that parent, so this is safe
	// and validation passes.
	t.Run("outgoing_cross_schema_passes", func(t *testing.T) {
		vgSetupOutgoing(t, ctx, srcDB, srcSchema)
		defer vgDropOutgoing(ctx, srcDB)

		checker := vgBuildChecker(t, srcDB, destDB, srcSchema, destSchema)
		if err := checker.RunAllChecks(ctx, false); err != nil {
			t.Fatalf("expected an outgoing cross-schema FK (in-graph child -> external parent) to pass "+
				"full preflight, got: %v", err)
		}
	})
}
