//go:build integration

package archiver

import (
	"errors"
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// chrAssertRawTables asserts the UNDECORATED-stripping helper is not hiding the
// observed TABLE_TYPE: the operator-visible PreflightError.Tables must carry it.
//
// chrAssertCheck compares names through chrTableNames, which strips everything from
// the first "(" — so "orders(VIEW)" and "orders" are indistinguishable to it. Without
// this assertion a build whose nonBaseTableNames returned bare names would pass the
// characterization suite unchanged.
func chrAssertRawTables(t *testing.T, err error, want []string) {
	t.Helper()
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if len(pe.Tables) != len(want) {
		t.Fatalf("PreflightError.Tables = %v, want %v", pe.Tables, want)
	}
	for i := range want {
		if pe.Tables[i] != want[i] {
			t.Fatalf("PreflightError.Tables = %v, want %v "+
				"(the observed TABLE_TYPE must reach the operator error)", pe.Tables, want)
		}
	}
}

// TestCharacterizationSourceViewRejected pins the explicit non-base-table policy added
// in phase 013: a VIEW named in the graph is rejected by TABLE_EXISTENCE_CHECK for all
// five commands.
//
// Before 2.0 a source view passed name existence and was rejected later by
// PRIMARY_KEY_CHECK — a view has no PRIMARY KEY — which told the operator to fix a
// primary key rather than that the object is not a table. Verified by removing the
// non-base rejection: this fixture falls through to PRIMARY_KEY_CHECK. The
// replacement is sanctioned as deviation D5, which introduces no new check ID.
func TestCharacterizationSourceViewRejected(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders_real (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecSource(t, ctx, "CREATE VIEW orders AS SELECT id FROM orders_real")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			chrAssertCheck(t, err, "TABLE_EXISTENCE_CHECK", []string{"orders"})
			chrAssertRawTables(t, err, []string{"orders(VIEW)"})
		})
	}
}

// TestCharacterizationDestinationViewRejected pins the destination half.
//
// Before 2.0 a destination view passed name existence and was rejected later by
// DEST_SCHEMA_COMPATIBILITY_CHECK, reporting a structural mismatch rather than the
// actual problem. Verified by removing the non-base rejection: this fixture falls
// through to DEST_SCHEMA_COMPATIBILITY_CHECK.
//
// purge passes because RunWithProfile gates the whole destination block on
// `profile != PreflightProfileSourceOnly && p.destinationDB != nil &&
// p.destinationDBName != ""`, and chrCommands maps purge to
// PreflightProfileSourceOnly. So purge never reaches ValidateDestinationTablesExist,
// and its source side here is a clean InnoDB base table with a matching PK — nothing
// else can fail it. Verified against RunWithProfile 2026-07-28.
func TestCharacterizationDestinationViewRejected(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders_real (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE VIEW orders AS SELECT id FROM orders_real")

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			if cmd.Name == "purge" {
				chrAssertPasses(t, err)
				return
			}
			chrAssertCheck(t, err, "DEST_TABLE_EXISTENCE_CHECK", []string{"orders"})
			chrAssertRawTables(t, err, []string{"orders(VIEW)"})
		})
	}
}
