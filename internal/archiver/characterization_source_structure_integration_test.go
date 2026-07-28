//go:build integration

package archiver

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// chrValidRootDDL is a minimal table that passes every check preceding the one under
// test: it exists, is InnoDB, has a single-column integer PRIMARY KEY named id, and
// has no invisible columns.
const chrValidRootDDL = "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB"

// TestCharacterizationTableExistence pins TABLE_EXISTENCE_CHECK: a graph table absent
// from the SOURCE schema fails for all five commands, naming exactly the missing
// tables. The destination has both tables, proving the check is source-scoped.
func TestCharacterizationTableExistence(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, chrValidRootDDL)
	f.ExecDest(t, ctx, chrValidRootDDL)
	f.ExecDest(t, ctx, "CREATE TABLE shipments (id bigint NOT NULL, order_id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			g := graph.NewGraph("orders", "id")
			chrAddChild(g, "orders", "id", "shipments", "order_id", "id")

			err := chrRun(t, ctx, f.Checker(t, g), cmd, false)
			chrAssertCheck(t, err, "TABLE_EXISTENCE_CHECK", []string{"shipments"})
		})
	}
}

// TestCharacterizationStorageEngine pins STORAGE_ENGINE_CHECK: a non-InnoDB graph
// table fails for all five commands. The fixture is otherwise fully valid so no
// earlier check can fire first.
func TestCharacterizationStorageEngine(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const myisamDDL = "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NULL, PRIMARY KEY (id)) ENGINE=MyISAM"
	f.ExecSource(t, ctx, myisamDDL)
	f.ExecDest(t, ctx, chrValidRootDDL)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			chrAssertCheck(t, err, "STORAGE_ENGINE_CHECK", []string{"orders"})
			chrAssertRawTables(t, err, []string{"orders(MyISAM)"})
		})
	}
}

// TestCharacterizationInvisibleColumnPlain pins INVISIBLE_COLUMN_CHECK for a plain
// INVISIBLE column (EXTRA = "INVISIBLE"). Tables payload is "<table>.<column>", so the
// harness's dotted-name preservation is exercised here.
func TestCharacterizationInvisibleColumnPlain(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, chrValidRootDDL)
	f.ExecSource(t, ctx, "ALTER TABLE orders ALTER COLUMN note SET INVISIBLE")
	f.ExecDest(t, ctx, chrValidRootDDL)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			chrAssertCheck(t, err, "INVISIBLE_COLUMN_CHECK", []string{"orders.note"})
		})
	}
}

// TestCharacterizationInvisibleColumnGenerated pins INVISIBLE_COLUMN_CHECK for a
// STORED GENERATED INVISIBLE column. The LIKE '%INVISIBLE%' predicate must catch this
// too (preflight.go:501-503) — a re-platformed check that only looked at a boolean
// "invisible" flag must still produce the same finding.
func TestCharacterizationInvisibleColumnGenerated(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders ("+
		"id bigint NOT NULL, "+
		"amount int NOT NULL, "+
		"doubled int GENERATED ALWAYS AS (amount * 2) STORED INVISIBLE, "+
		"PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders ("+
		"id bigint NOT NULL, "+
		"amount int NOT NULL, "+
		"doubled int GENERATED ALWAYS AS (amount * 2) STORED INVISIBLE, "+
		"PRIMARY KEY (id)) ENGINE=InnoDB")

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			chrAssertCheck(t, err, "INVISIBLE_COLUMN_CHECK", []string{"orders.doubled"})
		})
	}
}

// TestCharacterizationSourceStructureClean is the negative control: the same fixtures
// without their defect must pass all three checks for all five commands. Without it,
// a check that fired unconditionally would look correct above.
func TestCharacterizationSourceStructureClean(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, chrValidRootDDL)
	f.ExecDest(t, ctx, chrValidRootDDL)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			chrAssertPasses(t, chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false))
		})
	}
}
