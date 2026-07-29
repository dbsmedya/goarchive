//go:build integration

package archiver

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// TestCharacterizationPKColumnMissing pins PK_COLUMN_CHECK: the configured
// primary_key names a column that does not exist in the source table at all.
func TestCharacterizationPKColumnMissing(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			g := graph.NewGraph("orders", "order_id") // no such column
			err := chrRun(t, ctx, f.Checker(t, g), cmd, false)
			chrAssertCheck(t, err, "PK_COLUMN_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationPKColumnUnconfigured pins the second PK_COLUMN_CHECK path: a
// graph table with no explicitly configured primary_key at all. Config validation
// normally prevents this, so the fixture builds the graph directly.
//
// DEVIATION from the plan's literal code, two-fold:
//
//  1. The plan used the child table name "lines". LINES is a MySQL reserved word
//     (confirmed against the live server: `CREATE TABLE lines (...)` fails with
//     Error 1064 "You have an error in your SQL syntax ... near 'lines (id
//     bigint ...'"). Renamed to "order_lines" throughout, matching phase 004's
//     convention of descriptive, non-reserved child table names (e.g. "shipments").
//  2. The plan built the child edge with a bare g.AddEdgeWithMeta(...) call.
//     graph.AddEdgeWithMeta only records the edge — it does NOT call AddNode — so
//     "order_lines" would be absent from g.Nodes and therefore invisible to
//     AllNodes(). ValidatePrimaryKeyColumns iterates exactly p.graph.AllNodes(), so
//     an invisible child table would never be checked and the test would pass
//     without exercising anything (the trap phase 003/004 documented). The fix
//     below calls g.AddNode explicitly — mirroring what chrAddChild does — but
//     deliberately omits the trailing g.SetPK call, since leaving the child
//     PK-unconfigured is the entire point of this test (chrAddChild always calls
//     SetPK, so it cannot be used here).
func TestCharacterizationPKColumnUnconfigured(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const parentDDL = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
	const childDDL = "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, PRIMARY KEY (id), KEY idx_o (order_id), CONSTRAINT fk_l_o FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, parentDDL)
	f.ExecSource(t, ctx, childDDL)
	f.ExecDest(t, ctx, parentDDL)
	f.ExecDest(t, ctx, childDDL)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			g := graph.NewGraph("orders", "id")
			g.AddNode("order_lines", &graph.Node{
				Name:           "order_lines",
				ForeignKey:     "order_id",
				ReferenceKey:   "id",
				DependencyType: "1-N",
			})
			g.AddEdgeWithMeta("orders", "order_lines", "order_id", "id", "1-N")
			// deliberately NOT calling g.SetPK("order_lines", ...)

			err := chrRun(t, ctx, f.Checker(t, g), cmd, false)
			chrAssertCheck(t, err, "PK_COLUMN_CHECK", []string{"order_lines"})
		})
	}
}

// TestCharacterizationPKColumnCase pins PK_COLUMN_CASE_CHECK: the column exists but
// its server-side spelling differs from the configured value only by ASCII letter
// case. information_schema.COLUMNS.COLUMN_NAME collates case-insensitively, so the
// lookup matches and the case comparison happens in Go (preflight.go:1550).
func TestCharacterizationPKColumnCase(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (Order_Id bigint NOT NULL, PRIMARY KEY (Order_Id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			g := graph.NewGraph("orders", "order_id") // wrong case
			err := chrRun(t, ctx, f.Checker(t, g), cmd, false)
			chrAssertCheck(t, err, "PK_COLUMN_CASE_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationPKColumnCaseBeatsMissing pins the intra-validator ordering:
// when one table has a case-only mismatch and another has a genuinely missing column,
// PK_COLUMN_CASE_CHECK is reported (preflight.go:1559) and PK_COLUMN_CHECK is not.
//
// DEVIATION from the plan's literal code, two-fold, same as
// TestCharacterizationPKColumnUnconfigured above: (1) "lines" renamed to
// "order_lines" — LINES is a MySQL reserved word and fails DDL unquoted; (2) the
// plan's bare g.AddEdgeWithMeta(...) call is replaced with chrAddChild, which is
// directly usable here (unlike the Unconfigured test) because the child's
// configured PK is not absent, it is a real-but-wrong value ("no_such_column"),
// which chrAddChild's childPK parameter expresses directly.
func TestCharacterizationPKColumnCaseBeatsMissing(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const parentDDL = "CREATE TABLE orders (Order_Id bigint NOT NULL, PRIMARY KEY (Order_Id)) ENGINE=InnoDB"
	const childDDL = "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, PRIMARY KEY (id), KEY idx_o (order_id), CONSTRAINT fk_l_o FOREIGN KEY (order_id) REFERENCES orders (Order_Id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, parentDDL)
	f.ExecSource(t, ctx, childDDL)
	f.ExecDest(t, ctx, parentDDL)
	f.ExecDest(t, ctx, childDDL)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			g := graph.NewGraph("orders", "order_id") // case-only mismatch
			chrAddChild(g, "orders", "Order_Id", "order_lines", "order_id", "no_such_column")

			err := chrRun(t, ctx, f.Checker(t, g), cmd, false)
			chrAssertCheck(t, err, "PK_COLUMN_CASE_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationCompositePK pins COMPOSITE_PK_CHECK: the configured column
// exists, but the table's PRIMARY KEY spans more than one column.
func TestCharacterizationCompositePK(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (id bigint NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			chrAssertCheck(t, err, "COMPOSITE_PK_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationPrimaryKeyNotThePK pins PRIMARY_KEY_CHECK: the configured
// column exists and the PK is single-column, but it is a DIFFERENT column.
func TestCharacterizationPrimaryKeyNotThePK(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (id bigint NOT NULL, ref_no bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "ref_no")), cmd, false)
			chrAssertCheck(t, err, "PRIMARY_KEY_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationPrimaryKeyAbsent pins the other PRIMARY_KEY_CHECK path: the
// table has no PRIMARY KEY at all.
func TestCharacterizationPrimaryKeyAbsent(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NULL) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			chrAssertCheck(t, err, "PRIMARY_KEY_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationCompositeBeatsPrimaryKey pins the intra-validator ordering:
// composite is the headline rejection and is reported first (preflight.go:321) even
// when another table has a PK-definition problem.
//
// DEVIATION from the plan's literal code, two-fold, same as
// TestCharacterizationPKColumnUnconfigured above: (1) "lines" renamed to
// "order_lines" — LINES is a MySQL reserved word and fails DDL unquoted; (2) the
// plan's bare g.AddEdgeWithMeta(...) call is replaced with chrAddChild, which is
// directly usable here because the child's configured PK ("id") is a real column
// that exists — the defect is that the table has no PRIMARY KEY at all, which is a
// property of the childDDL, not of how the graph is built.
func TestCharacterizationCompositeBeatsPrimaryKey(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const parentDDL = "CREATE TABLE orders (id bigint NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id)) ENGINE=InnoDB"
	const childDDL = "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, KEY idx_o (order_id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, parentDDL)
	f.ExecSource(t, ctx, childDDL)
	f.ExecDest(t, ctx, parentDDL)
	f.ExecDest(t, ctx, childDDL)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			g := graph.NewGraph("orders", "id")
			chrAddChild(g, "orders", "id", "order_lines", "order_id", "id") // exists, but order_lines has no PRIMARY KEY

			err := chrRun(t, ctx, f.Checker(t, g), cmd, false)
			chrAssertCheck(t, err, "COMPOSITE_PK_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationRootPKTypeUnsupported pins BOTH the failure and its SHAPE:
// ROOT_PK_TYPE_UNSUPPORTED is a plain string-prefixed error, NOT a *PreflightError.
// Spec §2 preserves that shape through the re-platforming, so it is asserted here.
//
// HISTORICAL HAZARD: through 1.8, ValidateRootPKNumeric was the one validator keyed on
// the session default schema (`WHERE TABLE_SCHEMA = DATABASE()`) rather than
// p.sourceDBName, so a query against the wrong pool would silently resolve against
// whatever schema the session happened to default to. f.Checker(t, g) binds to
// f.SourceDB, which newChrFixture opens as a dedicated pool whose DSN default schema is
// f.SourceSchema — never a borrowed shared pool — so that was never actually a risk
// here. Phase 017 moved the validator onto the configured source schema via the run's
// memoized PrimaryKeys fact, so this hazard no longer applies at all; the fixture stays
// dedicated per-test for the unrelated reason recorded on chrFixture (reason 2: `USE`
// session-state leakage on a shared pool).
func TestCharacterizationRootPKTypeUnsupported(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (code varchar(32) NOT NULL, PRIMARY KEY (code)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "code")), cmd, false)
			chrAssertPlainErrorPrefix(t, err, "ROOT_PK_TYPE_UNSUPPORTED:")
		})
	}
}

// TestCharacterizationRootPKIntegerVariantsPass pins the accepted root PK types:
// tinyint, smallint, mediumint, int, integer, bigint — signed or unsigned.
//
// Through 1.8 this classification was attributed to isIntegerRootPKType. As of phase
// 017, ValidateRootPKNumeric classifies via the library's PKInfo.IsInteger
// (validations.CheckPKIntegerType), derived from validations.isIntegerDataType —
// verified byte-identical to isIntegerRootPKType by reading (same six types, same
// strings.ToLower), not by a unit test, because CheckPKIntegerType trusts the supplied
// IsInteger field and never itself classifies DataType. This test is what actually
// exercises the library's real derivation against MySQL, which is precisely why no
// unit-level equivalence test is needed. isIntegerRootPKType itself is unaffected and
// stays alive as loadRootPKMeta's production consumer (phase 018 migrates that path).
func TestCharacterizationRootPKIntegerVariantsPass(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)

	for _, colType := range []string{
		"tinyint", "smallint", "mediumint", "int", "bigint",
		"tinyint unsigned", "bigint unsigned",
	} {
		t.Run(colType, func(t *testing.T) {
			f := newChrFixture(t, ctx)
			ddl := "CREATE TABLE orders (id " + colType + " NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
			f.ExecSource(t, ctx, ddl)
			f.ExecDest(t, ctx, ddl)

			chrAssertPasses(t, chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")),
				chrCommands[0], false))
		})
	}
}
