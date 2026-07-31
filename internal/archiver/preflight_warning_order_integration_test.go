//go:build integration

package archiver

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// TestSchemaCompatEmitsAdvisoriesOnlyForNonFatalTables pins BOTH halves of spec §3.3's
// declared evaluation order:
//
//	(a) a non-fatal table's advisory still emits when a DIFFERENT table is fatal; and
//	(b) advisories belonging to the FATAL table are suppressed.
//
// Both halves need asserting because they are enforced in different places. (a) is the
// warn-loop position in preflight.go (Step 3). (b) is the `continue` in the same loop —
// and it is load-bearing precisely because evaluateSchemaCompatibility does NOT stop at
// the first fatal: it records the fatal and keeps collecting warnings from later diffs.
// That is why order_lines carries a collation difference IN ADDITION to its fatal type
// mismatch; without it, deleting the `continue` would kill no test.
//
// 1.8 emitted advisories for columns preceding the fatal column. That change is a
// permissible output improvement (spec §1.1), not a ledger deviation — no pass/fail
// outcome differs.
func TestSchemaCompatEmitsAdvisoriesOnlyForNonFatalTables(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	// orders: collation-only difference -> advisory, table stays non-fatal.
	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	// order_lines: qty type mismatch -> FATAL, plus a note collation difference that
	// would warn on its own. The advisory must be suppressed because the table is fatal.
	// (`LINES` is a MySQL reserved word — `CREATE TABLE lines` fails with Error 1064.)
	f.ExecSource(t, ctx, "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, qty int NOT NULL, note varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL, PRIMARY KEY (id), KEY idx_o (order_id), CONSTRAINT fk_l_o FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, qty bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL, PRIMARY KEY (id), KEY idx_o (order_id), CONSTRAINT fk_l_o FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB")

	// chrAddChild, NOT AddEdgeWithMeta. AddEdgeWithMeta records only the edge; the table
	// would be absent from g.Nodes and therefore from Graph.AllNodes(), which is what
	// preflightRun.tables is built from (preflight_facts.go:92). order_lines would never
	// be inspected at all, schema compatibility would pass, and FK_COVERAGE_CHECK would
	// then fail on the now-uncovered FK — a green-looking test asserting the wrong check.
	g := graph.NewGraph("orders", "id")
	chrAddChild(g, "orders", "id", "order_lines", "order_id", "id")

	checker := f.Checker(t, g)
	checker.SetVerification(chrCountVerification())

	err := chrRun(t, ctx, checker, chrCommands[0], false)
	chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"order_lines"})

	// Warnings are prefixed "Table <name> " at preflight.go:808, which is what separates
	// the two tables' otherwise identical collation advisories.
	warns := f.WarnMessages()
	if !chrAnyContains(warns, "Table orders column note: collation differs") {
		t.Fatalf("the non-fatal table's advisory must survive another table's fatal; got: %v", warns)
	}
	if chrAnyContains(warns, "Table order_lines") {
		t.Fatalf("advisories from the FATAL table must be suppressed; got: %v", warns)
	}
}

// TestSchemaCompatEmitsAdvisoriesWhenClean is the control: with no fatal table anywhere,
// the same advisory appears. It proves the assertion above is not passing simply because
// the advisory can never be produced.
func TestSchemaCompatEmitsAdvisoriesWhenClean(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL, PRIMARY KEY (id)) ENGINE=InnoDB")

	checker := f.Checker(t, graph.NewGraph("orders", "id"))
	checker.SetVerification(chrCountVerification())

	chrAssertPasses(t, chrRun(t, ctx, checker, chrCommands[0], false))
	if !chrAnyContains(f.WarnMessages(), "Table orders column note: collation differs") {
		t.Fatalf("expected the collation advisory on a clean run, got: %v", f.WarnMessages())
	}
}
