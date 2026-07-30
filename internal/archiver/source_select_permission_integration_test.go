//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

const srcSelectDDL = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"

// srcSelectPartialRevokes turns @@global.partial_revokes ON for one test and restores the
// prior value in cleanup. Under partial revokes a bare GLOBAL grant no longer proves any
// specific object, so the library resolves object-level SELECT to GrantUnconfirmed while
// 1.8's hasGlobalPrivilege still sees the global row and passes
// FK_COVERAGE_VISIBILITY_CHECK. That combination is the ONLY way a SELECT-less account
// reaches SOURCE_SELECT_PERMISSION_CHECK on the four visibility-enforcing commands before
// phase 025. See "The reachability problem" in this phase's plan.
//
// It mutates a GLOBAL server variable, so every caller must run serially.
// DO NOT add t.Parallel() anywhere in this file.
func srcSelectPartialRevokes(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var original string
	if err := db.QueryRowContext(ctx, "SELECT @@global.partial_revokes").Scan(&original); err != nil {
		t.Skipf("server does not expose @@global.partial_revokes: %v", err)
	}
	if _, err := db.ExecContext(ctx, "SET GLOBAL partial_revokes = ON"); err != nil {
		t.Skipf("cannot enable partial_revokes: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(),
			"SET GLOBAL partial_revokes = "+original); err != nil {
			t.Logf("restore partial_revokes: %v", err)
		}
	})
}

// srcSelectDest builds the destination account every fixture here shares. Its grants are
// DIRECT and schema-scoped, so they stay GrantPresent even with partial revokes on the
// source server — and the destination is a different server anyway (port 3307).
func srcSelectDest(t *testing.T, ctx context.Context, f *chrFixture) *sql.DB {
	t.Helper()
	acct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
	return chrOpenAs(t, acct, f.DestSchema)
}

// TestSourceSelectGrantAbsentOnCopyOnly exercises the genuine GrantAbsent path —
// construction (A). The account holds DELETE and INSERT at TABLE scope, which makes the
// table visible in information_schema, and holds SELECT nowhere at all. copy-only skips
// FK visibility, so this reaches the check with a true absence rather than an
// unconfirmable global row.
//
// This fixture is also the primary floor for the PrivilegeSelect → PrivilegeDelete
// mutant: the account provably HOLDS table-scope DELETE, so a check asking for DELETE
// would pass and this assertion would fail.
//
// No PROCESS grant: copy-only skips visibility both now and after phase 025.
func TestSourceSelectGrantAbsentOnCopyOnly(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, srcSelectDDL)
	f.ExecDest(t, ctx, srcSelectDDL)

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT DELETE, INSERT ON `"+f.SourceSchema+"`.`orders` TO {{ACCOUNT}}")

	c := f.CheckerAs(t, graph.NewGraph("orders", "id"),
		chrOpenAs(t, srcAcct, f.SourceSchema), srcSelectDest(t, ctx, f))

	err := chrRun(t, ctx, c, chrCommands[2], false) // copy-only
	chrAssertCheck(t, err, "SOURCE_SELECT_PERMISSION_CHECK", []string{"orders"})
}

// TestSourceSelectAppliesToAllFiveCommands is D4's headline contract and the reason the
// check sits outside the delete-capable profile block: EVERY command reads source rows or
// estimates from them, so every command must reject this account.
//
// Construction (B). SELECT is held ONLY at global scope, so with partial revokes on it is
// GrantUnconfirmed for `orders` — while 1.8's hasGlobalPrivilege still sees that global row
// and lets all five commands past FK_COVERAGE_VISIBILITY_CHECK. DELETE is granted DIRECTLY
// at schema scope so that SOURCE_DELETE (position 17) would pass: this test must fail on
// SELECT and nothing else.
func TestSourceSelectAppliesToAllFiveCommands(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, srcSelectDDL)
	f.ExecDest(t, ctx, srcSelectDDL)

	srcSelectPartialRevokes(t, ctx, f.SourceDB)

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}",
		"GRANT DELETE ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}")
	srcDB := chrOpenAs(t, srcAcct, f.SourceSchema)
	dstDB := srcSelectDest(t, ctx, f)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAs(t, graph.NewGraph("orders", "id"), srcDB, dstDB)
			err := chrRun(t, ctx, c, cmd, false)
			chrAssertCheck(t, err, "SOURCE_SELECT_PERMISSION_CHECK", []string{"orders"})
		})
	}
}

// TestSourceSelectRunsBeforeDeletePermission pins the ordering spec §3.2 specifies: an
// account missing BOTH privileges reports the SELECT check, because 16 precedes 17.
// Construction (B), with DELETE deliberately withheld this time.
func TestSourceSelectRunsBeforeDeletePermission(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, srcSelectDDL)
	f.ExecDest(t, ctx, srcSelectDDL)

	srcSelectPartialRevokes(t, ctx, f.SourceDB)

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}")

	c := f.CheckerAs(t, graph.NewGraph("orders", "id"),
		chrOpenAs(t, srcAcct, f.SourceSchema), srcSelectDest(t, ctx, f))

	err := chrRun(t, ctx, c, chrCommands[0], false) // archive: runs both checks
	chrAssertCheck(t, err, "SOURCE_SELECT_PERMISSION_CHECK", []string{"orders"})
}

// TestSourceSelectRunsAfterInternalFKCoverage pins the other half of the ordering: with an
// uncovered internal FK (15) AND an unprovable SELECT (16), the earlier check wins.
//
// The graph shape is copied from TestCharacterizationInternalFKCoverage
// (characterization_fk_integration_test.go:427) — the proven construction. Note the table
// is `order_lines`, which is what chrLinesDDL creates; an earlier draft of this plan said
// `lines`, which does not exist and would have died at TABLE_EXISTENCE_CHECK.
func TestSourceSelectRunsAfterInternalFKCoverage(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, chrOrdersDDL)
	f.ExecSource(t, ctx, chrLinesDDL)
	f.ExecDest(t, ctx, chrOrdersDDL)
	f.ExecDest(t, ctx, chrLinesDDL)

	srcSelectPartialRevokes(t, ctx, f.SourceDB)

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}")

	// Both tables in the graph, NO edge between them. A bare AddNode is correct here —
	// chrAddChild would create the very edge this fixture needs absent.
	g := graph.NewGraph("orders", "id")
	g.SetPK("order_lines", "id")
	g.AddNode("order_lines", nil)

	c := f.CheckerAs(t, g, chrOpenAs(t, srcAcct, f.SourceSchema), srcSelectDest(t, ctx, f))

	err := chrRun(t, ctx, c, chrCommands[0], false) // archive
	chrAssertCheck(t, err, "INTERNAL_FK_COVERAGE", nil)
}

// TestSourceSelectSatisfied is the positive control, and it runs UNDER partial revokes on
// purpose: that makes the DIRECT schema-scope SELECT row load-bearing. The global SELECT
// row alone would only ever be GrantUnconfirmed, so a pass here can only come from the
// direct grant. Global SELECT is still required — it is what clears 1.8's visibility
// check at position 13 for the four enforcing commands.
func TestSourceSelectSatisfied(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, srcSelectDDL)
	f.ExecDest(t, ctx, srcSelectDDL)

	srcSelectPartialRevokes(t, ctx, f.SourceDB)

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT, DELETE ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}")
	srcDB := chrOpenAs(t, srcAcct, f.SourceSchema)
	dstDB := srcSelectDest(t, ctx, f)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAs(t, graph.NewGraph("orders", "id"), srcDB, dstDB)
			chrAssertPasses(t, chrRun(t, ctx, c, cmd, false))
		})
	}
}
