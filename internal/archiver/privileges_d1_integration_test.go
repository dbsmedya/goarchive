//go:build integration

package archiver

import (
	"context"
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

const d1DDL = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"

// TestD1DirectGrantPasses is the positive control for invariant I2: a DIRECT
// schema-level grant evaluates GrantPresent and passes.
func TestD1DirectGrantPasses(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, d1DDL)
	f.ExecDest(t, ctx, d1DDL)

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}",
		"GRANT DELETE ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}") // DELETE is DIRECT + schema-scoped: that is what this test proves
	dstAcct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")

	c := f.CheckerAs(t, graph.NewGraph("orders", "id"),
		chrOpenAs(t, srcAcct, f.SourceSchema), chrOpenAs(t, dstAcct, f.DestSchema))
	chrAssertPasses(t, chrRun(t, ctx, c, chrCommands[1], false)) // purge: source-only profile
}

// TestD1RoleHeldGrantFails is deviation D1's headline behaviour change: a privilege held
// only through an ACTIVE ROLE evaluates GrantUnconfirmed and fails closed. GoArchive 1.8
// resolved CURRENT_ROLE() and passed this account.
func TestD1RoleHeldGrantFails(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, d1DDL)
	f.ExecDest(t, ctx, d1DDL)

	roleName := "chr_role_" + f.SourceSchema
	if _, err := f.SourceDB.ExecContext(ctx, "DROP ROLE IF EXISTS `"+roleName+"`"); err != nil {
		t.Fatalf("drop role: %v", err)
	}
	if _, err := f.SourceDB.ExecContext(ctx, "CREATE ROLE `"+roleName+"`"); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() {
		if _, err := f.SourceDB.ExecContext(context.Background(), "DROP ROLE IF EXISTS `"+roleName+"`"); err != nil {
			t.Logf("cleanup drop role: %v", err)
		}
	})
	if _, err := f.SourceDB.ExecContext(ctx,
		"GRANT SELECT, DELETE ON `"+f.SourceSchema+"`.* TO `"+roleName+"`"); err != nil {
		t.Fatalf("grant to role: %v", err)
	}

	// The account gets PROCESS and GLOBAL SELECT directly, but NOT delete — DELETE
	// arrives only through the default-activated role. That is the whole fixture.
	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}",
		"GRANT `"+roleName+"` TO {{ACCOUNT}}",
		"SET DEFAULT ROLE ALL TO {{ACCOUNT}}")
	dstAcct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")

	c := f.CheckerAs(t, graph.NewGraph("orders", "id"),
		chrOpenAs(t, srcAcct, f.SourceSchema), chrOpenAs(t, dstAcct, f.DestSchema))

	err := chrRun(t, ctx, c, chrCommands[1], false) // purge
	chrAssertCheck(t, err, "SOURCE_DELETE_PERMISSION_CHECK", []string{"orders"})
}

// TestD1PartialRevokesGlobalGrantFailsDirectGrantPasses proves the false-pass D1 closes.
// With @@global.partial_revokes ON, a bare global grant no longer proves any object; a
// direct schema grant does.
//
// The test toggles a GLOBAL server variable, so it must run alone. It restores the
// original value in cleanup.
func TestD1PartialRevokesGlobalGrantFailsDirectGrantPasses(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, d1DDL)
	f.ExecDest(t, ctx, d1DDL)

	var original string
	if err := f.SourceDB.QueryRowContext(ctx, "SELECT @@global.partial_revokes").Scan(&original); err != nil {
		t.Skipf("server does not expose @@global.partial_revokes: %v", err)
	}
	if _, err := f.SourceDB.ExecContext(ctx, "SET GLOBAL partial_revokes = ON"); err != nil {
		t.Skipf("cannot enable partial_revokes: %v", err)
	}
	t.Cleanup(func() {
		if _, err := f.SourceDB.ExecContext(context.Background(),
			"SET GLOBAL partial_revokes = "+original); err != nil {
			t.Logf("restore partial_revokes: %v", err)
		}
	})

	dstAcct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
	dstDB := chrOpenAs(t, dstAcct, f.DestSchema)

	t.Run("global_only_fails", func(t *testing.T) {
		// SELECT is granted DIRECTLY at schema scope (in addition to the global grant)
		// so SOURCE_SELECT_PERMISSION_CHECK (phase 022, D4) resolves GrantPresent and
		// does not mask the DELETE verdict this subtest exists to pin: DELETE stays
		// global-only, so under partial revokes it is still GrantUnconfirmed and
		// SOURCE_DELETE_PERMISSION_CHECK still fires. Without this addition,
		// SOURCE_SELECT_PERMISSION_CHECK would fire first (it now runs before
		// SOURCE_DELETE_PERMISSION_CHECK), which is D4 working as designed but would
		// invalidate this subtest's original DELETE-specific assertion.
		acct := chrCreateAccount(t, ctx, f.SourceDB, "source",
			"GRANT PROCESS, SELECT, DELETE ON *.* TO {{ACCOUNT}}",
			"GRANT SELECT ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}")
		c := f.CheckerAs(t, graph.NewGraph("orders", "id"), chrOpenAs(t, acct, f.SourceSchema), dstDB)
		err := chrRun(t, ctx, c, chrCommands[1], false)
		chrAssertCheck(t, err, "SOURCE_DELETE_PERMISSION_CHECK", []string{"orders"})
	})

	t.Run("global_plus_direct_schema_grant_passes", func(t *testing.T) {
		acct := chrCreateAccount(t, ctx, f.SourceDB, "source",
			"GRANT PROCESS, SELECT, DELETE ON *.* TO {{ACCOUNT}}",
			"GRANT SELECT, DELETE ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}")
		c := f.CheckerAs(t, graph.NewGraph("orders", "id"), chrOpenAs(t, acct, f.SourceSchema), dstDB)
		chrAssertPasses(t, chrRun(t, ctx, c, chrCommands[1], false))
	})
}
