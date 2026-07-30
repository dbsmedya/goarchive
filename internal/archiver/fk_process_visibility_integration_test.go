//go:build integration

package archiver

import (
	"context"
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

const d2DDL = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"

// ============================================================================
// Deviation D2 (spec §4, phase 025): FK_COVERAGE_VISIBILITY_CHECK's completeness
// proof moves from 1.8's global-SELECT reasoning to the library's PROCESS-gated
// InnoDB registry read (VisibilityComplete). These fixtures need real MySQL:
// sqlmock cannot model whether the server actually grants or denies the
// PROCESS-gated INNODB_FOREIGN query, which is exactly the fact under test.
// ============================================================================

// TestD2EffectivePROCESSPasses is the positive control: an account holding PROCESS
// DIRECTLY passes FK_COVERAGE_VISIBILITY_CHECK on every command that enforces it,
// because the library's PROCESS-gated InnoDB registry read succeeds and reports
// VisibilityComplete.
func TestD2EffectivePROCESSPasses(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, d2DDL)
	f.ExecDest(t, ctx, d2DDL)

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}",
		"GRANT DELETE ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}")
	dstAcct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
	srcDB := chrOpenAs(t, srcAcct, f.SourceSchema)
	dstDB := chrOpenAs(t, dstAcct, f.DestSchema)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAs(t, graph.NewGraph("orders", "id"), srcDB, dstDB)
			chrAssertPasses(t, chrRun(t, ctx, c, cmd, false))
		})
	}
}

// TestD2RoleHeldPROCESSPasses proves invariant I1's provenance rule (spec §3.1): the
// proof is the SUCCESS of the statement, not direct provenance of the privilege, so a
// role-held PROCESS is exactly as good as a directly granted one. This is the opposite
// of deviation D1, where a role-held privilege evaluates GrantUnconfirmed and fails
// closed (see TestD1RoleHeldGrantFails and Ambiguity 1) — D1's proof is privilege
// bookkeeping the account cannot read for its own roles, while D2's proof is that the
// PROCESS-gated read itself succeeded, which a role grants identically to a direct one.
//
// SELECT and DELETE are granted DIRECTLY so this fixture isolates the PROCESS
// provenance question; only PROCESS arrives through the default-activated role.
func TestD2RoleHeldPROCESSPasses(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, d2DDL)
	f.ExecDest(t, ctx, d2DDL)

	roleName := "chr_role_process_" + f.SourceSchema
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
	if _, err := f.SourceDB.ExecContext(ctx, "GRANT PROCESS ON *.* TO `"+roleName+"`"); err != nil {
		t.Fatalf("grant PROCESS to role: %v", err)
	}

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}",
		"GRANT DELETE ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}",
		"GRANT `"+roleName+"` TO {{ACCOUNT}}",
		"SET DEFAULT ROLE ALL TO {{ACCOUNT}}")
	dstAcct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")

	c := f.CheckerAs(t, graph.NewGraph("orders", "id"),
		chrOpenAs(t, srcAcct, f.SourceSchema), chrOpenAs(t, dstAcct, f.DestSchema))
	chrAssertPasses(t, chrRun(t, ctx, c, chrCommands[0], false)) // archive: enforces visibility
}

// TestD2NoPROCESSFailsClosedExceptCopyOnly is D2's fail-closed headline: an account
// with global SELECT (so the run gets far enough to exercise visibility at all — see
// Ambiguity 4, a wholly unprivileged account dies earlier at TABLE_EXISTENCE_CHECK) but
// NO PROCESS anywhere fails FK_COVERAGE_VISIBILITY_CHECK for archive, purge, dry-run and
// validate — the four commands that enforce it — while copy-only, which drops the
// visibility flavour entirely, still passes cleanly.
//
// The copy-only pass is genuine, not a fixture accident that merely avoids failing on
// visibility: @@global.partial_revokes is left OFF here (deliberately, unlike the D1/D4
// fixtures elsewhere in this package that toggle it ON), so the account's global SELECT
// resolves to GrantPresent (grants.go, resolve) and SOURCE_SELECT_PERMISSION_CHECK at
// position 16 is satisfied too — copy-only reaches the end of its profile with no error
// at all.
func TestD2NoPROCESSFailsClosedExceptCopyOnly(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, d2DDL)
	f.ExecDest(t, ctx, d2DDL)

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}")
	dstAcct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
	srcDB := chrOpenAs(t, srcAcct, f.SourceSchema)
	dstDB := chrOpenAs(t, dstAcct, f.DestSchema)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAs(t, graph.NewGraph("orders", "id"), srcDB, dstDB)
			err := chrRun(t, ctx, c, cmd, false)
			if cmd.Name == "copy-only" {
				chrAssertPasses(t, err)
				return
			}
			chrAssertCheck(t, err, "FK_COVERAGE_VISIBILITY_CHECK", nil)
		})
	}
}

// TestD2VisibilityBeatsCoverage proves the visibility flavour is judged FIRST even
// though CheckFKClosure appends its finding LAST (see the library facts this phase's
// plan verifies). The fixture plants BOTH defects simultaneously: an external,
// uncovered incoming FK (chrCreateExternalChild), which alone would report
// FK_COVERAGE_CHECK, and a source account with no PROCESS anywhere, which alone would
// report FK_COVERAGE_VISIBILITY_CHECK. RunWithProfile must report the visibility check,
// preserving 1.8's stage order — swapping the two ValidateForeignKey* calls in
// RunWithProfile flips this assertion to FK_COVERAGE_CHECK.
func TestD2VisibilityBeatsCoverage(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, d2DDL)
	f.ExecDest(t, ctx, d2DDL)

	outside := chrExternalSchemaName(f.SourceSchema)
	chrCreateExternalChild(t, ctx, f, outside, "RESTRICT")

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}")
	dstAcct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")

	c := f.CheckerAs(t, graph.NewGraph("orders", "id"),
		chrOpenAs(t, srcAcct, f.SourceSchema), chrOpenAs(t, dstAcct, f.DestSchema))

	err := chrRun(t, ctx, c, chrCommands[0], false) // archive: enforces visibility
	chrAssertCheck(t, err, "FK_COVERAGE_VISIBILITY_CHECK", nil)
}

// TestD2CopyOnlyStillEnforcesCoverage proves the documented asymmetry from the other
// side: copy-only drops the visibility flavour but still enforces the external-edge
// flavour. The source account here HOLDS PROCESS, so visibility is never in question —
// CheckFKClosure would not even emit a visibility finding — yet copy-only must still
// reject the external FK chrCreateExternalChild plants against the in-graph "orders"
// table.
func TestD2CopyOnlyStillEnforcesCoverage(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, d2DDL)
	f.ExecDest(t, ctx, d2DDL)

	outside := chrExternalSchemaName(f.SourceSchema)
	chrCreateExternalChild(t, ctx, f, outside, "CASCADE")

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}")
	dstAcct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")

	c := f.CheckerAs(t, graph.NewGraph("orders", "id"),
		chrOpenAs(t, srcAcct, f.SourceSchema), chrOpenAs(t, dstAcct, f.DestSchema))

	err := chrRun(t, ctx, c, chrCommands[2], false) // copy-only: skips visibility only
	chrAssertCheck(t, err, "FK_COVERAGE_CHECK", nil)
}

// TestSourceSelectGrantAbsentAllFiveCommands is inherited from phase 022 (D4) as a
// required part of THIS phase, not an optional extra: it could not be written before
// now. Phase 022's TestSourceSelectAppliesToAllFiveCommands could only reach
// SOURCE_SELECT_PERMISSION_CHECK on all five commands via construction (B) — partial
// revokes plus a bare global SELECT, which the library resolves to GrantUnconfirmed,
// not a genuine GrantAbsent — because until visibility moved onto the PROCESS-gated
// proof, the four visibility-enforcing commands died at FK_COVERAGE_VISIBILITY_CHECK
// demanding global SELECT before SOURCE_SELECT was ever reached.
//
// Now that visibility clears on PROCESS alone, an account holding PROCESS plus
// table-scoped DELETE/INSERT (so the table is visible in information_schema, per
// TestSourceSelectGrantAbsentOnCopyOnly's construction (A)) but SELECT NOWHERE AT ALL
// reaches SOURCE_SELECT_PERMISSION_CHECK on all five commands with a genuine absence.
//
// Do NOT delete or "clean up" phase 022's partial-revokes tests: they cover the
// GrantUnconfirmed path, this one covers GrantAbsent — different library verdicts,
// both load-bearing under D1's fail-closed rule (deviation D1 / invariant I2: only
// GrantPresent passes).
func TestSourceSelectGrantAbsentAllFiveCommands(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, srcSelectDDL)
	f.ExecDest(t, ctx, srcSelectDDL)

	srcAcct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
		"GRANT DELETE, INSERT ON `"+f.SourceSchema+"`.`orders` TO {{ACCOUNT}}")
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
