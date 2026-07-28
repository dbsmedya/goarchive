//go:build integration

package archiver

import (
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// ============================================================================
// Permission checks — JOB_SCHEMA_PERMISSION_CHECK, DEST_WRITE_PERMISSION_CHECK,
// SOURCE_DELETE_PERMISSION_CHECK — plus today's GLOBAL-PRIVILEGE SHORT-CIRCUIT.
//
// Every test here runs preflight as a disposable, precisely-scoped account
// (characterization_accounts_test.go) rather than as root, because root holds
// every privilege globally and tablesMissingPrivilege returns immediately on a
// global hit (preflight.go:1390-1392) — as root, all three checks are vacuous.
//
// RELEVANT PART OF THE FIXED CHECK ORDER (RunWithProfile, preflight.go:138-240).
// Each test's assertion is only meaningful if the checks BEFORE the asserted one
// pass, so the exact position matters:
//
//	 1 TABLE_EXISTENCE / PK_COLUMN / COMPOSITE_PK / ROOT_PK_TYPE       source
//	 2 STORAGE_ENGINE / INVISIBLE_COLUMN                               source
//	 3 JOB_SCHEMA_PERMISSION_CHECK        (all five commands)          destination
//	 4 DEST_TABLE_EXISTENCE                (skipped by purge)          destination
//	 5 DEST_SCHEMA_COMPATIBILITY           (skipped by purge)          destination
//	 6 DEST_WRITE_PERMISSION_CHECK         (skipped by purge)          destination
//	 7 DEST_INSERT_TRIGGER                 (skipped by purge)          destination
//	 8 FK_INDEX / FK_COVERAGE_VISIBILITY / FK_COVERAGE / INTERNAL_FK   source
//	 9 SOURCE_DELETE_PERMISSION_CHECK      (archive, purge, validate)  source
//	10 DELETE_TRIGGER / CASCADE warning    (archive, purge, validate)  source
//
// Consequences exploited below:
//
//   - A test that restricts the DESTINATION account leaves every source-side
//     check (1, 2, 8, 9, 10) running as root, so none of them can fire.
//   - A test that restricts the SOURCE account leaves every destination-side
//     check (3-7) running as root. Steps 1, 2 and 8 still run as the restricted
//     account, so those fixtures grant SELECT globally — which also satisfies
//     FK_COVERAGE_VISIBILITY_CHECK (step 8), the only other check that inspects
//     privileges on the source.
//   - JOB_SCHEMA (3) strictly precedes DEST_WRITE (6) and requires INSERT at
//     SCHEMA scope on the tracking schema with NO per-table fallback. In the
//     default fixture the tracking schema IS the destination schema, so any grant
//     that clears JOB_SCHEMA also clears DEST_WRITE. The DEST_WRITE fixtures
//     therefore use a SEPARATE tracking schema (chrTrackingSchema).
//
// D1 CONTEXT — READ BEFORE TOUCHING THESE ASSERTIONS IN PHASES 020-022.
//
// Deviation D1 narrows passing to GrantPresent only; GrantAbsent,
// GrantUnconfirmed and GrantUnknown all fail closed. It is tempting to read the
// two pass-asserting tests below as D1's amendment targets. They are NOT.
//
//   - TestCharacterizationDestWriteTableScopeSatisfies — a DIRECT table grant
//     proves the object, so it stays GrantPresent. Phase 020 states it "must pass
//     unamended". A phase-020 diff that touches it is a REGRESSION.
//
//   - TestCharacterizationGlobalPrivilegeShortCircuit — without
//     @@global.partial_revokes a global grant still evaluates GrantPresent, and
//     the test containers run with it OFF. So this also survives D1 unchanged.
//     Phase 021 does not amend it; it ADDS a separate partial-revokes fixture
//     (TestD1PartialRevokesGlobalGrantFailsDirectGrantPasses) that turns
//     partial_revokes ON to demonstrate the behaviour D1 actually changes.
//
// What D1 does change is the ROLE-HELD case, which this file does not pin: 1.8
// resolves CURRENT_ROLE() and passes such an account, while D1 fails it closed.
// Phase 021 adds that fixture too.
//
// In short: no assertion in this file is a D1 amendment target. If a later phase
// needs one of these to flip, that is a signal to re-read this comment first.
// ============================================================================

// chrPermDDL is the single-table fixture shared by every test in this file. No
// child tables: the permission checks iterate graph.AllNodes() uniformly and a
// second table would only add noise. Aggregation across tables is already pinned
// by phase 006.
const chrPermDDL = "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB"

// TestCharacterizationJobSchemaPermission pins JOB_SCHEMA_PERMISSION_CHECK: the
// destination account must hold CREATE, SELECT, INSERT and UPDATE on the tracking
// schema, at GLOBAL or SCHEMA scope only. The fixture withholds exactly one of the
// four (CREATE) and holds the other three, so the failure cannot be attributed to
// anything but this check.
//
// It fires for ALL FIVE commands, including purge: the tracking-schema check sits
// OUTSIDE the `profile != PreflightProfileSourceOnly` guard (preflight.go:176-182)
// because archiver_job is written by archive, purge and copy-only alike.
//
// Tables is asserted EMPTY — this check reports the missing privileges in Message
// and never populates Tables, which is what distinguishes it from a
// DEST_WRITE_PERMISSION_CHECK failure carrying "orders".
func TestCharacterizationJobSchemaPermission(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, chrPermDDL)
	f.ExecDest(t, ctx, chrPermDDL)

	// SELECT/INSERT/UPDATE but deliberately NO CREATE on the tracking schema.
	acct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
	destAsAcct := chrOpenAs(t, acct, f.DestSchema)

	// Proof the fixture withholds what it claims to withhold: zero CREATE rows at
	// every scope, and the other three privileges present at schema scope.
	chrAssertGrantScopes(t, ctx, f.DestDB, acct, "CREATE", f.DestSchema, 0, 0, 0)
	for _, held := range []string{"SELECT", "INSERT", "UPDATE"} {
		chrAssertGrantScopes(t, ctx, f.DestDB, acct, held, f.DestSchema, 0, 1, 0)
	}

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAs(t, graph.NewGraph("orders", "id"), f.SourceDB, destAsAcct)
			err := chrRun(t, ctx, c, cmd, false)
			chrAssertCheck(t, err, "JOB_SCHEMA_PERMISSION_CHECK", []string{})
		})
	}
}

// TestCharacterizationJobSchemaPermissionSatisfied is the positive control for the
// test above: the same fixture with CREATE added passes for every command. The two
// tests differ by exactly one privilege, which is what proves the negative test
// fails for the reason it claims.
//
// Because a pass-asserting test cannot be mutation-tested by removing the defect,
// the grant shape is read back from information_schema: all four privileges are
// held at SCHEMA scope only, none globally and none per-table.
func TestCharacterizationJobSchemaPermissionSatisfied(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, chrPermDDL)
	f.ExecDest(t, ctx, chrPermDDL)

	acct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
	destAsAcct := chrOpenAs(t, acct, f.DestSchema)

	for _, priv := range []string{"CREATE", "SELECT", "INSERT", "UPDATE"} {
		chrAssertGrantScopes(t, ctx, f.DestDB, acct, priv, f.DestSchema, 0, 1, 0)
	}

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAs(t, graph.NewGraph("orders", "id"), f.SourceDB, destAsAcct)
			chrAssertPasses(t, chrRun(t, ctx, c, cmd, false))
		})
	}

	// non-vacuous: proves the passes above are not vacuous. The SAME account is
	// re-pointed (via CheckerAsWithJobSchema) at a DIFFERENT tracking schema it
	// holds no privileges on whatsoever. If preflight had silently stopped
	// evaluating ValidateJobSchemaPermissions, this would incorrectly pass too;
	// instead it must still hit JOB_SCHEMA_PERMISSION_CHECK.
	t.Run("non-vacuous", func(t *testing.T) {
		otherJobSchema := chrTrackingSchema(t, ctx, f.DestDB)
		c := f.CheckerAsWithJobSchema(t, graph.NewGraph("orders", "id"), f.SourceDB, destAsAcct, otherJobSchema)
		err := chrRun(t, ctx, c, chrCommands[0], false)
		chrAssertCheck(t, err, "JOB_SCHEMA_PERMISSION_CHECK", []string{})
	})
}

// TestCharacterizationJobSchemaNoTableScopeFallback pins the deliberate ASYMMETRY
// between the two destination permission checks: schemaMissingPrivileges consults
// USER_PRIVILEGES and SCHEMA_PRIVILEGES only (preflight.go:1305-1335) — it never
// looks at TABLE_PRIVILEGES, unlike tablesMissingPrivilege.
//
// The fixture is the realistic operator scenario: a DBA who has already created
// archiver_job by hand and granted INSERT on that one table. GoArchive still
// refuses, because the per-job archiver_job_log_<id> tables do not exist yet and
// cannot be granted on.
func TestCharacterizationJobSchemaNoTableScopeFallback(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, chrPermDDL)
	f.ExecDest(t, ctx, chrPermDDL)

	jobSchema := chrTrackingSchema(t, ctx, f.DestDB)
	f.ExecDest(t, ctx, "CREATE TABLE `"+jobSchema+"`.`archiver_job` (id bigint NOT NULL AUTO_INCREMENT, job_name varchar(64) NOT NULL, PRIMARY KEY (id), UNIQUE KEY uq_job_name (job_name)) ENGINE=InnoDB")

	acct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, UPDATE ON `"+jobSchema+"`.* TO {{ACCOUNT}}",
		"GRANT INSERT ON `"+jobSchema+"`.`archiver_job` TO {{ACCOUNT}}",
		"GRANT SELECT ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
	destAsAcct := chrOpenAs(t, acct, f.DestSchema)

	// INSERT is held, but only at TABLE scope on the tracking schema.
	chrAssertGrantScopes(t, ctx, f.DestDB, acct, "INSERT", jobSchema, 0, 0, 1)

	c := f.CheckerAsWithJobSchema(t, graph.NewGraph("orders", "id"), f.SourceDB, destAsAcct, jobSchema)
	err := chrRun(t, ctx, c, chrCommands[0], false)
	chrAssertCheck(t, err, "JOB_SCHEMA_PERMISSION_CHECK", []string{})
}

// TestCharacterizationDestWritePermission pins DEST_WRITE_PERMISSION_CHECK: INSERT
// is required on every graph table, the failure names the offending tables, and the
// check is SKIPPED by purge (PreflightProfileSourceOnly).
//
// FIXTURE ISOLATION (the plan's Step 4 hazard, which did occur): JOB_SCHEMA runs
// first and needs INSERT at SCHEMA scope on the tracking schema. With the default
// fixture's jobSchema == DestSchema, the only grant that clears JOB_SCHEMA also
// clears DEST_WRITE. This test therefore uses a SEPARATE tracking schema and
// CheckerAsWithJobSchema: INSERT is granted on the tracking schema and withheld
// everywhere in the data schema.
func TestCharacterizationDestWritePermission(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, chrPermDDL)
	f.ExecDest(t, ctx, chrPermDDL)

	jobSchema := chrTrackingSchema(t, ctx, f.DestDB)
	acct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+jobSchema+"`.* TO {{ACCOUNT}}",
		"GRANT SELECT ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
	destAsAcct := chrOpenAs(t, acct, f.DestSchema)

	// The isolation, stated as data: INSERT exists at schema scope on the tracking
	// schema and NOWHERE on the data schema. Without this, a fixture that leaked
	// schema-scope INSERT into the data schema would fail on JOB_SCHEMA earlier and
	// the DEST_WRITE assertion would never be reached.
	chrAssertGrantScopes(t, ctx, f.DestDB, acct, "INSERT", jobSchema, 0, 1, 0)
	chrAssertGrantScopes(t, ctx, f.DestDB, acct, "INSERT", f.DestSchema, 0, 0, 0)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAsWithJobSchema(t, graph.NewGraph("orders", "id"), f.SourceDB, destAsAcct, jobSchema)
			err := chrRun(t, ctx, c, cmd, false)
			if cmd.Name == "purge" {
				chrAssertPasses(t, err)
				return
			}
			chrAssertCheck(t, err, "DEST_WRITE_PERMISSION_CHECK", []string{"orders"})
		})
	}
}

// TestCharacterizationDestWriteTableScopeSatisfies is the positive control for the
// test above AND the pin on today's per-table fallback: the fixture is identical
// except for one extra table-scope grant, and every command now passes.
//
// The privilege is held at TABLE scope only — proved from information_schema — so
// the pass can only come from tablesMissingPrivilege's third query
// (TABLE_PRIVILEGES, preflight.go:1408-1421).
//
// This outcome SURVIVES deviation D1 unchanged: a direct table grant proves the
// object, so it evaluates GrantPresent. Phase 020 states it "must pass unamended".
// A phase-020 diff that touches this assertion is a REGRESSION, not an amendment.
func TestCharacterizationDestWriteTableScopeSatisfies(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, chrPermDDL)
	f.ExecDest(t, ctx, chrPermDDL)

	jobSchema := chrTrackingSchema(t, ctx, f.DestDB)
	acct := chrCreateAccount(t, ctx, f.DestDB, "destination",
		"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+jobSchema+"`.* TO {{ACCOUNT}}",
		"GRANT SELECT ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}",
		"GRANT INSERT ON `"+f.DestSchema+"`.`orders` TO {{ACCOUNT}}")
	destAsAcct := chrOpenAs(t, acct, f.DestSchema)

	chrAssertGrantScopes(t, ctx, f.DestDB, acct, "INSERT", f.DestSchema, 0, 0, 1)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAsWithJobSchema(t, graph.NewGraph("orders", "id"), f.SourceDB, destAsAcct, jobSchema)
			chrAssertPasses(t, chrRun(t, ctx, c, cmd, false))
		})
	}

	// non-vacuous: proves the passes above are not vacuous. Run AFTER the
	// positive subtests (widening the destination column breaks the fixture
	// they rely on) and confirm DEST_SCHEMA_COMPATIBILITY_CHECK fires for the
	// SAME account/tables once the destination column type is made to differ
	// from the source. If preflight had silently stopped evaluating
	// ValidateDestinationSchemaCompatibility for this account/fixture shape,
	// this would incorrectly pass too.
	t.Run("non-vacuous", func(t *testing.T) {
		f.ExecDest(t, ctx, "ALTER TABLE orders MODIFY note varchar(128) NULL")
		c := f.CheckerAsWithJobSchema(t, graph.NewGraph("orders", "id"), f.SourceDB, destAsAcct, jobSchema)
		err := chrRun(t, ctx, c, chrCommands[0], false)
		chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"orders"})
	})
}

// TestCharacterizationSourceDeletePermission pins SOURCE_DELETE_PERMISSION_CHECK,
// which runs only under PreflightProfileFull and PreflightProfileSourceOnly —
// archive, purge and validate. copy-only and dry-run PASS the identical fixture.
//
// The account holds SELECT globally, which is load-bearing twice over: it lets the
// earlier source-side structural checks read information_schema, and it satisfies
// FK_COVERAGE_VISIBILITY_CHECK (step 8) — the only other privilege-sensitive source
// check, and the one that would otherwise fire first for archive/purge/dry-run/
// validate and mask this assertion entirely.
func TestCharacterizationSourceDeletePermission(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, chrPermDDL)
	f.ExecDest(t, ctx, chrPermDDL)

	acct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}")
	sourceAsAcct := chrOpenAs(t, acct, f.SourceSchema)

	// SELECT globally present; DELETE absent at every scope.
	chrAssertGrantScopes(t, ctx, f.SourceDB, acct, "SELECT", f.SourceSchema, 1, 0, 0)
	chrAssertGrantScopes(t, ctx, f.SourceDB, acct, "DELETE", f.SourceSchema, 0, 0, 0)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAs(t, graph.NewGraph("orders", "id"), sourceAsAcct, f.DestDB)
			err := chrRun(t, ctx, c, cmd, false)
			switch cmd.Name {
			case "archive", "purge", "validate":
				chrAssertCheck(t, err, "SOURCE_DELETE_PERMISSION_CHECK", []string{"orders"})
			default: // copy-only, dry-run
				chrAssertPasses(t, err)
			}
		})
	}
}

// TestCharacterizationGlobalPrivilegeShortCircuit pins today's short-circuit: a
// GLOBAL grant satisfies the per-table check and tablesMissingPrivilege returns
// immediately, without consulting SCHEMA_PRIVILEGES or TABLE_PRIVILEGES
// (preflight.go:1390-1392).
//
// The account is the SAME as the test above with DELETE added globally, and the
// grant shape is read back to prove the pass cannot come from anywhere else: the
// account has ZERO schema-scope and ZERO table-scope DELETE rows, so the only row
// that can satisfy the check is the global one.
//
// This outcome SURVIVES deviation D1 on the default test containers, which run
// with @@global.partial_revokes OFF: without partial revokes a global grant still
// evaluates GrantPresent, so the account still passes. Phase 021 does not amend
// this assertion — it ADDS TestD1PartialRevokesGlobalGrantFailsDirectGrantPasses,
// which turns partial_revokes ON and is where the behaviour D1 changes becomes
// observable. A phase-021 diff that touches this assertion is a REGRESSION.
func TestCharacterizationGlobalPrivilegeShortCircuit(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)
	f.ExecSource(t, ctx, chrPermDDL)
	f.ExecDest(t, ctx, chrPermDDL)

	acct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT SELECT, DELETE ON *.* TO {{ACCOUNT}}")
	sourceAsAcct := chrOpenAs(t, acct, f.SourceSchema)

	// DELETE is held ONLY at global scope: no schema row, no table row.
	chrAssertGrantScopes(t, ctx, f.SourceDB, acct, "DELETE", f.SourceSchema, 1, 0, 0)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAs(t, graph.NewGraph("orders", "id"), sourceAsAcct, f.DestDB)
			chrAssertPasses(t, chrRun(t, ctx, c, cmd, false))
		})
	}

	// non-vacuous: proves the passes above are not vacuous. Run AFTER the
	// positive subtests (dropping the destination table breaks the fixture they
	// rely on) and confirm an EARLIER, unrelated check —
	// DEST_TABLE_EXISTENCE_CHECK, step 4 — still fires for the same account. If
	// preflight had silently stopped running checks for this account/fixture
	// shape, the passes above would prove nothing.
	t.Run("non-vacuous", func(t *testing.T) {
		f.ExecDest(t, ctx, "DROP TABLE orders")
		c := f.CheckerAs(t, graph.NewGraph("orders", "id"), sourceAsAcct, f.DestDB)
		err := chrRun(t, ctx, c, chrCommands[0], false)
		chrAssertCheck(t, err, "DEST_TABLE_EXISTENCE_CHECK", []string{"orders"})
	})
}
