//go:build integration

package archiver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.uber.org/zap/zapcore"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// ============================================================================
// Foreign-key family, DELETE triggers, and the CASCADE warning —
// FK_INDEX_CHECK, FK_COVERAGE_VISIBILITY_CHECK, FK_COVERAGE_CHECK,
// INTERNAL_FK_COVERAGE, DELETE_TRIGGER_CHECK, --force-triggers, WarnCascadeRules.
//
// Pins 1.8 behaviour BEFORE any of these validators is re-platformed onto
// dbsgomysql (phases 024-027). The important asymmetry recorded here: copy-only
// is EXEMPT from FK_COVERAGE_VISIBILITY_CHECK but still runs FK_COVERAGE_CHECK —
// copy-only never issues a source DELETE, so no external cascade can fire, but an
// uncovered cross-schema incoming FK is still a graph-modelling error.
//
// Deviations from the plan (phase-009-characterization-fk-family.md):
//
//  1. NONE structural. The plan file was already reconciled before dispatch: the
//     child table is `order_lines` (never `lines`, a MySQL reserved word — Error
//     1064), and every graph is built with chrAddChild, never a bare
//     AddEdgeWithMeta, except the one place the plan deliberately calls for a bare
//     AddNode: TestCharacterizationInternalFKCoverage, where the absent edge is
//     the thing under test. This file follows the plan's Step 1 code as written.
//
//  2. Two rigor-mandated non-vacuity proofs are added as PERMANENT sub-tests,
//     because both are natural extensions of the test they live in rather than
//     ephemeral scratch edits:
//
//       - TestCharacterizationFKIndexNeverFires/FKIndexCheckDebugFired:
//         FK_INDEX_CHECK cannot be provoked end-to-end (MySQL guarantees the
//         supporting index), so the PASS asserted for every command proves
//         nothing about whether preflight actually reached and evaluated
//         ValidateForeignKeyIndexes — a checker that silently did nothing would
//         produce the same PASS. This sub-test asserts the checker's own
//         Debug-level completion log ("FK index check PASSED (%d foreign keys
//         verified)", preflight.go:606) fired with a non-zero count, proving the
//         checker actually ran. The sibling sub-test
//         DestSchemaCompatibilityStillFires is weaker and does NOT prove this —
//         it only shows the harness reaches preflight at all, via a DIFFERENT,
//         EARLIER check (preflight.go:138-246) that returns before
//         ValidateForeignKeyIndexes is ever called; see its own comment.
//
//       - TestCharacterizationCascadeWarning/RestrictRuleNoWarning: a warning
//         that fires whenever ANY foreign key exists (regardless of ON DELETE
//         rule) would produce the same PASS+warning as one that correctly checks
//         for CASCADE specifically. This sub-test swaps CASCADE for RESTRICT on
//         the identical fixture and confirms the warning disappears for archive,
//         purge and validate.
//
//  3. TestCharacterizationDeleteTriggerForced's "prove twice" requirement (the
//     phase brief: "confirm the same fixture DOES fail with forceTriggers=false")
//     is satisfied by the EXISTING, separate TestCharacterizationDeleteTrigger,
//     which runs the byte-identical fixture DDL (same orders/audit_log/trigger)
//     with forceTriggers=false and asserts DELETE_TRIGGER_CHECK fires for
//     archive/purge/validate. No duplicate test was added; noted here so a
//     reviewer does not read its absence as a gap.
//
//  4. The remaining five failure-asserting mutation proofs the phase brief calls
//     for (FKCoverageCrossSchema, InternalFKCoverage, InternalFKColumnMismatch,
//     DeleteTrigger, FKCoverageVisibility — "remove the defect, confirm the check
//     STOPS firing, restore") are run as ephemeral edit/run/revert cycles against
//     this file rather than committed as code. For each of them, "removing the
//     defect" means deleting the exact fixture line the test exists to pin (e.g.
//     dropping the external `_ext` schema, or restoring the missing graph edge) —
//     permanently encoding that would leave a second, always-passing copy of the
//     same test with no assertion value of its own. Results are recorded in the
//     phase-009 execution report, not in this file.
// ============================================================================

// chrTwoTableGraph builds the standard parent/child graph used across this file.
//
// NOTE (plan reconciliation, 2026-07-28): the child table is `order_lines`, not
// `lines` — LINES is a MySQL reserved word and `CREATE TABLE lines` fails with
// Error 1064 (verified against the 8.4 test server). Phases 005 and 006 already
// shipped with this name; match them. Likewise the node is registered with
// chrAddChild, NOT a bare AddEdgeWithMeta — see the harness comment at
// characterization_support_test.go:243.
func chrTwoTableGraph() *graph.Graph {
	g := graph.NewGraph("orders", "id")
	chrAddChild(g, "orders", "id", "order_lines", "order_id", "id")
	return g
}

const chrOrdersDDL = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
const chrLinesDDL = "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, " +
	"PRIMARY KEY (id), KEY idx_o (order_id), " +
	"CONSTRAINT fk_ol_o FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB"

// chrMismatchedDestLinesDDL is order_lines with order_id widened from bigint to
// int — a known-fatal difference used only by the non-vacuity proof in
// TestCharacterizationFKIndexNeverFires. Deliberately carries no FOREIGN KEY
// constraint: DEST_SCHEMA_COMPATIBILITY_CHECK compares columns, not constraints,
// and the destination is never required to hold the FK at all.
const chrMismatchedDestLinesDDL = "CREATE TABLE order_lines (id bigint NOT NULL, order_id int NOT NULL, " +
	"PRIMARY KEY (id), KEY idx_o (order_id)) ENGINE=InnoDB"

// TestCharacterizationFKIndexNeverFires records that FK_INDEX_CHECK cannot be
// provoked end-to-end. MySQL creates a supporting child index for every InnoDB foreign
// key and refuses to drop it, so a well-formed graph always passes. The check's logic
// is covered at unit level in preflight_test.go via sqlmock.
//
// Phase 024 replaces this validator with validations.CheckFKIndexed, which tests the
// same invariant from the other direction ("a finding means the fact did not come from
// a conforming InnoDB source"). Both are unprovokable here; that equivalence is the
// point of this test.
func TestCharacterizationFKIndexNeverFires(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, chrOrdersDDL)
	f.ExecSource(t, ctx, chrLinesDDL)
	f.ExecDest(t, ctx, chrOrdersDDL)
	f.ExecDest(t, ctx, chrLinesDDL)

	// Prove MySQL refuses to remove the supporting index, which is why the check
	// cannot fire.
	_, err := f.SourceDB.ExecContext(ctx, "ALTER TABLE `"+f.SourceSchema+"`.order_lines DROP INDEX idx_o")
	if err == nil {
		t.Fatal("expected MySQL to refuse dropping the FK's supporting index; it did not — " +
			"FK_INDEX_CHECK may be provokable after all, re-examine phase 024")
	}

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			chrAssertPasses(t, chrRun(t, ctx, f.Checker(t, chrTwoTableGraph()), cmd, false))
		})
	}

	// Non-vacuity proof (rigor: pass-asserting tests must be proven, not assumed).
	// The PASS above cannot distinguish "FK_INDEX_CHECK is genuinely unprovokable"
	// from "ValidateForeignKeyIndexes never ran." DEST_SCHEMA_COMPATIBILITY_CHECK
	// (see DestSchemaCompatibilityStillFires below) CANNOT stand in as that proof:
	// it is an earlier check in RunWithProfile's fixed order (preflight.go:138-246)
	// that returns before ValidateForeignKeyIndexes is ever reached, so a fixture
	// that provokes it never exercises FK_INDEX_CHECK at all. Instead, assert the
	// checker's own Debug-level completion log directly: ValidateForeignKeyIndexes
	// logs "FK index check PASSED (%d foreign keys verified)" on success
	// (preflight.go:606). Require both that the message fired at least once across
	// the five command runs above and that the parsed count is non-zero — a zero
	// count would mean an empty fact set (wrong selector, wrong schema arg, or a
	// lazily-acquired fact never acquired), precisely the phase-024 failure mode
	// this must catch.
	t.Run("FKIndexCheckDebugFired", func(t *testing.T) {
		const prefix = "FK index check PASSED ("
		var found bool
		for _, entry := range f.Logs.FilterLevelExact(zapcore.DebugLevel).All() {
			if !strings.HasPrefix(entry.Message, prefix) {
				continue
			}
			var count int
			if _, err := fmt.Sscanf(entry.Message, "FK index check PASSED (%d foreign keys verified)", &count); err != nil {
				t.Fatalf("failed to parse FK count from debug message %q: %v", entry.Message, err)
			}
			if count == 0 {
				t.Fatalf("FK index check logged zero foreign keys verified: %q", entry.Message)
			}
			found = true
		}
		if !found {
			t.Fatal(`expected a "FK index check PASSED" debug log from ValidateForeignKeyIndexes; ` +
				"none was recorded — FK_INDEX_CHECK may not have run")
		}
	})

	// DestSchemaCompatibilityStillFires is a WEAKER, narrower proof than the
	// sub-test above: it shows only that this fixture's checker reaches
	// RunWithProfile's preflight chain at all, by injecting a known-fatal
	// destination column-type difference on the SAME two-table fixture shape and
	// confirming DEST_SCHEMA_COMPATIBILITY_CHECK — an earlier, unrelated check in
	// RunWithProfile's fixed order (preflight.go:138-246) — actually fires. It does
	// NOT prove FK_INDEX_CHECK is reached (that check runs later, at line 204, and
	// this fixture's fatal error returns long before then) and must not be read as
	// doing so. It is kept because it is still useful: it proves the harness itself
	// (fixture wiring, Checker(), chrRun) is not silently vacuous.
	t.Run("DestSchemaCompatibilityStillFires", func(t *testing.T) {
		f2 := newChrFixture(t, ctx)
		f2.ExecSource(t, ctx, chrOrdersDDL)
		f2.ExecSource(t, ctx, chrLinesDDL)
		f2.ExecDest(t, ctx, chrOrdersDDL)
		f2.ExecDest(t, ctx, chrMismatchedDestLinesDDL)

		err := chrRun(t, ctx, f2.Checker(t, chrTwoTableGraph()), chrCommands[0], false) // archive
		chrAssertCheck(t, err, "DEST_SCHEMA_COMPATIBILITY_CHECK", []string{"order_lines"})
	})
}

// TestCharacterizationFKCoverageCrossSchema pins FK_COVERAGE_CHECK: a table OUTSIDE the
// graph (here, in another schema entirely) holding a foreign key INTO a graph table is
// fatal for every ON DELETE rule and for ALL FIVE commands — copy-only included.
func TestCharacterizationFKCoverageCrossSchema(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)

	for _, rule := range []string{"CASCADE", "SET NULL", "RESTRICT", "NO ACTION"} {
		t.Run(rule, func(t *testing.T) {
			f := newChrFixture(t, ctx)
			f.ExecSource(t, ctx, chrOrdersDDL)
			f.ExecSource(t, ctx, chrLinesDDL)
			f.ExecDest(t, ctx, chrOrdersDDL)
			f.ExecDest(t, ctx, chrLinesDDL)

			outside := f.SourceSchema + "_ext"
			chrCreateExternalChild(t, ctx, f, outside, rule)

			for _, cmd := range chrCommands {
				t.Run(cmd.Name, func(t *testing.T) {
					err := chrRun(t, ctx, f.Checker(t, chrTwoTableGraph()), cmd, false)
					chrAssertCheck(t, err, "FK_COVERAGE_CHECK", nil)
				})
			}
		})
	}
}

// chrCreateExternalChild creates schema `outside` holding a table whose foreign key
// points at the fixture's in-graph `orders` table, with the given ON DELETE rule.
func chrCreateExternalChild(t *testing.T, ctx context.Context, f *chrFixture, outside, onDelete string) {
	t.Helper()

	nullability := "NOT NULL"
	if onDelete == "SET NULL" {
		nullability = "NULL"
	}

	for _, stmt := range []string{
		"DROP DATABASE IF EXISTS `" + outside + "`",
		"CREATE DATABASE `" + outside + "`",
	} {
		if _, err := f.SourceDB.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("external child setup %q: %v", stmt, err)
		}
	}
	// Registered immediately after CREATE DATABASE, before the FK-bearing table
	// below exists — the precedent chrCreateAccount sets deliberately
	// (characterization_accounts_test.go:121-123). On the passing path ordering
	// doesn't matter, but a KILLED run (this project's documented failure mode)
	// dying between here and the table's creation would otherwise leave
	// `<outside>` behind WITH its FK still pointing into the fixture's source
	// schema; the next run's newChrFixture then fails its own
	// `DROP DATABASE chr_src_N` with Error 3730 ("Cannot drop table ... referenced
	// by a foreign key constraint"), bricking every characterization test in the
	// package until `make test-reset`.
	t.Cleanup(func() {
		if _, err := f.SourceDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+outside+"`"); err != nil {
			t.Logf("cleanup drop %s: %v", outside, err)
		}
	})

	stmt := fmt.Sprintf(
		"CREATE TABLE `%s`.audit (id bigint NOT NULL, order_id bigint %s, "+
			"PRIMARY KEY (id), KEY idx_o (order_id), "+
			"CONSTRAINT fk_audit_orders FOREIGN KEY (order_id) REFERENCES `%s`.orders (id) ON DELETE %s"+
			") ENGINE=InnoDB",
		outside, nullability, f.SourceSchema, onDelete)
	if _, err := f.SourceDB.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("external child setup %q: %v", stmt, err)
	}
}

// TestCharacterizationFKCoverageVisibility pins FK_COVERAGE_VISIBILITY_CHECK and the
// documented copy-only asymmetry: an account WITHOUT global SELECT fails closed for
// archive, purge, dry-run and validate, but copy-only is EXEMPT and proceeds.
//
// This is deviation D2's amendment target, and PHASE 025 owns that amendment — not
// phase 026. Phase 025 is titled "(D2)" and states "This phase lands D2"; phase 026's
// non-negotiables require the characterization suite to stay green and UNAMENDED.
// Phase 025 replaces the global-SELECT proof with the library's PROCESS-gated
// VisibilityComplete proof.
//
// The per-command applicability (copy-only exempt) must survive that change UNCHANGED
// — only the proof mechanism moves, not which commands enforce the check. A phase-025
// diff that alters the copy-only exemption is a regression, not part of D2.
func TestCharacterizationFKCoverageVisibility(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, chrOrdersDDL)
	f.ExecSource(t, ctx, chrLinesDDL)
	f.ExecDest(t, ctx, chrOrdersDDL)
	f.ExecDest(t, ctx, chrLinesDDL)

	// Scoped SELECT + DELETE on the fixture schema only — deliberately NOT global.
	acct := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT SELECT, DELETE ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}")
	sourceAsAcct := chrOpenAs(t, acct, f.SourceSchema)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			c := f.CheckerAs(t, chrTwoTableGraph(), sourceAsAcct, f.DestDB)
			err := chrRun(t, ctx, c, cmd, false)
			if cmd.Name == "copy-only" {
				chrAssertPasses(t, err)
				return
			}
			chrAssertCheck(t, err, "FK_COVERAGE_VISIBILITY_CHECK", nil)
		})
	}
}

// TestCharacterizationInternalFKCoverage pins INTERNAL_FK_COVERAGE: a foreign key
// between two IN-GRAPH tables that has no corresponding graph edge is fatal for all
// five commands.
func TestCharacterizationInternalFKCoverage(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, chrOrdersDDL)
	f.ExecSource(t, ctx, chrLinesDDL)
	f.ExecDest(t, ctx, chrOrdersDDL)
	f.ExecDest(t, ctx, chrLinesDDL)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			// Both tables are in the graph but there is NO edge between them.
			// This is the ONE place a bare AddNode is correct: chrAddChild would
			// create the edge that this test needs to be absent.
			g := graph.NewGraph("orders", "id")
			g.SetPK("order_lines", "id")
			g.AddNode("order_lines", nil)

			err := chrRun(t, ctx, f.Checker(t, g), cmd, false)
			chrAssertCheck(t, err, "INTERNAL_FK_COVERAGE", nil)
		})
	}
}

// TestCharacterizationInternalFKColumnMismatch pins the second INTERNAL_FK_COVERAGE
// path: an edge exists but its configured foreign_key does not match the database's.
func TestCharacterizationInternalFKColumnMismatch(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const mismatchDDL = "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, " +
		"alt_id bigint NULL, PRIMARY KEY (id), KEY idx_o (order_id), KEY idx_a (alt_id), " +
		"CONSTRAINT fk_ol_o FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB"

	f.ExecSource(t, ctx, chrOrdersDDL)
	f.ExecSource(t, ctx, mismatchDDL)
	f.ExecDest(t, ctx, chrOrdersDDL)
	f.ExecDest(t, ctx, mismatchDDL)

	// The graph edge names alt_id; the database's constraint is on order_id.
	g := graph.NewGraph("orders", "id")
	chrAddChild(g, "orders", "id", "order_lines", "alt_id", "id") // DB says order_id

	err := chrRun(t, ctx, f.Checker(t, g), chrCommands[0], false) // archive
	chrAssertCheck(t, err, "INTERNAL_FK_COVERAGE", nil)
}

// TestCharacterizationDeleteTrigger pins DELETE_TRIGGER_CHECK and its per-command
// applicability: it runs only for archive, purge and validate (Full | SourceOnly).
func TestCharacterizationDeleteTrigger(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	f.ExecSource(t, ctx, chrOrdersDDL)
	f.ExecSource(t, ctx, "CREATE TABLE audit_log (id bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (id)) ENGINE=InnoDB")
	f.ExecSource(t, ctx, "CREATE TRIGGER trg_orders_del AFTER DELETE ON orders FOR EACH ROW INSERT INTO audit_log VALUES (NULL)")
	f.ExecDest(t, ctx, chrOrdersDDL)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			err := chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, false)
			switch cmd.Name {
			case "archive", "purge", "validate":
				chrAssertCheck(t, err, "DELETE_TRIGGER_CHECK", []string{"orders"})
			default: // copy-only, dry-run
				chrAssertPasses(t, err)
			}
		})
	}
}

// TestCharacterizationDeleteTriggerForced pins --force-triggers: the same fixture
// PASSES WITH A WARNING ("DELETE triggers detected (proceeding due to
// --force-triggers): ...", preflight.go:759) when forceTriggers is true, for the
// commands that actually evaluate ValidateTriggers (archive, purge, validate).
//
// Non-vacuity: a bare PASS cannot prove anything here (a checker that ignores
// forceTriggers entirely, or drops the warning, would also just pass), so each
// archive/purge/validate row also asserts the warning text itself. The "prove
// twice" counterpart — confirming the identical fixture FAILS with
// forceTriggers=false — is TestCharacterizationDeleteTrigger above, which uses
// byte-identical DDL (same orders/audit_log/trigger) and asserts
// DELETE_TRIGGER_CHECK fires for archive/purge/validate.
//
// copy-only and dry-run PASS here too, but NOT because of --force-triggers: both
// commands hard-code forceTriggers=false at their call sites
// (cmd/goarchive/cmd/copyonly.go:99, dryrun.go:112) and ValidateTriggers only
// runs for profile Full | SourceOnly (preflight.go:228,234) — copy-only and
// dry-run use PreflightProfileNonDestructive, so the check is skipped outright
// regardless of forceTriggers. Their rows below assert the warning is ABSENT and
// exist only to pin that profile gate; they are not real --force-triggers
// coverage and must not be read as such.
func TestCharacterizationDeleteTriggerForced(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			// Fresh fixture per command: f.WarnMessages() accumulates across every
			// Checker() run against one fixture's Logs, so a shared fixture across
			// this loop (as TestCharacterizationDeleteTrigger uses, safely, because
			// it never asserts on Warn) would make a per-iteration warning
			// assertion meaningless after the first command. Mirrors
			// TestCharacterizationCascadeWarning below.
			f := newChrFixture(t, ctx)
			f.ExecSource(t, ctx, chrOrdersDDL)
			f.ExecSource(t, ctx, "CREATE TABLE audit_log (id bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (id)) ENGINE=InnoDB")
			f.ExecSource(t, ctx, "CREATE TRIGGER trg_orders_del AFTER DELETE ON orders FOR EACH ROW INSERT INTO audit_log VALUES (NULL)")
			f.ExecDest(t, ctx, chrOrdersDDL)

			chrAssertPasses(t, chrRun(t, ctx, f.Checker(t, graph.NewGraph("orders", "id")), cmd, true))

			warned := chrAnyContains(f.WarnMessages(), "DELETE triggers detected")
			switch cmd.Name {
			case "archive", "purge", "validate":
				if !warned {
					t.Fatalf("expected a DELETE-trigger warning for %s, got: %v", cmd.Name, f.WarnMessages())
				}
			default: // copy-only, dry-run — ValidateTriggers is not reached (profile-gated off)
				if warned {
					t.Fatalf("did not expect a DELETE-trigger warning for %s, got: %v", cmd.Name, f.WarnMessages())
				}
			}
		})
	}
}

const chrCascadeLinesDDL = "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, " +
	"PRIMARY KEY (id), KEY idx_o (order_id), " +
	"CONSTRAINT fk_ol_o FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE) ENGINE=InnoDB"

// chrRestrictLinesDDL is chrCascadeLinesDDL with ON DELETE RESTRICT instead of
// CASCADE, used only by the CascadeWarning non-vacuity proof below.
const chrRestrictLinesDDL = "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, " +
	"PRIMARY KEY (id), KEY idx_o (order_id), " +
	"CONSTRAINT fk_ol_o FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE RESTRICT) ENGINE=InnoDB"

// TestCharacterizationCascadeWarning pins the CASCADE rule as a WARNING, not an error,
// and pins its applicability: WarnCascadeRules runs only for Full | SourceOnly, so
// copy-only and dry-run emit nothing.
func TestCharacterizationCascadeWarning(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)

	for _, cmd := range chrCommands {
		t.Run(cmd.Name, func(t *testing.T) {
			f := newChrFixture(t, ctx)
			f.ExecSource(t, ctx, chrOrdersDDL)
			f.ExecSource(t, ctx, chrCascadeLinesDDL)
			f.ExecDest(t, ctx, chrOrdersDDL)
			f.ExecDest(t, ctx, chrCascadeLinesDDL)

			chrAssertPasses(t, chrRun(t, ctx, f.Checker(t, chrTwoTableGraph()), cmd, false))

			warned := chrAnyContains(f.WarnMessages(), "ON DELETE CASCADE rules detected")
			switch cmd.Name {
			case "archive", "purge", "validate":
				if !warned {
					t.Fatalf("expected a CASCADE warning for %s, got: %v", cmd.Name, f.WarnMessages())
				}
			default: // copy-only, dry-run — WarnCascadeRules is not reached
				if warned {
					t.Fatalf("did not expect a CASCADE warning for %s, got: %v", cmd.Name, f.WarnMessages())
				}
			}
		})
	}

	// Non-vacuity proof (rigor: pass-asserting tests must be proven twice over). A
	// warning that fires whenever ANY foreign key exists — regardless of its ON
	// DELETE rule — would produce the identical PASS+warning as one that correctly
	// detects CASCADE specifically. Swap CASCADE for RESTRICT on the identical
	// fixture and confirm the warning disappears for the three commands that reach
	// WarnCascadeRules.
	t.Run("RestrictRuleNoWarning", func(t *testing.T) {
		for _, cmd := range []chrCommand{chrCommands[0], chrCommands[1], chrCommands[4]} { // archive, purge, validate
			t.Run(cmd.Name, func(t *testing.T) {
				f := newChrFixture(t, ctx)
				f.ExecSource(t, ctx, chrOrdersDDL)
				f.ExecSource(t, ctx, chrRestrictLinesDDL)
				f.ExecDest(t, ctx, chrOrdersDDL)
				f.ExecDest(t, ctx, chrRestrictLinesDDL)

				chrAssertPasses(t, chrRun(t, ctx, f.Checker(t, chrTwoTableGraph()), cmd, false))

				if chrAnyContains(f.WarnMessages(), "ON DELETE CASCADE rules detected") {
					t.Fatalf("expected NO CASCADE warning under ON DELETE RESTRICT for %s, got: %v", cmd.Name, f.WarnMessages())
				}
			})
		}
	})
}
