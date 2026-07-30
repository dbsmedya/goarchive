//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// ============================================================================
// Applicability matrix, first-failure ordering, and the inspection-error vs
// finding distinction.
//
// Phases 004-009 pinned each check in isolation. This file pins the two
// properties that only exist BETWEEN checks, which a strangler migration is
// most likely to break silently:
//
//  1. The applicability matrix at docs/README_VALIDATION.md:64-85 (20 checks x
//     5 commands), turned into a table-driven test so a phase that accidentally
//     moves a check into or out of a profile fails loudly.
//  2. Abort-on-first-failure ordering: when a fixture carries several
//     simultaneous defects, exactly one ID surfaces, and chrCheckOrder pins
//     which one — see the mutation methodology note below.
//
// It also pins the inspection-error vs finding distinction: a metadata QUERY
// that fails (e.g. a closed pool or a dead server) must surface as a plain
// wrapped error, never as a *PreflightError that looks like a schema verdict —
// that distinction is load-bearing under the library, where Inspector methods
// return *validations.ObjectError for inspection failures and []Finding for
// verdicts. A privilege-filtered information_schema read is NOT a query
// failure — it is a finding: information_schema filters rows the account
// cannot see rather than erroring, so an unprivileged account still gets a
// *PreflightError verdict (e.g. TABLE_EXISTENCE_CHECK, "table not found"),
// not a plain error. TestCharacterizationInspectionErrorShape's first half
// pins exactly this.
//
// Deviations from the plan (phase-010-characterization-matrix-and-ordering.md):
//
//  1. chrCheckOrder's doc comment differs from the plan's ORIGINAL text, which
//     said phase 023 amends this slice for deviation D4
//     (SOURCE_SELECT_PERMISSION_CHECK). The operator corrected this on
//     2026-07-28 — AFTER dispatching this phase — and relayed the fix directly
//     to this agent mid-task: the D4 amendment is SPLIT ACROSS TWO PHASES.
//     Phase 022 (not 023) adds ValidateSourceSelectPermissions to
//     RunWithProfile and must edit THIS SLICE, inserting
//     "SOURCE_SELECT_PERMISSION_CHECK" between INTERNAL_FK_COVERAGE and
//     SOURCE_DELETE_PERMISSION_CHECK, in the same PR. Phase 023 only adds the
//     chrMatrix row and the docs change — it does NOT touch this slice. The
//     wording below is the operator-corrected text, copied verbatim from the
//     corrected plan file, so a phase-022 implementer cannot mistake which
//     half of D4 is theirs.
//
//  2. chrCheckOrder is made LOAD-BEARING rather than decorative, but only
//     WITHIN THIS TEST FILE — not against RunWithProfile itself. Per operator
//     instruction, TestCharacterizationFirstFailureOrdering now asserts, for
//     every ordering case, that the winning check's chrCheckOrder index is
//     STRICTLY EARLIER than the index of every OTHER defect the fixture
//     deliberately plants (via chrCheckOrderIndex, a small helper added in
//     THIS file only — not the shared harness, per the operator's brief).
//     This is a purely test-internal comparison: it checks each case's
//     declared wantID/otherIDs against the chrCheckOrder slice and never
//     inspects RunWithProfile's actual execution order. It constrains 7 of
//     the slice's 18 entries via 5 ordering-case pairs. A future phase that
//     reorders checks inside RunWithProfile without also reordering
//     chrCheckOrder is caught by NOTHING here — nor is swapping two entries
//     that no ordering case exercises together (e.g. SOURCE_DELETE_PERMISSION_
//     CHECK and INTERNAL_FK_COVERAGE), nor a phase that forgets to
//     add its own new entry to the slice (phase 022 did not). Runtime detection of a genuinely
//     wrong first-failure ID still comes from chrAssertCheck, which the plan
//     already had. The plan's own Ambiguity 2 warning is NOT superseded by
//     this test: the correspondence between chrCheckOrder and RunWithProfile's
//     real order must still be checked BY EYE in every phase that touches
//     RunWithProfile.
//
//  3. Mutation-testing methodology (rigor requirement, not a plan deviation but
//     recorded here per that requirement): every "must fire" cell was proven
//     by removing the row's fixture defect (via ephemeral edit/run/revert
//     against a throwaway scratch file, never against this file or
//     preflight.go) and confirming the check that had fired now either passes
//     outright or a DIFFERENT, later check fires instead — never the same ID
//     for a coincidental reason. Every "must NOT fire" cell was checked by
//     confirming the excluded command PASSES OUTRIGHT on the row's
//     single-defect fixture (proving the profile skip is reached and no other
//     defect masks it, since these fixtures are deliberately single-defect).
//     Every ordering case was re-run with its WINNING defect removed, and the
//     row's other planted defect was confirmed to surface in its place. Full
//     per-row/per-case results are recorded in this phase's PR description;
//     none of it is committed as code, matching the precedent set by
//     characterization_fk_integration_test.go's own deviation-4 note.
//
//  4. TestCharacterizationApplicabilityMatrix's not-applicable branch asserts
//     MORE than the plan specified. The plan's form only rejected
//     `pe.Check == row.ID`, which would read a wrapped inspection error, or an
//     unrelated check firing, as green — the cell could pass for the wrong
//     reason. Verified during this phase: all five excluded cells (purge x
//     DEST_TABLE_EXISTENCE_CHECK, DEST_SCHEMA_COMPATIBILITY_CHECK,
//     DEST_INSERT_TRIGGER_CHECK; copy-only and dry-run x DELETE_TRIGGER_CHECK)
//     PASS OUTRIGHT (err == nil) today. This is a deliberate strengthening
//     beyond what the plan sanctioned: excluded cells now assert `err == nil`
//     outright, not merely "not this ID". If a future phase makes one of
//     these commands genuinely report some other, unrelated check on these
//     fixtures, this assertion fails where the plan's weaker form would have
//     silently passed — that is the intended tripwire, not a bug.
// ============================================================================

// chrCheckOrder is the fixed execution order of RunWithProfile as of 1.8, taken from
// internal/archiver/preflight.go:138-246. Phases 013-031 must not change it, with
// exactly one sanctioned exception (deviation D4), and that exception is SPLIT ACROSS
// TWO PHASES — check which half you are:
//
//   - PHASE 022 owned THIS SLICE. It added ValidateSourceSelectPermissions to
//     RunWithProfile and inserted "SOURCE_SELECT_PERMISSION_CHECK" here, between
//     INTERNAL_FK_COVERAGE and SOURCE_DELETE_PERMISSION_CHECK, in the same PR. Done.
//     (phase-022 "Files": Modify … characterization_matrix_integration_test.go —
//     chrCheckOrder.)
//   - PHASE 023 owns the chrMatrix ROW and the docs, NOT this slice.
//     (phase-023 "Files": Modify … characterization_matrix_integration_test.go —
//     new matrix row.)
//
// Corrected 2026-07-28: the plan said phase 023 amended this slice. It does not.
// Getting this wrong is not cosmetic — TestCharacterizationFirstFailureOrdering
// CONSUMES this slice (see deviation 2 above), so phase 022's insertion of the check
// into RunWithProfile had to update it here too, or the ordering assertions would be
// silently comparing against a stale order.
var chrCheckOrder = []string{
	"TABLE_EXISTENCE_CHECK",
	"PK_COLUMN_CASE_CHECK",     // and PK_COLUMN_CHECK, same validator
	"COMPOSITE_PK_CHECK",       // and PRIMARY_KEY_CHECK, same validator
	"ROOT_PK_TYPE_UNSUPPORTED", // plain error, not *PreflightError
	"STORAGE_ENGINE_CHECK",
	"INVISIBLE_COLUMN_CHECK",
	"JOB_SCHEMA_PERMISSION_CHECK",
	"DEST_TABLE_EXISTENCE_CHECK",
	"DEST_SCHEMA_COMPATIBILITY_CHECK",
	"DEST_WRITE_PERMISSION_CHECK",
	"DEST_INSERT_TRIGGER_CHECK",
	"FK_INDEX_CHECK",
	"FK_COVERAGE_VISIBILITY_CHECK",
	"FK_COVERAGE_CHECK",
	"INTERNAL_FK_COVERAGE",
	"SOURCE_SELECT_PERMISSION_CHECK", // D4, phase 022 — the one sanctioned insertion
	"SOURCE_DELETE_PERMISSION_CHECK",
	"DELETE_TRIGGER_CHECK",
}

// chrCheckOrderAliases maps a check ID that RunWithProfile genuinely emits, but that
// is folded into another entry's comment in chrCheckOrder (same validator, same
// position), to the chrCheckOrder entry that stands in for it. Without this, a future
// ordering case planting one of these as a defect (a natural fixture — e.g. a
// PRIMARY_KEY_CHECK case) would die in chrCheckOrderIndex with "not listed in
// chrCheckOrder", reading as a test bug rather than a real ordering failure.
var chrCheckOrderAliases = map[string]string{
	"PK_COLUMN_CHECK":     "PK_COLUMN_CASE_CHECK",     // same validator, same position
	"PRIMARY_KEY_CHECK":   "COMPOSITE_PK_CHECK",       // same validator, same position
	"ROOT_PK_TYPE_LOOKUP": "ROOT_PK_TYPE_UNSUPPORTED", // same validator, same position
}

// chrCheckOrderIndex returns id's position in chrCheckOrder, or fails the test if id
// is not listed and not a known alias (chrCheckOrderAliases) of a listed entry. Added
// in this file only, per the operator's instruction not to grow the shared harness for
// a single consumer.
func chrCheckOrderIndex(t *testing.T, id string) int {
	t.Helper()
	if alias, ok := chrCheckOrderAliases[id]; ok {
		id = alias
	}
	for i, want := range chrCheckOrder {
		if want == id {
			return i
		}
	}
	t.Fatalf("%s is not listed in chrCheckOrder", id)
	return -1
}

// chrMatrixRow declares, for one check ID, which commands must report it when its
// fixture is applied. It mirrors docs/README_VALIDATION.md:64-85 exactly.
type chrMatrixRow struct {
	ID string
	// Applies lists the command names that MUST report ID for this row's fixture.
	// Commands not listed must NOT report it — they either pass or report a
	// different, later check, which the test distinguishes.
	Applies []string
	// Fixture prepares the schemas and returns the graph to run against.
	Fixture func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph
	// Restricted, when non-nil, returns the source and destination pools to run as, for
	// a permission or visibility row. The runner (TestCharacterizationApplicabilityMatrix)
	// consults it: a non-nil value opens the checker against the returned pools instead
	// of the fixture admin pools. SOURCE_SELECT_PERMISSION_CHECK is the first row to set
	// it (phase 023). A nil value (every other row today) means "run as the fixture admin
	// pools" — the prior behaviour is preserved for rows that don't need restriction.
	Restricted func(t *testing.T, ctx context.Context, f *chrFixture) (src, dst *sql.DB)
}

var chrAll = []string{"archive", "purge", "copy-only", "dry-run", "validate"}

func chrExcept(names ...string) []string {
	skip := make(map[string]bool, len(names))
	for _, n := range names {
		skip[n] = true
	}
	var out []string
	for _, n := range chrAll {
		if !skip[n] {
			out = append(out, n)
		}
	}
	return out
}

func chrContains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

var chrMatrix = []chrMatrixRow{
	{
		ID:      "TABLE_EXISTENCE_CHECK",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			// `order_lines`, not `lines`: LINES is a MySQL reserved word (Error 1064).
			// chrAddChild, not a bare AddEdgeWithMeta: the latter records the edge
			// without the node, so the table vanishes from AllNodes().
			f.ExecDest(t, ctx, "CREATE TABLE order_lines (id bigint NOT NULL, order_id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			g := graph.NewGraph("orders", "id")
			chrAddChild(g, "orders", "id", "order_lines", "order_id", "id")
			return g
		},
	},
	{
		ID:      "PK_COLUMN_CHECK",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			const ddl = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
			f.ExecSource(t, ctx, ddl)
			f.ExecDest(t, ctx, ddl)
			return graph.NewGraph("orders", "order_id")
		},
	},
	{
		ID:      "PK_COLUMN_CASE_CHECK",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			const ddl = "CREATE TABLE orders (Order_Id bigint NOT NULL, PRIMARY KEY (Order_Id)) ENGINE=InnoDB"
			f.ExecSource(t, ctx, ddl)
			f.ExecDest(t, ctx, ddl)
			return graph.NewGraph("orders", "order_id")
		},
	},
	{
		ID:      "COMPOSITE_PK_CHECK",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			const ddl = "CREATE TABLE orders (id bigint NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id)) ENGINE=InnoDB"
			f.ExecSource(t, ctx, ddl)
			f.ExecDest(t, ctx, ddl)
			return graph.NewGraph("orders", "id")
		},
	},
	{
		ID:      "PRIMARY_KEY_CHECK",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			const ddl = "CREATE TABLE orders (id bigint NOT NULL, ref_no bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
			f.ExecSource(t, ctx, ddl)
			f.ExecDest(t, ctx, ddl)
			return graph.NewGraph("orders", "ref_no")
		},
	},
	{
		ID:      "STORAGE_ENGINE_CHECK",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=MyISAM")
			f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			return graph.NewGraph("orders", "id")
		},
	},
	{
		ID:      "INVISIBLE_COLUMN_CHECK",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			const ddl = "CREATE TABLE orders (id bigint NOT NULL, note varchar(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
			f.ExecSource(t, ctx, ddl)
			f.ExecSource(t, ctx, "ALTER TABLE orders ALTER COLUMN note SET INVISIBLE")
			f.ExecDest(t, ctx, ddl)
			return graph.NewGraph("orders", "id")
		},
	},
	{
		ID:      "DEST_TABLE_EXISTENCE_CHECK",
		Applies: chrExcept("purge"),
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			return graph.NewGraph("orders", "id")
		},
	},
	{
		ID:      "DEST_SCHEMA_COMPATIBILITY_CHECK",
		Applies: chrExcept("purge"),
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(10,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(12,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			return graph.NewGraph("orders", "id")
		},
	},
	{
		ID:      "DEST_INSERT_TRIGGER_CHECK",
		Applies: chrExcept("purge"),
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			const ddl = "CREATE TABLE orders (id bigint NOT NULL, n int NOT NULL DEFAULT 0, PRIMARY KEY (id)) ENGINE=InnoDB"
			f.ExecSource(t, ctx, ddl)
			f.ExecDest(t, ctx, ddl)
			f.ExecDest(t, ctx, "CREATE TRIGGER trg_ins BEFORE INSERT ON orders FOR EACH ROW SET NEW.n = NEW.n + 1")
			return graph.NewGraph("orders", "id")
		},
	},
	{
		ID:      "FK_COVERAGE_CHECK",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			chrCreateExternalChild(t, ctx, f, chrExternalSchemaName(f.SourceSchema), "RESTRICT")
			return graph.NewGraph("orders", "id")
		},
	},
	{
		ID:      "INTERNAL_FK_COVERAGE",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			f.ExecSource(t, ctx, chrOrdersDDL)
			f.ExecSource(t, ctx, chrLinesDDL)
			f.ExecDest(t, ctx, chrOrdersDDL)
			f.ExecDest(t, ctx, chrLinesDDL)
			// `order_lines`, matching chrLinesDDL above. If this said "lines"
			// the row would assert the WRONG check twice over: "lines" does not
			// exist so TABLE_EXISTENCE_CHECK fires first, and `order_lines`
			// would be out-of-graph so its FK reads as EXTERNAL and yields
			// FK_COVERAGE_CHECK rather than INTERNAL_FK_COVERAGE.
			//
			// The bare AddNode is correct HERE and only here: the absent edge is
			// precisely what INTERNAL_FK_COVERAGE must detect. Everywhere else in
			// this file use chrAddChild.
			g := graph.NewGraph("orders", "id")
			g.SetPK("order_lines", "id")
			g.AddNode("order_lines", nil)
			return g
		},
	},
	{
		ID:      "DELETE_TRIGGER_CHECK",
		Applies: []string{"archive", "purge", "validate"},
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			const ddl = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
			f.ExecSource(t, ctx, ddl)
			f.ExecSource(t, ctx, "CREATE TABLE audit_log (id bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (id)) ENGINE=InnoDB")
			f.ExecSource(t, ctx, "CREATE TRIGGER trg_del AFTER DELETE ON orders FOR EACH ROW INSERT INTO audit_log VALUES (NULL)")
			f.ExecDest(t, ctx, ddl)
			return graph.NewGraph("orders", "id")
		},
	},
	{
		ID:      "SOURCE_SELECT_PERMISSION_CHECK",
		Applies: chrAll,
		Fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
			f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
			return graph.NewGraph("orders", "id")
		},
		Restricted: func(t *testing.T, ctx context.Context, f *chrFixture) (*sql.DB, *sql.DB) {
			// Construction (B) from phase 022's "reachability problem" — the ONLY shape
			// that reaches SOURCE_SELECT on all five commands before phase 025.
			//
			// Applies is chrAll, and four of those five commands enforce FK visibility,
			// which until phase 025 is 1.8 code demanding GLOBAL SELECT
			// (ValidateForeignKeyMetadataVisibility, via hasGlobalPrivilege). So:
			// partial revokes ON + a bare
			// GLOBAL SELECT row. 1.8's check sees the global row and passes at position
			// 13; the library resolves object-level SELECT to GrantUnconfirmed, so this
			// check fails closed at 16. DELETE is granted DIRECTLY so SOURCE_DELETE (17)
			// would pass and cannot be mistaken for the failure under test.
			srcSelectPartialRevokes(t, ctx, f.SourceDB)
			src := chrCreateAccount(t, ctx, f.SourceDB, "source",
				"GRANT PROCESS ON *.* TO {{ACCOUNT}}",
				"GRANT SELECT ON *.* TO {{ACCOUNT}}",
				"GRANT DELETE ON `"+f.SourceSchema+"`.* TO {{ACCOUNT}}")
			dst := chrCreateAccount(t, ctx, f.DestDB, "destination",
				"GRANT CREATE, SELECT, INSERT, UPDATE ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
			return chrOpenAs(t, src, f.SourceSchema), chrOpenAs(t, dst, f.DestSchema)
		},
	},
}

// Rows deliberately omitted from chrMatrix, with their reason:
//
//   - ROOT_PK_TYPE_UNSUPPORTED — it is a plain error, not a *PreflightError, so it
//     does not fit the row shape. Phase 005 covers all five commands for it.
//   - FK_INDEX_CHECK — unprovokable end-to-end (phase 009 Ambiguity 1;
//     TestCharacterizationFKIndexNeverFires covers all five commands: each asserts
//     a PASS, and each independently proves ValidateForeignKeyIndexes actually ran
//     with a non-zero foreign-key count, via a fixture owned by that command alone
//     (not a single proof aggregated across all five runs — a per-command fixture
//     is required for the proof to mean anything per command).
//   - JOB_SCHEMA_PERMISSION_CHECK, DEST_WRITE_PERMISSION_CHECK,
//     SOURCE_DELETE_PERMISSION_CHECK, FK_COVERAGE_VISIBILITY_CHECK — they need
//     restricted accounts, which phases 008 and 009 already assert across all five
//     commands. Adding a Restricted hook here would duplicate that work. The
//     Restricted hook is now wired into the runner (phase 023, Step 6) and in use by
//     SOURCE_SELECT_PERMISSION_CHECK below, so these four rows could be added the
//     same way if the duplication with phases 008/009 is ever worth removing — that
//     remains a choice for a future phase, not a limitation of the mechanism.
//
// Every omission must stay listed here. A row that silently disappears is exactly
// the drift this suite exists to catch.
//
// Cross-checked mechanically against docs/README_VALIDATION.md:64-85 (Step 7): the
// 14 IDs in chrMatrix plus these 6 omitted IDs total exactly the 20 check IDs
// RunWithProfile now emits (deviation D4; the check added by phase 022, published by
// phase 023). Every row's Applies matches the doc's ✅/❌ columns exactly for all 20
// checks the doc now covers. No documentation discrepancy was found; see the PR
// description for the row-by-row cross-check.

// TestCharacterizationApplicabilityMatrix asserts, for each row, that the listed
// commands report the row's ID and the unlisted commands do NOT. It is the executable
// form of the table in docs/README_VALIDATION.md:64-85.
func TestCharacterizationApplicabilityMatrix(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)

	for _, row := range chrMatrix {
		t.Run(row.ID, func(t *testing.T) {
			for _, cmd := range chrCommands {
				t.Run(cmd.Name, func(t *testing.T) {
					f := newChrFixture(t, ctx)
					g := row.Fixture(t, ctx, f)

					var c *PreflightChecker
					if row.Restricted != nil {
						src, dst := row.Restricted(t, ctx, f)
						c = f.CheckerAs(t, g, src, dst)
					} else {
						c = f.Checker(t, g)
					}
					err := chrRun(t, ctx, c, cmd, false)

					if chrContains(row.Applies, cmd.Name) {
						chrAssertCheck(t, err, row.ID, nil)
						return
					}
					// Not applicable: verified (deviation 4 above) that all five excluded
					// cells PASS OUTRIGHT today, so assert err == nil directly rather than
					// merely "not this ID" — a wrapped inspection error, or an unrelated
					// check firing, would otherwise read as green for the wrong reason.
					if err != nil {
						t.Fatalf("%s must be SKIPPED for %s (expected no error at all), got: %v", row.ID, cmd.Name, err)
					}
				})
			}
		})
	}
}

// TestCharacterizationFirstFailureOrdering pins abort-on-first-failure: a fixture
// carrying several simultaneous defects reports the EARLIEST one in chrCheckOrder, and
// only that one.
//
// otherIDs lists the check ID(s) of every OTHER defect the fixture deliberately
// plants besides the winner. For each case the test asserts wantID's chrCheckOrder
// index is strictly earlier than every ID in otherIDs — making chrCheckOrder a real,
// consumed assertion rather than documentation nothing reads (deviation 2 above).
func TestCharacterizationFirstFailureOrdering(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)

	cases := []struct {
		name     string
		wantID   string
		otherIDs []string
		fixture  func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph
	}{
		{
			// Missing source table AND a composite PK on another table AND a MyISAM
			// engine. TABLE_EXISTENCE_CHECK is first in the order and must win.
			name:     "table_existence_beats_everything",
			wantID:   "TABLE_EXISTENCE_CHECK",
			otherIDs: []string{"COMPOSITE_PK_CHECK", "STORAGE_ENGINE_CHECK"},
			fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
				f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id)) ENGINE=MyISAM")
				f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id)) ENGINE=InnoDB")
				// chrAddChild, not a bare AddEdgeWithMeta — otherwise `ghost` never
				// reaches AllNodes(), ValidateTablesExist never looks for it, and
				// TABLE_EXISTENCE_CHECK cannot fire. This assertion would then pass
				// for the wrong reason, or fail on COMPOSITE_PK_CHECK instead.
				g := graph.NewGraph("orders", "id")
				chrAddChild(g, "orders", "id", "ghost", "order_id", "id")
				return g
			},
		},
		{
			// Composite PK AND MyISAM on the SAME table. COMPOSITE_PK_CHECK precedes
			// STORAGE_ENGINE_CHECK.
			name:     "composite_pk_beats_storage_engine",
			wantID:   "COMPOSITE_PK_CHECK",
			otherIDs: []string{"STORAGE_ENGINE_CHECK"},
			fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
				f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id)) ENGINE=MyISAM")
				f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id)) ENGINE=InnoDB")
				return graph.NewGraph("orders", "id")
			},
		},
		{
			// Invisible column AND a destination type mismatch. INVISIBLE_COLUMN_CHECK
			// is a source check and precedes every destination check.
			name:     "invisible_column_beats_dest_compat",
			wantID:   "INVISIBLE_COLUMN_CHECK",
			otherIDs: []string{"DEST_SCHEMA_COMPATIBILITY_CHECK"},
			fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
				f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(10,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
				f.ExecSource(t, ctx, "ALTER TABLE orders ALTER COLUMN amount SET INVISIBLE")
				f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(12,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
				return graph.NewGraph("orders", "id")
			},
		},
		{
			// Destination compatibility failure AND a source DELETE trigger.
			// DEST_SCHEMA_COMPATIBILITY_CHECK precedes DELETE_TRIGGER_CHECK.
			name:     "dest_compat_beats_delete_trigger",
			wantID:   "DEST_SCHEMA_COMPATIBILITY_CHECK",
			otherIDs: []string{"DELETE_TRIGGER_CHECK"},
			fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
				f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(10,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
				f.ExecSource(t, ctx, "CREATE TABLE audit_log (id bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (id)) ENGINE=InnoDB")
				f.ExecSource(t, ctx, "CREATE TRIGGER trg_del AFTER DELETE ON orders FOR EACH ROW INSERT INTO audit_log VALUES (NULL)")
				f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, amount decimal(12,2) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
				return graph.NewGraph("orders", "id")
			},
		},
		{
			// FK coverage failure AND a source DELETE trigger. FK_COVERAGE_CHECK
			// precedes DELETE_TRIGGER_CHECK.
			name:     "fk_coverage_beats_delete_trigger",
			wantID:   "FK_COVERAGE_CHECK",
			otherIDs: []string{"DELETE_TRIGGER_CHECK"},
			fixture: func(t *testing.T, ctx context.Context, f *chrFixture) *graph.Graph {
				f.ExecSource(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
				f.ExecSource(t, ctx, "CREATE TABLE audit_log (id bigint NOT NULL AUTO_INCREMENT, PRIMARY KEY (id)) ENGINE=InnoDB")
				f.ExecSource(t, ctx, "CREATE TRIGGER trg_del AFTER DELETE ON orders FOR EACH ROW INSERT INTO audit_log VALUES (NULL)")
				f.ExecDest(t, ctx, "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB")
				chrCreateExternalChild(t, ctx, f, chrExternalSchemaName(f.SourceSchema), "RESTRICT")
				return graph.NewGraph("orders", "id")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newChrFixture(t, ctx)
			g := tc.fixture(t, ctx, f)
			err := chrRun(t, ctx, f.Checker(t, g), chrCommands[0], false) // archive
			chrAssertCheck(t, err, tc.wantID, nil)

			wantIdx := chrCheckOrderIndex(t, tc.wantID)
			for _, other := range tc.otherIDs {
				otherIdx := chrCheckOrderIndex(t, other)
				if wantIdx >= otherIdx {
					t.Fatalf("chrCheckOrder places %s (index %d) at or after %s (index %d); "+
						"the ordering assertion is no longer consistent with the documented order",
						tc.wantID, wantIdx, other, otherIdx)
				}
			}
		})
	}
}

// TestCharacterizationInspectionErrorShape pins the distinction spec slice 1 calls the
// "inspection-error vs finding" distinction: when a metadata QUERY fails, preflight
// returns a plain wrapped error, NOT a *PreflightError. A *PreflightError means "the
// schema is wrong"; a plain error means "we could not find out".
//
// This matters more under the library, where Inspector methods return
// *validations.ObjectError for inspection failures and []Finding for verdicts —
// phases 013-031 must map the former to plain errors and only the latter to
// *PreflightError.
//
// SCOPE CAVEAT: the closed-pool probe below dies at the FIRST query
// RunWithProfile issues, which is ValidateTablesExist. So this test pins the
// inspection-error shape only for that one check — it does NOT prove every
// other check in RunWithProfile follows the same shape when ITS query fails;
// a check reached later never gets a chance to run against the closed pool.
// Per the plan, each of phases 013-031 MUST separately assert the
// inspection-error-vs-finding shape for the specific check it replaces, at
// unit level with sqlmock (forcing that check's own query to fail) — this
// integration test cannot do that for them, since it can only observe
// whichever check happens to run first.
func TestCharacterizationInspectionErrorShape(t *testing.T) {
	_, ctx := SetupIntegrationTest(t)
	f := newChrFixture(t, ctx)

	const ddl = "CREATE TABLE orders (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
	f.ExecSource(t, ctx, ddl)
	f.ExecDest(t, ctx, ddl)

	// An account with NO privilege on the fixture schema at all: information_schema
	// simply returns no rows for it rather than erroring, so this proves the
	// "invisible schema" path, not a query error.
	acct := chrCreateAccount(t, ctx, f.SourceDB, "source")
	sourceAsAcct := chrOpenAs(t, acct, "")

	c := f.CheckerAs(t, graph.NewGraph("orders", "id"), sourceAsAcct, f.DestDB)
	err := chrRun(t, ctx, c, chrCommands[0], false)
	chrAssertCheck(t, err, "TABLE_EXISTENCE_CHECK", []string{"orders"})

	// Now force a genuine query failure by closing the pool before the run.
	acct2 := chrCreateAccount(t, ctx, f.SourceDB, "source",
		"GRANT SELECT ON *.* TO {{ACCOUNT}}")
	closedDB := chrOpenAs(t, acct2, f.SourceSchema)
	if cerr := closedDB.Close(); cerr != nil {
		t.Fatalf("close pool: %v", cerr)
	}

	c2 := f.CheckerAs(t, graph.NewGraph("orders", "id"), closedDB, f.DestDB)
	err2 := chrRun(t, ctx, c2, chrCommands[0], false)
	if err2 == nil {
		t.Fatal("expected an inspection error from a closed pool, got nil")
	}
	var pe *PreflightError
	if errors.As(err2, &pe) {
		t.Fatalf("an inspection failure must NOT be a *PreflightError; got check %s", pe.Check)
	}
}
