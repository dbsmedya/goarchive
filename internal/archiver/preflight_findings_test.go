package archiver

import (
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"github.com/dbsmedya/goarchive/internal/graph"
)

// TestFindingsToPreflightErrorMapsExpectedCheck proves a matching finding becomes a
// *PreflightError carrying goarchive's ID and the finding's table names.
func TestFindingsToPreflightErrorMapsExpectedCheck(t *testing.T) {
	findings := []validations.Finding{
		{Check: validations.IDTablesExist, Tables: []string{"lines"}},
		{Check: validations.IDTablesExist, Tables: []string{"orders"}},
	}
	err := findingsToPreflightError("TABLE_EXISTENCE_CHECK", "Tables not found in source database",
		findings, validations.IDTablesExist)
	if err == nil {
		t.Fatal("expected a *PreflightError")
	}
	if err.Check != "TABLE_EXISTENCE_CHECK" {
		t.Fatalf("Check = %q", err.Check)
	}
	if strings.Join(err.Tables, ",") != "lines,orders" {
		t.Fatalf("Tables = %v, want [lines orders] in sorted presentation order", err.Tables)
	}
}

// TestFindingsToPreflightErrorTableOrderIsStable proves presentation does not inherit
// graph map iteration order. Both inputs contain the same findings in a different order;
// deleting the sort in findingsToPreflightError makes the second assertion fail.
func TestFindingsToPreflightErrorTableOrderIsStable(t *testing.T) {
	forward := []validations.Finding{
		{Check: validations.IDTablesExist, Tables: []string{"zebra", "alpha"}},
		{Check: validations.IDTablesExist, Tables: []string{"middle"}},
	}
	reversed := []validations.Finding{
		{Check: validations.IDTablesExist, Tables: []string{"middle"}},
		{Check: validations.IDTablesExist, Tables: []string{"alpha", "zebra"}},
	}

	first := findingsToPreflightError("TABLE_EXISTENCE_CHECK", "missing", forward, validations.IDTablesExist)
	second := findingsToPreflightError("TABLE_EXISTENCE_CHECK", "missing", reversed, validations.IDTablesExist)
	if first == nil || second == nil {
		t.Fatal("matching findings must produce errors")
	}
	const want = "alpha,middle,zebra"
	if got := strings.Join(first.Tables, ","); got != want {
		t.Fatalf("forward Tables = %q, want %q", got, want)
	}
	if got := strings.Join(second.Tables, ","); got != want {
		t.Fatalf("reversed Tables = %q, want %q", got, want)
	}
}

// TestFindingsToPreflightErrorNoFindings proves an empty finding set is a pass.
func TestFindingsToPreflightErrorNoFindings(t *testing.T) {
	if err := findingsToPreflightError("X", "m", nil, validations.IDTablesExist); err != nil {
		t.Fatalf("expected nil for no findings, got %v", err)
	}
}

// TestUnexpectedFindingFailsClosed enforces the spec's fail-closed rule: a finding
// whose Check the stage does not recognise aborts preflight with an explicit error and
// is NOT silently ignored.
func TestUnexpectedFindingFailsClosed(t *testing.T) {
	err := unexpectedFindingError("table_existence", validations.Finding{Check: "SOMETHING_NEW"})
	if err == nil {
		t.Fatal("an unrecognised finding must abort preflight")
	}
	if !strings.Contains(err.Error(), "SOMETHING_NEW") {
		t.Fatalf("error must name the unrecognised check: %v", err)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatal("a fail-closed abort is an engine error, not a schema verdict; it must not be a *PreflightError")
	}
}

// TestInspectionErrorIsNotAPreflightError enforces the inspection-error vs finding
// distinction phase 010 characterized.
func TestInspectionErrorIsNotAPreflightError(t *testing.T) {
	err := inspectionError("table_existence", errors.New("connection refused"))
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatal("an inspection failure must not be reported as a *PreflightError")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("cause must be preserved: %v", err)
	}
}

// TestNonBaseTableNamesDecoratesType proves views (and any other non-BASE TABLE
// object) are surfaced with the observed TABLE_TYPE, so the operator sees why.
func TestNonBaseTableNamesDecoratesType(t *testing.T) {
	found := []validations.TableInfo{
		{Table: "orders", Type: "BASE TABLE", Engine: "InnoDB"},
		{Table: "order_summary", Type: "VIEW"},
		{Table: "sys_thing", Type: "SYSTEM VIEW"},
	}
	got := nonBaseTableNames(found)
	if len(got) != 2 {
		t.Fatalf("nonBaseTableNames = %v, want 2 entries", got)
	}
	if got[0] != "order_summary(VIEW)" || got[1] != "sys_thing(SYSTEM VIEW)" {
		t.Fatalf("nonBaseTableNames = %v", got)
	}
}

// TestNonBaseTableNamesEmptyForAllBaseTables is the negative control.
func TestNonBaseTableNamesEmptyForAllBaseTables(t *testing.T) {
	found := []validations.TableInfo{{Table: "orders", Type: "BASE TABLE", Engine: "InnoDB"}}
	if got := nonBaseTableNames(found); len(got) != 0 {
		t.Fatalf("nonBaseTableNames = %v, want empty", got)
	}
}

// TestUnexpectedFactsError pins every property callers depend on: the message carries
// the PREFLIGHT_UNEXPECTED_FACTS prefix, names the recognised check, the actual payload
// type, the expected type and the stage — never calls the check "unrecognised" — and the
// result is a plain error, NOT a *PreflightError. That last one is the load-bearing
// distinction: a *PreflightError means "the schema is wrong", while this means "this
// build cannot judge the library's answer".
func TestUnexpectedFactsError(t *testing.T) {
	f := validations.Finding{
		Check: validations.IDStorageEngine,
		Facts: "not-a-TableInfo",
	}
	err := unexpectedFactsError("storage_engine", f, "validations.TableInfo")
	if err == nil {
		t.Fatal("unexpectedFactsError returned nil")
	}

	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("must be a plain error, got *PreflightError: %v", pe)
	}

	msg := err.Error()
	if !strings.HasPrefix(msg, "PREFLIGHT_UNEXPECTED_FACTS:") {
		t.Fatalf("message must carry the PREFLIGHT_UNEXPECTED_FACTS prefix, got %q", msg)
	}
	if strings.Contains(msg, "unrecognised") {
		t.Fatalf("a recognised check must not be reported as unrecognised: %q", msg)
	}
	for _, want := range []string{
		"storage_engine",            // stage
		validations.IDStorageEngine, // the recognised check
		"string",                    // the ACTUAL payload type, via %T
		"validations.TableInfo",     // the expected type
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q does not mention %q", msg, want)
		}
	}
}

// TestPrivilegeOffendersReportsStateNotWording proves the stage branches on the typed
// PrivilegeFact — including the exact GrantState — rather than on message text, and
// that every non-GrantPresent state is surfaced (deviation D1 / invariant I2).
func TestPrivilegeOffendersReportsStateNotWording(t *testing.T) {
	findings := []validations.Finding{
		{Check: validations.IDSchemaPrivileges, Facts: validations.PrivilegeFact{
			Schema: "jobs", Privilege: validations.PrivilegeCreate, State: validations.GrantAbsent}},
		{Check: validations.IDSchemaPrivileges, Facts: validations.PrivilegeFact{
			Schema: "jobs", Privilege: validations.PrivilegeUpdate, State: validations.GrantUnconfirmed}},
		{Check: validations.IDSchemaPrivileges, Facts: validations.PrivilegeFact{
			Schema: "jobs", Privilege: validations.PrivilegeInsert, State: validations.GrantUnknown}},
	}
	got, err := privilegeOffenders("job_schema", findings)
	if err != nil {
		t.Fatalf("privilegeOffenders: %v", err)
	}
	want := []string{"CREATE(absent)", "UPDATE(unconfirmed)", "INSERT(unknown)"}
	if len(got) != len(want) {
		t.Fatalf("privilegeOffenders = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("privilegeOffenders = %v, want %v", got, want)
		}
	}
}

// TestPrivilegeOffendersFailsClosed enforces the fail-closed rule and, critically, pins
// WHICH helper reports each fault — modelled on TestTriggerOffendersFailsClosed below.
//
// There is no "empty facts slice" case here, unlike the trigger version: PrivilegeFact
// is a struct, not a slice, so a failed type assertion always yields a well-formed zero
// value (validations.PrivilegeFact{}) rather than something a length check could catch.
// The wrong-facts-type case is therefore the only way to reach unexpectedFactsError
// here, and it is the load-bearing one: deleting the `fact, ok := ...` type assertion
// and using f.Facts directly would panic instead of failing closed with a plain error,
// and this case is what catches that.
func TestPrivilegeOffendersFailsClosed(t *testing.T) {
	cases := []struct {
		name       string
		finding    validations.Finding
		wantPrefix string
	}{
		{
			name:       "unknown_check_id",
			finding:    validations.Finding{Check: "SOMETHING_NEW"},
			wantPrefix: "PREFLIGHT_UNKNOWN_FINDING",
		},
		{
			name: "wrong_facts_type",
			finding: validations.Finding{
				Check: validations.IDSchemaPrivileges,
				Facts: "not-a-PrivilegeFact",
			},
			wantPrefix: "PREFLIGHT_UNEXPECTED_FACTS",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := privilegeOffenders("job_schema", []validations.Finding{tc.finding})
			if err == nil {
				t.Fatalf("must abort, got offenders %v and nil error", got)
			}
			var pe *PreflightError
			if errors.As(err, &pe) {
				t.Fatalf("fail-closed aborts are plain errors, got *PreflightError: %v", pe)
			}
			if !strings.HasPrefix(err.Error(), tc.wantPrefix) {
				t.Fatalf("wrong helper reported this fault: want prefix %q, got: %v",
					tc.wantPrefix, err)
			}
			if !strings.Contains(err.Error(), "job_schema") {
				t.Fatalf("error must name the stage, got: %v", err)
			}
		})
	}
}

// TestTriggerOffendersDecoratesFirstTriggerPerTable proves the shared translation keeps
// goarchive's "<table>(<trigger>)" shape, one entry per table.
//
// The facts are fed in DELIBERATELY UNSORTED and passed through CheckTriggersPresent,
// because that is where the sort lives — triggerOffenders only takes element [0]. A test
// that hand-fed pre-sorted facts would pass identically against a library that did not
// sort at all, and would prove nothing about the determinism this phase claims.
//
// Table order is fixed here because the fact slice is a literal; it is NOT fixed when
// facts come from Inspector.Triggers over graph.AllNodes(). See the ordering contract.
func TestTriggerOffendersDecoratesFirstTriggerPerTable(t *testing.T) {
	facts := []validations.TriggerInfo{
		// AFTER first in input; BEFORE must still win after the library's sort.
		{Table: "orders", Name: "trg_a", Event: "DELETE", Timing: "AFTER"},
		{Table: "orders", Name: "trg_b", Event: "DELETE", Timing: "BEFORE"},
		{Table: "order_lines", Name: "trg_x", Event: "DELETE", Timing: "AFTER"},
	}

	findings := validations.CheckTriggersPresent(facts, validations.TriggerDelete)
	got, err := triggerOffenders("delete_triggers", findings)
	if err != nil {
		t.Fatalf("triggerOffenders: %v", err)
	}

	want := []string{"orders(trg_b)", "order_lines(trg_x)"}
	if len(got) != len(want) {
		t.Fatalf("triggerOffenders = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("triggerOffenders = %v, want %v", got, want)
		}
	}
}

// TestTriggerOffendersFailsClosed enforces the fail-closed rule and, critically, pins
// WHICH helper reports each fault.
//
// The wantPrefix assertion is the load-bearing one. Without it this implementation
// passes every case:
//
//	if !ok || len(triggers) == 0 {
//	    return nil, unexpectedFindingError(stage, f)   // WRONG helper
//	}
//
// It is non-nil, it is a plain error, and it names the stage — so "aborts correctly"
// cannot tell it apart from the right one. It would report a check goarchive DOES
// recognise as "an unrecognised validation check", which is the exact misdirection the
// two-helper split exists to prevent.
//
// Note on the guard's two operands: `!ok` is SUBSUMED by `len(triggers) == 0`, because a
// failed type assertion yields the zero value — a nil slice. Removing `!ok ||` therefore
// changes no behaviour and no test can detect it. It is kept for explicitness, not
// because it is independently load-bearing, and this comment exists so nobody later
// writes a test claiming to cover it.
func TestTriggerOffendersFailsClosed(t *testing.T) {
	cases := []struct {
		name       string
		finding    validations.Finding
		wantPrefix string
	}{
		{
			name:       "unknown_check_id",
			finding:    validations.Finding{Check: "SOMETHING_NEW"},
			wantPrefix: "PREFLIGHT_UNKNOWN_FINDING",
		},
		{
			name: "wrong_facts_type",
			finding: validations.Finding{
				Check:  validations.IDTriggersPresent,
				Tables: []string{"orders"},
				Facts:  "not-a-TriggerInfo-slice",
			},
			wantPrefix: "PREFLIGHT_UNEXPECTED_FACTS",
		},
		{
			name: "empty_facts_slice",
			finding: validations.Finding{
				Check:  validations.IDTriggersPresent,
				Tables: []string{"orders"},
				Facts:  []validations.TriggerInfo{},
			},
			wantPrefix: "PREFLIGHT_UNEXPECTED_FACTS",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := triggerOffenders("delete_triggers", []validations.Finding{tc.finding})
			if err == nil {
				t.Fatalf("must abort, got offenders %v and nil error", got)
			}
			var pe *PreflightError
			if errors.As(err, &pe) {
				t.Fatalf("fail-closed aborts are plain errors, got *PreflightError: %v", pe)
			}
			if !strings.HasPrefix(err.Error(), tc.wantPrefix) {
				t.Fatalf("wrong helper reported this fault: want prefix %q, got: %v",
					tc.wantPrefix, err)
			}
			if !strings.Contains(err.Error(), "delete_triggers") {
				t.Fatalf("error must name the stage, got: %v", err)
			}
		})
	}
}

// TestUnindexedFKColumnsReportsChildColumns proves the migrated check keeps the
// "<table>.<column>" Tables shape. A composite foreign key yields one entry per child
// column, matching 1.8, where information_schema returned one row per column.
// CheckFKIndexed preserves input order, so the expected order is the input order.
func TestUnindexedFKColumnsReportsChildColumns(t *testing.T) {
	fks := []validations.ForeignKey{
		{ConstraintName: "fk_a", ChildSchema: "srcdb", ChildTable: "order_lines",
			ChildColumns: []string{"order_id"}, ParentTable: "orders", Indexed: false},
		{ConstraintName: "fk_b", ChildSchema: "srcdb", ChildTable: "items",
			ChildColumns: []string{"a", "b"}, ParentTable: "orders", Indexed: false},
		{ConstraintName: "fk_c", ChildSchema: "srcdb", ChildTable: "ok",
			ChildColumns: []string{"x"}, ParentTable: "orders", Indexed: true},
	}
	got, err := unindexedFKColumns("fk_index", validations.CheckFKIndexed(fks))
	if err != nil {
		t.Fatalf("unindexedFKColumns: %v", err)
	}
	want := []string{"order_lines.order_id", "items.a", "items.b"}
	if len(got) != len(want) {
		t.Fatalf("unindexedFKColumns = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unindexedFKColumns = %v, want %v", got, want)
		}
	}
}

// TestUnindexedFKColumnsRejectsUnknownCheck proves an unrecognised Finding.Check aborts
// rather than being silently skipped — a dropped finding is a check that stopped running
// (spec §2). The error must be plain, never a *PreflightError: it reports that this build
// is out of date, not that the schema is wrong.
func TestUnindexedFKColumnsRejectsUnknownCheck(t *testing.T) {
	got, err := unindexedFKColumns("fk_index", []validations.Finding{{Check: "SOMETHING_NEW"}})
	if err == nil {
		t.Fatalf("must abort, got columns %v and nil error", got)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("fail-closed abort must be a plain error, got *PreflightError: %v", pe)
	}
	if !strings.HasPrefix(err.Error(), "PREFLIGHT_UNKNOWN_FINDING") {
		t.Fatalf("wrong helper reported this fault, got: %v", err)
	}
	if !strings.Contains(err.Error(), "fk_index") {
		t.Fatalf("error must name the stage, got: %v", err)
	}
}

// TestPartitionClosureFindingsSplitsByFactsType proves the happy-path split: a
// ForeignKey-typed finding becomes an external-edge entry and a MetadataVisibility-typed
// finding becomes a visibility entry, both under the single IDFKClosure check id.
func TestPartitionClosureFindingsSplitsByFactsType(t *testing.T) {
	extFK := validations.ForeignKey{ConstraintName: "fk_ext", ChildTable: "audit_log", ParentTable: "orders"}
	findings := []validations.Finding{
		{Check: validations.IDFKClosure, Facts: extFK},
		{Check: validations.IDFKClosure, Facts: validations.VisibilityUnconfirmed},
	}

	external, visibility, err := partitionClosureFindings(findings)
	if err != nil {
		t.Fatalf("partitionClosureFindings: %v", err)
	}
	if len(external) != 1 || external[0].ConstraintName != "fk_ext" {
		t.Fatalf("external = %v, want one ForeignKey fk_ext", external)
	}
	if len(visibility) != 1 || visibility[0] != validations.VisibilityUnconfirmed {
		t.Fatalf("visibility = %v, want one VisibilityUnconfirmed", visibility)
	}
}

// TestPartitionClosureFindingsRejectsUnknownCheck proves an unrecognised Finding.Check
// aborts rather than being silently skipped — a dropped finding is a check that stopped
// running (spec §2).
func TestPartitionClosureFindingsRejectsUnknownCheck(t *testing.T) {
	external, visibility, err := partitionClosureFindings(
		[]validations.Finding{{Check: "SOMETHING_NEW"}})
	if err == nil {
		t.Fatalf("must abort, got external=%v visibility=%v and nil error", external, visibility)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("fail-closed abort must be a plain error, got *PreflightError: %v", pe)
	}
	if !strings.HasPrefix(err.Error(), "PREFLIGHT_UNKNOWN_FINDING") {
		t.Fatalf("wrong helper reported this fault, got: %v", err)
	}
	if !strings.Contains(err.Error(), "fk_closure") {
		t.Fatalf("error must name the stage, got: %v", err)
	}
}

// TestPartitionClosureFindingsRejectsWrongFactsType proves a RECOGNISED check arriving
// with a Facts payload that is neither ForeignKey nor MetadataVisibility aborts via
// unexpectedFactsError. The default arm must name BOTH accepted types, since either is
// valid for FK_CLOSURE.
func TestPartitionClosureFindingsRejectsWrongFactsType(t *testing.T) {
	external, visibility, err := partitionClosureFindings([]validations.Finding{
		{Check: validations.IDFKClosure, Facts: "not-a-ForeignKey-or-MetadataVisibility"},
	})
	if err == nil {
		t.Fatalf("must abort, got external=%v visibility=%v and nil error", external, visibility)
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("fail-closed abort must be a plain error, got *PreflightError: %v", pe)
	}
	if !strings.HasPrefix(err.Error(), "PREFLIGHT_UNEXPECTED_FACTS") {
		t.Fatalf("wrong helper reported this fault, got: %v", err)
	}
	for _, want := range []string{"fk_closure", "validations.ForeignKey", "validations.MetadataVisibility"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error must mention %q, got: %v", want, err)
		}
	}
}

// TestUnindexedFKColumnsRejectsWrongFactsType proves a RECOGNISED check arriving with an
// unexpected Facts payload aborts via unexpectedFactsError. Facts holds ForeignKey BY
// VALUE, so a pointer payload is wrong too — assert that case specifically, because a
// value-vs-pointer slip is silent: every finding would be rejected at runtime while the
// happy-path test above, which builds findings through CheckFKIndexed, still passes.
func TestUnindexedFKColumnsRejectsWrongFactsType(t *testing.T) {
	fk := validations.ForeignKey{ChildTable: "order_lines", ChildColumns: []string{"order_id"}}
	cases := []struct {
		name    string
		finding validations.Finding
	}{
		{
			name: "wrong_type",
			finding: validations.Finding{
				Check: validations.IDFKIndexed,
				Facts: "not-a-ForeignKey",
			},
		},
		{
			name: "pointer_instead_of_value",
			finding: validations.Finding{
				Check: validations.IDFKIndexed,
				Facts: &fk,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := unindexedFKColumns("fk_index", []validations.Finding{tc.finding})
			if err == nil {
				t.Fatalf("must abort, got columns %v and nil error", got)
			}
			var pe *PreflightError
			if errors.As(err, &pe) {
				t.Fatalf("fail-closed abort must be a plain error, got *PreflightError: %v", pe)
			}
			if !strings.HasPrefix(err.Error(), "PREFLIGHT_UNEXPECTED_FACTS") {
				t.Fatalf("wrong helper reported this fault, got: %v", err)
			}
			if !strings.Contains(err.Error(), "fk_index") {
				t.Fatalf("error must name the stage, got: %v", err)
			}
		})
	}
}

// TestReconcileInternalFKsNoEdge pins the "no graph edge" discrepancy.
func TestReconcileInternalFKsNoEdge(t *testing.T) {
	g := graph.NewGraph("orders", "id")
	g.SetPK("order_lines", "id")
	g.AddNode("order_lines", nil)

	fks := []validations.ForeignKey{{
		ConstraintName: "fk_ol_o", ChildTable: "order_lines", ChildColumns: []string{"order_id"},
		ParentTable: "orders", ParentColumns: []string{"id"},
	}}
	got := reconcileInternalFKs(fks, g)
	if len(got) != 1 {
		t.Fatalf("reconcileInternalFKs = %v, want 1 discrepancy", got)
	}
	if !strings.Contains(got[0], "no graph edge") {
		t.Fatalf("discrepancy must say why: %q", got[0])
	}
}

// TestReconcileInternalFKsColumnMismatch pins the "FK column mismatch" discrepancy.
func TestReconcileInternalFKsColumnMismatch(t *testing.T) {
	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", nil)
	g.SetPK("order_lines", "id")
	g.AddEdgeWithMeta("orders", "order_lines", "alt_id", "id", "")

	fks := []validations.ForeignKey{{
		ConstraintName: "fk_ol_o", ChildTable: "order_lines", ChildColumns: []string{"order_id"},
		ParentTable: "orders", ParentColumns: []string{"id"},
	}}
	got := reconcileInternalFKs(fks, g)
	if len(got) != 1 || !strings.Contains(got[0], "FK column mismatch") {
		t.Fatalf("reconcileInternalFKs = %v", got)
	}
}

// TestReconcileInternalFKsReferenceMismatch pins the "reference column mismatch"
// discrepancy: the DB references a column that is not the configured parent PK.
func TestReconcileInternalFKsReferenceMismatch(t *testing.T) {
	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", nil)
	g.SetPK("order_lines", "id")
	g.AddEdgeWithMeta("orders", "order_lines", "order_id", "id", "")

	fks := []validations.ForeignKey{{
		ConstraintName: "fk_ol_o", ChildTable: "order_lines", ChildColumns: []string{"order_id"},
		ParentTable: "orders", ParentColumns: []string{"legacy_id"},
	}}
	got := reconcileInternalFKs(fks, g)
	if len(got) != 1 || !strings.Contains(got[0], "reference column mismatch") {
		t.Fatalf("reconcileInternalFKs = %v", got)
	}
}

// TestReconcileInternalFKsCompositeIsRejected pins the composite-FK decision: a graph
// edge carries exactly one foreign_key, so a multi-column foreign key between two graph
// tables cannot be represented and is reported as a discrepancy. 1.8 reached the same
// FATAL outcome by accident (one information_schema row per column, at least one of
// which mismatched); 2.0 says so explicitly.
//
// The edge below MATCHES on its single column, so the composite guard is the only thing
// that can produce a discrepancy here — the test cannot pass for the wrong reason.
func TestReconcileInternalFKsCompositeIsRejected(t *testing.T) {
	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", nil)
	g.SetPK("order_lines", "id")
	g.AddEdgeWithMeta("orders", "order_lines", "order_id", "id", "")

	fks := []validations.ForeignKey{{
		ConstraintName: "fk_ol_o", ChildTable: "order_lines",
		ChildColumns: []string{"order_id", "tenant_id"},
		ParentTable:  "orders", ParentColumns: []string{"id", "tenant_id"},
	}}
	got := reconcileInternalFKs(fks, g)
	if len(got) != 1 || !strings.Contains(got[0], "multi-column foreign key") {
		t.Fatalf("reconcileInternalFKs = %v", got)
	}
}

// TestReconcileInternalFKsSelfReferenceSkipped pins the 1.8 exemption for
// self-referencing constraints (e.g. category.parent_id -> category.id). No edge exists,
// so without the exemption this would report "no graph edge".
func TestReconcileInternalFKsSelfReferenceSkipped(t *testing.T) {
	g := graph.NewGraph("category", "id")
	fks := []validations.ForeignKey{{
		ConstraintName: "fk_self", ChildTable: "category", ChildColumns: []string{"parent_id"},
		ParentTable: "category", ParentColumns: []string{"id"},
	}}
	if got := reconcileInternalFKs(fks, g); len(got) != 0 {
		t.Fatalf("self-referencing FK must be skipped, got %v", got)
	}
}

// TestReconcileInternalFKsMatching is the negative control.
func TestReconcileInternalFKsMatching(t *testing.T) {
	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", nil)
	g.SetPK("order_lines", "id")
	g.AddEdgeWithMeta("orders", "order_lines", "order_id", "id", "")

	fks := []validations.ForeignKey{{
		ConstraintName: "fk_ol_o", ChildTable: "order_lines", ChildColumns: []string{"order_id"},
		ParentTable: "orders", ParentColumns: []string{"id"},
	}}
	if got := reconcileInternalFKs(fks, g); len(got) != 0 {
		t.Fatalf("matching configuration must produce no discrepancy, got %v", got)
	}
}

// TestDedupeCascadeEdgesCollapsesWithinGraphConstraints proves the deduplication spec
// §3.5 requires. A constraint whose child AND parent are both graph tables appears in
// both the IncomingTo and OutgoingFrom results; without dedup it would be warned twice.
func TestDedupeCascadeEdgesCollapsesWithinGraphConstraints(t *testing.T) {
	shared := validations.ForeignKey{
		ConstraintName: "fk_ol_o",
		ChildSchema:    "srcdb", ChildTable: "order_lines", ChildColumns: []string{"order_id"},
		ParentSchema: "srcdb", ParentTable: "orders", ParentColumns: []string{"id"},
		OnDelete: "CASCADE",
	}
	external := validations.ForeignKey{
		ConstraintName: "fk_audit",
		ChildSchema:    "other", ChildTable: "audit", ChildColumns: []string{"order_id"},
		ParentSchema: "srcdb", ParentTable: "orders", ParentColumns: []string{"id"},
		OnDelete: "CASCADE",
	}

	incoming := validations.CheckCascadeRules([]validations.ForeignKey{shared, external})
	outgoing := validations.CheckCascadeRules([]validations.ForeignKey{shared})

	got, err := dedupeCascadeEdges("cascade", incoming, outgoing)
	if err != nil {
		t.Fatalf("dedupeCascadeEdges: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("dedupeCascadeEdges = %v, want 2 distinct constraints", got)
	}
	// First-seen order, NOT sorted — the helper is order-preserving by contract.
	if got[0] != "srcdb.order_lines.order_id->srcdb.orders.id" {
		t.Fatalf("got[0] = %q", got[0])
	}
	if got[1] != "other.audit.order_id->srcdb.orders.id" {
		t.Fatalf("got[1] = %q", got[1])
	}
}

// TestDedupeCascadeEdgesKeyIsChildSideTriple proves the dedup key is
// (ChildSchema, ChildTable, ConstraintName): two constraints sharing a name but living
// on different child tables are distinct and both reported.
func TestDedupeCascadeEdgesKeyIsChildSideTriple(t *testing.T) {
	a := validations.ForeignKey{
		ConstraintName: "fk_parent",
		ChildSchema:    "srcdb", ChildTable: "order_lines", ChildColumns: []string{"order_id"},
		ParentSchema: "srcdb", ParentTable: "orders", ParentColumns: []string{"id"},
		OnDelete: "CASCADE",
	}
	b := a
	b.ChildTable = "items"

	got, err := dedupeCascadeEdges("cascade", validations.CheckCascadeRules([]validations.ForeignKey{a, b}))
	if err != nil {
		t.Fatalf("dedupeCascadeEdges: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("dedupeCascadeEdges = %v, want both constraints", got)
	}
}

// TestDedupeCascadeEdgesRejectsUnknownCheck enforces the fail-closed rule on the check id.
//
// unexpectedFindingError's actual prefix is PREFLIGHT_UNKNOWN_FINDING (preflight_findings.go),
// as asserted by every other consumer of that helper in this file; the plan text for this
// test cited PREFLIGHT_UNEXPECTED_FINDING, which does not match production behaviour.
func TestDedupeCascadeEdgesRejectsUnknownCheck(t *testing.T) {
	_, err := dedupeCascadeEdges("cascade", []validations.Finding{{Check: "SOMETHING_NEW"}})
	if err == nil {
		t.Fatal("an unrecognised finding must abort preflight")
	}
	if !strings.Contains(err.Error(), "PREFLIGHT_UNKNOWN_FINDING") {
		t.Fatalf("wrong error shape: %v", err)
	}
}

// TestDedupeCascadeEdgesRejectsWrongFactsType enforces the fail-closed rule on the facts
// payload, in both shapes that matter.
//
// The *validations.ForeignKey case is the load-bearing one: the library sets
// Facts: *key — BY VALUE — so a pointer assertion compiles cleanly and then fails at
// runtime for every finding. That is a live regression risk every time this helper is
// edited, not a hypothetical one. The plainly-wrong type is the ordinary guard.
func TestDedupeCascadeEdgesRejectsWrongFactsType(t *testing.T) {
	fk := validations.ForeignKey{
		ConstraintName: "fk_ol_o",
		ChildSchema:    "srcdb", ChildTable: "order_lines", ChildColumns: []string{"order_id"},
		ParentSchema: "srcdb", ParentTable: "orders", ParentColumns: []string{"id"},
		OnDelete: "CASCADE",
	}

	for _, tc := range []struct {
		name  string
		facts any
	}{
		{"plainly wrong type", "not a foreign key"},
		{"pointer instead of value", &fk},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dedupeCascadeEdges("cascade", []validations.Finding{
				{Check: validations.IDCascadeRules, Facts: tc.facts},
			})
			if err == nil {
				t.Fatal("an unrecognised facts payload must abort preflight")
			}
			if !strings.Contains(err.Error(), "PREFLIGHT_UNEXPECTED_FACTS") {
				t.Fatalf("wrong error shape: %v", err)
			}
		})
	}
}
