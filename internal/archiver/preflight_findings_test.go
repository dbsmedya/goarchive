package archiver

import (
	"errors"
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// TestFindingsToPreflightErrorMapsExpectedCheck proves a matching finding becomes a
// *PreflightError carrying goarchive's ID and the finding's table names.
func TestFindingsToPreflightErrorMapsExpectedCheck(t *testing.T) {
	findings := []validations.Finding{
		{Check: validations.IDTablesExist, Tables: []string{"orders"}},
		{Check: validations.IDTablesExist, Tables: []string{"lines"}},
	}
	err := findingsToPreflightError("TABLE_EXISTENCE_CHECK", "Tables not found in source database",
		findings, validations.IDTablesExist)
	if err == nil {
		t.Fatal("expected a *PreflightError")
	}
	if err.Check != "TABLE_EXISTENCE_CHECK" {
		t.Fatalf("Check = %q", err.Check)
	}
	if strings.Join(err.Tables, ",") != "orders,lines" {
		t.Fatalf("Tables = %v, want [orders lines] in finding order", err.Tables)
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
