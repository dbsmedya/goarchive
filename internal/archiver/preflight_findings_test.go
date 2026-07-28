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
