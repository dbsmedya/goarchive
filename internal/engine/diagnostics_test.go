package engine

import (
	"errors"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

func diags(kinds map[diagnosticKey]uint64) insertDiagnostics {
	var n uint64
	for _, c := range kinds {
		n += c
	}
	return insertDiagnostics{Count: n, Retained: n, ByKind: kinds}
}

func TestClassifyInsertDiagnostics(t *testing.T) {
	conversion := map[diagnosticKey]uint64{{"Warning", 1265}: 2, {"Warning", 1366}: 1}
	for _, tc := range []struct {
		name   string
		v      jobspec.Verification
		d      insertDiagnostics
		want   string // "" accepted, else the identifier
		counts map[uint16]uint64
	}{
		{"none clean", jobspec.VerifyNone, diags(nil), "", map[uint16]uint64{}},
		{"none accepts conversions", jobspec.VerifyNone, diags(conversion), "", map[uint16]uint64{1265: 2, 1366: 1}},
		{"sha256 refuses a conversion", jobspec.VerifySHA256, diags(conversion), "INSERT_DIAGNOSTIC_REJECTED", nil},
		{"count refuses a conversion", jobspec.VerifyCount, diags(conversion), "INSERT_DIAGNOSTIC_REJECTED", nil},
		{"a duplicate warning is refused", jobspec.VerifyNone, diags(map[diagnosticKey]uint64{{"Warning", 1062}: 1}), "INSERT_DIAGNOSTIC_REJECTED", nil},
		{"another code is refused", jobspec.VerifyNone, diags(map[diagnosticKey]uint64{{"Note", 1287}: 1}), "INSERT_DIAGNOSTIC_REJECTED", nil},
		{"an error level is refused", jobspec.VerifyNone, diags(map[diagnosticKey]uint64{{"Error", 1265}: 1}), "INSERT_DIAGNOSTIC_REJECTED", nil},
		{"an incomplete list is unproven", jobspec.VerifyNone, insertDiagnostics{Count: 3, Retained: 2, ByKind: map[diagnosticKey]uint64{{"Warning", 1265}: 2}}, "INSERT_DIAGNOSTICS_UNPROVEN", nil},
	} {
		got, err := classifyInsertDiagnostics("t", tc.v, tc.d)
		var de *insertDiagnosticError
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		case tc.want == "":
			if len(got) != len(tc.counts) {
				t.Errorf("%s: accepted %v, want %v", tc.name, got, tc.counts)
			}
			for code, n := range tc.counts {
				if got[code] != n {
					t.Errorf("%s: accepted %v, want %v", tc.name, got, tc.counts)
				}
			}
		case !errors.As(err, &de) || de.Identifier != tc.want:
			t.Errorf("%s: err = %v, want %s", tc.name, err, tc.want)
		}
	}
}

func TestInsertRows(t *testing.T) {
	for _, tc := range []struct{ want, columns, maxErrors, rows int }{
		{1000, 10, 1024, 1000},
		{1000, 100, 0, 655}, // 65,535 placeholders
		{1000, 10, 64, 64},  // every condition of the statement must be retained
		{1, 70000, 1024, 1}, // never below one row
		{500, 0, 0, 500},
	} {
		if got := insertRows(tc.want, tc.columns, tc.maxErrors); got != tc.rows {
			t.Errorf("insertRows(%d, %d, %d) = %d, want %d", tc.want, tc.columns, tc.maxErrors, got, tc.rows)
		}
	}
}
