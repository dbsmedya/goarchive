package archiver

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func diagnosticReport(codes ...uint16) insertDiagnostics {
	d := insertDiagnostics{Count: uint64(len(codes)), Retained: uint64(len(codes)), ByKind: map[diagnosticKey]uint64{}}
	for _, code := range codes {
		d.ByKind[diagnosticKey{"Warning", code}]++
	}
	return d
}
func TestInsertDiagnosticPolicy(t *testing.T) {
	normal := insertDiagnosticContext{Table: "t", UsesIgnore: true, VerificationMethod: "sha256"}
	skip := insertDiagnosticContext{Table: "t", SkipVerification: true, VerificationMethod: "sha256"}
	sample := insertDiagnosticContext{Table: "t", UsesIgnore: true, Sample: true}
	for _, tc := range []struct {
		name string
		c    insertDiagnosticContext
		d    insertDiagnostics
		want string
	}{
		{"clean", normal, diagnosticReport(), ""}, {"duplicate", normal, diagnosticReport(1062), ""}, {"default_conversion", normal, diagnosticReport(1264), "INSERT_DIAGNOSTIC_REJECTED"}, {"mixed", normal, diagnosticReport(1264, 1264, 1062), "INSERT_DIAGNOSTIC_REJECTED"}, {"skip_conversion", skip, diagnosticReport(1264, 1265, 1292, 1366), ""}, {"skip_duplicate", skip, diagnosticReport(1062), "INSERT_DIAGNOSTIC_REJECTED"}, {"unknown", skip, diagnosticReport(9999), "INSERT_DIAGNOSTIC_REJECTED"}, {"fk_skip", skip, diagnosticReport(1452), "INSERT_DIAGNOSTIC_REJECTED"}, {"null_default", skip, diagnosticReport(1048), "INSERT_DIAGNOSTIC_REJECTED"}, {"sample_duplicate", sample, diagnosticReport(1062), ""}, {"sample_conversion_default", sample, diagnosticReport(1264), "INSERT_DIAGNOSTIC_REJECTED"}, {"missing_details", normal, insertDiagnostics{Count: 2, Retained: 1, ByKind: map[diagnosticKey]uint64{{"Warning", 1062}: 1}}, "INSERT_DIAGNOSTICS_UNPROVEN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := classifyInsertDiagnostics(tc.c, tc.d)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("policy want %s, got %v", tc.want, err)
			}
		})
	}
}
func TestInsertDiagnosticPolicyCodeRegistry(t *testing.T) {
	for _, code := range []uint16{1264, 1265, 1292, 1366, 1263, 1266, 1293, 1364, 1365, 1406} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("code%d_skip%v", code, skip), func(t *testing.T) {
				allowed := skip && (code == 1264 || code == 1265 || code == 1292 || code == 1366)
				_, err := classifyInsertDiagnostics(insertDiagnosticContext{SkipVerification: skip, VerificationMethod: "count"}, diagnosticReport(code))
				if (err == nil) != allowed {
					t.Fatalf("default accepted code%d or registry mismatch: skip=%v err=%v", code, skip, err)
				}
			})
		}
	}
}
func TestInsertDiagnosticPolicyLevels(t *testing.T) {
	for _, level := range []string{"Note", "Warning", "Error", "other"} {
		for _, skip := range []bool{false, true} {
			for _, code := range []uint16{1264, 1062} {
				t.Run(fmt.Sprintf("%s_%d_%v", level, code, skip), func(t *testing.T) {
					d := insertDiagnostics{Count: 1, Retained: 1, ByKind: map[diagnosticKey]uint64{{level, code}: 1}}
					_, err := classifyInsertDiagnostics(insertDiagnosticContext{SkipVerification: skip, VerificationMethod: "count"}, d)
					allowed := skip && code == 1264 && (level == "Note" || level == "Warning")
					if (err == nil) != allowed {
						t.Fatalf("level policy=%s/%d skip=%v err=%v", level, code, skip, err)
					}
				})
			}
		}
	}
}
func TestInsertDiagnosticPolicyContext(t *testing.T) {
	for _, c := range []insertDiagnosticContext{{UsesIgnore: true, VerificationMethod: "count"}, {UsesIgnore: true, VerificationMethod: "sha256", SkipVerification: true}, {UsesIgnore: true, VerificationMethod: "sha256", HasSecondaryUnique: true}} {
		_, err := classifyInsertDiagnostics(c, diagnosticReport())
		if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTICS_UNPROVEN") {
			t.Fatalf("inconsistent context accepted: %v", err)
		}
	}
	_, err := classifyInsertDiagnostics(insertDiagnosticContext{Sample: true, UsesIgnore: true, SkipVerification: true}, diagnosticReport(1062, 1264))
	if err != nil {
		t.Fatal(err)
	}
	for _, codes := range [][]uint16{{1264, 9999}, {9999, 1264}} {
		_, err := classifyInsertDiagnostics(insertDiagnosticContext{SkipVerification: true}, diagnosticReport(codes...))
		if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTIC_REJECTED") || !strings.Contains(err.Error(), "9999") {
			t.Fatalf("recognized conversion masked unknown diagnostic: %v", err)
		}
	}
	d := diagnosticReport(1062)
	d.Count = 0
	_, err = classifyInsertDiagnostics(insertDiagnosticContext{}, d)
	if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTICS_UNPROVEN") {
		t.Fatal(err)
	}
}
func TestInsertDiagnosticErrors(t *testing.T) {
	cause := errors.New("sentinel")
	for _, id := range []string{"INSERT_DIAGNOSTIC_REJECTED", "INSERT_DIAGNOSTICS_UNPROVEN"} {
		e := &insertDiagnosticError{Identifier: id, Table: "t", Cause: cause}
		if !errors.Is(e, cause) || !strings.Contains(e.Error(), id) {
			t.Fatal(e)
		}
	}
}
func TestEffectiveInsertRows(t *testing.T) {
	for _, tc := range []struct{ rows, cols, cap, want int }{{5000, 13, 1024, 1024}, {5000, 14, 0, 4681}, {100, 3, 1024, 100}, {5000, 13, 1, 1}, {5000, 13, 0, 5000}} {
		if got := effectiveInsertRows(tc.rows, tc.cols, tc.cap); got != tc.want {
			t.Fatalf("clamp(%+v)=%d", tc, got)
		}
	}
}
