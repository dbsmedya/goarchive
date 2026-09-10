package archiver

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"
)

func TestInsertErrorCauseText(t *testing.T) {
	for _, identifier := range []string{"INSERT_DIAGNOSTIC_REJECTED", "INSERT_DIAGNOSTICS_UNPROVEN"} {
		t.Run(identifier, func(t *testing.T) {
			cause := &mysql.MySQLError{Number: 1044, Message: "permission sentinel"}
			insertErr := &insertDiagnosticError{
				Identifier: identifier,
				Table:      "t",
				Reason:     "read failed",
				Count:      1,
				Retained:   1,
				Preview:    []insertDiagnostic{{Level: "Warning", Code: 1264, Message: "preview"}},
				Cause:      cause,
			}
			err := fmt.Errorf("outer: %w", insertErr)

			if !strings.Contains(err.Error(), cause.Error()) {
				t.Fatalf("INSERT_CAUSE_HIDDEN: rendered error omits MySQL cause: %v", err)
			}
			if !strings.Contains(err.Error(), identifier+": table=t count=1 retained=1: read failed") {
				t.Fatalf("insert identifier or context missing: %v", err)
			}
			if identifier == "INSERT_DIAGNOSTIC_REJECTED" && !strings.Contains(err.Error(), "diagnostics=[{Warning 1264 preview}]") {
				t.Fatalf("rejected diagnostic preview missing: %v", err)
			}
			if !errors.Is(err, cause) {
				t.Fatalf("cause missing from unwrap chain: %v", err)
			}
			var mysqlErr *mysql.MySQLError
			if !errors.As(err, &mysqlErr) || mysqlErr != cause {
				t.Fatalf("numbered MySQL cause missing from chain: %v", err)
			}
			var got *insertDiagnosticError
			if !errors.As(err, &got) || got != insertErr {
				t.Fatalf("insert diagnostic error missing from chain: %v", err)
			}
		})
	}
}

func TestInsertErrorNilCause(t *testing.T) {
	preview := []insertDiagnostic{{Level: "Warning", Code: 1264, Message: "preview"}}
	for _, tc := range []struct {
		identifier string
		want       string
	}{
		{
			identifier: "INSERT_DIAGNOSTIC_REJECTED",
			want:       "INSERT_DIAGNOSTIC_REJECTED: table=t count=1 retained=1: read failed; diagnostics=[{Warning 1264 preview}]",
		},
		{
			identifier: "INSERT_DIAGNOSTICS_UNPROVEN",
			want:       "INSERT_DIAGNOSTICS_UNPROVEN: table=t count=1 retained=1: read failed",
		},
	} {
		t.Run(tc.identifier, func(t *testing.T) {
			err := &insertDiagnosticError{
				Identifier: tc.identifier,
				Table:      "t",
				Reason:     "read failed",
				Count:      1,
				Retained:   1,
				Preview:    preview,
			}
			if got := err.Error(); got != tc.want {
				t.Fatalf("nil-cause formatting changed: got %q want %q", got, tc.want)
			}
		})
	}
}
