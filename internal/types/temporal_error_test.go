package types

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"
)

func TestTemporalErrorCauseText(t *testing.T) {
	cause := &mysql.MySQLError{Number: 1044, Message: "permission sentinel"}
	temporalErr := &TemporalReadContractError{
		Table:     "t",
		Operation: "copy",
		Detail:    "read failed",
		Cause:     cause,
	}
	err := fmt.Errorf("outer: %w", temporalErr)

	if !strings.Contains(err.Error(), cause.Error()) {
		t.Fatalf("TEMPORAL_CAUSE_HIDDEN: rendered error omits MySQL cause: %v", err)
	}
	if !strings.Contains(err.Error(), "TEMPORAL_READ_CONTRACT: table=t operation=copy: read failed") {
		t.Fatalf("temporal identifier or context missing: %v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("cause missing from unwrap chain: %v", err)
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr != cause {
		t.Fatalf("numbered MySQL cause missing from chain: %v", err)
	}
	var got *TemporalReadContractError
	if !errors.As(err, &got) || got != temporalErr {
		t.Fatalf("temporal error missing from chain: %v", err)
	}
}

func TestTemporalErrorNilCause(t *testing.T) {
	err := &TemporalReadContractError{
		Table:     "t",
		Operation: "copy",
		Detail:    "read\nfailed\rnow",
	}
	want := "TEMPORAL_READ_CONTRACT: table=t operation=copy: read failed now"
	if got := err.Error(); got != want {
		t.Fatalf("nil-cause formatting changed: got %q want %q", got, want)
	}
	if got := fmt.Errorf("outer: %w", err).Error(); got != "outer: "+want {
		t.Fatalf("outer nil-cause formatting changed: got %q", got)
	}
}
