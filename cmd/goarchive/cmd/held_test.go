package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// A run that the server held while a newly elected primary applied its backlog, and that the session's
// wait_timeout then ended with error 3781, is reported as a named stop with its remedy, whichever statement
// returned it (spec §4.2 rule 3, §4.7; GR C4). One shared helper, applied before any output drops the error's
// chain: the closing log entry, the command's diagnostic and validate's line for a job show the same text.

const heldStop = "the server held a statement of this run while a newly elected Group Replication primary applied its backlog, " +
	"and the session's wait_timeout passed first (MySQL error 3781). The statement did not run. " +
	"Run again when the primary is writable; the next run recovers what the log holds."

// err3781 is the error a held statement ends with (M5's direct client, fact 22).
func err3781() error {
	return &mysql.MySQLError{Number: 3781, SQLState: [5]byte{'H', 'Y', '0', '0', '0'},
		Message: "Timeout on wait for view after joining group"}
}

// TestHeldStatementTimeout: the error chain of a preflight statement, a recovery statement and a batch
// statement that holds 3781 is the named stop; a cancelled context and other error numbers are not.
func TestHeldStatementTimeout(t *testing.T) {
	me := err3781()
	for name, err := range map[string]error{
		"a preflight statement": fmt.Errorf("read the source's Group Replication state: %w", me),
		"a recovery statement":  fmt.Errorf("engine: recovery: engine: delete from order_items: %w", me),
		"a batch statement":     fmt.Errorf("engine: batch 3: engine: insert into order_items: %w", me),
		"the error itself":      me,
	} {
		if got := heldStatementTimeout(err); got != heldStop {
			t.Errorf("%s: heldStatementTimeout = %q, want the named stop", name, got)
		}
	}
	for name, err := range map[string]error{
		"nil":                   nil,
		"a cancelled context":   fmt.Errorf("engine: batch 1: %w", context.Canceled),
		"a deadline":            context.DeadlineExceeded,
		"3100":                  fmt.Errorf("engine: batch 1: %w", &mysql.MySQLError{Number: 3100, Message: "hook"}),
		"3231":                  &mysql.MySQLError{Number: 3231, Message: "write set"},
		"1290":                  &mysql.MySQLError{Number: 1290, Message: "read-only"},
		"another number, 37810": &mysql.MySQLError{Number: 37810, Message: "no"},
		"a plain error":         errors.New("connection reset"),
		"the text alone":        errors.New("MySQL error 3781 in a string"),
	} {
		if got := heldStatementTimeout(err); got != "" {
			t.Errorf("%s: heldStatementTimeout = %q, want none", name, got)
		}
	}
}

// TestFailureTextIsTheNamedStopOnlyFor3781: where a failure is printed, the error reads as it did, except for
// 3781, which reads as the named stop.
func TestFailureTextIsTheNamedStopOnlyFor3781(t *testing.T) {
	other := fmt.Errorf("engine: batch 1: %w", &mysql.MySQLError{Number: 1290, Message: "read-only"})
	if got := failureText(other); got != other.Error() {
		t.Errorf("failureText(other) = %q, want %q", got, other.Error())
	}
	if got := failureText(fmt.Errorf("engine: batch 2: %w", err3781())); got != heldStop {
		t.Errorf("failureText(3781) = %q, want the named stop", got)
	}
}

// TestTheClosingLogEntryShowsTheNamedStop: a run that ends with 3781 closes its log with an error that is the
// named stop, and keeps the statement's own error in a field; any other error is logged as it was.
func TestTheClosingLogEntryShowsTheNamedStop(t *testing.T) {
	cause := fmt.Errorf("engine: batch 2: engine: insert into order_items: %w", err3781())
	entry := lastLogEntry(t, nil, cause)
	if entry["error"] != heldStop {
		t.Errorf("the log's error = %v, want the named stop", entry["error"])
	}
	if got, _ := entry["cause"].(string); got != cause.Error() {
		t.Errorf("the log's cause = %q, want the statement's own error %q", got, cause.Error())
	}
	plain := errors.New("engine: batch 2: something else")
	entry = lastLogEntry(t, nil, plain)
	if entry["error"] != plain.Error() {
		t.Errorf("the log's error = %v, want %q", entry["error"], plain)
	}
	if _, has := entry["cause"]; has {
		t.Errorf("an ordinary failure has a cause field: %v", entry)
	}
}

// TestTheDiagnosticShowsTheNamedStop: the command's diagnostic on stderr is the named stop, once, and the exit
// code is 1 as for any failed run.
func TestTheDiagnosticShowsTheNamedStop(t *testing.T) {
	saved := started
	started = true
	t.Cleanup(func() { started = saved })
	var stderr bytes.Buffer
	if code := finish(fmt.Errorf("engine: batch 4: %w", err3781()), &stderr); code != exitFailed {
		t.Errorf("exit code = %d, want %d", code, exitFailed)
	}
	if got, want := stderr.String(), "Error: "+heldStop+"\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
	stderr.Reset()
	other := fmt.Errorf("engine: batch 4: %w", &mysql.MySQLError{Number: 1290, Message: "read-only"})
	if code := finish(other, &stderr); code != exitFailed || stderr.String() != "Error: "+other.Error()+"\n" {
		t.Errorf("another error: exit %d, stderr %q, want %d and %q", code, stderr.String(), exitFailed, "Error: "+other.Error()+"\n")
	}
}

// TestValidateShowsTheNamedStopOnTheFailingJob: validate prints each job's failure, then returns an aggregate
// that no longer holds the cause, so the named stop is applied where each failure is printed.
func TestValidateShowsTheNamedStopOnTheFailingJob(t *testing.T) {
	var out bytes.Buffer
	printFailure(&out, fmt.Errorf("read the source's Group Replication state: %w", err3781()))
	if got, want := out.String(), "   FAILED: "+heldStop+"\n\n"; got != want {
		t.Errorf("held job's line = %q, want %q", got, want)
	}
	out.Reset()
	other := errors.New("PK_COLUMN_CHECK: the primary key does not match")
	printFailure(&out, other)
	if got, want := out.String(), "   FAILED: "+other.Error()+"\n\n"; got != want {
		t.Errorf("another failure's line = %q, want %q", got, want)
	}
	if strings.Contains(out.String(), "3781") {
		t.Errorf("an ordinary failure mentions 3781: %q", out.String())
	}
}
