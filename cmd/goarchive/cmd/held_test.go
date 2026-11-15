package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"github.com/go-sql-driver/mysql"
)

// A run that the server held while a newly elected primary applied its backlog, and that the session's
// wait_timeout then ended with error 3781, is reported as a named stop with its remedy, whichever statement
// returned it (spec §4.2 rule 3, §4.7; GR C4). One shared helper, applied before any output drops the error's
// chain: the closing log entry, the command's diagnostic and validate's line for a job show the same text.

const heldStop = "the server held a statement of this run while a newly elected Group Replication primary applied its backlog, " +
	"and the session's wait_timeout passed first (MySQL error 3781). The statement did not run. " +
	"Run again when the primary is writable; the next run recovers what the log holds."

// heldBeforeStop is spec §4.7's message for MySQL error 3797 (rev 5.3): the same hold under a session whose
// group_replication_consistency is BEFORE or BEFORE_AND_AFTER. It is not the 3781 message: 3797 is not only a
// failover's error, since a BEFORE read on a secondary whose applier is behind ends with it too, so the text
// names both cases, does not say which it was, and says whose setting the value is.
const heldBeforeStop = "the server made a statement of this run wait until this member had applied the group's earlier transactions, " +
	"and the wait ended first (MySQL error 3797). The statement did not run. " +
	"Sessions whose group_replication_consistency is BEFORE or BEFORE_AND_AFTER wait this way, on a newly elected primary " +
	"that is applying its backlog and on a secondary whose applier is behind, for at most the session's wait_timeout. " +
	"GoArchive does not set either value: it is the server's or the account's setting. " +
	"Run again when the member has caught up; the next run recovers what the log holds."

// err3797 is the error a held statement ends with under BEFORE or BEFORE_AND_AFTER, as the library's fence table
// recorded it (six servers, sessions with wait_timeout 3).
func err3797() error {
	return &mysql.MySQLError{Number: 3797, SQLState: [5]byte{'H', 'Y', '0', '0', '0'},
		Message: "Error while waiting for group transactions commit on group_replication_consistency= 'BEFORE'"}
}

// err3781 is the error a held statement ends with, as M5's direct client got it (README_COMPAT, M5).
func err3781() error {
	return &mysql.MySQLError{Number: 3781, SQLState: [5]byte{'H', 'Y', '0', '0', '0'},
		Message: "Timeout exceeded for held statement while new Group Replication primary member is applying backlog."}
}

// TestHeldStatementTimeout: the error chain of a preflight statement, a recovery statement and a batch
// statement that holds 3781 is the named stop; a cancelled context and other error numbers are not.
func TestHeldStatementTimeout(t *testing.T) {
	me := err3781()
	for name, err := range map[string]error{
		"a preflight statement": fmt.Errorf("read the source's Group Replication state: %w", me),
		// the library's own error type, as a held fact read returns it: its Unwrap keeps the number in the chain
		"a library fact read":  fmt.Errorf("read the source's Group Replication state: %w", &dbsrepl.OpError{Op: "group_replication", Err: me}),
		"a recovery statement": fmt.Errorf("engine: recovery: engine: delete from order_items: %w", me),
		"a batch statement":    fmt.Errorf("engine: batch 3: engine: insert into order_items: %w", me),
		"the error itself":     me,
	} {
		if got := heldStatementTimeout(err); got != heldStop {
			t.Errorf("%s: heldStatementTimeout = %q, want the named stop", name, got)
		}
	}
	// 3797 is a named stop of its own (rev 5.3), found by number in the same chains, and a hold that began at the
	// start of a transaction is found too: the library found START TRANSACTION itself held on a fenced primary.
	m97 := err3797()
	for name, err := range map[string]error{
		"a preflight statement":    fmt.Errorf("read the source's Group Replication state: %w", m97),
		"a library fact read":      fmt.Errorf("read the source's Group Replication state: %w", &dbsrepl.OpError{Op: "group_replication", Err: m97}),
		"a recovery statement":     fmt.Errorf("engine: recovery: engine: delete from order_items: %w", m97),
		"a batch statement":        fmt.Errorf("engine: batch 3: engine: insert into order_items: %w", m97),
		"a transaction that began": fmt.Errorf("engine: batch 3: begin the copy transaction: %w", m97),
		"a read before any table":  fmt.Errorf("engine: batch 1: start transaction: %w", m97),
		"the error itself":         m97,
	} {
		if got := heldStatementTimeout(err); got != heldBeforeStop {
			t.Errorf("3797, %s: heldStatementTimeout = %q, want the 3797 message", name, got)
		}
		if got := heldStatementTimeout(err); got == heldStop {
			t.Errorf("3797, %s: reported with the 3781 message", name)
		}
	}
	// the two numbers are told apart, in both directions
	if got := heldStatementTimeout(fmt.Errorf("batch 1: %w", err3781())); got != heldStop || got == heldBeforeStop {
		t.Errorf("3781 = %q, want its own message and not the 3797 one", got)
	}
	for name, err := range map[string]error{
		"nil":                   nil,
		"a cancelled context":   fmt.Errorf("engine: batch 1: %w", context.Canceled),
		"a deadline":            context.DeadlineExceeded,
		"3100":                  fmt.Errorf("engine: batch 1: %w", &mysql.MySQLError{Number: 3100, Message: "hook"}),
		"3231":                  &mysql.MySQLError{Number: 3231, Message: "write set"},
		"1290":                  &mysql.MySQLError{Number: 1290, Message: "read-only"},
		"another number, 37810": &mysql.MySQLError{Number: 37810, Message: "no"},
		"3796":                  &mysql.MySQLError{Number: 3796, Message: "no"},
		"3798":                  &mysql.MySQLError{Number: 3798, Message: "no"},
		"a cancelled 3797 wait": fmt.Errorf("engine: batch 2: %w", context.Canceled),
		"the 3797 text alone":   errors.New("MySQL error 3797 in a string"),
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
	// finish prints to stderr only when no JSON diagnostic is set, which a test that ran Main leaves behind
	savedStarted, savedJSON := started, diagJSON
	started, diagJSON = true, nil
	t.Cleanup(func() { started, diagJSON = savedStarted, savedJSON })
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

// TestEveryPlaceThatPrintsAFailureShowsThe3797Stop: the closing log entry, the command's diagnostic and validate's
// line for a job show §4.7's 3797 message, with the statement's own error kept in the log's cause field, and the
// exit code stays 1 (spec §4.2 rule 3, rev 5.3). The 3781 message is not shown for it, and 3797's text is not
// shown for 3781.
func TestEveryPlaceThatPrintsAFailureShowsThe3797Stop(t *testing.T) {
	cause := fmt.Errorf("engine: batch 2: begin the copy transaction: %w", err3797())
	if got := failureText(cause); got != heldBeforeStop {
		t.Errorf("failureText(3797) = %q, want the 3797 message", got)
	}

	entry := lastLogEntry(t, nil, cause)
	if entry["error"] != heldBeforeStop {
		t.Errorf("the log's error = %v, want the 3797 message", entry["error"])
	}
	if got, _ := entry["cause"].(string); got != cause.Error() {
		t.Errorf("the log's cause = %q, want the statement's own error %q", got, cause.Error())
	}

	savedStarted, savedJSON := started, diagJSON
	started, diagJSON = true, nil
	t.Cleanup(func() { started, diagJSON = savedStarted, savedJSON })
	var stderr bytes.Buffer
	if code := finish(cause, &stderr); code != exitFailed {
		t.Errorf("exit code = %d, want %d", code, exitFailed)
	}
	if got, want := stderr.String(), "Error: "+heldBeforeStop+"\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}

	var out bytes.Buffer
	printFailure(&out, fmt.Errorf("read the source's Group Replication state: %w", err3797()))
	if got, want := out.String(), "   FAILED: "+heldBeforeStop+"\n\n"; got != want {
		t.Errorf("held job's line = %q, want %q", got, want)
	}
	if strings.Contains(out.String(), "3781") || strings.Contains(out.String(), "newly elected Group Replication primary applied its backlog, and the session") {
		t.Errorf("the 3797 line carries the 3781 message: %q", out.String())
	}
	out.Reset()
	printFailure(&out, fmt.Errorf("engine: batch 1: %w", err3781()))
	if strings.Contains(out.String(), "3797") {
		t.Errorf("the 3781 line carries the 3797 message: %q", out.String())
	}
}

// TestThe3797MessageNamesBothCasesAndWhoseValueItIs pins what spec §4.2 rule 3 asks of the text: the two ways
// the wait arises, no claim of which one it was, and that GoArchive does not set the value.
func TestThe3797MessageNamesBothCasesAndWhoseValueItIs(t *testing.T) {
	for _, want := range []string{
		"on a newly elected primary that is applying its backlog",
		"on a secondary whose applier is behind",
		"BEFORE or BEFORE_AND_AFTER",
		"GoArchive does not set either value: it is the server's or the account's setting",
		"The statement did not run",
		"MySQL error 3797",
	} {
		if !strings.Contains(heldStatementTimeout(err3797()), want) {
			t.Errorf("the 3797 message lacks %q", want)
		}
	}
	for _, claim := range []string{"the primary was", "this member is a", "because the primary", "was newly elected"} {
		if strings.Contains(heldStatementTimeout(err3797()), claim) {
			t.Errorf("the 3797 message claims which case it was (%q)", claim)
		}
	}
}
