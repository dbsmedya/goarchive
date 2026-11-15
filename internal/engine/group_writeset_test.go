package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// GRT-1, the second error (spec §4.4 rev 5.2, fact 25): the same limit also bounds a transaction's write set,
// and MySQL refuses the statement that takes it over with error 3231. A GroupWriteSetError is recognised
// where the transaction ends (fact 12's three sites), on the error it ends with, whatever wrapper that error
// carries; unlike 3100 it states the cause, because MySQL's error does, and its remedy is unconditional.

// writeSet3231 is the error a statement gets from MySQL (fact 25).
func writeSet3231() error {
	return &mysql.MySQLError{Number: 3231, SQLState: [5]byte{'H', 'Y', '0', '0', '0'},
		Message: "The size of writeset data for the current transaction exceeds a limit imposed by an external component. " +
			"If using Group Replication check 'group_replication_transaction_size_limit'."}
}

// TestGroupWriteSetErrorWrapsEveryWritingStatement is spec §8's *the write-set wrap*: 3231 from each of the five
// statements that write in the four transactions, as its transaction ends with it, becomes a GroupWriteSetError
// with the right side, transaction, setting, value and roots status, and the original error in its chain.
func TestGroupWriteSetErrorWrapsEveryWritingStatement(t *testing.T) {
	me := writeSet3231()
	for _, tc := range []struct {
		name    string
		err     error // as the statement's own wrapper returns it
		commit  Commit
		side    string
		setting string
		value   int
		roots   tracking.Status
	}{
		{"a delete chunk's DELETE", fmt.Errorf("engine: delete from order_items: %w", me), CommitDelete, "source", "batch_delete_size", 500, tracking.StatusCopied},
		{"a copy INSERT", fmt.Errorf("engine: insert into order_items: %w", me), CommitCopy, "destination", "batch_size", 1000, tracking.StatusCopied},
		{"the log INSERT of the copy transaction", fmt.Errorf("tracking: log 2 root(s) as copied for job %q: %w", "j", me), CommitCopy, "destination", "batch_size", 1000, tracking.StatusCopied},
		{"a purge's log INSERT before its deletes", fmt.Errorf("tracking: log 2 root(s) as deleting for job %q: %w", "j", me), CommitLogBefore, "destination", "batch_size", 1000, tracking.StatusDeleting},
		{"the log UPDATE after the deletes", fmt.Errorf("tracking: mark 2 root(s) completed for job %q: %w", "j", me), CommitLogAfter, "destination", "batch_size", 1000, tracking.StatusCopied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := limitRun(jobspec.Archive, limitFacts("ONLINE", 150000000))
			got := r.groupWriteSetError(tc.err, tc.commit, "the transaction", tc.roots)
			if got == nil {
				t.Fatal("a 3231 with a known limit was not wrapped")
			}
			if got.Side != tc.side || got.Commit != tc.commit || got.Setting != tc.setting || got.Value != tc.value || got.Roots != tc.roots || got.Limit.Bytes != 150000000 {
				t.Errorf("wrap = side %q commit %d %s=%d roots %s limit %d; want %q, %d, %s=%d, %s, 150000000",
					got.Side, got.Commit, got.Setting, got.Value, got.Roots, got.Limit.Bytes, tc.side, tc.commit, tc.setting, tc.value, tc.roots)
			}
			var inChain *mysql.MySQLError
			if err := error(got); !errors.As(err, &inChain) || inChain != me {
				t.Error("the original MySQL error is not in the chain")
			}
			if r.groupCommitError(tc.err, tc.commit, "the transaction", tc.roots) != nil {
				t.Error("a 3231 was taken for a 3100")
			}
			if plain := limitRun(jobspec.Archive, preflight.Group{}); plain.groupWriteSetError(tc.err, tc.commit, "the transaction", tc.roots) != nil {
				t.Error("an error on a side with no known limit was wrapped")
			}
		})
	}

	t.Run("a known limit of 0 is wrapped too", func(t *testing.T) {
		r := limitRun(jobspec.Archive, limitFacts("ONLINE", 0))
		if got := r.groupWriteSetError(fmt.Errorf("engine: delete from t: %w", me), CommitDelete, "delete from t", tracking.StatusCopied); got == nil || got.Limit.Bytes != 0 || !got.Limit.Known {
			t.Errorf("a 3231 with a known limit of 0 = %v, want it wrapped", got)
		}
	})

	t.Run("a side's limit is its own", func(t *testing.T) {
		g := limitFacts("ONLINE", 150000000)
		g.Destination.TransactionSizeLimitKnown = false
		r := limitRun(jobspec.Archive, g)
		if r.groupWriteSetError(me, CommitCopy, "copy transaction", tracking.StatusCopied) != nil {
			t.Error("a destination commit was wrapped by the source's limit")
		}
		if r.groupWriteSetError(me, CommitDelete, "delete from t", tracking.StatusCopied) == nil {
			t.Error("a source delete with a known limit was not wrapped")
		}
	})

	t.Run("what is not a 3231 is left as it is", func(t *testing.T) {
		r := limitRun(jobspec.Archive, limitFacts("ONLINE", 150000000))
		for _, other := range []error{
			nil,
			errors.New("connection reset"),
			context.Canceled,
			hook3100(), // a commit's 3100 stays a GroupCommitError, and is not this one
			&mysql.MySQLError{Number: 1213, Message: "Deadlock found"},
			&mysql.MySQLError{Number: 3232, Message: "not 3231"},
			// the other errors of these transactions are their own and are not touched
			&DuplicateError{Table: "t", Key: "1", Raw: "Duplicate entry '1' for key 't.PRIMARY'"},
			&PacketError{Table: "t", Column: "c", Bytes: 70000000},
			&RecurringRootError{Job: "j", Type: jobspec.Archive, Root: "7", Err: &mysql.MySQLError{Number: 1062}},
			&KeySetError{Table: "t", Statement: "delete", Requested: 2, Matched: 1},
		} {
			if got := r.groupWriteSetError(other, CommitDelete, "delete from t", tracking.StatusCopied); got != nil {
				t.Errorf("%v was wrapped: %v", other, got)
			}
		}
		// ... and a commit's 3100 still becomes the GroupCommitError, with the message the 3100 tests pin
		if r.groupCommitError(hook3100(), CommitDelete, "delete from t", tracking.StatusCopied) == nil {
			t.Error("a 3100 at a commit is no longer a GroupCommitError")
		}
	})
}

// TestGroupWriteSetErrorMessages pins the whole message for each case of spec §4.4 rev 5.2: an ONLINE member
// source (the spec's example), a source that was not a running member, the destination, a limit of 0, which
// keeps the remedy, and for each of the four transactions what is and is not lost. It states the cause, as
// MySQL's error does, gives the limit as the start read it, and names the setting; it gives no per-row figure
// and does not send the operator to the error log.
func TestGroupWriteSetErrorMessages(t *testing.T) {
	const cause = "MySQL refused it because the transaction's write set exceeded a limit. "
	const grows = "The write set grows with the number of rows the transaction changes and with their keys, not with the size of the rows. "
	const bound = "Group Replication bounds a transaction's write set by group_replication_transaction_size_limit, "
	statement := func(what, where string) string {
		return "engine: " + what + ": the statement failed with MySQL error 3231 on " + where + cause
	}
	atLimit := func(n string) string {
		return bound + "which was " + n + " bytes on this server when the run started. " + grows
	}
	const noLimit = bound + "which imposed no limit (0) on this server when the run started and has not been read since. "
	member := "the source, which was an ONLINE member of Group Replication group " + limitGroup + " when the run started. "
	notRunning := "the source, where the Group Replication plugin is active but which was not a running member when the run started. "
	dest := "the destination, where the Group Replication plugin is active (group " + limitGroup + "). "
	lostDelete := func(st string) string {
		return "Nothing is lost: the roots stay logged as " + st + ", and the rerun's recovery finishes them."
	}
	const (
		lostCopy      = "Nothing is lost: the batch was rolled back, so nothing of it was copied, logged or deleted, and the rerun starts it again."
		lostLogBefore = "Nothing is lost: nothing of the batch was logged or deleted, and the rerun starts it again."
	)
	lostLogAfter := func(st string) string {
		return "Nothing is lost: the batch's rows are deleted and its roots stay logged as " + st + ", and the rerun's recovery completes them."
	}
	tests := []struct {
		name   string
		typ    jobspec.Type
		facts  preflight.Group
		commit Commit
		what   string
		roots  tracking.Status
		want   string
	}{
		{"a delete chunk on an ONLINE member, the spec's example", jobspec.Archive, limitFacts("ONLINE", 65536),
			CommitDelete, "delete from order_items", tracking.StatusCopied,
			statement("delete from order_items", member) + atLimit("65536") + "Lower batch_delete_size (now 500) and rerun. " + lostDelete("copied")},
		{"a delete chunk of a purge", jobspec.Purge, limitFacts("ONLINE", 65536),
			CommitDelete, "delete from order_items", tracking.StatusDeleting,
			statement("delete from order_items", member) + atLimit("65536") + "Lower batch_delete_size (now 500) and rerun. " + lostDelete("deleting")},
		{"a source that was not a running member", jobspec.Archive, limitFacts("RECOVERING", 65536),
			CommitDelete, "delete from order_items", tracking.StatusCopied,
			statement("delete from order_items", notRunning) + atLimit("65536") + "Lower batch_delete_size (now 500) and rerun. " + lostDelete("copied")},
		{"the copy transaction on the destination", jobspec.Archive, limitFacts("ONLINE", 65536),
			CommitCopy, "copy transaction", tracking.StatusCopied,
			statement("copy transaction", dest) + atLimit("65536") + "Lower batch_size (now 1000) and rerun. " + lostCopy},
		{"a log write before the deletes", jobspec.Purge, limitFacts("ONLINE", 65536),
			CommitLogBefore, "log write", tracking.StatusDeleting,
			statement("log write", dest) + atLimit("65536") + "Lower batch_size (now 1000) and rerun. " + lostLogBefore},
		{"the log write after the deletes, archive", jobspec.Archive, limitFacts("ONLINE", 65536),
			CommitLogAfter, "log write", tracking.StatusCopied,
			statement("log write", dest) + atLimit("65536") + "Lower batch_size (now 1000) and rerun. " + lostLogAfter("copied")},
		{"the log write after the deletes, purge", jobspec.Purge, limitFacts("ONLINE", 65536),
			CommitLogAfter, "log write", tracking.StatusDeleting,
			statement("log write", dest) + atLimit("65536") + "Lower batch_size (now 1000) and rerun. " + lostLogAfter("deleting")},
		{"a limit of 0 keeps the remedy", jobspec.Archive, limitFacts("ONLINE", 0),
			CommitDelete, "delete from order_items", tracking.StatusCopied,
			statement("delete from order_items", member) + noLimit + "Lower batch_delete_size (now 500) and rerun. " + lostDelete("copied")},
		{"a limit of 0 on the destination", jobspec.Archive, limitFacts("ONLINE", 0),
			CommitCopy, "copy transaction", tracking.StatusCopied,
			statement("copy transaction", dest) + noLimit + "Lower batch_size (now 1000) and rerun. " + lostCopy},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := limitRun(tc.typ, tc.facts).groupWriteSetError(writeSet3231(), tc.commit, tc.what, tc.roots)
			if got == nil {
				t.Fatal("not wrapped")
			}
			if msg := got.Error(); msg != tc.want {
				t.Errorf("message:\n got %s\nwant %s", msg, tc.want)
			}
		})
	}

	t.Run("a destination whose plugin has no group name", func(t *testing.T) {
		g := limitFacts("ONLINE", 65536)
		g.Destination.GroupName = ""
		msg := limitRun(jobspec.Archive, g).groupWriteSetError(writeSet3231(), CommitCopy, "copy transaction", tracking.StatusCopied).Error()
		if !strings.Contains(msg, " on the destination, where the Group Replication plugin is active. ") || strings.Contains(msg, "(group ") {
			t.Errorf("message names a group it does not have: %s", msg)
		}
	})

	t.Run("the message keeps to what is known", func(t *testing.T) {
		for _, tc := range tests {
			msg := limitRun(tc.typ, tc.facts).groupWriteSetError(writeSet3231(), tc.commit, tc.what, tc.roots).Error()
			for _, word := range []string{"error log", "8 bytes", "16 bytes", "per row", "If it was", "does not say whether"} {
				if strings.Contains(msg, word) {
					t.Errorf("%s: the message holds %q, which it does not say: %s", tc.name, word, msg)
				}
			}
		}
	})
}
