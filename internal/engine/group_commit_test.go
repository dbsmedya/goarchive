package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// GRT-1 (spec §4.4, GR C3): a commit that MySQL refuses with error 3100 at one of the three commit sites
// becomes a GroupCommitError when that side's transaction size limit was known at the start, and says what
// the facts of the start say and no more: 3100 is the hook's general failure, so the message never claims
// the size was the cause.

const (
	limitGroup  = "8a94f357-0000-4000-8000-0000000000aa"
	limitMember = "11111111-1111-4111-8111-111111111111"
)

// hook3100 is the error a commit gets from the replication hook (M1, M2b).
func hook3100() error {
	return &mysql.MySQLError{Number: 3100, SQLState: [5]byte{'H', 'Y', '0', '0', '0'},
		Message: "Error on observer while running replication hook 'before_commit'."}
}

// limitFacts are the facts a run starts with: the source an ONLINE member (state) with limit bytes, the
// destination a server whose plugin is ACTIVE with the group name and the same limit.
func limitFacts(state string, limit uint64) preflight.Group {
	gr := dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: limitGroup, TransactionSizeLimit: limit, TransactionSizeLimitKnown: true}
	return preflight.Group{Source: gr, Destination: gr,
		Members: []dbsrepl.GroupMember{{MemberID: limitMember, State: state, Role: "PRIMARY", Self: true}}}
}

// limitRun is a run of typ with those facts, a batch_size of 1000 and a batch_delete_size of 500.
func limitRun(typ jobspec.Type, g preflight.Group) *run {
	return &run{typ: typ, group: g, size: 1000,
		cfg: Config{Processing: config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500}}}
}

// TestGroupCommitErrorWrapsOnly3100WithAKnownLimit is spec §8's *the wrap*: error 3100 at each commit
// becomes a GroupCommitError with the right side, setting and value and the original error still in its
// chain; an unknown limit leaves the error as it was; any other error number is untouched.
func TestGroupCommitErrorWrapsOnly3100WithAKnownLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		commit  Commit
		side    string
		setting string
		value   int
	}{
		{"a delete chunk", CommitDelete, "source", "batch_delete_size", 500},
		{"the copy transaction", CommitCopy, "destination", "batch_size", 1000},
		{"a log write before the deletes", CommitLogBefore, "destination", "batch_size", 1000},
		{"a log write after the deletes", CommitLogAfter, "destination", "batch_size", 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := limitRun(jobspec.Archive, limitFacts("ONLINE", 150000000))
			me := hook3100()
			// the error as a commit site sees it: wrapped by the driver or the guard
			got := r.groupCommitError(fmt.Errorf("commit: %w", me), tc.commit, "the commit", tracking.StatusCopied)
			if got == nil {
				t.Fatal("a 3100 with a known limit was not wrapped")
			}
			if got.Side != tc.side || got.Commit != tc.commit || got.Setting != tc.setting || got.Value != tc.value || got.Limit.Bytes != 150000000 {
				t.Errorf("wrap = side %q commit %d setting %s=%d limit %d, want %q, %d, %s=%d, 150000000",
					got.Side, got.Commit, got.Setting, got.Value, got.Limit.Bytes, tc.side, tc.commit, tc.setting, tc.value)
			}
			var inChain *mysql.MySQLError
			if err := error(got); !errors.As(err, &inChain) || inChain != me {
				t.Error("the original MySQL error is not in the chain")
			}

			// the same error with no known limit on that side: left as it is
			plain := limitRun(jobspec.Archive, preflight.Group{})
			if plain.groupCommitError(me, tc.commit, "the commit", tracking.StatusCopied) != nil {
				t.Error("an error on a side with no known limit was wrapped")
			}
		})
	}

	r := limitRun(jobspec.Archive, limitFacts("ONLINE", 150000000))
	for _, other := range []error{
		nil,
		errors.New("connection reset"),
		context.Canceled,
		&mysql.MySQLError{Number: 1213, Message: "Deadlock found"},
		&mysql.MySQLError{Number: 1062, Message: "Duplicate entry"},
		&mysql.MySQLError{Number: 3101, Message: "Plugin instructed the server to rollback the current transaction."},
		&mysql.MySQLError{Number: 31, Message: "not 3100"},
	} {
		if got := r.groupCommitError(other, CommitDelete, "delete from t", tracking.StatusCopied); got != nil {
			t.Errorf("%v was wrapped: %v", other, got)
		}
	}

	// A side's limit is its own: a known limit on the destination does not wrap a source commit.
	g := limitFacts("ONLINE", 150000000)
	g.Source = dbsrepl.GroupReplication{}
	r = limitRun(jobspec.Archive, g)
	if r.groupCommitError(hook3100(), CommitDelete, "delete from t", tracking.StatusCopied) != nil {
		t.Error("a source commit was wrapped by the destination's limit")
	}
	if r.groupCommitError(hook3100(), CommitCopy, "copy transaction", tracking.StatusCopied) == nil {
		t.Error("a destination commit with a known limit was not wrapped")
	}
}

// TestGroupCommitErrorMessages pins the whole message for each case of spec §4.4 (findings 3 and 6): an
// ONLINE member source, a source that was not a running member, the destination, a known limit of 0, and
// for each commit what is and is not lost. It names the setting, claims no cause, and leaves a membership
// that was not observed out of the text.
func TestGroupCommitErrorMessages(t *testing.T) {
	const hook = "the commit failed with MySQL error 3100 (replication hook 'before_commit')"
	const reason = "Group Replication refuses a transaction larger than group_replication_transaction_size_limit, " +
		"which was 150000000 bytes on this server when the run started. " +
		"MySQL's error does not say whether that was the reason; "
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
	member := "the source, which was an ONLINE member of Group Replication group " + limitGroup + " when the run started. "
	notRunning := "the source, where the Group Replication plugin is active but which was not a running member when the run started. "
	dest := "the destination, where the Group Replication plugin is active (group " + limitGroup + "). "

	tests := []struct {
		name   string
		typ    jobspec.Type
		facts  preflight.Group
		commit Commit
		what   string
		roots  tracking.Status
		want   string
	}{
		{"a delete chunk of an archive on an ONLINE member, the spec's own example", jobspec.Archive, limitFacts("ONLINE", 150000000),
			CommitDelete, "delete from order_items", tracking.StatusCopied,
			"engine: delete from order_items: " + hook + " on " + member + reason + "the member's error log does. " +
				"If it was, lower batch_delete_size (now 500) and rerun. " + lostDelete("copied")},
		{"a delete chunk of a purge", jobspec.Purge, limitFacts("ONLINE", 150000000),
			CommitDelete, "delete from order_items", tracking.StatusDeleting,
			"engine: delete from order_items: " + hook + " on " + member + reason + "the member's error log does. " +
				"If it was, lower batch_delete_size (now 500) and rerun. " + lostDelete("deleting")},
		{"a source that was not a running member", jobspec.Archive, limitFacts("RECOVERING", 150000000),
			CommitDelete, "delete from order_items", tracking.StatusCopied,
			"engine: delete from order_items: " + hook + " on " + notRunning + reason + "the server's error log does. " +
				"If it was, lower batch_delete_size (now 500) and rerun. " + lostDelete("copied")},
		{"the copy transaction on the destination", jobspec.Archive, limitFacts("ONLINE", 150000000),
			CommitCopy, "copy transaction", tracking.StatusCopied,
			"engine: copy transaction: " + hook + " on " + dest + reason + "the server's error log does. " +
				"If it was, lower batch_size (now 1000) and rerun. " + lostCopy},
		{"a log write before the deletes", jobspec.Purge, limitFacts("ONLINE", 150000000),
			CommitLogBefore, "log write", tracking.StatusDeleting,
			"engine: log write: " + hook + " on " + dest + reason + "the server's error log does. " +
				"If it was, lower batch_size (now 1000) and rerun. " + lostLogBefore},
		{"a log write after the deletes, archive", jobspec.Archive, limitFacts("ONLINE", 150000000),
			CommitLogAfter, "log write", tracking.StatusCopied,
			"engine: log write: " + hook + " on " + dest + reason + "the server's error log does. " +
				"If it was, lower batch_size (now 1000) and rerun. " + lostLogAfter("copied")},
		{"a log write after the deletes, purge", jobspec.Purge, limitFacts("ONLINE", 150000000),
			CommitLogAfter, "log write", tracking.StatusDeleting,
			"engine: log write: " + hook + " on " + dest + reason + "the server's error log does. " +
				"If it was, lower batch_size (now 1000) and rerun. " + lostLogAfter("deleting")},
		{"a known limit of 0 is history and offers no remedy", jobspec.Archive, limitFacts("ONLINE", 0),
			CommitDelete, "delete from order_items", tracking.StatusCopied,
			"engine: delete from order_items: " + hook + " on " + member +
				"group_replication_transaction_size_limit imposed no limit (0) on this server when the run started. " +
				"The reason for the failure is in the member's error log. " + lostDelete("copied")},
		{"a known limit of 0 on the destination", jobspec.Archive, limitFacts("ONLINE", 0),
			CommitCopy, "copy transaction", tracking.StatusCopied,
			"engine: copy transaction: " + hook + " on " + dest +
				"group_replication_transaction_size_limit imposed no limit (0) on this server when the run started. " +
				"The reason for the failure is in the server's error log. " + lostCopy},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := limitRun(tc.typ, tc.facts).groupCommitError(hook3100(), tc.commit, tc.what, tc.roots)
			if got == nil {
				t.Fatal("not wrapped")
			}
			if msg := got.Error(); msg != tc.want {
				t.Errorf("message:\n got %s\nwant %s", msg, tc.want)
			}
		})
	}

	t.Run("a destination whose plugin has no group name", func(t *testing.T) {
		g := limitFacts("ONLINE", 150000000)
		g.Destination.GroupName = ""
		msg := limitRun(jobspec.Archive, g).groupCommitError(hook3100(), CommitCopy, "copy transaction", tracking.StatusCopied).Error()
		if !strings.Contains(msg, " on the destination, where the Group Replication plugin is active. ") || strings.Contains(msg, "(group ") {
			t.Errorf("message names a group it does not have: %s", msg)
		}
	})

	t.Run("no message claims the cause", func(t *testing.T) {
		for _, tc := range tests {
			msg := limitRun(tc.typ, tc.facts).groupCommitError(hook3100(), tc.commit, tc.what, tc.roots).Error()
			for _, claim := range []string{"exceeds the", "was too large", "because", "caused by"} {
				if strings.Contains(msg, claim) {
					t.Errorf("%s: the message claims a cause (%q): %s", tc.name, claim, msg)
				}
			}
		}
	})
}
