package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// KeySetError fails a key-addressed statement whose result does not match the keys it was
// given (spec §7.1a): a copy read that returned a key not requested, or missed one; a delete
// whose row count differs from its chunk; a recovery read on either server that returned a
// key not requested. Nothing the statement did is kept: the copy's destination transaction
// or the delete chunk's transaction rolls back, and a recovery read deletes nothing.
type KeySetError struct {
	Table       string
	Statement   string // "copy read", "delete", "recovery read", "recovery destination read"
	Requested   int64  // distinct keys bound
	Matched     int64  // requested keys the statement reached
	Extra       int64  // rows that are not one of the requested keys, or repeat one
	Destination bool   // the statement ran on the destination
}

func (e *KeySetError) Error() string {
	if e.Destination {
		return fmt.Sprintf("engine: %s on %s reached %d of %d requested keys, plus %d other rows; nothing was deleted. "+
			"The destination returned key bytes the source did not: either a destination row was rewritten since its copy, "+
			"or the two servers return keys differently (PAD_CHAR_TO_FULL_LENGTH in one server's global sql_mode only)",
			e.Statement, e.Table, e.Matched, e.Requested, e.Extra)
	}
	return fmt.Sprintf("engine: %s on %s reached %d of %d requested keys, plus %d other rows; nothing it did is kept. "+
		"Either a row of the job changed while it ran, which breaks the cold-data contract "+
		"(docs/README_LIMITATIONS.md), or a key did not round-trip through MySQL", e.Statement, e.Table, e.Matched, e.Requested, e.Extra)
}

// VerificationError fails a batch whose copy does not verify (spec §7.4): the destination
// transaction rolls back, so nothing of the batch is copied or logged.
type VerificationError struct {
	Table       string
	Method      string // "count" or "sha256"
	Requested   int64
	Source      int64 // rows found on the source
	Destination int64 // rows found in the destination transaction
	Detail      string
	// JSONColumns are the table's JSON columns, on a sha256 hash mismatch (JSN-1): a JSON value
	// copied as text can arrive changed, and the message says so.
	JSONColumns []string
}

func (e *VerificationError) Error() string {
	var hint string
	if len(e.JSONColumns) > 0 {
		hint = fmt.Sprintf("; %s has JSON columns (%s): a JSON value copied as text can arrive changed (a DECIMAL such as 12.50 as 12.5; "+
			"on an 8.0 or 8.4 destination, some doubles), see docs/README_COMPAT.md → JSON values that do not survive a text copy",
			e.Table, strings.Join(e.JSONColumns, ", "))
	}
	return fmt.Sprintf("engine: %s verification of %s failed: %d keys requested, %d source rows, %d destination rows%s%s; the batch was rolled back",
		e.Method, e.Table, e.Requested, e.Source, e.Destination, e.Detail, hint)
}

// DuplicateError is a destination INSERT that met a row the destination already holds.
type DuplicateError struct {
	Table string
	Key   string // the conflicting value as MySQL reports it
	Raw   string
	// CopyOnly adds the remedy of spec §6.6: a copy-only rerun of roots this job copied
	// before needs resume: true.
	CopyOnly bool
}

func (e *DuplicateError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "engine: the destination table %s already holds the row %s (%s); the batch was rolled back", e.Table, e.Key, e.Raw)
	if e.CopyOnly {
		b.WriteString(". To skip the roots this job copied before, set resume: true on the job")
	}
	return b.String()
}

// RecurringRootError is a log INSERT that met a root the job completed before (RKP-1, RKP-2). In
// the loop the log holds only completed roots, since recovery finished or refused the rest. Root
// is the value as MySQL reports it (a composite key joined by '-'). MySQL names only the first
// duplicate of the statement.
type RecurringRootError struct {
	Job  string
	Type jobspec.Type
	Root string
	Err  error // the *mysql.MySQLError
}

func (e *RecurringRootError) Error() string {
	if e.Type == jobspec.CopyOnly {
		return fmt.Sprintf("engine: the root '%s' is already completed in job %q's log: the job copied it before. "+
			"Of this batch, nothing was copied or logged. To skip the roots it copied, set resume: true on the job (%v)",
			e.Root, e.Job, e.Err)
	}
	verb := "archive"
	if e.Type == jobspec.Purge {
		verb = "purge"
	}
	return fmt.Sprintf("engine: the root '%s' is already completed in job %q's log: a root it %sd before is back in its where range, "+
		"outside the run contract (docs/README_LIMITATIONS.md, \"No DDL and no concurrent writes during a run (contract)\"). "+
		"Of this batch, nothing was logged or deleted. To %s it again, delete its completed log row "+
		"(docs/README_JOBS_SCHEMA.md, \"Maintenance: what is safe to delete\"), then rerun (%v)",
		e.Root, e.Job, verb, verb, e.Err)
}

func (e *RecurringRootError) Unwrap() error { return e.Err }

// PacketError is a destination INSERT of the copy that did not fit the destination's
// max_allowed_packet (M7c-F1). The driver sends large values as long data, so a row or a
// statement larger than the packet fits; what does not is one value larger than it, named by
// Column and Bytes (the largest value of the INSERT). Without a Column, the statement as a
// whole was refused, by the driver or the server.
type PacketError struct {
	Table, Column string
	Bytes         int
}

func (e *PacketError) Error() string {
	const done = "the batch was rolled back; of this batch, nothing was logged or deleted"
	const see = "See docs/README_LIMITATIONS.md, \"A value larger than the destination's max_allowed_packet fails its batch\""
	if e.Column == "" {
		return fmt.Sprintf("engine: an INSERT into %s is larger than the destination's max_allowed_packet: %s. "+
			"Raise max_allowed_packet on the destination, or lower batch_size, then rerun. %s", e.Table, done, see)
	}
	return fmt.Sprintf("engine: a value of %d bytes in %s.%s is larger than the destination's max_allowed_packet: %s. "+
		"Raise max_allowed_packet on the destination above %d bytes, then rerun. %s", e.Bytes, e.Table, e.Column, done, e.Bytes, see)
}

// JobRunningError refuses a run because another run holds the job's lock (startup). The run
// did nothing. Holder is the holding destination connection, or 0 when it released the lock a
// moment before the lookup.
type JobRunningError struct {
	Job    string
	Holder int64
}

func (e *JobRunningError) Error() string {
	if e.Holder == 0 {
		return fmt.Sprintf("engine: job %q is already running: its advisory lock was held by another session a moment ago", e.Job)
	}
	return fmt.Sprintf("engine: job %q is already running: its advisory lock is held by another session (destination connection %d)", e.Job, e.Holder)
}

// TableLockedError refuses a run because another GoArchive run holds the source lock on one
// of its tables (source-table-locks spec §6). The run did nothing. HolderConnectionID is the
// holding source connection, or 0 when it released the lock a moment before the lookup.
type TableLockedError struct {
	Database, Table    string
	HolderConnectionID int64
}

func (e *TableLockedError) Error() string {
	holder := fmt.Sprintf("is in use by another GoArchive run (source connection %d)", e.HolderConnectionID)
	if e.HolderConnectionID == 0 {
		holder = "was in use by another GoArchive run a moment ago"
	}
	return fmt.Sprintf("engine: the source table %s.%s %s. Jobs whose tables overlap run one at a time; this run did nothing",
		e.Database, e.Table, holder)
}

// UncopiedError stops archive recovery before any delete of a page (GH #7): rows of its
// logged roots' trees that the destination does not hold. Nothing of the page was deleted,
// and its roots stay copied, so every start refuses again until the rows are resolved.
type UncopiedError struct {
	Missing []MissingRows // in copy order, the root table first
}

// MissingRows is one table's rows the destination does not hold.
type MissingRows struct {
	Table string
	Root  bool   // the job's root table: logged as copied, so the destination lost them
	Count int64  // rows missing
	Keys  string // the first shownKeys, formatted by formatKeys
}

func (e *UncopiedError) Error() string {
	var rows int64
	var child, root bool
	parts := make([]string, len(e.Missing))
	for i, m := range e.Missing {
		rows += m.Count
		noun := "row(s)"
		if m.Root {
			noun, root = "logged root(s)", true
		} else {
			child = true
		}
		parts[i] = fmt.Sprintf("%s: %d %s (%s)", m.Table, m.Count, noun, m.Keys)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "engine: archive recovery stopped before any delete: the destination does not hold %d row(s) of this page's logged roots (%s). "+
		"Nothing of this page was deleted, and its roots stay copied.", rows, strings.Join(parts, "; "))
	if child {
		b.WriteString("\nA missing child row was either written into its root's tree after the root was copied, against the run contract " +
			"(docs/README_LIMITATIONS.md), or lost by the destination (a partial restore, a retention job on a child table). " +
			"Copy it into the destination and rerun: recovery then deletes it with its tree. " +
			"Remove it from the source only once you know it is not wanted: the source may hold its last copy.")
	}
	if root {
		b.WriteString("\nA missing root was logged as copied in the transaction that copied it, so the destination lost it " +
			"(a restore, a failover that lost transactions, a retention job on the archive tables). " +
			"Restore it to the destination and rerun, or abandon the logged roots (docs/README_JOBS_SCHEMA.md, \"Abandoning logged roots\").")
	}
	b.WriteString("\nSee docs/README_OPERATIONS.md, \"Crash recovery\".")
	return b.String()
}

// shownKeys is how many keys a message names before it counts the rest.
const shownKeys = 10

// formatKeys formats the first shownKeys keys for a message, and counts the rest.
func formatKeys(keys []key.Key) string {
	n := min(len(keys), shownKeys)
	parts := make([]string, n)
	for i, k := range keys[:n] {
		parts[i] = formatKey(k)
	}
	s := strings.Join(parts, ", ")
	if more := len(keys) - n; more > 0 {
		s += fmt.Sprintf(" and %d more", more)
	}
	return s
}

// formatKey formats one key: text, binary, decimal and temporal values quoted with Go
// escapes, numbers as they are; a composite key in parentheses.
func formatKey(k key.Key) string {
	parts := make([]string, len(k))
	for i, v := range k {
		switch x := v.(type) {
		case []byte:
			parts[i] = strconv.Quote(string(x))
		case string:
			parts[i] = strconv.Quote(x)
		default:
			parts[i] = fmt.Sprint(x)
		}
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// Commit names which of a run's commits failed.
type Commit int

const (
	// CommitDelete is one delete chunk on the source.
	CommitDelete Commit = iota
	// CommitCopy is a batch's copy transaction on the destination: the rows and their log entries.
	CommitCopy
	// CommitLogBefore is a purge's log write on the destination before any delete.
	CommitLogBefore
	// CommitLogAfter is the log write that completes a batch's roots after their deletes.
	CommitLogAfter
)

// GroupCommitError is a commit that MySQL refused with error 3100, the replication hook's failure, on a
// server whose Group Replication transaction size limit was known when the run started (GRT-1, spec
// §4.4). Error 3100 does not say why: a transaction over group_replication_transaction_size_limit is one
// reason, and a member that stops or leaves the group during the commit is another (M2b). So the message
// gives the limit as the start read it, says that the member's error log holds the reason, and offers the
// setting that sizes the transaction. The values are the start's, and the message says so: nothing is
// read at the failure. The original error stays in the chain.
type GroupCommitError struct {
	Side    string // "source" or "destination"
	Commit  Commit
	What    string              // the statement or phase, as the message leads with it: "delete from order_items"
	Limit   preflight.SizeLimit // the side's limit and membership at the start
	Setting string              // the setting that sizes this transaction: batch_delete_size or batch_size
	Value   int                 // its value now
	Roots   tracking.Status     // what the batch's roots are logged as after the failure (delete chunks and the log write after them)
	Err     error               // the commit's error, which holds the *mysql.MySQLError
}

func (e *GroupCommitError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "engine: %s: the commit failed with MySQL error 3100 (replication hook 'before_commit') on %s. ", e.What, e.where())
	log := "the server's"
	if e.Limit.Member == preflight.MemberOnline {
		log = "the member's"
	}
	if e.Limit.Bytes == 0 {
		// A known 0 is the server's own "no limit" for this variable, said as history: the limit may have changed
		// since, and nothing is ruled out, so no remedy is offered from a batch size.
		fmt.Fprintf(&b, "group_replication_transaction_size_limit imposed no limit (0) on this server when the run started. "+
			"The reason for the failure is in %s error log. ", log)
	} else {
		fmt.Fprintf(&b, "Group Replication refuses a transaction larger than group_replication_transaction_size_limit, "+
			"which was %d bytes on this server when the run started. MySQL's error does not say whether that was the reason; "+
			"%s error log does. If it was, lower %s (now %d) and rerun. ", e.Limit.Bytes, log, e.Setting, e.Value)
	}
	b.WriteString(e.lost())
	return b.String()
}

func (e *GroupCommitError) where() string { return groupWhere(e.Limit) }

func (e *GroupCommitError) lost() string { return groupLost(e.Commit, e.Roots) }

func (e *GroupCommitError) Unwrap() error { return e.Err }

// groupWhere is the phrase after "on" in the message of either error of the limit: what the start observed
// of the side, and no more. A known limit proves the plugin is active, not that the server is a running
// member, and a destination's view is not read.
func groupWhere(l preflight.SizeLimit) string {
	switch l.Member {
	case preflight.MemberOnline:
		return "the source, which was an ONLINE member of Group Replication group " + l.Group + " when the run started"
	case preflight.MemberNotRunning:
		return "the source, where the Group Replication plugin is active but which was not a running member when the run started"
	}
	if l.Group != "" {
		return "the destination, where the Group Replication plugin is active (group " + l.Group + ")"
	}
	return "the destination, where the Group Replication plugin is active"
}

// groupLost says what the failed transaction left, which is today's outcome of each (spec §4.4): the engine
// rolls it back as it does after any error, and nothing of it commits.
func groupLost(c Commit, roots tracking.Status) string {
	switch c {
	case CommitDelete:
		return "Nothing is lost: the roots stay logged as " + roots.String() + ", and the rerun's recovery finishes them."
	case CommitCopy:
		return "Nothing is lost: the batch was rolled back, so nothing of it was copied, logged or deleted, and the rerun starts it again."
	case CommitLogBefore:
		return "Nothing is lost: nothing of the batch was logged or deleted, and the rerun starts it again."
	}
	return "Nothing is lost: the batch's rows are deleted and its roots stay logged as " + roots.String() + ", and the rerun's recovery completes them."
}

// GroupWriteSetError is a statement that MySQL refused with error 3231 in one of the four transactions of
// fact 12's three sites, on a server whose Group Replication transaction size limit was known when the run
// started (GRT-1, spec §4.4, fact 25). The same variable bounds a transaction's write set, which grows with
// the number of rows it changes and their keys and not with their size, and MySQL refuses the statement that
// takes the write set over it. Unlike 3100, whose cause the error does not name, 3231 says why, so the
// message states the cause and its remedy is unconditional: a smaller transaction has a smaller write set
// whatever the limit is. A known limit of 0 keeps the remedy: the cause is known, only the value may be out
// of date. It carries what GroupCommitError carries, and the original error stays in the chain. The
// transaction is rolled back by the engine, as after any statement error: MySQL leaves it open, and a COMMIT
// would fail with 3100, but no run commits after a failed statement.
type GroupWriteSetError struct {
	Side    string // "source" or "destination"
	Commit  Commit // the transaction the statement was in
	What    string // the transaction, as the message leads with it: "delete from order_items"
	Limit   preflight.SizeLimit
	Setting string          // batch_delete_size or batch_size
	Value   int             // its value now
	Roots   tracking.Status // what the batch's roots are logged as after the failure
	Err     error           // the statement's error, which holds the *mysql.MySQLError
}

func (e *GroupWriteSetError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "engine: %s: the statement failed with MySQL error 3231 on %s. MySQL refused it because the transaction's write set exceeded a limit. ",
		e.What, groupWhere(e.Limit))
	if e.Limit.Bytes == 0 {
		// A known 0 is the server's own "no limit" for the variable, said as history: the cause is known, so the
		// remedy stays; only the value may be out of date.
		b.WriteString("Group Replication bounds a transaction's write set by group_replication_transaction_size_limit, " +
			"which imposed no limit (0) on this server when the run started and has not been read since. ")
	} else {
		fmt.Fprintf(&b, "Group Replication bounds a transaction's write set by group_replication_transaction_size_limit, "+
			"which was %d bytes on this server when the run started. "+
			"The write set grows with the number of rows the transaction changes and with their keys, not with the size of the rows. ", e.Limit.Bytes)
	}
	fmt.Fprintf(&b, "Lower %s (now %d) and rerun. ", e.Setting, e.Value)
	b.WriteString(groupLost(e.Commit, e.Roots))
	return b.String()
}

func (e *GroupWriteSetError) Unwrap() error { return e.Err }
