package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/key"
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
}

func (e *VerificationError) Error() string {
	return fmt.Sprintf("engine: %s verification of %s failed: %d keys requested, %d source rows, %d destination rows%s; the batch was rolled back",
		e.Method, e.Table, e.Requested, e.Source, e.Destination, e.Detail)
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
