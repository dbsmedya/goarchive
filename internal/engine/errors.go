package engine

import (
	"fmt"
	"strings"
)

// KeySetError fails a key-addressed statement whose result does not match the keys it was
// given (spec §7.1a): a copy read that returned a key not requested, or missed one; a delete
// whose row count differs from its chunk. Nothing the statement did is kept: the copy's
// destination transaction or the delete chunk's transaction rolls back.
type KeySetError struct {
	Table     string
	Statement string // "copy read", "delete", "recovery read"
	Requested int64  // distinct keys bound
	Matched   int64  // requested keys the statement reached
	Extra     int64  // rows that are not one of the requested keys, or repeat one
}

func (e *KeySetError) Error() string {
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
