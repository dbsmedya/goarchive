package engine

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// INSERT diagnostics, kept from 2.x (spec §7.4, §9). After every INSERT the engine reads the
// statement's conditions on the same transaction and refuses any it cannot prove harmless.
// 3.0 has no INSERT IGNORE (decision 8), so a duplicate is an error, never a condition, and
// conversions (1264, 1265, 1292, 1366) are accepted only under verification: none, which
// the operator chose knowing nothing compares the values (spec §3).

const diagnosticCountSQL = "SHOW COUNT(*) WARNINGS"
const diagnosticDetailsSQL = "SHOW WARNINGS"
const diagnosticSessionSQL = "SELECT @@SESSION.max_error_count, @@SESSION.sql_notes, @@SESSION.sql_mode"

type insertDiagnostic struct {
	Level   string
	Code    uint16
	Message string
}

type diagnosticKey struct {
	Level string
	Code  uint16
}

type insertDiagnostics struct {
	Count, Retained uint64
	ByKind          map[diagnosticKey]uint64
	Preview         []insertDiagnostic
}

type diagnosticSession struct {
	MaxErrorCount int
	SQLNotes      bool
	SQLMode       string
}

// insertDiagnosticError is INSERT_DIAGNOSTIC_REJECTED (a condition the policy refuses) or
// INSERT_DIAGNOSTICS_UNPROVEN (the conditions could not be read completely).
type insertDiagnosticError struct {
	Identifier, Table, Reason string
	Count, Retained           uint64
	Preview                   []insertDiagnostic
	Cause                     error
}

func (e *insertDiagnosticError) Error() string {
	message := fmt.Sprintf("%s: table=%s count=%d retained=%d: %s", e.Identifier, e.Table, e.Count, e.Retained, e.Reason)
	if e.Identifier == "INSERT_DIAGNOSTIC_REJECTED" {
		message += fmt.Sprintf("; diagnostics=%v", e.Preview)
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *insertDiagnosticError) Unwrap() error { return e.Cause }

type diagnosticQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// readDiagnosticSession reads the destination session's diagnostic capacity once per copy.
// Without sql_notes a note would go unseen, so it must be on.
func readDiagnosticSession(ctx context.Context, conn *sql.Conn) (diagnosticSession, error) {
	var s diagnosticSession
	fail := func(reason string, cause error) (diagnosticSession, error) {
		return s, &insertDiagnosticError{Identifier: "INSERT_DIAGNOSTICS_UNPROVEN", Reason: reason, Cause: cause}
	}
	if err := conn.QueryRowContext(ctx, diagnosticSessionSQL).Scan(&s.MaxErrorCount, &s.SQLNotes, &s.SQLMode); err != nil {
		return fail("diagnostic session unavailable", err)
	}
	if s.MaxErrorCount < 0 || s.MaxErrorCount > 65535 {
		return fail("diagnostic session unavailable: capacity out of range", nil)
	}
	if !s.SQLNotes {
		return fail("sql_notes must be 1", nil)
	}
	return s, nil
}

func diagnosticPreview(message string) string {
	message = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(message)
	if len(message) > 240 {
		message = strings.ToValidUTF8(message[:240], "")
	}
	return message
}

// collectInsertDiagnostics reads the conditions of the statement q just ran.
func collectInsertDiagnostics(ctx context.Context, q diagnosticQueryer, table string) (insertDiagnostics, error) {
	d := insertDiagnostics{ByKind: map[diagnosticKey]uint64{}}
	fail := func(reason string, cause error) (insertDiagnostics, error) {
		return d, &insertDiagnosticError{Identifier: "INSERT_DIAGNOSTICS_UNPROVEN", Table: table, Reason: reason, Count: d.Count, Retained: d.Retained, Cause: cause}
	}
	if err := q.QueryRowContext(ctx, diagnosticCountSQL).Scan(&d.Count); err != nil {
		return fail("warning count unreadable", err)
	}
	if d.Count == 0 {
		return d, nil
	}
	rows, err := q.QueryContext(ctx, diagnosticDetailsSQL)
	if err != nil {
		return fail("warning details unreadable", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var item insertDiagnostic
		if err := rows.Scan(&item.Level, &item.Code, &item.Message); err != nil {
			return fail("warning row unreadable", err)
		}
		d.Retained++
		d.ByKind[diagnosticKey{item.Level, item.Code}]++
		if len(d.Preview) < 5 {
			item.Message = diagnosticPreview(item.Message)
			d.Preview = append(d.Preview, item)
		}
	}
	if err := rows.Err(); err != nil {
		return fail("warning iteration failed", err)
	}
	if err := rows.Close(); err != nil {
		return fail("warning close failed", err)
	}
	return d, nil
}

func sortedDiagnosticKeys(m map[diagnosticKey]uint64) []diagnosticKey {
	keys := make([]diagnosticKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Code == keys[j].Code {
			return keys[i].Level < keys[j].Level
		}
		return keys[i].Code < keys[j].Code
	})
	return keys
}

// isConversion reports the conditions a strict INSERT can raise while still writing a
// changed value: out of range, truncated, incorrect value.
func isConversion(code uint16) bool {
	return code == 1264 || code == 1265 || code == 1292 || code == 1366
}

// classifyInsertDiagnostics returns the conversions accepted under verification: none, by
// code, or the error that fails the batch. Every condition is refused otherwise.
func classifyInsertDiagnostics(table string, v jobspec.Verification, d insertDiagnostics) (map[uint16]uint64, error) {
	reject := func(reason string) error {
		return &insertDiagnosticError{Identifier: "INSERT_DIAGNOSTIC_REJECTED", Table: table, Reason: reason, Count: d.Count, Retained: d.Retained, Preview: d.Preview}
	}
	unproven := func(reason string) error {
		return &insertDiagnosticError{Identifier: "INSERT_DIAGNOSTICS_UNPROVEN", Table: table, Reason: reason, Count: d.Count, Retained: d.Retained}
	}
	var sum uint64
	for _, n := range d.ByKind {
		if n > ^uint64(0)-sum {
			return nil, unproven("diagnostic count overflow")
		}
		sum += n
	}
	if d.Count != d.Retained || sum != d.Retained {
		return nil, unproven("incomplete diagnostic list; configure diagnostic retention or reduce INSERT size")
	}
	accepted := map[uint16]uint64{}
	for _, k := range sortedDiagnosticKeys(d.ByKind) {
		if k.Level != "Warning" && k.Level != "Note" {
			return nil, reject("unexpected diagnostic level")
		}
		if v == jobspec.VerifyNone && isConversion(k.Code) {
			accepted[k.Code] += d.ByKind[k]
			continue
		}
		return nil, reject(fmt.Sprintf("condition %d is not allowed", k.Code))
	}
	return accepted, nil
}

// maxPlaceholders is MySQL's limit on the parameters of one prepared statement.
const maxPlaceholders = 65535

// insertRows is how many rows one INSERT of columns columns carries: at most want, within
// the placeholder limit, and no more than the session retains conditions for, so every
// condition of the statement can be read back.
func insertRows(want, columns, maxErrorCount int) int {
	n := want
	if columns > 0 && n > maxPlaceholders/columns {
		n = maxPlaceholders / columns
	}
	if maxErrorCount > 0 && n > maxErrorCount {
		n = maxErrorCount
	}
	if n < 1 {
		n = 1
	}
	return n
}
