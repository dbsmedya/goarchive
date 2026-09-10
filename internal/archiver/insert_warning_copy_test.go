package archiver

import (
	"context"
	"database/sql/driver"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/types"
	"github.com/go-sql-driver/mysql"
	"regexp"
	"strings"
	"testing"
)

func insertBoundary(t *testing.T, skip, clean, duplicate, separate bool) (*copyOperation, error) {
	t.Helper()
	owner, m := diagnosticMock(t)
	pool := owner
	if separate {
		pool, _ = diagnosticMock(t)
	}
	cp, err := NewCopyPhase(pool, pool, graph.NewGraph("vp_warn", "id"), config.SafetyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cp.SetDiagnosticPolicy(config.VerificationConfig{Method: "sha256", SkipVerification: skip}, false)
	cp.SetStrictInsert(skip)
	m.ExpectBegin()
	tx, err := owner.Begin()
	if err != nil {
		t.Fatal(err)
	}
	q := "INSERT IGNORE INTO `vp_warn` (`id`, `n`) VALUES (?, ?)"
	if skip {
		q = strings.Replace(q, "INSERT IGNORE", "INSERT", 1)
	}
	exec := m.ExpectExec(regexp.QuoteMeta(q)).WithArgs(int64(1), int64(999))
	if duplicate {
		exec.WillReturnError(&mysql.MySQLError{Number: 1062, Message: "Duplicate entry '1' for key 'PRIMARY'"})
	} else {
		exec.WillReturnResult(sqlmock.NewResult(0, 1))
		if clean {
			expectCleanDiagnostics(m)
		} else {
			m.ExpectQuery(regexp.QuoteMeta(diagnosticCountSQL)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			m.ExpectQuery(regexp.QuoteMeta(diagnosticDetailsSQL)).WillReturnRows(sqlmock.NewRows([]string{"Level", "Code", "Message"}).AddRow("Warning", 1264, "range"))
		}
	}
	op := &copyOperation{Session: diagnosticSession{MaxErrorCount: 1024, SQLNotes: true}, Accepted: map[string]map[uint16]uint64{}}
	_, err = cp.execInsertBatch(context.Background(), tx, "vp_warn", []string{"id", "n"}, 1, []interface{}{int64(1), int64(999)}, op)
	m.ExpectRollback()
	if e := tx.Rollback(); e != nil {
		t.Fatal(e)
	}
	return op, err
}
func TestCopyInsertWarningsDefaultRefuses(t *testing.T) {
	_, err := insertBoundary(t, false, false, false, false)
	if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTIC_REJECTED") {
		t.Fatalf("warning guard missing: %v", err)
	}
}
func TestCopyInsertWarningsClean(t *testing.T) {
	_, err := insertBoundary(t, false, true, false, false)
	if err != nil {
		t.Fatal(err)
	}
}
func TestCopyInsertWarningsSkipAccepts(t *testing.T) {
	op, err := insertBoundary(t, true, false, false, false)
	if err != nil || op.Accepted["vp_warn"][1264] != 1 {
		t.Fatal(op, err)
	}
}
func TestCopyInsertWarningsSkipDuplicateError(t *testing.T) {
	_, err := insertBoundary(t, true, false, true, false)
	var dup *ErrDestinationDuplicate
	if !errors.As(err, &dup) {
		t.Fatal(err)
	}
}
func TestDiagnosticSessionOwnership(t *testing.T) {
	_, err := insertBoundary(t, false, false, false, true)
	if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTIC_REJECTED") || !strings.Contains(err.Error(), "1264") {
		t.Fatalf("diagnostic ownership: expected INSERT_DIAGNOSTIC_REJECTED containing 1264: %v", err)
	}
}
func copyBatchFixture(t *testing.T, n, cols, cap int, late, positive, temporal bool) (*CopyPhase, *CopyStats, error) {
	t.Helper()
	src, sm := diagnosticMock(t)
	dst, dm := diagnosticMock(t)
	pk := "id"
	kind := map[string]types.TemporalKind{}
	names := wideColumnNames(cols)
	if temporal {
		pk = "d"
		names[0] = pk
		kind[pk] = types.TemporalDate
	}
	cp, err := NewCopyPhase(src, dst, graph.NewGraph("vp_warn", pk), config.SafetyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cp.SetBatchSize(n)
	cp.SetColumnMetadata(map[string]types.ColumnMetadata{"vp_warn": {Names: names, Temporal: kind}})
	cp.SetStrictInsert(true)
	cp.SetDiagnosticPolicy(config.VerificationConfig{Method: "count", SkipVerification: late}, false)
	dm.ExpectQuery(regexp.QuoteMeta(diagnosticSessionSQL)).WillReturnRows(sqlmock.NewRows([]string{"capacity", "notes", "mode"}).AddRow(cap, true, ""))
	dm.ExpectBegin()
	dm.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	rows := sqlmock.NewRows(names)
	keys := make([]interface{}, n)
	for i := 0; i < n; i++ {
		values := make([]interface{}, cols)
		for j := range values {
			values[j] = int64(i + 1)
		}
		keys[i] = int64(i + 1)
		if temporal {
			values[0] = "2020-03-02"
			keys[i] = values[0]
		}
		row := make([]driver.Value, len(values))
		for j := range values {
			row[j] = values[j]
		}
		rows.AddRow(row...)
	}
	query, err := buildSelectColumnsQuery("vp_warn", pk, cp.columnMetadata["vp_warn"], n)
	if err != nil {
		t.Fatal(err)
	}
	sm.ExpectQuery(regexp.QuoteMeta(query)).WillReturnRows(rows)
	chunk := effectiveInsertRows(n, cols, cap)
	for off := 0; off < n; off += chunk {
		end := off + chunk
		if end > n {
			end = n
		}
		q := cp.buildInsertBatchQuery("vp_warn", names, end-off)
		dm.ExpectExec(regexp.QuoteMeta(q)).WillReturnResult(sqlmock.NewResult(0, int64(end-off)))
		warning := positive || late
		if !warning {
			expectCleanDiagnostics(dm)
		} else {
			dm.ExpectQuery(regexp.QuoteMeta(diagnosticCountSQL)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			details := sqlmock.NewRows([]string{"Level", "Code", "Message"})
			if cap > 0 {
				code := 1264
				if late && off > 0 {
					code = 9999
				}
				details.AddRow("Warning", code, "fixture")
			}
			dm.ExpectQuery(regexp.QuoteMeta(diagnosticDetailsSQL)).WillReturnRows(details)
			if cap == 0 || !late || off > 0 {
				break
			}
		}
	}
	fail := positive || late
	if fail {
		dm.ExpectRollback()
	} else {
		dm.ExpectCommit()
	}
	stats, err := cp.Copy(context.Background(), &RecordSet{Records: map[string][]interface{}{"vp_warn": keys}})
	return cp, stats, err
}
func TestCopyCapacityDefault1024(t *testing.T) {
	_, stats, err := copyBatchFixture(t, 5000, 13, 1024, false, false, false)
	if err != nil || stats.RowsCopied != 5000 {
		t.Fatal(stats, err)
	}
}
func TestCopyLateWarningRollsBack(t *testing.T) {
	cp, _, err := copyBatchFixture(t, 2, 2, 1, true, false, false)
	if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTIC_REJECTED") || len(cp.ConversionTotals()) != 0 {
		t.Fatalf("late warning did not roll back batch/totals: %v %+v", err, cp.ConversionTotals())
	}
}
func TestCopyWarningCapacityZeroClean(t *testing.T) {
	_, stats, err := copyBatchFixture(t, 1, 2, 0, false, false, false)
	if err != nil || stats.RowsCopied != 1 {
		t.Fatal(stats, err)
	}
}
func TestCopyWarningCapacityZeroPositive(t *testing.T) {
	_, _, err := copyBatchFixture(t, 1, 2, 0, false, true, false)
	if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTICS_UNPROVEN") {
		t.Fatalf("zero capacity accepted warnings: %v", err)
	}
}
func TestCopyTemporalIdentityValid(t *testing.T) {
	_, stats, err := copyBatchFixture(t, 1, 2, 1024, false, false, true)
	if err != nil || stats.RowsCopied != 1 {
		t.Fatal(stats, err)
	}
}
