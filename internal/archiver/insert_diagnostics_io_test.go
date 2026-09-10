package archiver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"regexp"
	"strings"
	"testing"
)

func diagnosticMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return db, m
}
func TestReadDiagnosticSession(t *testing.T) {
	cause := errors.New("session unavailable")
	for _, tc := range []struct {
		name             string
		cap, notes, mode driver.Value
		queryErr         error
		bad              bool
	}{{"valid", int64(1024), true, "STRICT_TRANS_TABLES", nil, false}, {"notes0", int64(1024), false, "", nil, true}, {"negative", int64(-1), true, "", nil, true}, {"large", int64(65536), true, "", nil, true}, {"scan", "bad", true, "", nil, true}, {"query", int64(1), true, "", cause, true}} {
		t.Run(tc.name, func(t *testing.T) {
			db, m := diagnosticMock(t)
			q := m.ExpectQuery(regexp.QuoteMeta(diagnosticSessionSQL))
			if tc.queryErr != nil {
				q.WillReturnError(tc.queryErr)
			} else {
				q.WillReturnRows(sqlmock.NewRows([]string{"capacity", "notes", "mode"}).AddRow(tc.cap, tc.notes, tc.mode))
			}
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			got, err := readDiagnosticSession(context.Background(), conn)
			if tc.bad {
				if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTICS_UNPROVEN") {
					t.Fatalf("diagnostic session proof missing: %v", err)
				}
				if tc.queryErr != nil && !errors.Is(err, cause) {
					t.Fatal(err)
				}
			} else if err != nil || got.MaxErrorCount != 1024 || !got.SQLNotes || got.SQLMode != "STRICT_TRANS_TABLES" {
				t.Fatalf("diagnostic session proof missing: %+v %v", got, err)
			}
		})
	}
	t.Run("cancellation", func(t *testing.T) {
		db, _ := diagnosticMock(t)
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = readDiagnosticSession(ctx, conn)
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}
func TestCollectInsertDiagnosticsClean(t *testing.T) {
	db, m := diagnosticMock(t)
	m.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	expectCleanDiagnostics(m)
	d, err := collectInsertDiagnostics(context.Background(), tx, "t")
	if err != nil || d.Count != 0 || d.Retained != 0 {
		t.Fatalf("clean report=%+v err=%v", d, err)
	}
	m.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
func TestCollectInsertDiagnosticsComplete(t *testing.T) {
	db, m := diagnosticMock(t)
	m.ExpectQuery(regexp.QuoteMeta(diagnosticCountSQL)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	m.ExpectQuery(regexp.QuoteMeta(diagnosticDetailsSQL)).WillReturnRows(sqlmock.NewRows([]string{"Level", "Code", "Message"}).AddRow("Warning", 1264, "one").AddRow("Warning", 1264, "two").AddRow("Warning", 1062, "duplicate"))
	d, err := collectInsertDiagnostics(context.Background(), db, "t")
	if err != nil || d.Count != 3 || d.Retained != 3 || d.ByKind[diagnosticKey{"Warning", 1264}] != 2 || d.ByKind[diagnosticKey{"Warning", 1062}] != 1 {
		t.Fatalf("complete report=%+v err=%v", d, err)
	}
}
func TestCollectInsertDiagnosticsIncomplete(t *testing.T) {
	db, m := diagnosticMock(t)
	m.ExpectQuery(regexp.QuoteMeta(diagnosticCountSQL)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	m.ExpectQuery(regexp.QuoteMeta(diagnosticDetailsSQL)).WillReturnRows(sqlmock.NewRows([]string{"Level", "Code", "Message"}).AddRow("Warning", 1062, "duplicate"))
	d, err := collectInsertDiagnostics(context.Background(), db, "t")
	if err != nil {
		t.Fatal(err)
	}
	_, err = classifyInsertDiagnostics(insertDiagnosticContext{UsesIgnore: true, VerificationMethod: "sha256"}, d)
	if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTICS_UNPROVEN") {
		t.Fatalf("incomplete diagnostics accepted: %v", err)
	}
}
func TestCollectInsertDiagnosticsFailures(t *testing.T) {
	cause := errors.New("read failed")
	for _, name := range []string{"count_query", "negative_count", "count_scan", "details_query", "details_scan", "iteration", "close", "canceled"} {
		t.Run(name, func(t *testing.T) {
			db, m := diagnosticMock(t)
			ctx := context.Background()
			sentinel := false
			if name == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else {
				q := m.ExpectQuery(regexp.QuoteMeta(diagnosticCountSQL))
				switch name {
				case "count_query":
					q.WillReturnError(cause)
					sentinel = true
				case "negative_count":
					q.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(-1))
				case "count_scan":
					q.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("bad"))
				default:
					q.WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
					detail := m.ExpectQuery(regexp.QuoteMeta(diagnosticDetailsSQL))
					switch name {
					case "details_query":
						detail.WillReturnError(cause)
						sentinel = true
					case "details_scan":
						detail.WillReturnRows(sqlmock.NewRows([]string{"Level", "Code", "Message"}).AddRow("Warning", "bad", "x"))
					case "iteration":
						detail.WillReturnRows(sqlmock.NewRows([]string{"Level", "Code", "Message"}).AddRow("Warning", 1264, "x").RowError(0, cause))
						sentinel = true
					case "close":
						detail.WillReturnRows(sqlmock.NewRows([]string{"Level", "Code", "Message"}).CloseError(cause))
						sentinel = true
					}
				}
			}
			_, err := collectInsertDiagnostics(ctx, db, "t")
			if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTICS_UNPROVEN") {
				t.Fatalf("unreadable diagnostics accepted: %v", err)
			}
			if sentinel && !errors.Is(err, cause) {
				t.Fatalf("cause lost: %v", err)
			}
			if name == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}
func TestCollectInsertDiagnosticsBoundedPreview(t *testing.T) {
	db, m := diagnosticMock(t)
	m.ExpectQuery(regexp.QuoteMeta(diagnosticCountSQL)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))
	rows := sqlmock.NewRows([]string{"Level", "Code", "Message"})
	for i := 0; i < 7; i++ {
		rows.AddRow("Warning", 1264, fmt.Sprintf("line\n%s", strings.Repeat("é", 200)))
	}
	m.ExpectQuery(regexp.QuoteMeta(diagnosticDetailsSQL)).WillReturnRows(rows)
	d, err := collectInsertDiagnostics(context.Background(), db, "t")
	if err != nil || d.Retained != 7 || len(d.Preview) != 5 {
		t.Fatal(d, err)
	}
	for _, item := range d.Preview {
		if len(item.Message) > 240 || strings.Contains(item.Message, "\n") {
			t.Fatal(item)
		}
	}
}
