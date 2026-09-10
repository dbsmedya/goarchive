package archiver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/dbsmedya/goarchive/internal/types"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
)

const maxAllowedPacketQuery = "SHOW VARIABLES LIKE 'max_allowed_packet'"

func TestPlaceholderCheck(t *testing.T) {
	// 60000 cols * 2 batch = 120000 > 65535 -> fail
	if err := checkPlaceholderLimit("wide", 60000, 2); err == nil {
		t.Fatalf("expected placeholder limit error for wide table")
	}
	// 5 cols * 1000 batch = 5000 < 65535 -> ok
	if err := checkPlaceholderLimit("narrow", 5, 1000); err != nil {
		t.Fatalf("unexpected error for narrow table: %v", err)
	}
	// Exact boundary: 65535 placeholders must fail (>=), 65534 must pass.
	if err := checkPlaceholderLimit("exact", 65535, 1); err == nil {
		t.Fatal("expected limit error at exact boundary (65535)")
	}
	if err := checkPlaceholderLimit("below", 65534, 1); err != nil {
		t.Fatalf("unexpected error one below boundary (65534): %v", err)
	}
}

func TestMaxAllowedPacket(t *testing.T) {
	t.Run("returns numeric server value", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer func() { _ = db.Close() }()

		mock.ExpectQuery(regexp.QuoteMeta(maxAllowedPacketQuery)).
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
				AddRow("max_allowed_packet", "67108864"))

		validator := &PayloadValidator{dest: db}
		got, err := validator.maxAllowedPacket(context.Background())
		if err != nil {
			t.Fatalf("maxAllowedPacket: %v", err)
		}
		if got != 67108864 {
			t.Fatalf("maxAllowedPacket = %d, want 67108864", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("sqlmock expectations: %v", err)
		}
	})

	t.Run("preserves query error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer func() { _ = db.Close() }()

		queryErr := errors.New("server variable query failed")
		mock.ExpectQuery(regexp.QuoteMeta(maxAllowedPacketQuery)).WillReturnError(queryErr)

		validator := &PayloadValidator{dest: db}
		_, err = validator.maxAllowedPacket(context.Background())
		if !errors.Is(err, queryErr) {
			t.Fatalf("maxAllowedPacket error = %v, want errors.Is(queryErr)", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("sqlmock expectations: %v", err)
		}
	})

	t.Run("preserves scan error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer func() { _ = db.Close() }()

		scanErr := errors.New("server variable row failed")
		mock.ExpectQuery(regexp.QuoteMeta(maxAllowedPacketQuery)).
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
				AddRow("max_allowed_packet", "67108864").
				RowError(0, scanErr))

		validator := &PayloadValidator{dest: db}
		_, err = validator.maxAllowedPacket(context.Background())
		if !errors.Is(err, scanErr) {
			t.Fatalf("maxAllowedPacket error = %v, want errors.Is(scanErr)", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("sqlmock expectations: %v", err)
		}
	})

	t.Run("rejects malformed numeric value with parse context", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer func() { _ = db.Close() }()

		mock.ExpectQuery(regexp.QuoteMeta(maxAllowedPacketQuery)).
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
				AddRow("max_allowed_packet", "not-a-number"))

		validator := &PayloadValidator{dest: db}
		_, err = validator.maxAllowedPacket(context.Background())
		if err == nil || !strings.Contains(err.Error(),
			`unexpected max_allowed_packet value "not-a-number"`) {
			t.Fatalf("maxAllowedPacket error = %v, want named malformed-value context", err)
		}
		var numErr *strconv.NumError
		if !errors.As(err, &numErr) {
			t.Fatalf("maxAllowedPacket error = %v, want wrapped *strconv.NumError", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("sqlmock expectations: %v", err)
		}
	})
}

func TestPayloadValidateWrapsMaxAllowedPacketErrorBeforeTableProcessing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	queryErr := errors.New("destination unavailable")
	mock.ExpectQuery(regexp.QuoteMeta(maxAllowedPacketQuery)).WillReturnError(queryErr)

	jobCfg := &config.JobConfig{RootTable: "customers", PrimaryKey: "id"}
	validator := NewPayloadValidator(nil, db, graph.NewGraph("customers", "id"), "test", jobCfg,
		config.SafetyConfig{}, 100, config.VerificationConfig{}, nil)

	err = validator.Validate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed to read destination max_allowed_packet") {
		t.Fatalf("Validate error = %v, want max_allowed_packet caller context", err)
	}
	if !errors.Is(err, queryErr) {
		t.Fatalf("Validate error = %v, want errors.Is(queryErr)", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func sampleDiagnosticCase(t *testing.T, skip bool, codes []uint16, incomplete bool, n, cap int) (int, int, error) {
	t.Helper()
	src, sm := diagnosticMock(t)
	dst, dm := diagnosticMock(t)
	cfg := config.DefaultConfig()
	cfg.Verification.SkipVerification = skip
	effective := cfg.GetJobVerification("sample")
	meta := types.ColumnMetadata{Names: []string{"id", "n"}, Temporal: map[string]types.TemporalKind{}}
	job := &config.JobConfig{RootTable: "vp_sample", PrimaryKey: "id", Where: "id>0"}
	p := NewPayloadValidator(src, dst, graph.NewGraph("vp_sample", "id"), "test", job, config.SafetyConfig{}, n, effective, nil)
	q, err := buildSampleQuery("vp_sample", "id", meta, "id>0", n)
	if err != nil {
		t.Fatal(err)
	}
	rows := sqlmock.NewRows(meta.Names)
	for i := 0; i < n; i++ {
		rows.AddRow(int64(i+1), int64(999))
	}
	sm.ExpectQuery(regexp.QuoteMeta(q)).WillReturnRows(rows)
	dm.ExpectQuery(regexp.QuoteMeta(diagnosticSessionSQL)).WillReturnRows(sqlmock.NewRows([]string{"capacity", "notes", "mode"}).AddRow(cap, true, ""))
	dm.ExpectBegin()
	dm.ExpectExec("SET FOREIGN_KEY_CHECKS = 0").WillReturnResult(sqlmock.NewResult(0, 0))
	chunk := effectiveInsertRows(n, 2, cap)
	fail := incomplete
	for _, code := range codes {
		if code != 1062 && (!skip || (code != 1264 && code != 1265 && code != 1292 && code != 1366)) {
			fail = true
		}
	}
	for off := 0; off < n; off += chunk {
		end := off + chunk
		if end > n {
			end = n
		}
		args := make([]driver.Value, 0, (end-off)*2)
		for i := off; i < end; i++ {
			args = append(args, int64(i+1), int64(999))
		}
		dm.ExpectExec(regexp.QuoteMeta(buildInsertIgnoreBatchQueryStandalone("vp_sample", meta.Names, end-off))).WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, int64(end-off)))
		if len(codes) == 0 {
			expectCleanDiagnostics(dm)
		} else {
			count := len(codes)
			if incomplete {
				count++
			}
			dm.ExpectQuery(regexp.QuoteMeta(diagnosticCountSQL)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
			details := sqlmock.NewRows([]string{"Level", "Code", "Message"})
			for _, code := range codes {
				details.AddRow("Warning", code, "fixture")
			}
			dm.ExpectQuery(regexp.QuoteMeta(diagnosticDetailsSQL)).WillReturnRows(details)
		}
		if fail {
			break
		}
	}
	if fail {
		dm.ExpectRollback()
		dm.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	} else {
		dm.ExpectExec("SET FOREIGN_KEY_CHECKS = 1").WillReturnResult(sqlmock.NewResult(0, 0))
		dm.ExpectRollback()
	}
	return p.measureSample(context.Background(), "vp_sample", meta)
}
func TestSampleConversionDefaultRefuses(t *testing.T) {
	_, _, err := sampleDiagnosticCase(t, false, []uint16{1264}, false, 1, 1024)
	if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTIC_REJECTED") {
		t.Fatalf("sample conversion accepted by default: %v", err)
	}
}
func TestSampleConversionSkipAccepts(t *testing.T) {
	rows, bytes, err := sampleDiagnosticCase(t, true, []uint16{1264}, false, 1, 1024)
	if err != nil || rows != 1 || bytes <= 0 {
		t.Fatalf("sample override failed: rows=%d bytes=%d err=%v", rows, bytes, err)
	}
}
func TestSampleDuplicateAndConversionOverride(t *testing.T) {
	_, _, err := sampleDiagnosticCase(t, true, []uint16{1062, 1264}, false, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
}
func TestSampleUnknownAlwaysRefuses(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprint(skip), func(t *testing.T) {
			_, _, err := sampleDiagnosticCase(t, skip, []uint16{9999}, false, 1, 1024)
			if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTIC_REJECTED") {
				t.Fatal(err)
			}
		})
	}
}
func TestSampleIncompleteAlwaysRefuses(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprint(skip), func(t *testing.T) {
			_, _, err := sampleDiagnosticCase(t, skip, []uint16{1062}, true, 1, 1024)
			if err == nil || !strings.Contains(err.Error(), "INSERT_DIAGNOSTICS_UNPROVEN") {
				t.Fatal(err)
			}
		})
	}
}
func TestSampleCapacityKeepsEstimate(t *testing.T) {
	rows, bytes, err := sampleDiagnosticCase(t, false, nil, false, 5000, 1024)
	want := len(buildInsertIgnoreBatchQueryStandalone("vp_sample", []string{"id", "n"}, 5000)) + 5000*2*8
	if err != nil || rows != 5000 || bytes != want {
		t.Fatalf("sample subdivision changed logical estimate: rows=%d bytes=%d want=%d err=%v", rows, bytes, want, err)
	}
}
