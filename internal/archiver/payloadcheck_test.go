package archiver

import (
	"context"
	"errors"
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
	validator := NewPayloadValidator(nil, db, graph.NewGraph("customers", "id"), jobCfg,
		config.SafetyConfig{}, 100, nil)

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
