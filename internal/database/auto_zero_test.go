package database

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestAutoIncrementZeroModeError(t *testing.T) {
	boom := errors.New("probe failed")
	err := &AutoIncrementZeroModeError{Mode: "STRICT_TRANS_TABLES", Cause: boom}
	if !strings.Contains(err.Error(), "AUTO_INCREMENT_ZERO_MODE_CHECK") || !errors.Is(err, boom) {
		t.Fatalf("missing named/wrapped mode error: %v", err)
	}
}

func TestAssertAutoIncrementZeroMode(t *testing.T) {
	for _, tc := range []struct {
		mode string
		fail bool
	}{
		{"NO_AUTO_VALUE_ON_ZERO", false}, {"STRICT_TRANS_TABLES,NO_AUTO_VALUE_ON_ZERO", false},
		{"", true}, {"STRICT_TRANS_TABLES", true}, {"XNO_AUTO_VALUE_ON_ZERO", true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SELECT @@SESSION.sql_mode").WillReturnRows(sqlmock.NewRows([]string{"mode"}).AddRow(tc.mode))
			err = assertAutoIncrementZeroMode(context.Background(), db)
			if (err != nil) != tc.fail {
				t.Fatalf("mode=%q err=%v want failure=%v", tc.mode, err, tc.fail)
			}
			if tc.fail && !strings.Contains(err.Error(), "AUTO_INCREMENT_ZERO_MODE_CHECK") {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAssertAutoIncrementZeroModeQueryFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	boom := errors.New("probe failed")
	mock.ExpectQuery("SELECT @@SESSION.sql_mode").WillReturnError(boom)
	err = assertAutoIncrementZeroMode(context.Background(), db)
	var target *AutoIncrementZeroModeError
	if !errors.As(err, &target) || !errors.Is(err, boom) || !strings.Contains(err.Error(), "AUTO_INCREMENT_ZERO_MODE_CHECK") {
		t.Fatalf("missing named/wrapped query error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
