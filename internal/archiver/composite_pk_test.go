package archiver

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/logger"
)

func TestValidateSingleColumnPrimaryKey_Composite(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	checker, err := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	// account has a 2-column PRIMARY KEY → must be rejected.
	mock.ExpectQuery("information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(2))

	err = checker.ValidateSingleColumnPrimaryKey(context.Background(), []string{"account"})
	if err == nil {
		t.Fatal("expected composite-PK rejection, got nil")
	}
	if !strings.Contains(err.Error(), "COMPOSITE_PK_CHECK") {
		t.Fatalf("expected COMPOSITE_PK_CHECK error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "account(2-column PRIMARY KEY)") {
		t.Fatalf("expected offending table in error, got: %v", err)
	}
}

func TestValidateSingleColumnPrimaryKey_SingleColumnPasses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	g := createPreflightTestGraph()
	checker, err := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	// Two tables, each with a single-column PRIMARY KEY → pass.
	mock.ExpectQuery("information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
	mock.ExpectQuery("information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))

	if err := checker.ValidateSingleColumnPrimaryKey(context.Background(), []string{"orders", "order_items"}); err != nil {
		t.Fatalf("expected single-column PKs to pass, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
