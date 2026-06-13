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
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("user").AddRow("host"))

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

func TestValidateSingleColumnPrimaryKey_NoPrimaryKey(t *testing.T) {
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

	// orders has NO PRIMARY KEY (zero rows) → must be rejected (review 003):
	// the configured primary_key is then almost certainly non-unique, so
	// delete-by-it would over-match rows outside the archived set.
	mock.ExpectQuery("information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}))

	err = checker.ValidateSingleColumnPrimaryKey(context.Background(), []string{"orders"})
	if err == nil {
		t.Fatal("expected no-PRIMARY-KEY rejection, got nil")
	}
	if !strings.Contains(err.Error(), "PRIMARY_KEY_CHECK") {
		t.Fatalf("expected PRIMARY_KEY_CHECK error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "orders(no PRIMARY KEY)") {
		t.Fatalf("expected offending table in error, got: %v", err)
	}
}

func TestValidateSingleColumnPrimaryKey_ConfiguredPKNotPrimary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The graph configures orders.primary_key = "id", but the table's real
	// PRIMARY KEY column is "legacy_id" → the mismatch must be rejected (review
	// 003): if "id" is non-unique, delete-by-it over-matches.
	g := createPreflightTestGraph()
	checker, err := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	mock.ExpectQuery("information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("legacy_id"))

	err = checker.ValidateSingleColumnPrimaryKey(context.Background(), []string{"orders"})
	if err == nil {
		t.Fatal("expected configured-PK-mismatch rejection, got nil")
	}
	if !strings.Contains(err.Error(), "PRIMARY_KEY_CHECK") {
		t.Fatalf("expected PRIMARY_KEY_CHECK error, got: %v", err)
	}
	if !strings.Contains(err.Error(), `is not the PRIMARY KEY column "legacy_id"`) {
		t.Fatalf("expected mismatch detail in error, got: %v", err)
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

	// Two tables, each with a single-column PRIMARY KEY matching the graph PK
	// ("id") → pass.
	mock.ExpectQuery("information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id"))
	mock.ExpectQuery("information_schema.STATISTICS").
		WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id"))

	if err := checker.ValidateSingleColumnPrimaryKey(context.Background(), []string{"orders", "order_items"}); err != nil {
		t.Fatalf("expected single-column PKs to pass, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
