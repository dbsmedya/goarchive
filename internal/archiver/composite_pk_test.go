package archiver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

func TestValidateSingleColumnPrimaryKey_Composite(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	g := graph.NewGraph("account", "user")
	checker, err := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	// account has a 2-column PRIMARY KEY → must be rejected.
	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
		}).
			AddRow("account", "user", "varchar", "varchar(64)").
			AddRow("account", "host", "varchar", "varchar(64)"))

	err = checker.ValidateSingleColumnPrimaryKey(context.Background(), newPreflightRun(checker))
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

	g := graph.NewGraph("orders", "id")
	checker, err := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	// orders has NO PRIMARY KEY: the LEFT JOIN yields a row with NULL COLUMN_NAME,
	// not zero rows → must be rejected (review 003): the configured primary_key is
	// then almost certainly non-unique, so delete-by-it would over-match rows
	// outside the archived set.
	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
		}).AddRow("orders", nil, nil, nil))

	err = checker.ValidateSingleColumnPrimaryKey(context.Background(), newPreflightRun(checker))
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
	g := graph.NewGraph("orders", "id")
	checker, err := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
		}).AddRow("orders", "legacy_id", "bigint", "bigint"))

	err = checker.ValidateSingleColumnPrimaryKey(context.Background(), newPreflightRun(checker))
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

	g := graph.NewGraph("orders", "id")
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.SetPK("order_items", "id")
	checker, err := NewPreflightChecker(db, "testdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	// Two tables, each with a single-column PRIMARY KEY matching the graph PK
	// ("id") → pass.
	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("testdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
		}).
			AddRow("orders", "id", "bigint", "bigint").
			AddRow("order_items", "id", "bigint", "bigint"))

	if err := checker.ValidateSingleColumnPrimaryKey(context.Background(), newPreflightRun(checker)); err != nil {
		t.Fatalf("expected single-column PKs to pass, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestJudgePrimaryKeyShapeCompositeBeatsDefinition proves composite is the headline
// rejection: it is reported even when another table has a PK-definition problem.
// ValidateSingleColumnPrimaryKey established this ordering in 1.8 by filling two slices
// and returning the composite one first; the characterization suite asserts it
// end-to-end (TestCharacterizationCompositeBeatsPrimaryKey).
func TestJudgePrimaryKeyShapeCompositeBeatsDefinition(t *testing.T) {
	facts := []validations.PKInfo{
		{Table: "orders", Kind: validations.PKComposite, Columns: []string{"id", "tenant_id"}},
		{Table: "shipments", Kind: validations.PKNone},
	}
	expected := map[string]string{"orders": "id", "shipments": "id"}

	err := judgePrimaryKeyShape(facts, expected)
	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pe.Check != "COMPOSITE_PK_CHECK" {
		t.Fatalf("Check = %q, want COMPOSITE_PK_CHECK", pe.Check)
	}
	if len(pe.Tables) != 1 || pe.Tables[0] != "orders(2-column PRIMARY KEY)" {
		t.Fatalf("Tables = %v", pe.Tables)
	}
}

// TestJudgePrimaryKeyShapeNoPK proves the absent-PK path keeps its decoration, and that
// the table is reported EXACTLY ONCE: a PKNone fact fails CheckPKExists, and
// CheckPKMatchesExpected skips it, so there must be no second entry.
func TestJudgePrimaryKeyShapeNoPK(t *testing.T) {
	facts := []validations.PKInfo{{Table: "orders", Kind: validations.PKNone}}
	err := judgePrimaryKeyShape(facts, map[string]string{"orders": "id"})

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pe.Check != "PRIMARY_KEY_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
	if len(pe.Tables) != 1 || pe.Tables[0] != "orders(no PRIMARY KEY)" {
		t.Fatalf("Tables = %v, want exactly one entry", pe.Tables)
	}
}

// TestJudgePrimaryKeyShapeWrongColumn proves the mismatch path keeps its decoration,
// which names both the configured and the actual column.
func TestJudgePrimaryKeyShapeWrongColumn(t *testing.T) {
	facts := []validations.PKInfo{
		{Table: "orders", Kind: validations.PKSingle, Columns: []string{"legacy_id"},
			DataType: "bigint", IsInteger: true},
	}
	err := judgePrimaryKeyShape(facts, map[string]string{"orders": "id"})

	var pe *PreflightError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PreflightError, got %T: %v", err, err)
	}
	if pe.Check != "PRIMARY_KEY_CHECK" {
		t.Fatalf("Check = %q", pe.Check)
	}
	if !strings.Contains(pe.Tables[0], `"id"`) || !strings.Contains(pe.Tables[0], `"legacy_id"`) {
		t.Fatalf("Tables[0] must name both the configured and actual column: %q", pe.Tables[0])
	}
}

// TestJudgePrimaryKeyShapeUnconfiguredTableIsSkipped pins that a table with no
// configured primary_key produces no verdict HERE — ValidatePrimaryKeyColumns rejects
// that case at position 2 with PK_COLUMN_CHECK, and it must keep that ID.
//
// Note what this does NOT cover: the skip it observes happens inside the library
// (CheckPKMatchesExpected returns early when expected[table] is ""), not in
// expectedPKs. The HasPK guard is covered by 8g. This test's value is as a pin against
// the v0.5.0 bump in phase 018 — if the library ever stops skipping an empty
// expectation, an unconfigured table starts producing a PRIMARY_KEY_CHECK here.
func TestJudgePrimaryKeyShapeUnconfiguredTableIsSkipped(t *testing.T) {
	facts := []validations.PKInfo{
		{Table: "orders", Kind: validations.PKSingle, Columns: []string{"legacy_id"},
			DataType: "bigint", IsInteger: true},
	}
	if err := judgePrimaryKeyShape(facts, map[string]string{}); err != nil {
		t.Fatalf("an unconfigured table must not be judged here, got %v", err)
	}
}

// TestJudgePrimaryKeyShapePasses is the negative control.
func TestJudgePrimaryKeyShapePasses(t *testing.T) {
	facts := []validations.PKInfo{
		{Table: "orders", Kind: validations.PKSingle, Columns: []string{"id"},
			DataType: "bigint", IsInteger: true},
	}
	if err := judgePrimaryKeyShape(facts, map[string]string{"orders": "id"}); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
}
