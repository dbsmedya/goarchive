package archiver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/graph"
)

const columnsQueryPattern = "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA"

// TestLoadRootPKMetaConfiguredColumnNotActualPK is mutant table row 1 and the whole
// point of this migration: a PrimaryKeys-based implementation reads the table's actual
// PRIMARY KEY ("id") and passes, then runs checkpoint arithmetic against a varchar
// column. loadRootPKMeta must instead validate the CONFIGURED column ("ref_no").
func TestLoadRootPKMetaConfiguredColumnNotActualPK(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(columnsQueryPattern).
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).
			AddRow("orders", "id", 1, "bigint", "bigint", "").
			AddRow("orders", "ref_no", 2, "varchar", "varchar(36)", ""))

	g := graph.NewGraph("orders", "ref_no")
	err = loadRootPKMeta(context.Background(), db, "srcdb", g)
	if err == nil {
		t.Fatal("expected ROOT_PK_TYPE_UNSUPPORTED, got nil")
	}
	if !strings.Contains(err.Error(), "ROOT_PK_TYPE_UNSUPPORTED") {
		t.Fatalf("expected ROOT_PK_TYPE_UNSUPPORTED, got: %v", err)
	}
	if !strings.Contains(err.Error(), "ref_no") {
		t.Fatalf("expected the configured column name in the error, got: %v", err)
	}
}

// TestLoadRootPKMetaCompositeRealPKStillWorks is mutant table row 2: 1.8 never
// inspected key shape, so a table whose real PK is composite (here simulated by the
// column fact simply carrying an unrelated extra column, since Inspector.Columns
// reports no primary-key information at all) must still pass when the configured
// column matches by name.
func TestLoadRootPKMetaCompositeRealPKStillWorks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(columnsQueryPattern).
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).
			AddRow("orders", "id", 1, "bigint", "bigint", "").
			AddRow("orders", "tenant_id", 2, "bigint", "bigint", ""))

	g := graph.NewGraph("orders", "id")
	if err := loadRootPKMeta(context.Background(), db, "srcdb", g); err != nil {
		t.Fatalf("expected pass, got: %v", err)
	}
	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok || dataType != "bigint" || unsigned {
		t.Fatalf("metadata = (%q, %v, %v), want (\"bigint\", false, true)", dataType, unsigned, ok)
	}
}

// TestLoadRootPKMetaRecordsUnsigned is mutant table row 3 and the ONLY test that kills
// mutant M11: tests 1, 2 and 4 all involve signed columns, so a build that hardcodes
// `unsigned = false` produces exactly the expected values there and survives. Both
// recorded values — data type AND signedness — must be asserted here.
func TestLoadRootPKMetaRecordsUnsigned(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(columnsQueryPattern).
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("orders", "id", 1, "bigint", "bigint unsigned", ""))

	g := graph.NewGraph("orders", "id")
	if err := loadRootPKMeta(context.Background(), db, "srcdb", g); err != nil {
		t.Fatalf("expected pass, got: %v", err)
	}
	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok || dataType != "bigint" || !unsigned {
		t.Fatalf("metadata = (%q, %v, %v), want (\"bigint\", true, true)", dataType, unsigned, ok)
	}
}

// TestLoadRootPKMetaMissingColumnIsPlainError is mutant table row 4: the configured
// column does not exist under any casing. The skip path has no preflight to attribute
// the failure to, so the result must be a plain error, never a *PreflightError. Naming
// the table and column is an improvement this phase adds — 1.8 returned
// "loadRootPKMeta: sql: no rows in result set", naming neither.
func TestLoadRootPKMetaMissingColumnIsPlainError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(columnsQueryPattern).
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("orders", "id", 1, "bigint", "bigint", ""))

	g := graph.NewGraph("orders", "LOG_ID")
	err = loadRootPKMeta(context.Background(), db, "srcdb", g)
	if err == nil {
		t.Fatal("expected an error for a missing configured column, got nil")
	}
	var pe *PreflightError
	if errors.As(err, &pe) {
		t.Fatalf("expected a plain error, got *PreflightError: %v", pe)
	}
	if !strings.Contains(err.Error(), "orders") || !strings.Contains(err.Error(), "LOG_ID") {
		t.Fatalf("expected the table and column named in the error, got: %v", err)
	}
}

// TestLoadRootPKMetaCaseFoldMatchPasses is the mandatory case-fold test (Step 11 item
// 2): the skip path preserves 1.8's case-insensitive COLUMN_NAME lookup, so a
// configured "log_id" matching the server's "LOG_ID" passes and records the metadata.
// This is not an endorsement of mis-cased config — normal preflight still rejects it at
// position 2 with PK_COLUMN_CASE_CHECK; this is the skip path preserving what 1.8 did
// when the operator opted out of that check.
func TestLoadRootPKMetaCaseFoldMatchPasses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(columnsQueryPattern).
		WithArgs("srcdb", "events").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("events", "LOG_ID", 1, "bigint", "bigint", ""))

	g := graph.NewGraph("events", "log_id")
	if err := loadRootPKMeta(context.Background(), db, "srcdb", g); err != nil {
		t.Fatalf("expected the case-fold match to pass, got: %v", err)
	}
	dataType, unsigned, ok := g.GetRootPKMeta()
	if !ok || dataType != "bigint" || unsigned {
		t.Fatalf("metadata = (%q, %v, %v), want (\"bigint\", false, true)", dataType, unsigned, ok)
	}
}
