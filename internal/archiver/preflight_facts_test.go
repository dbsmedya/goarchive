package archiver

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

// TestPreflightRunMemoizesSourceTables proves the run-scoped cache fetches the source
// table fact exactly once even when several stages ask for it. sqlmock fails the test
// if a second query is issued, because only one expectation is registered.
func TestPreflightRunMemoizesSourceTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("orders", "BASE TABLE", "InnoDB"))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	first, err := run.sourceTables(context.Background())
	if err != nil {
		t.Fatalf("first sourceTables: %v", err)
	}
	second, err := run.sourceTables(context.Background())
	if err != nil {
		t.Fatalf("second sourceTables: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0].Table != second[0].Table {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the fact was fetched more than once: %v", err)
	}
}

// TestPreflightRunMemoizesErrors proves a failed fetch is memoized too: a stage that
// asks again must get the same error without re-querying, so one broken connection
// cannot produce two different verdicts within one run.
func TestPreflightRunMemoizesErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("information_schema.TABLES").WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	run := newPreflightRun(p)

	_, firstErr := run.sourceTables(context.Background())
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}

	_, secondErr := run.sourceTables(context.Background())
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected source-table query was not observed: %v", err)
	}
}

// TestPreflightRunIsNotStoredOnChecker enforces spec §2: the cache is per-run. Two
// runs over the same checker must not share facts.
func TestPreflightRunIsNotStoredOnChecker(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("orders", "BASE TABLE", "InnoDB")
	}
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(rows())
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(rows())

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())

	if _, err := newPreflightRun(p).sourceTables(context.Background()); err != nil {
		t.Fatalf("run 1: %v", err)
	}
	if _, err := newPreflightRun(p).sourceTables(context.Background()); err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the second run reused the first run's cache: %v", err)
	}
}
