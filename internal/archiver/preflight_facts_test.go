package archiver

import (
	"context"
	"database/sql"
	"errors"
	"strings"
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
		t.Fatalf("expected source-table query was not observed: %v", err)
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

// chrDestCheckerForFacts builds a checker whose source pool is srcDB and whose
// destination pool is dstDB, so a destination-fact test can register its sqlmock
// expectation on the destination mock alone.
func chrDestCheckerForFacts(t *testing.T, srcDB, dstDB *sql.DB) *PreflightChecker {
	t.Helper()
	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(srcDB, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}
	if err := p.ConfigureDestination(dstDB, "dstdb", "dstdb"); err != nil {
		t.Fatalf("ConfigureDestination: %v", err)
	}
	return p
}

// TestPreflightRunMemoizesDestTables is the destination-side counterpart to
// TestPreflightRunMemoizesSourceTables: the fact is fetched exactly once even when
// several stages ask for it.
//
// The load-bearing assertions are the SECOND call's error check and the value
// comparison. Only one expectation is registered, so an unmemoized second call
// re-queries and gets "all expectations were already fulfilled".
//
// ExpectationsWereMet() is NOT what catches that, and this was verified by mutation
// rather than assumed: with the memoization guard removed it still returns nil,
// because the single registered expectation WAS consumed. It is asserted here only to
// prove the query happened at all (i.e. not zero times).
func TestPreflightRunMemoizesDestTables(t *testing.T) {
	srcDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	dstDB, dstMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	dstMock.ExpectQuery("information_schema.TABLES").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "TABLE_TYPE", "ENGINE"}).
			AddRow("orders", "BASE TABLE", "InnoDB"))

	run := newPreflightRun(chrDestCheckerForFacts(t, srcDB, dstDB))

	first, err := run.destTables(context.Background())
	if err != nil {
		t.Fatalf("first destTables: %v", err)
	}
	second, err := run.destTables(context.Background())
	if err != nil {
		t.Fatalf("second destTables: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0].Table != second[0].Table {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := dstMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected destination-table query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesDestErrors proves a failed destination fetch is memoized
// too. Success and error memoization are separate promises: an implementation that
// caches values but retries failures would pass the test above and fail this one.
//
// The identity assertion (errors.Is(secondErr, firstErr)) is what makes this
// non-vacuous. Asserting only that both errors are non-nil passes with memoization
// removed entirely, because a consumed sqlmock expectation still returns an error on
// the next call and ExpectationsWereMet() still reports nil — both verified by
// mutation, not assumed.
func TestPreflightRunMemoizesDestErrors(t *testing.T) {
	srcDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	dstDB, dstMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	wantErr := errors.New("boom")
	dstMock.ExpectQuery("information_schema.TABLES").WillReturnError(wantErr)

	run := newPreflightRun(chrDestCheckerForFacts(t, srcDB, dstDB))
	ctx := context.Background()

	_, firstErr := run.destTables(ctx)
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}

	_, secondErr := run.destTables(ctx)
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := dstMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected destination-table query was not observed: %v", err)
	}
}

// TestPreflightRunDestTablesNotConfigured proves destTables reports the
// unconfigured destination itself, rather than panicking on a nil inspector.
// newPreflightRun only builds dstInspector when a destination is configured.
func TestPreflightRunDestTablesNotConfigured(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}
	// Do NOT call ConfigureDestination.

	run := newPreflightRun(p)
	if run.dstInspector != nil {
		t.Fatal("dstInspector must not be constructed without a configured destination")
	}
	_, err = run.destTables(context.Background())
	if err == nil {
		t.Fatal("destTables must report the unconfigured destination")
	}
	if !strings.Contains(err.Error(), "destination database not configured") {
		t.Fatalf("unexpected error: %v", err)
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

// TestPreflightRunMemoizesInvisibleColumns proves the fact is fetched exactly once even
// when several stages ask for it. Only one expectation is registered, so an unmemoized
// second call re-queries and gets "all expectations were already fulfilled".
func TestPreflightRunMemoizesInvisibleColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.COLUMNS").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "COLUMN_NAME"}).
			AddRow("orders", "note"))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	first, err := run.invisibleColumns(context.Background())
	if err != nil {
		t.Fatalf("first invisibleColumns: %v", err)
	}
	second, err := run.invisibleColumns(context.Background())
	if err != nil {
		t.Fatalf("second invisibleColumns: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0].Table != second[0].Table {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected invisible-column query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesInvisibleColumnsErrors proves a failed fetch is memoized too.
//
// The identity assertion (errors.Is(secondErr, firstErr)) is what makes this
// non-vacuous. Asserting only that both errors are non-nil passes with memoization
// removed entirely, because a consumed sqlmock expectation still returns an error on the
// next call and ExpectationsWereMet() still reports nil — both verified by mutation in
// phase 012, not assumed.
func TestPreflightRunMemoizesInvisibleColumnsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	run := newPreflightRun(p)
	ctx := context.Background()

	_, firstErr := run.invisibleColumns(ctx)
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}

	_, secondErr := run.invisibleColumns(ctx)
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected invisible-column query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesSourceDeleteTriggers proves the source fact is fetched exactly
// once. The WithArgs assertion is load-bearing: it pins the DELETE event, so a fetch
// issued with the wrong event fails here rather than silently receiving these rows.
func TestPreflightRunMemoizesSourceDeleteTriggers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("srcdb", "DELETE").
		WillReturnRows(sqlmock.NewRows([]string{
			"EVENT_OBJECT_TABLE", "TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING",
		}).AddRow("orders", "trg_del", "DELETE", "AFTER"))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	first, err := run.sourceDeleteTriggers(context.Background())
	if err != nil {
		t.Fatalf("first sourceDeleteTriggers: %v", err)
	}
	second, err := run.sourceDeleteTriggers(context.Background())
	if err != nil {
		t.Fatalf("second sourceDeleteTriggers: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0].Name != second[0].Name {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected source DELETE-trigger query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesSourceDeleteTriggersErrors proves a failed source fetch is
// memoized. The identity assertion is what makes it non-vacuous: asserting only that
// both errors are non-nil passes with memoization removed entirely, because a consumed
// sqlmock expectation still returns an error on the next call.
func TestPreflightRunMemoizesSourceDeleteTriggersErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("srcdb", "DELETE").
		WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	run := newPreflightRun(p)
	ctx := context.Background()

	_, firstErr := run.sourceDeleteTriggers(ctx)
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}
	_, secondErr := run.sourceDeleteTriggers(ctx)
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected source DELETE-trigger query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesDestInsertTriggers is the destination counterpart. WithArgs
// pins the INSERT event on the destination pool.
func TestPreflightRunMemoizesDestInsertTriggers(t *testing.T) {
	srcDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	dstDB, dstMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	dstMock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("dstdb", "INSERT").
		WillReturnRows(sqlmock.NewRows([]string{
			"EVENT_OBJECT_TABLE", "TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING",
		}).AddRow("orders", "trg_ins", "INSERT", "BEFORE"))

	run := newPreflightRun(chrDestCheckerForFacts(t, srcDB, dstDB))
	ctx := context.Background()

	first, err := run.destInsertTriggers(ctx)
	if err != nil {
		t.Fatalf("first destInsertTriggers: %v", err)
	}
	second, err := run.destInsertTriggers(ctx)
	if err != nil {
		t.Fatalf("second destInsertTriggers: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0].Name != second[0].Name {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := dstMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected destination INSERT-trigger query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesDestInsertTriggersErrors is the destination error counterpart.
func TestPreflightRunMemoizesDestInsertTriggersErrors(t *testing.T) {
	srcDB, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = srcDB.Close() }()
	dstDB, dstMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = dstDB.Close() }()

	wantErr := errors.New("boom")
	dstMock.ExpectQuery("information_schema.TRIGGERS").
		WithArgs("dstdb", "INSERT").
		WillReturnError(wantErr)

	run := newPreflightRun(chrDestCheckerForFacts(t, srcDB, dstDB))
	ctx := context.Background()

	_, firstErr := run.destInsertTriggers(ctx)
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}
	_, secondErr := run.destInsertTriggers(ctx)
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := dstMock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected destination INSERT-trigger query was not observed: %v", err)
	}
}
