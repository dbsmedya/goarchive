package archiver

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
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

// TestPreflightRunMemoizesInvisibleColumns proves invisibleColumns is now a PROJECTION
// over sourceColumns (P1 correction): only ONE Columns-shaped expectation is
// registered, and both calls are satisfied from it — there is no separate
// InvisibleColumns query left to issue. A single row set carries one invisible column
// ("note", EXTRA = "INVISIBLE") and one visible column ("id"), so the derivation must
// also prove it filters correctly, not just that it memoizes.
func TestPreflightRunMemoizesInvisibleColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).
			AddRow("orders", "id", 1, "bigint", "bigint", "").
			AddRow("orders", "note", 2, "varchar", "varchar(64)", "INVISIBLE"))

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
	if len(first[0].Columns) != 1 || first[0].Columns[0] != "note" {
		t.Fatalf("expected only the invisible column \"note\", got %v", first[0].Columns)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected exactly one Columns query, got: %v", err)
	}
}

// TestPreflightRunMemoizesInvisibleColumnsErrors proves a failed fetch is memoized too,
// via the underlying sourceColumns fact (P1 correction): only ONE Columns-shaped
// expectation is registered.
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
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("srcdb", "orders").
		WillReturnError(wantErr)

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
		t.Fatalf("expected exactly one Columns query, got: %v", err)
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

// TestPreflightRunMemoizesPrimaryKeys proves the run-scoped cache fetches the PK fact
// exactly once even when several stages ask for it. sqlmock fails the test if a second
// query is issued, because only one expectation is registered.
func TestPreflightRunMemoizesPrimaryKeys(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("srcdb").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE",
		}).AddRow("orders", "id", "bigint", "bigint unsigned"))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	first, err := run.primaryKeys(context.Background())
	if err != nil {
		t.Fatalf("first primaryKeys: %v", err)
	}
	second, err := run.primaryKeys(context.Background())
	if err != nil {
		t.Fatalf("second primaryKeys: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0].Table != second[0].Table {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected PrimaryKeys query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesPrimaryKeysErrors proves a failed fetch is memoized too. The
// identity assertion (errors.Is(secondErr, firstErr)) is what makes this non-vacuous:
// asserting only that both errors are non-nil passes against a build with no
// memoization at all, because a consumed sqlmock expectation still returns an error on
// the next call.
func TestPreflightRunMemoizesPrimaryKeysErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("information_schema.STATISTICS AS s").
		WithArgs("srcdb").
		WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	run := newPreflightRun(p)
	ctx := context.Background()

	_, firstErr := run.primaryKeys(ctx)
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}
	_, secondErr := run.primaryKeys(ctx)
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected PrimaryKeys query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesSourceColumns proves the column fact is fetched once and the
// same value is returned on a second call.
func TestPreflightRunMemoizesSourceColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("srcdb", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "COLUMN_TYPE", "EXTRA",
		}).AddRow("orders", "id", 1, "bigint", "bigint", ""))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	first, err := run.sourceColumns(context.Background())
	if err != nil {
		t.Fatalf("first sourceColumns: %v", err)
	}
	second, err := run.sourceColumns(context.Background())
	if err != nil {
		t.Fatalf("second sourceColumns: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0].Table != second[0].Table {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected Columns query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesSourceColumnsErrors proves a failed fetch is memoized too. The
// identity assertion (errors.Is(secondErr, firstErr)) is what makes this non-vacuous:
// asserting only that both errors are non-nil passes against a build with no
// memoization at all, because a consumed sqlmock expectation still returns an error on
// the next call.
func TestPreflightRunMemoizesSourceColumnsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, EXTRA").
		WithArgs("srcdb", "orders").
		WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	run := newPreflightRun(p)
	ctx := context.Background()

	_, firstErr := run.sourceColumns(ctx)
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}
	_, secondErr := run.sourceColumns(ctx)
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected Columns query was not observed: %v", err)
	}
}

// TestPreflightRunRootPKInfoSelectsByRootName proves rootPKInfo selects the root's fact
// BY NAME, not by position. The cache is pre-populated with the root fact SECOND and no
// sqlmock expectation is registered, so an implementation that indexed facts[0] would
// return order_lines' fact here — deterministically wrong, since Graph.AllNodes()
// ranges over a map and makes the real fetch order a coin flip.
func TestPreflightRunRootPKInfoSelectsByRootName(t *testing.T) {
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

	run := newPreflightRun(p)
	run.pks = []validations.PKInfo{
		{Table: "order_lines", Kind: validations.PKSingle, Columns: []string{"line_id"},
			DataType: "smallint", IsInteger: true},
		{Table: "orders", Kind: validations.PKSingle, Columns: []string{"id"},
			DataType: "bigint", IsInteger: true, Unsigned: true},
	}
	run.pksLoaded = true

	pk, found, err := run.rootPKInfo(context.Background())
	if err != nil {
		t.Fatalf("rootPKInfo: %v", err)
	}
	if !found {
		t.Fatal("expected root PK fact to be found")
	}
	if pk.Table != "orders" {
		t.Fatalf("rootPKInfo returned %q, want %q", pk.Table, "orders")
	}
}

// TestPreflightRunMemoizesDestGrantsErrors proves a failed destGrants fetch is
// memoized. A successful validations.Grants cannot be constructed outside the
// library (all fields are unexported and there is no constructor), so this uses the
// error path only: register a single failing SELECT CURRENT_USER() expectation and
// call destGrants twice.
//
// errors.Is(secondErr, wantErr) catches memoization removal, because a retried query
// has no sqlmock expectation left and returns sqlmock's own "all expectations already
// fulfilled" error instead of wantErr. errors.Is(secondErr, firstErr) additionally
// catches an implementation that re-derives the same underlying cause but wraps it
// into a fresh error value on every accessor call.
func TestPreflightRunMemoizesDestGrantsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("grants failed")
	mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnError(wantErr)

	p := &PreflightChecker{destinationDB: db, destinationDBName: "destdb"}
	run := &preflightRun{checker: p}

	_, firstErr := run.destGrants(context.Background())
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}
	_, secondErr := run.destGrants(context.Background())
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected failing grants query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesSourceGrantsErrors proves a failed sourceGrants fetch is
// memoized. Mirrors TestPreflightRunMemoizesDestGrantsErrors; only the checker's
// source fields and the accessor under test differ.
//
// errors.Is(secondErr, wantErr) catches memoization removal, because a retried query
// has no sqlmock expectation left and returns sqlmock's own "all expectations already
// fulfilled" error instead of wantErr. errors.Is(secondErr, firstErr) additionally
// catches an implementation that re-derives the same underlying cause but wraps it
// into a fresh error value on every accessor call.
func TestPreflightRunMemoizesSourceGrantsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("grants failed")
	mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnError(wantErr)

	p := &PreflightChecker{db: db, sourceDBName: "sourcedb"}
	run := &preflightRun{checker: p}

	_, firstErr := run.sourceGrants(context.Background())
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}
	_, secondErr := run.sourceGrants(context.Background())
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected failing grants query was not observed: %v", err)
	}
}

// TestPreflightRunExpectedPKsOmitsUnconfiguredTables proves expectedPKs omits a table
// with no configured primary_key rather than defaulting it to "id". Graph.GetPK
// defaults to "id" when unset, so an implementation that dropped the HasPK guard would
// emit out["order_lines"] = "id" here instead of omitting the entry.
func TestPreflightRunExpectedPKsOmitsUnconfiguredTables(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", &graph.Node{Name: "order_lines"}) // deliberately no SetPK

	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	got := run.expectedPKs()
	if len(got) != 1 || got["orders"] != "id" {
		t.Fatalf("expectedPKs = %v, want only the configured root", got)
	}
	if _, ok := got["order_lines"]; ok {
		t.Fatal("a table with no configured primary_key must be omitted, not defaulted to \"id\"")
	}
}

// TestPreflightRunMemoizesFKOutgoing proves the outgoing FK fetch happens once per run.
// Only the INNODB_FOREIGN expectation is registered: the library tries that registry
// first and returns without touching the information_schema fallback when it succeeds,
// so a single expectation is the whole primary path. A second query of any kind would
// therefore go unmatched and surface as an error.
func TestPreflightRunMemoizesFKOutgoing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("INNODB_FOREIGN AS f").
		WillReturnRows(sqlmock.NewRows([]string{
			"ID", "FOR_NAME", "REF_NAME", "N_COLS", "TYPE",
			"FOR_COL_NAME", "REF_COL_NAME", "POS",
		}).AddRow("srcdb/fk_orders_users", "srcdb/orders", "srcdb/users", 1, 0, "user_id", "id", 1))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	first, err := run.fkOutgoing(context.Background())
	if err != nil {
		t.Fatalf("first fkOutgoing: %v", err)
	}
	second, err := run.fkOutgoing(context.Background())
	if err != nil {
		t.Fatalf("second fkOutgoing: %v", err)
	}
	if len(first.Keys) != 1 || len(second.Keys) != 1 || first.Keys[0].ConstraintName != second.Keys[0].ConstraintName {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected INNODB_FOREIGN query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesFKOutgoingErrors proves a failed fetch is memoized too. Both
// sources must be made to fail — the library falls back to information_schema on any
// primary error and only reports failure when both fail, joining the causes — so two
// expectations are registered and errors.Is against wantErr holds through errors.Join.
//
// The identity assertion (errors.Is(secondErr, firstErr)) is what makes this non-vacuous:
// asserting only that both errors are non-nil passes against a build with no memoization
// at all, because a consumed sqlmock expectation still returns an error on the next call.
func TestPreflightRunMemoizesFKOutgoingErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("INNODB_FOREIGN AS f").WillReturnError(wantErr)
	mock.ExpectQuery("KEY_COLUMN_USAGE AS kcu").WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	run := newPreflightRun(p)
	ctx := context.Background()

	_, firstErr := run.fkOutgoing(ctx)
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}
	_, secondErr := run.fkOutgoing(ctx)
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected FK queries were not observed: %v", err)
	}
}

// TestPreflightRunFKOutgoingUsesOutgoingSelector pins the selector as the property it
// asserts, rather than as a side effect of its fixture.
//
// TestPreflightRunMemoizesFKOutgoing does happen to fail under a mutated selector too —
// its constraint's parent (users) is outside the graph, so Within filters it out and
// IncomingTo never matches it, and both leave Keys empty against a len(Keys) == 1 check.
// But that is incidental to what it asserts. Adding the parent to that test's graph, or
// any other ordinary fixture edit, would silently remove the discrimination with no test
// failing and no reviewer prompted to look. This test makes the property explicit and
// keeps it pinned across such edits.
//
// The fixture is one constraint whose CHILD is in the graph and whose PARENT is not
// (same schema, so only graph membership differs). Per foreignKeyMatchesSelector:
//   - OutgoingFrom  → returned (matches on child table)
//   - Within        → filtered out (parent table not in the requested set)
//   - IncomingTo    → never matched (matches on parent table) AND queries a different
//     column, so the registered expectation goes unmatched
//
// The assertion is therefore that the key survives, with a non-empty Keys slice. The
// query expectation is pinned to "f.FOR_NAME IN" (the column OutgoingFrom/Within query
// on) rather than left unconstrained, so a mutation to IncomingTo (which queries
// "f.REF_NAME IN" instead) goes unmatched and surfaces as an error, not a silently wrong
// result.
func TestPreflightRunFKOutgoingUsesOutgoingSelector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(`f\.FOR_NAME IN`).
		WillReturnRows(sqlmock.NewRows([]string{
			"ID", "FOR_NAME", "REF_NAME", "N_COLS", "TYPE",
			"FOR_COL_NAME", "REF_COL_NAME", "POS",
		}).AddRow("srcdb/fk_meta", "srcdb/order_lines", "srcdb/archive_meta", 1, 0, "meta_id", "id", 1))

	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", &graph.Node{Name: "order_lines", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.SetPK("order_lines", "id")
	g.AddEdge("orders", "order_lines")

	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	result, err := run.fkOutgoing(context.Background())
	if err != nil {
		t.Fatalf("fkOutgoing: %v", err)
	}
	if len(result.Keys) == 0 {
		t.Fatal("expected the order_lines->archive_meta key to survive OutgoingFrom, got none")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected INNODB_FOREIGN query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesFKIncoming proves the incoming FK fetch happens once per run.
// Only the INNODB_FOREIGN expectation is registered: the library tries that registry
// first and returns without touching the information_schema fallback when it succeeds,
// so a single expectation is the whole primary path. A second query of any kind would
// therefore go unmatched and surface as an error.
func TestPreflightRunMemoizesFKIncoming(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("INNODB_FOREIGN AS f").
		WillReturnRows(sqlmock.NewRows([]string{
			"ID", "FOR_NAME", "REF_NAME", "N_COLS", "TYPE",
			"FOR_COL_NAME", "REF_COL_NAME", "POS",
		}).AddRow("srcdb/fk_users_orders", "srcdb/users", "srcdb/orders", 1, 0, "order_id", "id", 1))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	first, err := run.fkIncoming(context.Background())
	if err != nil {
		t.Fatalf("first fkIncoming: %v", err)
	}
	second, err := run.fkIncoming(context.Background())
	if err != nil {
		t.Fatalf("second fkIncoming: %v", err)
	}
	if len(first.Keys) != 1 || len(second.Keys) != 1 || first.Keys[0].ConstraintName != second.Keys[0].ConstraintName {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected INNODB_FOREIGN query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesFKIncomingErrors proves a failed fetch is memoized too. Both
// sources must be made to fail — the library falls back to information_schema on any
// primary error and only reports failure when both fail, joining the causes — so two
// expectations are registered and errors.Is against wantErr holds through errors.Join.
//
// The identity assertion (errors.Is(secondErr, firstErr)) is what makes this non-vacuous:
// asserting only that both errors are non-nil passes against a build with no memoization
// at all, because a consumed sqlmock expectation still returns an error on the next call.
func TestPreflightRunMemoizesFKIncomingErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("INNODB_FOREIGN AS f").WillReturnError(wantErr)
	mock.ExpectQuery("KEY_COLUMN_USAGE AS kcu").WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	run := newPreflightRun(p)
	ctx := context.Background()

	_, firstErr := run.fkIncoming(ctx)
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}
	_, secondErr := run.fkIncoming(ctx)
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected FK queries were not observed: %v", err)
	}
}

// TestPreflightRunFKIncomingUsesIncomingSelector pins the selector as the property it
// asserts, rather than as a side effect of its fixture.
//
// The fixture is one constraint whose PARENT is in the graph and whose CHILD is not
// (same schema, so only graph membership differs). Per foreignKeyMatchesSelector:
//   - IncomingTo    → returned (matches on parent table)
//   - OutgoingFrom  → filtered out (matches on child table, which is not "orders")
//   - Within        → filtered out (requires the child table in the requested set too)
//
// The assertion is therefore that the key survives, with a non-empty Keys slice. The
// query expectation is pinned to "f.REF_NAME IN" (the column IncomingTo queries on)
// rather than left unconstrained, so a mutation to OutgoingFrom or Within (which query
// "f.FOR_NAME IN" instead) goes unmatched and surfaces as an error, not a silently wrong
// result. This is the discrimination the fkIncoming selector proof needs: an ordinary
// fixture edit that happened to remove it would be incidental, not asserted — this test
// makes it explicit and keeps it pinned.
func TestPreflightRunFKIncomingUsesIncomingSelector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(`f\.REF_NAME IN`).
		WillReturnRows(sqlmock.NewRows([]string{
			"ID", "FOR_NAME", "REF_NAME", "N_COLS", "TYPE",
			"FOR_COL_NAME", "REF_COL_NAME", "POS",
		}).AddRow("srcdb/fk_meta", "srcdb/audit_log", "srcdb/orders", 1, 0, "order_id", "id", 1))

	g := graph.NewGraph("orders", "id")
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	result, err := run.fkIncoming(context.Background())
	if err != nil {
		t.Fatalf("fkIncoming: %v", err)
	}
	if len(result.Keys) == 0 {
		t.Fatal("expected the audit_log->orders key to survive IncomingTo, got none")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected INNODB_FOREIGN query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesFKWithin proves the within-graph FK fetch happens once per
// run. Only the INNODB_FOREIGN expectation is registered: the library tries that
// registry first and returns without touching the information_schema fallback when it
// succeeds, so a single expectation is the whole primary path. A second query of any
// kind would therefore go unmatched and surface as an error.
func TestPreflightRunMemoizesFKWithin(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("INNODB_FOREIGN AS f").
		WillReturnRows(sqlmock.NewRows([]string{
			"ID", "FOR_NAME", "REF_NAME", "N_COLS", "TYPE",
			"FOR_COL_NAME", "REF_COL_NAME", "POS",
		}).AddRow("srcdb/fk_ol_o", "srcdb/order_lines", "srcdb/orders", 1, 0, "order_id", "id", 1))

	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", nil)
	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	first, err := run.fkWithin(context.Background())
	if err != nil {
		t.Fatalf("first fkWithin: %v", err)
	}
	second, err := run.fkWithin(context.Background())
	if err != nil {
		t.Fatalf("second fkWithin: %v", err)
	}
	if len(first.Keys) != 1 || len(second.Keys) != 1 || first.Keys[0].ConstraintName != second.Keys[0].ConstraintName {
		t.Fatalf("memoized value differs: %v vs %v", first, second)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected INNODB_FOREIGN query was not observed: %v", err)
	}
}

// TestPreflightRunMemoizesFKWithinErrors proves a failed fetch is memoized too. Both
// sources must be made to fail — the library falls back to information_schema on any
// primary error and only reports failure when both fail, joining the causes — so two
// expectations are registered and errors.Is against wantErr holds through errors.Join.
//
// The identity assertion (errors.Is(secondErr, firstErr)) is what makes this non-vacuous:
// asserting only that both errors are non-nil passes against a build with no memoization
// at all, because a consumed sqlmock expectation still returns an error on the next call.
func TestPreflightRunMemoizesFKWithinErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	wantErr := errors.New("boom")
	mock.ExpectQuery("INNODB_FOREIGN AS f").WillReturnError(wantErr)
	mock.ExpectQuery("KEY_COLUMN_USAGE AS kcu").WillReturnError(wantErr)

	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", nil)
	p, _ := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	run := newPreflightRun(p)
	ctx := context.Background()

	_, firstErr := run.fkWithin(ctx)
	if !errors.Is(firstErr, wantErr) {
		t.Fatalf("first call must wrap %v, got %v", wantErr, firstErr)
	}
	_, secondErr := run.fkWithin(ctx)
	if !errors.Is(secondErr, wantErr) {
		t.Fatalf("second call must return the memoized error wrapping %v, got %v",
			wantErr, secondErr)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second call returned a different error: first=%v second=%v",
			firstErr, secondErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected FK queries were not observed: %v", err)
	}
}

// TestPreflightRunFKWithinUsesWithinSelector pins the selector as the property it
// asserts. The two mutants die by DIFFERENT mechanisms, and neither alone is enough:
//
//   - Within -> OutgoingFrom: NOT detectable by query shape. Both issue the identical
//     "f.FOR_NAME IN (...)" predicate (library fact 1); they differ only in the
//     post-parse filter. The FIXTURE kills this one: two constraints share the child
//     order_lines, one with an in-graph parent (orders) and one with an out-of-graph
//     parent (audit_log). Within keeps one; OutgoingFrom keeps both, failing len == 1.
//
//   - Within -> IncomingTo: NOT detectable by the length assertion. IncomingTo matches
//     on the PARENT table, so on this same fixture it also returns exactly one key
//     (the orders-parented constraint) and would satisfy len == 1. The QUERY PIN kills
//     this one: IncomingTo predicates on "f.REF_NAME IN", which the `f\.FOR_NAME IN`
//     expectation does not match, so the call falls through to an unexpected
//     information_schema query and surfaces as an error.
//
// Remove either mechanism and one mutant survives.
func TestPreflightRunFKWithinUsesWithinSelector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(`f\.FOR_NAME IN`).
		WillReturnRows(sqlmock.NewRows([]string{
			"ID", "FOR_NAME", "REF_NAME", "N_COLS", "TYPE",
			"FOR_COL_NAME", "REF_COL_NAME", "POS",
		}).
			AddRow("srcdb/fk_ol_o", "srcdb/order_lines", "srcdb/orders", 1, 0, "order_id", "id", 1).
			AddRow("srcdb/fk_ol_a", "srcdb/order_lines", "srcdb/audit_log", 1, 0, "audit_id", "id", 1))

	// BOTH tables must be real nodes: AddEdgeWithMeta alone does not register a node,
	// so AllNodes() — and therefore r.tables — would omit order_lines entirely.
	g := graph.NewGraph("orders", "id")
	g.AddNode("order_lines", nil)

	p, err := NewPreflightChecker(db, "srcdb", g, logger.NewDefault())
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}

	run := newPreflightRun(p)
	result, err := run.fkWithin(context.Background())
	if err != nil {
		t.Fatalf("fkWithin: %v", err)
	}
	if len(result.Keys) != 1 {
		t.Fatalf("Within must keep only the in-graph-parent constraint, got %d: %v",
			len(result.Keys), result.Keys)
	}
	if result.Keys[0].ParentTable != "orders" {
		t.Fatalf("surviving key must be the orders-parented one, got %+v", result.Keys[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected INNODB_FOREIGN query was not observed: %v", err)
	}
}
