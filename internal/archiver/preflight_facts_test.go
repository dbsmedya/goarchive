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
