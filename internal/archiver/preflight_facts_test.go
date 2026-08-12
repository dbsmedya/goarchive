package archiver

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

type inspectorFactoryCall struct {
	querier validations.Querier
	schema  string
}

func newTypedFactRun(
	t *testing.T,
	source *fakePreflightInspector,
	destination *fakePreflightInspector,
) (*PreflightChecker, *preflightRun, *[]inspectorFactoryCall) {
	t.Helper()

	g := graph.NewGraph("orders", "id")
	p := &PreflightChecker{
		db:           new(sql.DB),
		sourceDBName: "srcdb",
		graph:        g,
		logger:       logger.NewDefault(),
	}
	readers := map[string]preflightInspector{"srcdb": source}
	if destination != nil {
		p.destinationDB = new(sql.DB)
		p.destinationDBName = "dstdb"
		p.jobSchemaName = "jobs"
		readers["dstdb"] = destination
	}

	calls := make([]inspectorFactoryCall, 0, len(readers))
	p.inspectorFactory = func(q validations.Querier, schema string) preflightInspector {
		calls = append(calls, inspectorFactoryCall{querier: q, schema: schema})
		reader, ok := readers[schema]
		if !ok {
			t.Fatalf("no fake inspector for schema %q", schema)
		}
		return reader
	}
	return p, newPreflightRun(p), &calls
}

func assertMemoizedError[T any](
	t *testing.T,
	read func() (T, error),
	want error,
) {
	t.Helper()
	_, firstErr := read()
	if !errors.Is(firstErr, want) {
		t.Fatalf("first error = %v, want errors.Is(_, %v)", firstErr, want)
	}
	_, secondErr := read()
	if !errors.Is(secondErr, want) {
		t.Fatalf("second error = %v, want errors.Is(_, %v)", secondErr, want)
	}
	if !errors.Is(secondErr, firstErr) {
		t.Fatalf("second error was reacquired: first=%v second=%v", firstErr, secondErr)
	}
}

func TestPreflightRunMemoizesTableFactsAndRoutesSides(t *testing.T) {
	ctx := context.Background()

	t.Run("source success", func(t *testing.T) {
		want := []validations.TableInfo{{Table: "orders", Type: "BASE TABLE", Engine: "InnoDB"}}
		source := &fakePreflightInspector{tablesResult: want}
		_, run, _ := newTypedFactRun(t, source, nil)

		first, err := run.sourceTables(ctx)
		if err != nil {
			t.Fatalf("first sourceTables: %v", err)
		}
		second, err := run.sourceTables(ctx)
		if err != nil {
			t.Fatalf("second sourceTables: %v", err)
		}
		if !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) {
			t.Fatalf("source tables = %v and %v, want %v", first, second, want)
		}
		if source.tablesCalls != 1 || !reflect.DeepEqual(source.tablesArgs, [][]string{{"orders"}}) {
			t.Fatalf("Tables calls/args = %d/%v, want 1/[[orders]]", source.tablesCalls, source.tablesArgs)
		}
	})

	t.Run("source error", func(t *testing.T) {
		wantErr := errors.New("source tables failed")
		source := &fakePreflightInspector{tablesErr: wantErr}
		_, run, _ := newTypedFactRun(t, source, nil)
		assertMemoizedError(t, func() ([]validations.TableInfo, error) { return run.sourceTables(ctx) }, wantErr)
		if source.tablesCalls != 1 {
			t.Fatalf("Tables calls = %d, want 1", source.tablesCalls)
		}
	})

	t.Run("destination success", func(t *testing.T) {
		sourceWant := []validations.TableInfo{{Table: "source-only"}}
		destinationWant := []validations.TableInfo{{Table: "orders", Engine: "InnoDB"}}
		source := &fakePreflightInspector{tablesResult: sourceWant}
		destination := &fakePreflightInspector{tablesResult: destinationWant}
		_, run, _ := newTypedFactRun(t, source, destination)

		first, err := run.destTables(ctx)
		if err != nil {
			t.Fatalf("first destTables: %v", err)
		}
		second, err := run.destTables(ctx)
		if err != nil {
			t.Fatalf("second destTables: %v", err)
		}
		if !reflect.DeepEqual(first, destinationWant) || !reflect.DeepEqual(second, destinationWant) {
			t.Fatalf("destination tables = %v and %v, want %v", first, second, destinationWant)
		}
		if destination.tablesCalls != 1 || source.tablesCalls != 0 {
			t.Fatalf("source/destination Tables calls = %d/%d, want 0/1", source.tablesCalls, destination.tablesCalls)
		}
	})

	t.Run("destination error", func(t *testing.T) {
		wantErr := errors.New("destination tables failed")
		source := &fakePreflightInspector{}
		destination := &fakePreflightInspector{tablesErr: wantErr}
		_, run, _ := newTypedFactRun(t, source, destination)
		assertMemoizedError(t, func() ([]validations.TableInfo, error) { return run.destTables(ctx) }, wantErr)
		if destination.tablesCalls != 1 || source.tablesCalls != 0 {
			t.Fatalf("source/destination Tables calls = %d/%d, want 0/1", source.tablesCalls, destination.tablesCalls)
		}
	})
}

func TestPreflightRunDestinationFactsRequireConfiguration(t *testing.T) {
	_, run, _ := newTypedFactRun(t, &fakePreflightInspector{}, nil)

	if _, err := run.destTables(context.Background()); err == nil {
		t.Fatal("destTables must reject an unconfigured destination")
	}
	if _, err := run.destInsertTriggers(context.Background()); err == nil {
		t.Fatal("destInsertTriggers must reject an unconfigured destination")
	}
	if _, err := run.tableSpecs(context.Background()); err == nil {
		t.Fatal("tableSpecs must reject an unconfigured destination")
	}
	if _, err := run.destGrants(context.Background()); err == nil {
		t.Fatal("destGrants must reject an unconfigured destination")
	}
}

func TestPreflightRunMemoizesColumnFacts(t *testing.T) {
	ctx := context.Background()
	want := []validations.TableColumns{{
		Table: "orders",
		Columns: []validations.ColumnInfo{
			{Name: "id", Ordinal: 1, DataType: "bigint"},
			{Name: "secret", Ordinal: 2, Invisible: true},
		},
	}}
	source := &fakePreflightInspector{columnsResult: want}
	_, run, _ := newTypedFactRun(t, source, nil)

	first, err := run.sourceColumns(ctx)
	if err != nil {
		t.Fatalf("first sourceColumns: %v", err)
	}
	second, err := run.sourceColumns(ctx)
	if err != nil {
		t.Fatalf("second sourceColumns: %v", err)
	}
	if !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) {
		t.Fatalf("columns = %v and %v, want %v", first, second, want)
	}
	if source.columnsCalls != 1 || !reflect.DeepEqual(source.columnsArgs, [][]string{{"orders"}}) {
		t.Fatalf("Columns calls/args = %d/%v, want 1/[[orders]]", source.columnsCalls, source.columnsArgs)
	}
}

func TestPreflightRunMemoizesColumnErrors(t *testing.T) {
	wantErr := errors.New("columns failed")
	source := &fakePreflightInspector{columnsErr: wantErr}
	_, run, _ := newTypedFactRun(t, source, nil)
	ctx := context.Background()

	assertMemoizedError(t, func() ([]validations.TableColumns, error) { return run.sourceColumns(ctx) }, wantErr)
	if source.columnsCalls != 1 {
		t.Fatalf("Columns calls = %d, want 1", source.columnsCalls)
	}
}

func TestPreflightRunMemoizesTriggerFactsAndRoutesEvents(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name  string
		event validations.TriggerEvent
		want  []validations.TriggerInfo
		read  func(*preflightRun) ([]validations.TriggerInfo, error)
		fake  func(*fakePreflightInspector, *fakePreflightInspector) *fakePreflightInspector
	}{
		{
			name: "source delete", event: validations.TriggerDelete,
			want: []validations.TriggerInfo{{Table: "orders", Name: "before_delete", Event: "DELETE"}},
			read: func(run *preflightRun) ([]validations.TriggerInfo, error) { return run.sourceDeleteTriggers(ctx) },
			fake: func(source, _ *fakePreflightInspector) *fakePreflightInspector { return source },
		},
		{
			name: "destination insert", event: validations.TriggerInsert,
			want: []validations.TriggerInfo{{Table: "orders", Name: "before_insert", Event: "INSERT"}},
			read: func(run *preflightRun) ([]validations.TriggerInfo, error) { return run.destInsertTriggers(ctx) },
			fake: func(_, destination *fakePreflightInspector) *fakePreflightInspector { return destination },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" success", func(t *testing.T) {
			source := &fakePreflightInspector{triggersResult: []validations.TriggerInfo{{Name: "wrong-side"}}}
			destination := &fakePreflightInspector{triggersResult: []validations.TriggerInfo{{Name: "wrong-side"}}}
			target := tt.fake(source, destination)
			target.triggersResult = tt.want
			_, run, _ := newTypedFactRun(t, source, destination)

			first, err := tt.read(run)
			if err != nil {
				t.Fatalf("first trigger read: %v", err)
			}
			second, err := tt.read(run)
			if err != nil {
				t.Fatalf("second trigger read: %v", err)
			}
			if !reflect.DeepEqual(first, tt.want) || !reflect.DeepEqual(second, tt.want) {
				t.Fatalf("triggers = %v and %v, want %v", first, second, tt.want)
			}
			if target.triggersCalls != 1 ||
				!reflect.DeepEqual(target.triggersArgs, [][]string{{"orders"}}) ||
				!reflect.DeepEqual(target.triggerEvents, []validations.TriggerEvent{tt.event}) {
				t.Fatalf("Triggers calls/args/events = %d/%v/%v, want 1/[[orders]]/[%v]",
					target.triggersCalls, target.triggersArgs, target.triggerEvents, tt.event)
			}
		})

		t.Run(tt.name+" error", func(t *testing.T) {
			wantErr := errors.New(tt.name + " failed")
			source := &fakePreflightInspector{}
			destination := &fakePreflightInspector{}
			target := tt.fake(source, destination)
			target.triggersErr = wantErr
			_, run, _ := newTypedFactRun(t, source, destination)
			assertMemoizedError(t, func() ([]validations.TriggerInfo, error) { return tt.read(run) }, wantErr)
			if target.triggersCalls != 1 ||
				!reflect.DeepEqual(target.triggersArgs, [][]string{{"orders"}}) ||
				!reflect.DeepEqual(target.triggerEvents, []validations.TriggerEvent{tt.event}) {
				t.Fatalf("Triggers calls/args/events = %d/%v/%v, want 1/[[orders]]/[%v]",
					target.triggersCalls, target.triggersArgs, target.triggerEvents, tt.event)
			}
		})
	}
}

func TestPreflightRunMemoizesPrimaryKeys(t *testing.T) {
	ctx := context.Background()
	want := []validations.PKInfo{{
		Table: "orders", Kind: validations.PKSingle, Columns: []string{"id"},
		DataType: "bigint", IsInteger: true, Unsigned: true,
	}}
	source := &fakePreflightInspector{primaryKeysResult: want}
	_, run, _ := newTypedFactRun(t, source, nil)

	first, err := run.primaryKeys(ctx)
	if err != nil {
		t.Fatalf("first primaryKeys: %v", err)
	}
	second, err := run.primaryKeys(ctx)
	if err != nil {
		t.Fatalf("second primaryKeys: %v", err)
	}
	if !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) {
		t.Fatalf("primary keys = %v and %v, want %v", first, second, want)
	}
	if source.primaryKeysCalls != 1 || !reflect.DeepEqual(source.primaryKeysArgs, [][]string{{"orders"}}) {
		t.Fatalf("PrimaryKeys calls/args = %d/%v, want 1/[[orders]]", source.primaryKeysCalls, source.primaryKeysArgs)
	}
}

func TestPreflightRunMemoizesPrimaryKeyErrors(t *testing.T) {
	wantErr := errors.New("primary keys failed")
	source := &fakePreflightInspector{primaryKeysErr: wantErr}
	_, run, _ := newTypedFactRun(t, source, nil)
	assertMemoizedError(t, func() ([]validations.PKInfo, error) {
		return run.primaryKeys(context.Background())
	}, wantErr)
	if source.primaryKeysCalls != 1 {
		t.Fatalf("PrimaryKeys calls = %d, want 1", source.primaryKeysCalls)
	}
}

func TestPreflightRunRootPKInfoSelectsByRootName(t *testing.T) {
	source := &fakePreflightInspector{primaryKeysResult: []validations.PKInfo{
		{Table: "order_lines", Kind: validations.PKSingle, Columns: []string{"line_id"}},
		{Table: "orders", Kind: validations.PKSingle, Columns: []string{"id"}},
	}}
	p, run, _ := newTypedFactRun(t, source, nil)
	p.graph.AddNode("order_lines", nil)

	got, found, err := run.rootPKInfo(context.Background())
	if err != nil {
		t.Fatalf("rootPKInfo: %v", err)
	}
	if !found || got.Table != "orders" || !reflect.DeepEqual(got.Columns, []string{"id"}) {
		t.Fatalf("rootPKInfo = %+v, %v; want orders.id", got, found)
	}
}

func TestPreflightRunMemoizesForeignKeysAndSelectors(t *testing.T) {
	ctx := context.Background()
	wantResult := validations.ForeignKeyResult{Keys: []validations.ForeignKey{{
		ConstraintName: "fk_orders_customer", ChildTable: "orders", ParentTable: "customers",
	}}}

	tests := []struct {
		name     string
		selector validations.FKSelector
		read     func(*preflightRun) (validations.ForeignKeyResult, error)
	}{
		{name: "outgoing", selector: validations.OutgoingFrom("orders"), read: func(run *preflightRun) (validations.ForeignKeyResult, error) { return run.fkOutgoing(ctx) }},
		{name: "incoming", selector: validations.IncomingTo("orders"), read: func(run *preflightRun) (validations.ForeignKeyResult, error) { return run.fkIncoming(ctx) }},
		{name: "within", selector: validations.Within("orders"), read: func(run *preflightRun) (validations.ForeignKeyResult, error) { return run.fkWithin(ctx) }},
	}

	for _, tt := range tests {
		t.Run(tt.name+" success", func(t *testing.T) {
			source := &fakePreflightInspector{foreignKeysResult: wantResult}
			_, run, _ := newTypedFactRun(t, source, nil)
			first, err := tt.read(run)
			if err != nil {
				t.Fatalf("first FK read: %v", err)
			}
			second, err := tt.read(run)
			if err != nil {
				t.Fatalf("second FK read: %v", err)
			}
			if !reflect.DeepEqual(first, wantResult) || !reflect.DeepEqual(second, wantResult) {
				t.Fatalf("foreign keys = %v and %v, want %v", first, second, wantResult)
			}
			if source.foreignKeysCalls != 1 || len(source.foreignKeySelectors) != 1 ||
				!reflect.DeepEqual(source.foreignKeySelectors[0], tt.selector) {
				t.Fatalf("ForeignKeys calls/selectors = %d/%v, want 1/%v", source.foreignKeysCalls, source.foreignKeySelectors, tt.selector)
			}
		})

		t.Run(tt.name+" error", func(t *testing.T) {
			wantErr := errors.New(tt.name + " foreign keys failed")
			source := &fakePreflightInspector{foreignKeysErr: wantErr}
			_, run, _ := newTypedFactRun(t, source, nil)
			assertMemoizedError(t, func() (validations.ForeignKeyResult, error) { return tt.read(run) }, wantErr)
			if source.foreignKeysCalls != 1 || len(source.foreignKeySelectors) != 1 ||
				!reflect.DeepEqual(source.foreignKeySelectors[0], tt.selector) {
				t.Fatalf("ForeignKeys calls/selectors = %d/%v, want 1/%v", source.foreignKeysCalls, source.foreignKeySelectors, tt.selector)
			}
		})
	}
}

func TestPreflightRunMemoizesTableSpecsAndRoutesSides(t *testing.T) {
	ctx := context.Background()
	sourceSpec := validations.TableSpec{Schema: "srcdb", Table: "orders", Captured: validations.SectionIndexes}
	destinationSpec := validations.TableSpec{Schema: "dstdb", Table: "orders", Captured: validations.SectionIndexes}
	source := &fakePreflightInspector{tableSpecResult: sourceSpec}
	destination := &fakePreflightInspector{tableSpecResult: destinationSpec}
	_, run, _ := newTypedFactRun(t, source, destination)

	first, err := run.tableSpecs(ctx)
	if err != nil {
		t.Fatalf("first tableSpecs: %v", err)
	}
	second, err := run.tableSpecs(ctx)
	if err != nil {
		t.Fatalf("second tableSpecs: %v", err)
	}
	want := []specPair{{Table: "orders", A: sourceSpec, B: destinationSpec}}
	if !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) {
		t.Fatalf("table specs = %v and %v, want %v", first, second, want)
	}
	if source.tableSpecCalls != 1 || destination.tableSpecCalls != 1 {
		t.Fatalf("source/destination TableSpec calls = %d/%d, want 1/1", source.tableSpecCalls, destination.tableSpecCalls)
	}
	if !reflect.DeepEqual(source.tableSpecRefs, []validations.TableRef{validations.Ref("srcdb", "orders")}) ||
		!reflect.DeepEqual(destination.tableSpecRefs, []validations.TableRef{validations.Ref("dstdb", "orders")}) {
		t.Fatalf("source/destination refs = %v/%v", source.tableSpecRefs, destination.tableSpecRefs)
	}
	if !reflect.DeepEqual(source.tableSpecOptionCounts, []int{1}) ||
		!reflect.DeepEqual(destination.tableSpecOptionCounts, []int{1}) {
		t.Fatalf("source/destination option counts = %v/%v, want [1]/[1]", source.tableSpecOptionCounts, destination.tableSpecOptionCounts)
	}
}

func TestPreflightRunMemoizesTableSpecErrors(t *testing.T) {
	tests := []struct {
		name            string
		sourceErr       error
		destinationErr  error
		wantSourceCalls int
		wantDestCalls   int
	}{
		{
			name: "source", sourceErr: errors.New("source table spec failed"),
			wantSourceCalls: 1, wantDestCalls: 0,
		},
		{
			name: "destination", destinationErr: errors.New("destination table spec failed"),
			wantSourceCalls: 1, wantDestCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &fakePreflightInspector{
				tableSpecResult: validations.TableSpec{Schema: "srcdb", Table: "orders"},
				tableSpecErr:    tt.sourceErr,
			}
			destination := &fakePreflightInspector{tableSpecErr: tt.destinationErr}
			_, run, _ := newTypedFactRun(t, source, destination)
			wantErr := tt.sourceErr
			if wantErr == nil {
				wantErr = tt.destinationErr
			}
			assertMemoizedError(t, func() ([]specPair, error) {
				return run.tableSpecs(context.Background())
			}, wantErr)
			if source.tableSpecCalls != tt.wantSourceCalls || destination.tableSpecCalls != tt.wantDestCalls {
				t.Fatalf("source/destination TableSpec calls = %d/%d, want %d/%d",
					source.tableSpecCalls, destination.tableSpecCalls, tt.wantSourceCalls, tt.wantDestCalls)
			}
		})
	}
}

func newGrantPool(t *testing.T) *sql.DB {
	t.Helper()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestPreflightRunMemoizesGrantsAndReleasesDedicatedConnection(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		schema string
		read   func(*preflightRun) (validations.Grants, error)
		setup  func(*PreflightChecker, *sql.DB)
	}{
		{
			name: "source", schema: "srcdb",
			read:  func(run *preflightRun) (validations.Grants, error) { return run.sourceGrants(ctx) },
			setup: func(p *PreflightChecker, db *sql.DB) { p.db = db },
		},
		{
			name: "destination", schema: "dstdb",
			read:  func(run *preflightRun) (validations.Grants, error) { return run.destGrants(ctx) },
			setup: func(p *PreflightChecker, db *sql.DB) { p.destinationDB = db },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" success", func(t *testing.T) {
			pool := newGrantPool(t)
			source := &fakePreflightInspector{}
			destination := &fakePreflightInspector{}
			p, _, calls := newTypedFactRun(t, source, destination)
			tt.setup(p, pool)
			run := newPreflightRun(p)
			target := source
			if tt.schema == "dstdb" {
				target = destination
			}

			first, err := tt.read(run)
			if err != nil {
				t.Fatalf("first grants read: %v", err)
			}
			second, err := tt.read(run)
			if err != nil {
				t.Fatalf("second grants read: %v", err)
			}
			if !reflect.DeepEqual(first, target.grantsResult) || !reflect.DeepEqual(second, target.grantsResult) {
				t.Fatalf("grants values differ across memoized reads")
			}
			if target.grantsCalls != 1 {
				t.Fatalf("Grants calls = %d, want 1", target.grantsCalls)
			}
			if pool.Stats().InUse != 0 {
				t.Fatalf("dedicated grants connection still in use: %+v", pool.Stats())
			}
			var boundToConn bool
			for _, call := range *calls {
				if call.schema == tt.schema {
					if _, ok := call.querier.(*sql.Conn); ok {
						boundToConn = true
					}
				}
			}
			if !boundToConn {
				t.Fatalf("%s Grants reader was not constructed with *sql.Conn: %v", tt.name, *calls)
			}
		})

		t.Run(tt.name+" error", func(t *testing.T) {
			pool := newGrantPool(t)
			wantErr := errors.New(tt.name + " grants failed")
			source := &fakePreflightInspector{}
			destination := &fakePreflightInspector{}
			target := source
			if tt.schema == "dstdb" {
				target = destination
			}
			target.grantsErr = wantErr
			p, _, _ := newTypedFactRun(t, source, destination)
			tt.setup(p, pool)
			run := newPreflightRun(p)
			assertMemoizedError(t, func() (validations.Grants, error) { return tt.read(run) }, wantErr)
			if target.grantsCalls != 1 {
				t.Fatalf("Grants calls = %d, want 1", target.grantsCalls)
			}
			if pool.Stats().InUse != 0 {
				t.Fatalf("dedicated grants connection still in use after error: %+v", pool.Stats())
			}
		})
	}
}

func TestPreflightRunCachesAreIsolatedBetweenRuns(t *testing.T) {
	source := &fakePreflightInspector{tablesResult: []validations.TableInfo{{Table: "orders"}}}
	p, firstRun, _ := newTypedFactRun(t, source, nil)
	ctx := context.Background()

	if _, err := firstRun.sourceTables(ctx); err != nil {
		t.Fatalf("first run first read: %v", err)
	}
	if _, err := firstRun.sourceTables(ctx); err != nil {
		t.Fatalf("first run second read: %v", err)
	}
	secondRun := newPreflightRun(p)
	if _, err := secondRun.sourceTables(ctx); err != nil {
		t.Fatalf("second run read: %v", err)
	}
	if source.tablesCalls != 2 {
		t.Fatalf("Tables calls = %d, want one per run (2 total)", source.tablesCalls)
	}
	if firstRun == secondRun {
		t.Fatal("newPreflightRun returned a shared run")
	}
}

func TestPreflightRunExpectedPKsOmitsUnconfiguredTables(t *testing.T) {
	source := &fakePreflightInspector{}
	p, run, _ := newTypedFactRun(t, source, nil)
	p.graph.AddNode("notes", nil)
	run.tables = p.graph.AllNodes()

	want := map[string]string{"orders": "id"}
	if got := run.expectedPKs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("expectedPKs = %v, want %v", got, want)
	}
}
