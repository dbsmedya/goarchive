package archiver

import (
	"context"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// fakePreflightInspector returns dbsgomysql's public semantic values. It does
// not implement Querier and cannot observe or reproduce library-private SQL.
type fakePreflightInspector struct {
	tablesResult []validations.TableInfo
	tablesErr    error
	tablesCalls  int
	tablesArgs   [][]string

	columnsResult []validations.TableColumns
	columnsErr    error
	columnsCalls  int
	columnsArgs   [][]string

	primaryKeysResult []validations.PKInfo
	primaryKeysErr    error
	primaryKeysCalls  int
	primaryKeysArgs   [][]string

	triggersResult []validations.TriggerInfo
	triggersErr    error
	triggersCalls  int
	triggersArgs   [][]string
	triggerEvents  []validations.TriggerEvent

	foreignKeysResult   validations.ForeignKeyResult
	foreignKeysErr      error
	foreignKeysCalls    int
	foreignKeySelectors []validations.FKSelector

	tableSpecResult       validations.TableSpec
	tableSpecErr          error
	tableSpecCalls        int
	tableSpecRefs         []validations.TableRef
	tableSpecOptionCounts []int

	grantsResult validations.Grants
	grantsErr    error
	grantsCalls  int
}

func (f *fakePreflightInspector) Tables(_ context.Context, tables []string) ([]validations.TableInfo, error) {
	f.tablesCalls++
	f.tablesArgs = append(f.tablesArgs, append([]string(nil), tables...))
	return f.tablesResult, f.tablesErr
}

func (f *fakePreflightInspector) Columns(_ context.Context, tables []string) ([]validations.TableColumns, error) {
	f.columnsCalls++
	f.columnsArgs = append(f.columnsArgs, append([]string(nil), tables...))
	return f.columnsResult, f.columnsErr
}

func (f *fakePreflightInspector) PrimaryKeys(_ context.Context, tables []string) ([]validations.PKInfo, error) {
	f.primaryKeysCalls++
	f.primaryKeysArgs = append(f.primaryKeysArgs, append([]string(nil), tables...))
	return f.primaryKeysResult, f.primaryKeysErr
}

func (f *fakePreflightInspector) Triggers(
	_ context.Context,
	tables []string,
	event validations.TriggerEvent,
) ([]validations.TriggerInfo, error) {
	f.triggersCalls++
	f.triggersArgs = append(f.triggersArgs, append([]string(nil), tables...))
	f.triggerEvents = append(f.triggerEvents, event)
	return f.triggersResult, f.triggersErr
}

func (f *fakePreflightInspector) ForeignKeys(
	_ context.Context,
	selector validations.FKSelector,
) (validations.ForeignKeyResult, error) {
	f.foreignKeysCalls++
	f.foreignKeySelectors = append(f.foreignKeySelectors, selector)
	return f.foreignKeysResult, f.foreignKeysErr
}

func (f *fakePreflightInspector) TableSpec(
	_ context.Context,
	ref validations.TableRef,
	opts ...validations.SpecOption,
) (validations.TableSpec, error) {
	f.tableSpecCalls++
	f.tableSpecRefs = append(f.tableSpecRefs, ref)
	f.tableSpecOptionCounts = append(f.tableSpecOptionCounts, len(opts))
	return f.tableSpecResult, f.tableSpecErr
}

func (f *fakePreflightInspector) Grants(_ context.Context) (validations.Grants, error) {
	f.grantsCalls++
	return f.grantsResult, f.grantsErr
}
