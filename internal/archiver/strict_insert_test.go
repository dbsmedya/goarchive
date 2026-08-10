package archiver

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

type tableSpecOutcome struct {
	spec validations.TableSpec
	err  error
}

type fakeDestinationTableSpecReader struct {
	outcomes     []tableSpecOutcome
	calls        int
	optionCounts []int
}

func (f *fakeDestinationTableSpecReader) TableSpec(
	_ context.Context,
	_ validations.TableRef,
	opts ...validations.SpecOption,
) (validations.TableSpec, error) {
	call := f.calls
	f.calls++
	f.optionCounts = append(f.optionCounts, len(opts))
	if call >= len(f.outcomes) {
		return validations.TableSpec{}, errors.New("unexpected TableSpec call")
	}
	return f.outcomes[call].spec, f.outcomes[call].err
}

func TestShouldUseStrictInsert(t *testing.T) {
	tests := []struct {
		name         string
		method       string
		skipVerify   bool
		hasUniqueIdx bool
		want         bool
	}{
		// count always forces strict insert (long-standing behavior).
		{"count method", "count", false, false, true},
		{"count + skip", "count", true, false, true},
		// sha256 with a real verification is the only safe INSERT IGNORE case.
		{"sha256 verifying", "sha256", false, false, false},
		// review P0-1: skip-verify removes the post-copy net → must be strict
		// even (especially) for non-count methods.
		{"sha256 + skip-verify", "sha256", true, false, true},
		{"empty method + skip-verify", "", true, false, true},
		// review P1-2: a destination secondary unique index → must be strict.
		{"sha256 + dest unique index", "sha256", false, true, true},
		{"sha256 verifying, no unique idx", "sha256", false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldUseStrictInsert(tt.method, tt.skipVerify, tt.hasUniqueIdx); got != tt.want {
				t.Fatalf("shouldUseStrictInsert(%q, %v, %v) = %v, want %v",
					tt.method, tt.skipVerify, tt.hasUniqueIdx, got, tt.want)
			}
		})
	}
}

func TestSecondaryUniqueIndexDescriptors_CompositeUniqueOnly(t *testing.T) {
	spec := validations.TableSpec{Indexes: []validations.IndexSpec{
		{Name: "PRIMARY", Unique: true, Parts: []validations.IndexPart{{Column: "id"}}},
		{Name: "idx_status", Parts: []validations.IndexPart{{Column: "status"}}},
		{Name: "uq_tenant_ref", Unique: true, Parts: []validations.IndexPart{{Column: "tenant_id"}, {Column: "ref_no"}}},
	}}

	want := []string{"orders.uq_tenant_ref"}
	if got := secondaryUniqueIndexDescriptors("orders", spec); !reflect.DeepEqual(got, want) {
		t.Fatalf("secondaryUniqueIndexDescriptors = %v, want %v", got, want)
	}
}

func TestDestinationSecondaryUniqueIndexesFrom_TypedFactsStableOrder(t *testing.T) {
	reader := &fakeDestinationTableSpecReader{outcomes: []tableSpecOutcome{
		{spec: validations.TableSpec{Table: "zeta", Indexes: []validations.IndexSpec{
			{Name: "uq_z", Unique: true},
			{Name: "PRIMARY", Unique: true},
		}}},
		{spec: validations.TableSpec{Table: "alpha", Indexes: []validations.IndexSpec{
			{Name: "uq_b", Unique: true},
			{Name: "uq_a", Unique: true},
		}}},
	}}

	got, err := destinationSecondaryUniqueIndexesFrom(
		context.Background(), reader, "destdb", []string{"zeta", "alpha"},
	)
	if err != nil {
		t.Fatalf("destinationSecondaryUniqueIndexesFrom: %v", err)
	}
	want := []string{"alpha.uq_a", "alpha.uq_b", "zeta.uq_z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("indexes = %v, want %v", got, want)
	}
	if reader.calls != 2 || !reflect.DeepEqual(reader.optionCounts, []int{1, 1}) {
		t.Fatalf("TableSpec calls/options = %d/%v, want 2/[1 1]", reader.calls, reader.optionCounts)
	}
}

func TestDestinationSecondaryUniqueIndexesFrom_PrimaryOnly(t *testing.T) {
	reader := &fakeDestinationTableSpecReader{outcomes: []tableSpecOutcome{{
		spec: validations.TableSpec{Table: "orders", Indexes: []validations.IndexSpec{{Name: "PRIMARY", Unique: true}}},
	}}}

	got, err := destinationSecondaryUniqueIndexesFrom(
		context.Background(), reader, "destdb", []string{"orders"},
	)
	if err != nil || got != nil {
		t.Fatalf("primary-only indexes = %v, err = %v; want nil, nil", got, err)
	}
}

func TestDestinationSecondaryUniqueIndexesFrom_SkipsMissingTableAndView(t *testing.T) {
	reader := &fakeDestinationTableSpecReader{outcomes: []tableSpecOutcome{
		{err: validations.ErrTableNotFound},
		{err: validations.ErrUnsupportedTableType},
		{spec: validations.TableSpec{Table: "orders", Indexes: []validations.IndexSpec{{Name: "PRIMARY", Unique: true}}}},
	}}

	got, err := destinationSecondaryUniqueIndexesFrom(
		context.Background(), reader, "destdb", []string{"missing", "orders_view", "orders"},
	)
	if err != nil || got != nil {
		t.Fatalf("skipped objects = %v, err = %v; want nil, nil", got, err)
	}
	if reader.calls != 3 {
		t.Fatalf("TableSpec calls = %d, want 3", reader.calls)
	}
}

func TestDestinationSecondaryUniqueIndexesFrom_PropagatesAndShortCircuits(t *testing.T) {
	wantErr := errors.New("metadata unavailable")
	reader := &fakeDestinationTableSpecReader{outcomes: []tableSpecOutcome{
		{spec: validations.TableSpec{Indexes: []validations.IndexSpec{{Name: "uq_ref", Unique: true}}}},
		{err: wantErr},
		{spec: validations.TableSpec{Indexes: []validations.IndexSpec{{Name: "must_not_run", Unique: true}}}},
	}}

	got, err := destinationSecondaryUniqueIndexesFrom(
		context.Background(), reader, "destdb", []string{"orders", "items", "later"},
	)
	if got != nil || !errors.Is(err, wantErr) {
		t.Fatalf("indexes = %v, err = %v; want nil wrapping %v", got, err, wantErr)
	}
	if reader.calls != 2 {
		t.Fatalf("TableSpec calls = %d, want short-circuit at 2", reader.calls)
	}
}

func TestDestinationSecondaryUniqueIndexes_NoArgsNoQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	// nil db, empty schema, or empty table set must short-circuit without a query.
	if got, err := destinationSecondaryUniqueIndexes(context.Background(), nil, "destdb", []string{"orders"}); err != nil || got != nil {
		t.Fatalf("nil db: got=%v err=%v", got, err)
	}
	if got, err := destinationSecondaryUniqueIndexes(context.Background(), db, "", []string{"orders"}); err != nil || got != nil {
		t.Fatalf("empty schema: got=%v err=%v", got, err)
	}
	if got, err := destinationSecondaryUniqueIndexes(context.Background(), db, "destdb", nil); err != nil || got != nil {
		t.Fatalf("empty tables: got=%v err=%v", got, err)
	}
	// No queries should have been issued.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected query issued: %v", err)
	}
}
