package archiver

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

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

func TestDestinationSecondaryUniqueIndexes_Found(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT DISTINCT TABLE_NAME, INDEX_NAME").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "INDEX_NAME"}).
			AddRow("orders", "uq_orders_ext_ref").
			AddRow("orders", "uq_orders_code"))

	got, err := destinationSecondaryUniqueIndexes(context.Background(), db, "destdb", []string{"orders", "customers"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 unique-index descriptors, got %v", got)
	}
	if got[0] != "orders.uq_orders_ext_ref" {
		t.Errorf("unexpected descriptor: %s", got[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestDestinationSecondaryUniqueIndexes_None(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT DISTINCT TABLE_NAME, INDEX_NAME").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "INDEX_NAME"}))

	got, err := destinationSecondaryUniqueIndexes(context.Background(), db, "destdb", []string{"orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no descriptors, got %v", got)
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
