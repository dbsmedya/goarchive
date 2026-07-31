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

// Index discovery (a table with a secondary unique index is reported; a table with only
// a PRIMARY KEY is not), the missing-table and view skips, and cross-table ordering all
// moved to real-DB coverage in pipeline_resume_integration_test.go: TableSpec issues
// several queries per table, and a goarchive unit test must not encode that private SQL
// (consumer-policy rule). The no-argument guards below are goarchive's own contract and
// need no server.

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
