package archiver

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestRootIDFetcher_FetchNextBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer func() { _ = db.Close() }()

	tests := []struct {
		name        string
		rootTable   string
		pkColumn    string
		criteria    string
		batchSize   int
		checkpoint  interface{}
		mockSetup   func()
		expectedIDs []interface{}
		expectErr   bool
	}{
		{
			name:       "Basic fetch with integer IDs",
			rootTable:  "users",
			pkColumn:   "user_id",
			criteria:   "",
			batchSize:  3,
			checkpoint: 0,
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"user_id"}).
					AddRow(1).
					AddRow(2).
					AddRow(3)
				mock.ExpectQuery("SELECT `user_id` FROM `users` WHERE \\(1=1\\) AND `user_id` > \\? ORDER BY `user_id` ASC LIMIT \\?").
					WithArgs(0, 3).
					WillReturnRows(rows)
			},
			expectedIDs: []interface{}{int64(1), int64(2), int64(3)},
			expectErr:   false,
		},
		{
			name:       "Fetch with criteria",
			rootTable:  "orders",
			pkColumn:   "id",
			criteria:   "status = 'closed'",
			batchSize:  2,
			checkpoint: 100,
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"id"}).
					AddRow(101).
					AddRow(102)
				mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(status = 'closed'\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
					WithArgs(100, 2).
					WillReturnRows(rows)
			},
			expectedIDs: []interface{}{int64(101), int64(102)},
			expectErr:   false,
		},
		{
			name:       "Empty result",
			rootTable:  "users",
			pkColumn:   "id",
			criteria:   "",
			batchSize:  5,
			checkpoint: 999,
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"id"})
				mock.ExpectQuery("SELECT `id` FROM `users` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
					WithArgs(999, 5).
					WillReturnRows(rows)
			},
			expectedIDs: nil,
			expectErr:   false,
		},
		{
			name:       "Database error",
			rootTable:  "users",
			pkColumn:   "id",
			criteria:   "",
			batchSize:  5,
			checkpoint: 0,
			mockSetup: func() {
				mock.ExpectQuery("SELECT `id` FROM `users`").
					WillReturnError(fmt.Errorf("connection failed"))
			},
			expectedIDs: nil,
			expectErr:   true,
		},
		{
			name:       "Custom PK column",
			rootTable:  "products",
			pkColumn:   "sku",
			criteria:   "",
			batchSize:  2,
			checkpoint: "A-000",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"sku"}).
					AddRow("A-001").
					AddRow("A-002")
				mock.ExpectQuery("SELECT `sku` FROM `products` WHERE \\(1=1\\) AND `sku` > \\? ORDER BY `sku` ASC LIMIT \\?").
					WithArgs("A-000", 2).
					WillReturnRows(rows)
			},
			expectedIDs: []interface{}{"A-001", "A-002"},
			expectErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockSetup()

			fetcher := NewRootIDFetcher(db, tt.rootTable, tt.pkColumn, tt.criteria, tt.batchSize, tt.checkpoint)
			ids, err := fetcher.FetchNextBatch(context.Background())

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err, "Test case: %s", tt.name)
				assert.Equal(t, tt.expectedIDs, ids, "Test case: %s", tt.name)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("there were unfulfilled expectations in %s: %s", tt.name, err)
			}
		})
	}
}

func TestRootIDFetcher_UpdateCheckpoint(t *testing.T) {
	fetcher := NewRootIDFetcher(nil, "t", "id", "", 10, 0)
	fetcher.UpdateCheckpoint(500)
	assert.Equal(t, 500, fetcher.checkpoint)
}

func TestRootIDFetcher_NilCheckpointStartsUnbounded(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id"}).AddRow(-1).AddRow(0).AddRow(1)
	mock.ExpectQuery("SELECT `id` FROM `users` WHERE \\(1=1\\) ORDER BY `id` ASC LIMIT \\?").
		WithArgs(3).
		WillReturnRows(rows)

	fetcher := NewRootIDFetcher(db, "users", "id", "", 3, nil)
	ids, err := fetcher.FetchNextBatch(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(-1), int64(0), int64(1)}, ids)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRootIDFetcher_EmptyStringCheckpointStartsUnbounded(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id"}).AddRow(0).AddRow(1)
	mock.ExpectQuery("SELECT `id` FROM `users` WHERE \\(1=1\\) ORDER BY `id` ASC LIMIT \\?").
		WithArgs(2).
		WillReturnRows(rows)

	fetcher := NewRootIDFetcher(db, "users", "id", "", 2, "")
	ids, err := fetcher.FetchNextBatch(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(0), int64(1)}, ids)
	assert.NoError(t, mock.ExpectationsWereMet())
}
