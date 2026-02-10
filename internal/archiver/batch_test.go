package archiver

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestRootIDFetcher_GetCheckpoint(t *testing.T) {
	fetcher := NewRootIDFetcher(nil, "t", "id", "", 10, 100)
	assert.Equal(t, 100, fetcher.GetCheckpoint())

	fetcher.UpdateCheckpoint(200)
	assert.Equal(t, 200, fetcher.GetCheckpoint())
}

func TestRootIDFetcher_NilCheckpointDefaultsToZero(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2)
	mock.ExpectQuery("SELECT `id` FROM `users` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(0, 2).
		WillReturnRows(rows)

	fetcher := NewRootIDFetcher(db, "users", "id", "", 2, nil)
	ids, err := fetcher.FetchNextBatch(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1), int64(2)}, ids)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestBatchProcessor_ProcessBatch_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id"}).
		AddRow(1).
		AddRow(2).
		AddRow(3)
	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(0, 10).
		WillReturnRows(rows)

	fetcher := NewRootIDFetcher(db, "orders", "id", "", 10, 0)
	log := logger.NewDefault()
	processor := NewBatchProcessor(fetcher, config.ProcessingConfig{BatchSize: 10}, log, "test-job")

	handlerCalled := false
	handler := func(ctx context.Context, ids []interface{}) error {
		handlerCalled = true
		assert.Equal(t, []interface{}{int64(1), int64(2), int64(3)}, ids)
		return nil
	}

	hasMore, err := processor.ProcessBatch(context.Background(), handler)

	assert.NoError(t, err)
	assert.True(t, hasMore)
	assert.True(t, handlerCalled)
	assert.Equal(t, 1, processor.batchCount)
	assert.Equal(t, 3, processor.totalProcessed)
	assert.Equal(t, int64(3), fetcher.GetCheckpoint())
}

func TestBatchProcessor_ProcessBatch_EmptyBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id"})
	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(999, 10).
		WillReturnRows(rows)

	fetcher := NewRootIDFetcher(db, "orders", "id", "", 10, 999)
	log := logger.NewDefault()
	processor := NewBatchProcessor(fetcher, config.ProcessingConfig{BatchSize: 10}, log, "test-job")

	handlerCalled := false
	handler := func(ctx context.Context, ids []interface{}) error {
		handlerCalled = true
		return nil
	}

	hasMore, err := processor.ProcessBatch(context.Background(), handler)

	assert.NoError(t, err)
	assert.False(t, hasMore)
	assert.False(t, handlerCalled)
	assert.Equal(t, 0, processor.batchCount)
}

func TestBatchProcessor_ProcessBatch_HandlerError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(0, 10).
		WillReturnRows(rows)

	fetcher := NewRootIDFetcher(db, "orders", "id", "", 10, 0)
	log := logger.NewDefault()
	processor := NewBatchProcessor(fetcher, config.ProcessingConfig{BatchSize: 10}, log, "test-job")

	handler := func(ctx context.Context, ids []interface{}) error {
		return fmt.Errorf("processing failed")
	}

	hasMore, err := processor.ProcessBatch(context.Background(), handler)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "processing failed")
	assert.False(t, hasMore)
}

func TestBatchProcessor_ProcessBatch_FetchError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(0, 10).
		WillReturnError(fmt.Errorf("connection lost"))

	fetcher := NewRootIDFetcher(db, "orders", "id", "", 10, 0)
	log := logger.NewDefault()
	processor := NewBatchProcessor(fetcher, config.ProcessingConfig{BatchSize: 10}, log, "test-job")

	handler := func(ctx context.Context, ids []interface{}) error {
		return nil
	}

	hasMore, err := processor.ProcessBatch(context.Background(), handler)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection lost")
	assert.False(t, hasMore)
}

func TestBatchProcessor_ProcessBatch_ContextCancelled(t *testing.T) {
	fetcher := NewRootIDFetcher(nil, "orders", "id", "", 10, 0)
	log := logger.NewDefault()
	processor := NewBatchProcessor(fetcher, config.ProcessingConfig{BatchSize: 10}, log, "test-job")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	handler := func(ctx context.Context, ids []interface{}) error {
		return nil
	}

	hasMore, err := processor.ProcessBatch(ctx, handler)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context cancelled")
	assert.False(t, hasMore)
}

func TestBatchProcessor_Run_CompletesAllBatches(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows1 := sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2)
	rows2 := sqlmock.NewRows([]string{"id"}).AddRow(3)
	rows3 := sqlmock.NewRows([]string{"id"})

	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(0, 2).
		WillReturnRows(rows1)
	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(2, 2).
		WillReturnRows(rows2)
	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(3, 2).
		WillReturnRows(rows3)

	fetcher := NewRootIDFetcher(db, "orders", "id", "", 2, 0)
	log := logger.NewDefault()
	processor := NewBatchProcessor(fetcher, config.ProcessingConfig{BatchSize: 2}, log, "test-job")

	handlerCallCount := 0
	handler := func(ctx context.Context, ids []interface{}) error {
		handlerCallCount++
		return nil
	}

	err = processor.Run(context.Background(), handler)

	assert.NoError(t, err)
	assert.Equal(t, 2, handlerCallCount)
	assert.Equal(t, 2, processor.batchCount)
	assert.Equal(t, 3, processor.totalProcessed)
}

func TestBatchProcessor_Run_GracefulShutdown(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2)
	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(0, 10).
		WillReturnRows(rows)

	fetcher := NewRootIDFetcher(db, "orders", "id", "", 10, 0)
	log := logger.NewDefault()
	processor := NewBatchProcessor(fetcher, config.ProcessingConfig{BatchSize: 10, SleepSeconds: 0.1}, log, "test-job")

	ctx, cancel := context.WithCancel(context.Background())

	handlerCallCount := 0
	handler := func(ctx context.Context, ids []interface{}) error {
		handlerCallCount++
		cancel()
		return nil
	}

	err = processor.Run(ctx, handler)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, 1, handlerCallCount)
	assert.Equal(t, 1, processor.batchCount)
}

func TestBatchProcessor_Run_HandlerErrorStopsProcessing(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(0, 10).
		WillReturnRows(rows)

	fetcher := NewRootIDFetcher(db, "orders", "id", "", 10, 0)
	log := logger.NewDefault()
	processor := NewBatchProcessor(fetcher, config.ProcessingConfig{BatchSize: 10}, log, "test-job")

	handler := func(ctx context.Context, ids []interface{}) error {
		return fmt.Errorf("batch processing error")
	}

	err = processor.Run(context.Background(), handler)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "batch processing error")
	assert.Equal(t, 1, processor.batchCount)
}

func TestBatchProcessor_GetStats(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2)
	mock.ExpectQuery("SELECT `id` FROM `orders` WHERE \\(1=1\\) AND `id` > \\? ORDER BY `id` ASC LIMIT \\?").
		WithArgs(0, 10).
		WillReturnRows(rows)

	fetcher := NewRootIDFetcher(db, "orders", "id", "", 10, 0)
	log := logger.NewDefault()
	processor := NewBatchProcessor(fetcher, config.ProcessingConfig{BatchSize: 10}, log, "test-job")

	batchCount, totalProcessed := processor.GetStats()
	assert.Equal(t, 0, batchCount)
	assert.Equal(t, 0, totalProcessed)

	handler := func(ctx context.Context, ids []interface{}) error {
		return nil
	}

	_, err = processor.ProcessBatch(context.Background(), handler)
	require.NoError(t, err)

	batchCount, totalProcessed = processor.GetStats()
	assert.Equal(t, 1, batchCount)
	assert.Equal(t, 2, totalProcessed)
}
