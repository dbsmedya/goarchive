package archiver

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbsmedya/goarchive/internal/config"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEstimator(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{
		Processing: config.ProcessingConfig{BatchSize: 10},
	}
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
	}
	g := createSimpleGraph()
	log := logger.NewDefault()

	estimator := NewEstimator(db, cfg, jobCfg, g, log)

	require.NotNil(t, estimator)
	assert.Equal(t, db, estimator.db)
	assert.Equal(t, cfg, estimator.cfg)
	assert.Equal(t, jobCfg, estimator.jobCfg)
	assert.Equal(t, g, estimator.graph)
	assert.NotNil(t, estimator.logger)
}

func TestNewEstimator_NilLogger(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{}
	jobCfg := &config.JobConfig{RootTable: "customers", PrimaryKey: "id"}
	g := createSimpleGraph()

	estimator := NewEstimator(db, cfg, jobCfg, g, nil)

	require.NotNil(t, estimator)
	assert.NotNil(t, estimator.logger) // Should create default logger
}

func TestEstimator_Estimate_Success(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{
		Processing: config.ProcessingConfig{BatchSize: 5},
	}
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
		Where:      "created_at < '2024-01-01'",
	}
	g := createSimpleGraph()
	estimator := NewEstimator(db, cfg, jobCfg, g, logger.NewDefault())

	// Mock root count query
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `customers` WHERE created_at").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(25))

	ctx := context.Background()
	result, err := estimator.Estimate(ctx)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "customers", result.RootTable)
	assert.Equal(t, int64(25), result.RootCount)
	assert.Equal(t, 5, result.BatchSize)
	assert.Equal(t, int64(5), result.EstimatedBatches) // 25 / 5 = 5 batches
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimator_Estimate_RootCountZero(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{
		Processing: config.ProcessingConfig{BatchSize: 10},
	}
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
		Where:      "1=0", // No matching rows
	}
	g := createSimpleGraph()
	estimator := NewEstimator(db, cfg, jobCfg, g, logger.NewDefault())

	// Mock: No rows match
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	ctx := context.Background()
	result, err := estimator.Estimate(ctx)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(0), result.RootCount)
	assert.Equal(t, int64(0), result.EstimatedBatches)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimator_Estimate_WithChildTables(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{
		Processing: config.ProcessingConfig{BatchSize: 5},
	}
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "customer_id",
				DependencyType: "1-N",
			},
		},
	}
	builder := graph.NewBuilder(jobCfg)
	g, _ := builder.Build()
	estimator := NewEstimator(db, cfg, jobCfg, g, logger.NewDefault())

	// Mock root count
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))

	// Mock child count (orders)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `orders`").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(50))

	ctx := context.Background()
	result, err := estimator.Estimate(ctx)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(10), result.RootCount)
	assert.Equal(t, int64(50), result.ChildCounts["orders"])
	assert.Equal(t, int64(2), result.EstimatedBatches) // 10 / 5 = 2 batches
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimator_Estimate_RootCountError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{
		Processing: config.ProcessingConfig{BatchSize: 5},
	}
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
	}
	g := createSimpleGraph()
	estimator := NewEstimator(db, cfg, jobCfg, g, logger.NewDefault())

	// Mock root count query error
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `customers`").WillReturnError(assert.AnError)

	ctx := context.Background()
	result, err := estimator.Estimate(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to estimate root count")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimator_Estimate_ChildCountError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{
		Processing: config.ProcessingConfig{BatchSize: 5},
	}
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "customer_id",
				DependencyType: "1-N",
			},
		},
	}
	builder := graph.NewBuilder(jobCfg)
	g, _ := builder.Build()
	estimator := NewEstimator(db, cfg, jobCfg, g, logger.NewDefault())

	// Mock root count success
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `customers`").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))

	// Mock child count error (should log warning but not fail)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `orders`").WillReturnError(assert.AnError)

	ctx := context.Background()
	result, err := estimator.Estimate(ctx)

	// Should succeed even if child count fails (logs warning)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(10), result.RootCount)
	assert.Equal(t, int64(0), result.ChildCounts["orders"]) // Defaults to 0 on error
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimator_Estimate_EmptyWhere(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{
		Processing: config.ProcessingConfig{BatchSize: 10},
	}
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
		Where:      "", // Empty WHERE clause - should default to "1=1"
	}
	g := createSimpleGraph()
	estimator := NewEstimator(db, cfg, jobCfg, g, logger.NewDefault())

	// Mock should query with "WHERE 1=1"
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `customers` WHERE 1=1").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(100))

	ctx := context.Background()
	result, err := estimator.Estimate(ctx)

	require.NoError(t, err)
	assert.Equal(t, int64(100), result.RootCount)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestEstimator_Estimate_BatchCalculation(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	tests := []struct {
		name            string
		rootCount       int64
		batchSize       int
		expectedBatches int64
	}{
		{
			name:            "Even division",
			rootCount:       100,
			batchSize:       10,
			expectedBatches: 10,
		},
		{
			name:            "Uneven division rounds up",
			rootCount:       101,
			batchSize:       10,
			expectedBatches: 11,
		},
		{
			name:            "Single batch",
			rootCount:       5,
			batchSize:       10,
			expectedBatches: 1,
		},
		{
			name:            "Many small batches",
			rootCount:       1000,
			batchSize:       7,
			expectedBatches: 143, // ceil(1000/7) = 143
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				Processing: config.ProcessingConfig{BatchSize: tt.batchSize},
			}
			jobCfg := &config.JobConfig{
				RootTable:  "customers",
				PrimaryKey: "id",
			}
			g := createSimpleGraph()
			estimator := NewEstimator(db, cfg, jobCfg, g, logger.NewDefault())

			mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `customers`").
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tt.rootCount))

			ctx := context.Background()
			result, err := estimator.Estimate(ctx)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedBatches, result.EstimatedBatches)
		})
	}
}

func TestEstimator_DisplayExecutionPlan(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{
		Processing:   config.ProcessingConfig{BatchSize: 10},
		Verification: config.VerificationConfig{Method: "count"},
	}
	jobCfg := &config.JobConfig{
		RootTable:  "customers",
		PrimaryKey: "id",
		Relations: []config.Relation{
			{
				Table:          "orders",
				PrimaryKey:     "id",
				ForeignKey:     "customer_id",
				DependencyType: "1-N",
			},
		},
	}
	builder := graph.NewBuilder(jobCfg)
	g, _ := builder.Build()
	estimator := NewEstimator(db, cfg, jobCfg, g, logger.NewDefault())

	result := &EstimateResult{
		RootTable:        "customers",
		RootCount:        100,
		ChildCounts:      map[string]int64{"orders": 500},
		EstimatedBatches: 10,
		BatchSize:        10,
		Config:           cfg,
		JobConfig:        jobCfg,
	}

	// This test just verifies the method doesn't panic
	// In real code, it prints to stdout
	estimator.DisplayExecutionPlan(result)
	// If we reach here without panic, test passes
}
