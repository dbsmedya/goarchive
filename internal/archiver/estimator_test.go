package archiver

import (
	"context"
	"io"
	"os"
	"strings"
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

	// Mock child count error (dry-run must not print a number it can't stand behind)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `orders`").WillReturnError(assert.AnError)

	ctx := context.Background()
	result, err := estimator.Estimate(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to estimate filtered count for orders")
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

func TestEstimateChildCountFiltersThroughRelationChain(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer func() { _ = db.Close() }()

	cfg := &config.Config{Processing: config.ProcessingConfig{BatchSize: 100, BatchDeleteSize: 50}}
	jobCfg := &config.JobConfig{RootTable: "customers", PrimaryKey: "id", Where: "id <= 3"}
	g := graph.NewGraph("customers", "id")
	g.AddNode("orders", &graph.Node{Name: "orders", ForeignKey: "customer_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdgeWithMeta("customers", "orders", "customer_id", "id", "1-N")
	g.AddNode("order_items", &graph.Node{Name: "order_items", ForeignKey: "order_id", ReferenceKey: "id", DependencyType: "1-N"})
	g.AddEdgeWithMeta("orders", "order_items", "order_id", "id", "1-N")
	g.SetPK("orders", "id")
	g.SetPK("order_items", "id")

	e := NewEstimator(db, cfg, jobCfg, g, logger.NewDefault())

	// Depth 1: orders filtered by root WHERE through customer_id IN (...)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `orders` WHERE `customer_id` IN \\(SELECT `id` FROM `customers` WHERE \\(id <= 3\\)\\)").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(7))
	count, err := e.estimateChildCount(context.Background(), "orders")
	if err != nil {
		t.Fatalf("estimateChildCount(orders): %v", err)
	}
	if count != 7 {
		t.Fatalf("orders count = %d, want 7", count)
	}

	// Depth 2: order_items chained through orders through customers
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `order_items` WHERE `order_id` IN \\(SELECT `id` FROM `orders` WHERE `customer_id` IN \\(SELECT `id` FROM `customers` WHERE \\(id <= 3\\)\\)\\)").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(21))
	count, err = e.estimateChildCount(context.Background(), "order_items")
	if err != nil {
		t.Fatalf("estimateChildCount(order_items): %v", err)
	}
	if count != 21 {
		t.Fatalf("order_items count = %d, want 21", count)
	}
}

func TestDisplayExecutionPlanShowsWhere(t *testing.T) {
	cfg := &config.Config{Processing: config.ProcessingConfig{BatchSize: 100, BatchDeleteSize: 50}}
	jobCfg := &config.JobConfig{RootTable: "orders", PrimaryKey: "id", Where: "created_at < '2024-01-01'"}
	g := graph.NewGraph("orders", "id")
	e := NewEstimator(nil, cfg, jobCfg, g, logger.NewDefault())
	result := &EstimateResult{RootTable: "orders", RootCount: 10, ChildCounts: map[string]int64{},
		BatchSize: 100, Config: cfg, JobConfig: jobCfg}

	out := captureStdout(t, func() { e.DisplayExecutionPlan(result) })
	if !strings.Contains(out, "WHERE: created_at < '2024-01-01'") {
		t.Fatalf("execution plan must show the WHERE clause, got:\n%s", out)
	}
}

// displayPlanWithReplication renders the dry-run plan for a config carrying rc.
func displayPlanWithReplication(t *testing.T, rc config.ReplicationConfig) string {
	t.Helper()
	cfg := &config.Config{
		Processing:   config.ProcessingConfig{BatchSize: 100, BatchDeleteSize: 50},
		Verification: config.VerificationConfig{Method: "count"},
		Replication:  rc,
	}
	jobCfg := &config.JobConfig{RootTable: "orders", PrimaryKey: "id"}
	g := graph.NewGraph("orders", "id")
	e := NewEstimator(nil, cfg, jobCfg, g, logger.NewDefault())
	result := &EstimateResult{RootTable: "orders", RootCount: 10, ChildCounts: map[string]int64{},
		BatchSize: 100, Config: cfg, JobConfig: jobCfg}

	return captureStdout(t, func() { e.DisplayExecutionPlan(result) })
}

// TestDisplayExecutionPlan_ReplicationEnabled pins the dry-run replication block
// for a two-server fleet, including the two channel-selection renderings that
// must stay unambiguous: [] → "all", ["", "billing"] → "<default>, billing".
func TestDisplayExecutionPlan_ReplicationEnabled(t *testing.T) {
	out := displayPlanWithReplication(t, config.ReplicationConfig{
		Enabled:                   true,
		SecondsBehindSourceWithin: 30,
		CheckInterval:             5,
		CacheTTL:                  15,
		Servers: []config.ReplicationServerConfig{
			{Host: "replica-a", Port: 3306, User: "monitor", Password: "sup3rs3cret", TLS: "disable", Type: "async"},
			{Host: "replica-b", Port: 3307, User: "monitor", Password: "sup3rs3cret", TLS: "disable", Type: "async",
				Channels: []string{"", "billing"}},
		},
	})

	want := []string{
		"  Replication lag monitoring: enabled (2 server(s))\n",
		"    Tolerance (seconds_behind_source_within): 30s\n",
		"    Check interval: 5s\n",
		"    Cache TTL: 15s\n",
		"    Server replica-a:3306: channels: all\n",
		"    Server replica-b:3307: channels: <default>, billing\n",
	}
	for _, line := range want {
		if !strings.Contains(out, line) {
			t.Errorf("dry-run output missing line %q; got:\n%s", line, out)
		}
	}

	if strings.Contains(out, "disabled") {
		t.Errorf("enabled fleet must not render the disabled line; got:\n%s", out)
	}
	if strings.Contains(out, "sup3rs3cret") {
		t.Error("dry-run output leaked a replication server password")
	}
}

// TestDisplayExecutionPlan_ChannelNameCannotSplitALine proves the rendered plan
// keeps one physical line per server even when a channel name carries \n or \r.
func TestDisplayExecutionPlan_ChannelNameCannotSplitALine(t *testing.T) {
	out := displayPlanWithReplication(t, config.ReplicationConfig{
		Enabled:                   true,
		SecondsBehindSourceWithin: 30,
		CheckInterval:             5,
		CacheTTL:                  15,
		Servers: []config.ReplicationServerConfig{
			{Host: "replica-a", Port: 3306, User: "monitor", TLS: "disable", Type: "async",
				Channels: []string{"billing\nforged: replication is fine"}},
		},
	})

	if !strings.Contains(out, "    Server replica-a:3306: channels: billing forged: replication is fine\n") {
		t.Errorf("channel name was not collapsed onto one line; got:\n%s", out)
	}

	// The forged fragment must never appear at the start of its own line.
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "forged") {
			t.Errorf("a channel name split the dry-run output into an extra line %q; full output:\n%s", line, out)
		}
	}
}

// TestDisplayExecutionPlan_ReplicationDisabled pins the unchanged disabled line.
func TestDisplayExecutionPlan_ReplicationDisabled(t *testing.T) {
	out := displayPlanWithReplication(t, config.ReplicationConfig{Enabled: false})

	if !strings.Contains(out, "  Replication lag monitoring: disabled\n") {
		t.Errorf("dry-run output missing the disabled line; got:\n%s", out)
	}
	if strings.Contains(out, "Replication lag monitoring: enabled") {
		t.Errorf("disabled config must not render the enabled block; got:\n%s", out)
	}
	if strings.Contains(out, "Tolerance") || strings.Contains(out, "Cache TTL") {
		t.Errorf("disabled config must not render fleet detail lines; got:\n%s", out)
	}
}

// TestDescribeChannels covers the three selection modes the dry-run must keep
// distinguishable.
func TestDescribeChannels(t *testing.T) {
	tests := []struct {
		name     string
		channels []string
		want     string
	}{
		{"nil means all channels", nil, "all"},
		{"empty slice means all channels", []string{}, "all"},
		{"empty string means the default channel", []string{""}, "<default>"},
		{"default plus named", []string{"", "billing"}, "<default>, billing"},
		{"named only", []string{"billing"}, "billing"},
		{"several named", []string{"billing", "reporting"}, "billing, reporting"},
		// Line integrity: config validation rejects duplicate channel names but
		// never their content, so a name carrying \n or \r must be collapsed to
		// spaces exactly as internal/replication.renderChannel does. Asserted
		// here rather than by importing that package.
		{"newline in a named channel", []string{"billing\nforged"}, "billing forged"},
		{"carriage return in a named channel", []string{"billing\rforged"}, "billing forged"},
		{"crlf in a named channel", []string{"billing\r\nforged"}, "billing  forged"},
		{"newline alongside the default channel", []string{"", "billing\nforged"}, "<default>, billing forged"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := describeChannels(tt.channels); got != tt.want {
				t.Errorf("describeChannels(%#v) = %q, want %q", tt.channels, got, tt.want)
			}
		})
	}
}

// captureStdout redirects os.Stdout for the duration of fn.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	data, _ := io.ReadAll(r)
	return string(data)
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
