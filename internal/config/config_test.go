package config

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Test source defaults
	if cfg.Source.Port != 3306 {
		t.Errorf("expected source port 3306, got %d", cfg.Source.Port)
	}
	if cfg.Source.TLS != "preferred" {
		t.Errorf("expected source TLS 'preferred', got %s", cfg.Source.TLS)
	}
	if cfg.Source.MaxConnections != 10 {
		t.Errorf("expected source max_connections 10, got %d", cfg.Source.MaxConnections)
	}

	// Test destination defaults
	if cfg.Destination.Port != 3306 {
		t.Errorf("expected destination port 3306, got %d", cfg.Destination.Port)
	}

	// Test replica defaults
	if cfg.Replica.Enabled != false {
		t.Errorf("expected replica disabled by default")
	}

	// Test processing defaults
	if cfg.Processing.BatchSize != 1000 {
		t.Errorf("expected batch_size 1000, got %d", cfg.Processing.BatchSize)
	}
	if cfg.Processing.BatchDeleteSize != 500 {
		t.Errorf("expected batch_delete_size 500, got %d", cfg.Processing.BatchDeleteSize)
	}

	// Test safety defaults
	if cfg.Safety.LagThreshold != 10 {
		t.Errorf("expected lag_threshold 10, got %d", cfg.Safety.LagThreshold)
	}

	// Test verification defaults
	if cfg.Verification.Method != "count" {
		t.Errorf("expected verification method 'count', got %s", cfg.Verification.Method)
	}

	// Test logging defaults
	if cfg.Logging.Level != "info" {
		t.Errorf("expected logging level 'info', got %s", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("expected logging format 'json', got %s", cfg.Logging.Format)
	}
}

func TestNestedRelations(t *testing.T) {
	// Test that nested relations structure works
	job := JobConfig{
		RootTable:  "orders",
		PrimaryKey: "id",
		Where:      "created_at < '2023-01-01'",
		Relations: []Relation{
			{
				Table:          "order_items",
				ForeignKey:     "order_id",
				DependencyType: "1-N",
			},
			{
				Table:          "shipments",
				ForeignKey:     "order_id",
				DependencyType: "1-1",
				Relations: []Relation{
					{
						Table:          "shipment_items",
						ForeignKey:     "shipment_id",
						DependencyType: "1-N",
					},
				},
			},
		},
	}

	if job.RootTable != "orders" {
		t.Errorf("expected root_table 'orders', got %s", job.RootTable)
	}
	if len(job.Relations) != 2 {
		t.Errorf("expected 2 relations, got %d", len(job.Relations))
	}

	// Check nested relation
	shipments := job.Relations[1]
	if len(shipments.Relations) != 1 {
		t.Errorf("expected 1 nested relation, got %d", len(shipments.Relations))
	}
	if shipments.Relations[0].Table != "shipment_items" {
		t.Errorf("expected nested table 'shipment_items', got %s", shipments.Relations[0].Table)
	}
}

func TestConfigJobsMap(t *testing.T) {
	// Test that jobs can be stored as a map
	cfg := &Config{
		Jobs: map[string]JobConfig{
			"archive_old_orders": {
				RootTable:  "orders",
				PrimaryKey: "id",
				Where:      "created_at < '2023-01-01'",
			},
			"archive_old_logs": {
				RootTable:  "logs",
				PrimaryKey: "id",
				Where:      "timestamp < '2023-01-01'",
			},
		},
	}

	if len(cfg.Jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(cfg.Jobs))
	}

	job, exists := cfg.Jobs["archive_old_orders"]
	if !exists {
		t.Error("expected 'archive_old_orders' job to exist")
	}
	if job.RootTable != "orders" {
		t.Errorf("expected root_table 'orders', got %s", job.RootTable)
	}
}
