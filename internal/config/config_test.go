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

func TestVerificationConfigEffectiveMethod(t *testing.T) {
	tests := []struct {
		name string
		cfg  VerificationConfig
		want string
	}{
		{name: "empty defaults to count", cfg: VerificationConfig{}, want: "count"},
		{name: "count remains count", cfg: VerificationConfig{Method: "count"}, want: "count"},
		{name: "sha256 remains sha256", cfg: VerificationConfig{Method: "sha256"}, want: "sha256"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.EffectiveMethod(); got != tt.want {
				t.Fatalf("EffectiveMethod() = %q, want %q", got, tt.want)
			}
		})
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

func TestGetJobVerification_JobOverridesGlobalSkipVerification(t *testing.T) {
	off := false
	global := VerificationConfig{
		Method:           "count",
		SkipVerification: true,
	}
	job := JobConfig{
		Verification: &VerificationOverrides{
			Method:           "sha256",
			SkipVerification: &off,
		},
	}

	result := job.GetJobVerification(global)

	if result.Method != "sha256" {
		t.Errorf("expected method sha256, got %s", result.Method)
	}
	if result.SkipVerification {
		t.Error("expected job-level skip_verification=false to override global true")
	}
}

func TestGetJobProcessing_ExplicitZeroSleepOverridesGlobal(t *testing.T) {
	zero := 0.0
	global := ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500, SleepSeconds: 5}
	jc := &JobConfig{Processing: &ProcessingOverrides{SleepSeconds: &zero}}
	merged := jc.GetJobProcessing(global)
	if merged.SleepSeconds != 0 {
		t.Fatalf("explicit sleep_seconds: 0 must override global, got %v", merged.SleepSeconds)
	}
	if merged.BatchSize != 1000 || merged.BatchDeleteSize != 500 {
		t.Fatalf("unset fields must inherit global, got %+v", merged)
	}
}

func TestGetJobVerification_JobCanReenableVerification(t *testing.T) {
	off := false
	global := VerificationConfig{Method: "count", SkipVerification: true}
	jc := &JobConfig{Verification: &VerificationOverrides{SkipVerification: &off}}
	merged := jc.GetJobVerification(global)
	if merged.SkipVerification {
		t.Fatal("explicit skip_verification: false must override global true")
	}
}

func TestGetJobVerification_UnsetSkipInherits(t *testing.T) {
	global := VerificationConfig{Method: "count", SkipVerification: true}
	jc := &JobConfig{Verification: &VerificationOverrides{Method: "sha256"}}
	merged := jc.GetJobVerification(global)
	if !merged.SkipVerification {
		t.Fatal("unset skip_verification must inherit global true")
	}
	if merged.Method != "sha256" {
		t.Fatalf("method = %q, want sha256", merged.Method)
	}
}

func TestGetJobVerification_NilJobVerificationUsesGlobal(t *testing.T) {
	global := VerificationConfig{
		Method:           "count",
		SkipVerification: true,
	}
	job := JobConfig{}

	result := job.GetJobVerification(global)

	if result.Method != "count" {
		t.Errorf("expected method count, got %s", result.Method)
	}
	if !result.SkipVerification {
		t.Error("expected global skip_verification=true when job verification is nil")
	}
}

func TestGetJobLogging_NilJobLoggingUsesGlobal(t *testing.T) {
	global := LoggingConfig{Level: "warn", Format: "json", Output: "/var/log/global.log", FileOnly: true}
	job := JobConfig{}

	result := job.GetJobLogging(global)

	if result != global {
		t.Errorf("expected global logging config when job logging is nil, got %+v", result)
	}
}

func TestGetJobLogging_JobOverridesFields(t *testing.T) {
	global := LoggingConfig{Level: "warn", Format: "json", Output: "stdout"}
	job := JobConfig{
		Logging: &LoggingConfig{
			Level:    "debug",
			Format:   "text",
			Output:   "/opt/goarchive/ShipmentErrorLogs.log",
			FileOnly: true,
		},
	}

	result := job.GetJobLogging(global)

	if result.Level != "debug" {
		t.Errorf("expected level debug, got %s", result.Level)
	}
	if result.Format != "text" {
		t.Errorf("expected format text, got %s", result.Format)
	}
	if result.Output != "/opt/goarchive/ShipmentErrorLogs.log" {
		t.Errorf("expected job output path, got %s", result.Output)
	}
	if !result.FileOnly {
		t.Error("expected job-level file_only=true")
	}
}

func TestGetJobLogging_PartialOverrideInheritsGlobal(t *testing.T) {
	global := LoggingConfig{Level: "warn", Format: "json", Output: "stdout"}
	job := JobConfig{
		Logging: &LoggingConfig{Output: "/var/log/job.log"},
	}

	result := job.GetJobLogging(global)

	if result.Level != "warn" {
		t.Errorf("expected inherited level warn, got %s", result.Level)
	}
	if result.Format != "json" {
		t.Errorf("expected inherited format json, got %s", result.Format)
	}
	if result.Output != "/var/log/job.log" {
		t.Errorf("expected job output path, got %s", result.Output)
	}
	if result.FileOnly {
		t.Error("expected file_only=false when job block does not set it")
	}
}

func TestConfigGetJobLogging_UnknownJobUsesGlobal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Logging = LoggingConfig{Level: "error", Format: "json", Output: "stderr"}

	result := cfg.GetJobLogging("missing_job")

	if result != cfg.Logging {
		t.Errorf("expected global logging for unknown job, got %+v", result)
	}
}
