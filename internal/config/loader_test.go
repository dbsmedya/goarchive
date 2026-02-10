package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test.yaml")

	configContent := `
source:
  host: localhost
  port: 3306
  user: testuser
  password: testpass
  database: testdb
  tls: disable
  max_connections: 5
  max_idle_connections: 2

destination:
  host: archive-host
  port: 3307
  user: archiveuser
  password: archivepass
  database: archivedb

jobs:
  test_job:
    root_table: orders
    primary_key: id
    where: "created_at < '2023-01-01'"
    relations:
      - table: order_items
        foreign_key: order_id
        dependency_type: "1-N"

processing:
  batch_size: 500
  batch_delete_size: 100
  sleep_seconds: 0.5

logging:
  level: debug
  format: text
  output: stdout
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Verify source config
	if cfg.Source.Host != "localhost" {
		t.Errorf("expected source host 'localhost', got %s", cfg.Source.Host)
	}
	if cfg.Source.Port != 3306 {
		t.Errorf("expected source port 3306, got %d", cfg.Source.Port)
	}
	if cfg.Source.User != "testuser" {
		t.Errorf("expected source user 'testuser', got %s", cfg.Source.User)
	}
	if cfg.Source.MaxConnections != 5 {
		t.Errorf("expected source max_connections 5, got %d", cfg.Source.MaxConnections)
	}

	// Verify destination config
	if cfg.Destination.Host != "archive-host" {
		t.Errorf("expected destination host 'archive-host', got %s", cfg.Destination.Host)
	}

	// Verify job config
	if len(cfg.Jobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(cfg.Jobs))
	}
	job, exists := cfg.Jobs["test_job"]
	if !exists {
		t.Error("expected 'test_job' to exist")
	}
	if job.RootTable != "orders" {
		t.Errorf("expected root_table 'orders', got %s", job.RootTable)
	}
	if len(job.Relations) != 1 {
		t.Errorf("expected 1 relation, got %d", len(job.Relations))
	}

	// Verify processing config
	if cfg.Processing.BatchSize != 500 {
		t.Errorf("expected batch_size 500, got %d", cfg.Processing.BatchSize)
	}

	// Verify logging config
	if cfg.Logging.Level != "debug" {
		t.Errorf("expected logging level 'debug', got %s", cfg.Logging.Level)
	}
}

func TestLoadWithEnvVars(t *testing.T) {
	// Set environment variables for test
	os.Setenv("TEST_DB_HOST", "env-host")
	os.Setenv("TEST_DB_USER", "env-user")
	os.Setenv("TEST_DB_PASS", "env-pass")
	defer func() {
		os.Unsetenv("TEST_DB_HOST")
		os.Unsetenv("TEST_DB_USER")
		os.Unsetenv("TEST_DB_PASS")
	}()

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test-env.yaml")

	configContent := `
source:
  host: ${TEST_DB_HOST}
  port: 3306
  user: ${TEST_DB_USER}
  password: ${TEST_DB_PASS}
  database: testdb
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Source.Host != "env-host" {
		t.Errorf("expected source host 'env-host', got %s", cfg.Source.Host)
	}
	if cfg.Source.User != "env-user" {
		t.Errorf("expected source user 'env-user', got %s", cfg.Source.User)
	}
	if cfg.Source.Password != "env-pass" {
		t.Errorf("expected source password 'env-pass', got %s", cfg.Source.Password)
	}
}

func TestExpandEnvVar(t *testing.T) {
	os.Setenv("TEST_VAR", "test-value")
	defer os.Unsetenv("TEST_VAR")

	tests := []struct {
		input    string
		expected string
	}{
		{"${TEST_VAR}", "test-value"},
		{"$TEST_VAR", "test-value"},
		{"prefix-${TEST_VAR}-suffix", "prefix-test-value-suffix"},
		{"${NONEXISTENT}", "${NONEXISTENT}"}, // Unset vars remain unchanged
		{"no-vars-here", "no-vars-here"},
	}

	for _, tt := range tests {
		result := expandEnvVar(tt.input)
		if result != tt.expected {
			t.Errorf("expandEnvVar(%q) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestGetJob(t *testing.T) {
	cfg := &Config{
		Jobs: map[string]JobConfig{
			"existing_job": {
				RootTable: "orders",
			},
		},
	}

	// Test existing job
	job, err := cfg.GetJob("existing_job")
	if err != nil {
		t.Errorf("unexpected error getting existing job: %v", err)
	}
	if job.RootTable != "orders" {
		t.Errorf("expected root_table 'orders', got %s", job.RootTable)
	}

	// Test non-existing job
	_, err = cfg.GetJob("nonexistent_job")
	if err == nil {
		t.Error("expected error for non-existing job")
	}
}

func TestListJobs(t *testing.T) {
	cfg := &Config{
		Jobs: map[string]JobConfig{
			"job_a": {},
			"job_b": {},
			"job_c": {},
		},
	}

	jobs := cfg.ListJobs()
	if len(jobs) != 3 {
		t.Errorf("expected 3 jobs, got %d", len(jobs))
	}

	// Check all jobs are present (order may vary)
	jobSet := make(map[string]bool)
	for _, j := range jobs {
		jobSet[j] = true
	}
	for _, expected := range []string{"job_a", "job_b", "job_c"} {
		if !jobSet[expected] {
			t.Errorf("expected job %q to be in list", expected)
		}
	}
}

func TestLoadNonExistentFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestApplyOverrides(t *testing.T) {
	// Start with a default config
	cfg := DefaultConfig()

	// Verify defaults
	if cfg.Logging.Level != "info" {
		t.Errorf("expected default log level 'info', got %s", cfg.Logging.Level)
	}
	if cfg.Processing.BatchSize != 1000 {
		t.Errorf("expected default batch size 1000, got %d", cfg.Processing.BatchSize)
	}
	if cfg.Verification.SkipVerification != false {
		t.Error("expected default skip_verify to be false")
	}

	// Apply some overrides
	cfg.ApplyOverrides("debug", "text", 500, 250, 2.5, true)

	// Verify overrides were applied
	if cfg.Logging.Level != "debug" {
		t.Errorf("expected log level 'debug' after override, got %s", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "text" {
		t.Errorf("expected log format 'text' after override, got %s", cfg.Logging.Format)
	}
	if cfg.Processing.BatchSize != 500 {
		t.Errorf("expected batch size 500 after override, got %d", cfg.Processing.BatchSize)
	}
	if cfg.Processing.BatchDeleteSize != 250 {
		t.Errorf("expected batch delete size 250 after override, got %d", cfg.Processing.BatchDeleteSize)
	}
	if cfg.Processing.SleepSeconds != 2.5 {
		t.Errorf("expected sleep seconds 2.5 after override, got %f", cfg.Processing.SleepSeconds)
	}
	if cfg.Verification.SkipVerification != true {
		t.Error("expected skip_verify to be true after override")
	}
}

func TestApplyOverridesZeroValues(t *testing.T) {
	// Start with a custom config
	cfg := &Config{
		Logging: LoggingConfig{
			Level:  "warn",
			Format: "json",
		},
		Processing: ProcessingConfig{
			BatchSize:       2000,
			BatchDeleteSize: 1000,
			SleepSeconds:    5.0,
		},
		Verification: VerificationConfig{
			SkipVerification: false,
		},
	}

	// Apply zero values (should NOT override)
	cfg.ApplyOverrides("", "", 0, 0, 0, false)

	// Verify original values are preserved
	if cfg.Logging.Level != "warn" {
		t.Errorf("expected log level 'warn' to be preserved, got %s", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("expected log format 'json' to be preserved, got %s", cfg.Logging.Format)
	}
	if cfg.Processing.BatchSize != 2000 {
		t.Errorf("expected batch size 2000 to be preserved, got %d", cfg.Processing.BatchSize)
	}
	if cfg.Processing.BatchDeleteSize != 1000 {
		t.Errorf("expected batch delete size 1000 to be preserved, got %d", cfg.Processing.BatchDeleteSize)
	}
	if cfg.Processing.SleepSeconds != 5.0 {
		t.Errorf("expected sleep seconds 5.0 to be preserved, got %f", cfg.Processing.SleepSeconds)
	}
	if cfg.Verification.SkipVerification != false {
		t.Error("expected skip_verify to remain false")
	}
}

func TestApplyOverridesPartial(t *testing.T) {
	// Start with a default config
	cfg := DefaultConfig()

	// Apply only some overrides
	cfg.ApplyOverrides("error", "", 0, 100, 0, true)

	// Verify only specified overrides were applied
	if cfg.Logging.Level != "error" {
		t.Errorf("expected log level 'error' after override, got %s", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" { // Should keep default
		t.Errorf("expected log format to remain 'json', got %s", cfg.Logging.Format)
	}
	if cfg.Processing.BatchSize != 1000 { // Should keep default (0 doesn't override)
		t.Errorf("expected batch size to remain 1000, got %d", cfg.Processing.BatchSize)
	}
	if cfg.Processing.BatchDeleteSize != 100 {
		t.Errorf("expected batch delete size 100 after override, got %d", cfg.Processing.BatchDeleteSize)
	}
	if cfg.Verification.SkipVerification != true {
		t.Error("expected skip_verify to be true after override")
	}
}
