package logger

import (
	"os"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected string // String representation of zapcore.Level
	}{
		{"debug", "debug"},
		{"info", "info"},
		{"", "info"}, // empty defaults to info
		{"warn", "warn"},
		{"error", "error"},
		{"unknown", "info"}, // unknown defaults to info
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			level := parseLevel(tt.input)
			if level.String() != tt.expected {
				t.Errorf("parseLevel(%q) = %v, expected %v", tt.input, level.String(), tt.expected)
			}
		})
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *config.LoggingConfig
		wantErr bool
	}{
		{
			name: "json format info level",
			cfg: &config.LoggingConfig{
				Level:  "info",
				Format: "json",
				Output: "stdout",
			},
			wantErr: false,
		},
		{
			name: "text format debug level",
			cfg: &config.LoggingConfig{
				Level:  "debug",
				Format: "text",
				Output: "stdout",
			},
			wantErr: false,
		},
		{
			name: "file output",
			cfg: &config.LoggingConfig{
				Level:  "warn",
				Format: "json",
				Output: "/tmp/test-log.json",
			},
			wantErr: false,
		},
		{
			name: "stderr output",
			cfg: &config.LoggingConfig{
				Level:  "error",
				Format: "text",
				Output: "stderr",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := New(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if logger == nil && !tt.wantErr {
				t.Error("New() returned nil logger without error")
			}
			if logger != nil {
				_ = logger.Sync()
			}
		})
	}

	// Cleanup test log file
	_ = os.Remove("/tmp/test-log.json")
}

func TestNewDefault(t *testing.T) {
	logger := NewDefault()
	if logger == nil {
		t.Fatal("NewDefault() returned nil")
	}

	// Should be able to log without panic
	logger.Info("test message")
	_ = logger.Sync()
}

func TestWithJob(t *testing.T) {
	cfg := &config.LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	}

	logger, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	jobLogger := logger.WithJob("test-job")
	if jobLogger == nil {
		t.Fatalf("WithJob() returned nil")
	}

	if jobLogger == logger {
		t.Error("WithJob() should return a new logger instance")
	}

	// Should be able to log without panic
	jobLogger.Info("test with job")
	_ = logger.Sync()
}

func TestWithBatch(t *testing.T) {
	cfg := &config.LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	}

	logger, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	batchLogger := logger.WithBatch(42)
	if batchLogger == nil {
		t.Fatalf("WithBatch() returned nil")
	}

	// Should be able to log without panic
	batchLogger.Info("test with batch")
	_ = logger.Sync()
}

func TestWithTable(t *testing.T) {
	cfg := &config.LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	}

	logger, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tableLogger := logger.WithTable("orders")
	if tableLogger == nil {
		t.Fatalf("WithTable() returned nil")
	}

	// Should be able to log without panic
	tableLogger.Info("test with table")
	_ = logger.Sync()
}

func TestWithFields(t *testing.T) {
	cfg := &config.LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	}

	logger, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	fields := map[string]interface{}{
		"custom_field": "value",
		"number":       123,
	}

	fieldLogger := logger.WithFields(fields)
	if fieldLogger == nil {
		t.Fatalf("WithFields() returned nil")
	}

	// Should be able to log without panic
	fieldLogger.Info("test with fields")
	_ = logger.Sync()
}

func TestChaining(t *testing.T) {
	cfg := &config.LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	}

	logger, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Chain multiple context methods
	chainedLogger := logger.WithJob("archive-orders").WithBatch(5).WithTable("orders")
	if chainedLogger == nil {
		t.Fatalf("Chained logger is nil")
	}

	// Should be able to log without panic
	chainedLogger.Info("test chained context")
	_ = logger.Sync()
}

func TestBuildEncoder(t *testing.T) {
	// Test JSON encoder
	jsonEncoder := buildEncoder("json")
	if jsonEncoder == nil {
		t.Error("buildEncoder('json') returned nil")
	}

	// Test text/console encoder
	textEncoder := buildEncoder("text")
	if textEncoder == nil {
		t.Error("buildEncoder('text') returned nil")
	}

	// Test default (unknown format should return text)
	defaultEncoder := buildEncoder("unknown")
	if defaultEncoder == nil {
		t.Error("buildEncoder('unknown') returned nil")
	}
}

func TestBuildWriters(t *testing.T) {
	// Test stdout
	stdoutWriter := buildWriters("stdout")
	if stdoutWriter == nil {
		t.Error("buildWriters('stdout') returned nil")
	}

	// Test stderr
	stderrWriter := buildWriters("stderr")
	if stderrWriter == nil {
		t.Error("buildWriters('stderr') returned nil")
	}

	// Test empty string (defaults to stdout)
	emptyWriter := buildWriters("")
	if emptyWriter == nil {
		t.Error("buildWriters('') returned nil")
	}

	// Test file output
	tmpFile := "/tmp/test-logger-output.log"
	fileWriter := buildWriters(tmpFile)
	if fileWriter == nil {
		t.Error("buildWriters(file) returned nil")
	}

	// Cleanup
	_ = os.Remove(tmpFile)
}

func TestSync(t *testing.T) {
	cfg := &config.LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: "stdout",
	}

	logger, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Sync should not error
	err = logger.Sync()
	// Note: Sync may return error on stdout, that's expected behavior
	_ = err
}

func TestLoggingOutput(t *testing.T) {
	// Create a temporary file for capturing output
	tmpFile, err := os.CreateTemp("", "logger-test-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	cfg := &config.LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: tmpFile.Name(),
	}

	logger, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Log some messages
	logger.Info("test info message")
	logger.Warn("test warn message")
	logger.WithJob("test-job").Info("message with job context")

	_ = logger.Sync()

	// Read the log file
	content, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	// Verify content contains our messages
	contentStr := string(content)
	if !strings.Contains(contentStr, "test info message") {
		t.Error("Log file should contain 'test info message'")
	}
	if !strings.Contains(contentStr, "test warn message") {
		t.Error("Log file should contain 'test warn message'")
	}
	if !strings.Contains(contentStr, "test-job") {
		t.Error("Log file should contain job context 'test-job'")
	}
}
