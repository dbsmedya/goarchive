package logger

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
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
	logPath := filepath.Join(t.TempDir(), "test-log.json")
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
				Output: logPath,
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
		{
			name: "invalid file output path",
			cfg: &config.LoggingConfig{
				Level:  "info",
				Format: "json",
				Output: "/this/path/does/not/exist/goarchive.log",
			},
			wantErr: true,
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
				_ = logger.Close()
			}
		})
	}
}

// TestNewCreatesLogFileOwnerOnly (GH #5): a log file GoArchive creates is readable by its
// owner only, since the log names tables, keys and servers. On Windows a mode sets only the
// read-only attribute, so there the file must stay writable.
func TestNewCreatesLogFileOwnerOnly(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "new.log")
	log, err := New(&config.LoggingConfig{Level: "info", Format: "json", Output: logPath, FileOnly: true})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	log.Info("mode check")
	_ = log.Close()

	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		if info.Mode().Perm()&0o200 == 0 {
			t.Errorf("log file mode = %v, want a writable file", info.Mode().Perm())
		}
		return
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("log file mode = %#o, want 0600", got)
	}
}

// TestNewKeepsExistingLogFileMode (GH #5): GoArchive appends to an existing log file and
// leaves its mode alone, so an operator whose log collector needs to read the file creates it
// first with the mode it needs.
func TestNewKeepsExistingLogFileMode(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "existing.log")
	if err := os.WriteFile(logPath, []byte("before\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(logPath, 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}

	log, err := New(&config.LoggingConfig{Level: "info", Format: "json", Output: logPath, FileOnly: true})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	log.Info("after")
	_ = log.Close()

	after, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode().Perm() != before.Mode().Perm() {
		t.Errorf("log file mode = %#o, want the existing %#o", after.Mode().Perm(), before.Mode().Perm())
	}
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(content), "before\n") || !strings.Contains(string(content), "after") {
		t.Errorf("log file = %q, want the old line then the new entry", content)
	}
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
	for _, format := range []string{"json", "text", "unknown"} {
		for _, console := range []bool{true, false} {
			if buildEncoder(format, console) == nil {
				t.Errorf("buildEncoder(%q, %v) returned nil", format, console)
			}
		}
	}
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

func TestCloseIgnoresConsoleSyncError(t *testing.T) {
	// Replace stdout with a pipe whose write end we close, so a real
	// Sync() on it would fail — mirrors fsync(/dev/stdout) returning
	// EINVAL on Linux terminals/pipes.
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() failed: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = orig
		_ = r.Close()
	}()

	log, err := New(&config.LoggingConfig{Level: "info", Format: "json", Output: "stdout"})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	_ = w.Close()

	if err := log.Close(); err != nil {
		t.Errorf("Close() should ignore console sync errors, got: %v", err)
	}
}

func TestFileOutputTextFormatNoColorCodes(t *testing.T) {
	logPath := t.TempDir() + "/out.log"

	log, err := New(&config.LoggingConfig{Level: "info", Format: "text", Output: logPath})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	log.Info("color check message")
	_ = log.Close()

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	if strings.Contains(string(content), "\x1b[") {
		t.Errorf("log file should not contain ANSI color codes, got: %q", content)
	}
	if !strings.Contains(string(content), "color check message") {
		t.Error("log file should contain the logged message")
	}
}

// captureStdoutDuring redirects os.Stdout while fn runs and returns what was
// written to it.
func captureStdoutDuring(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() failed: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fn()

	_ = w.Close()
	captured, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatalf("reading captured stdout failed: %v", err)
	}
	return string(captured)
}

func TestFileOnlySuppressesStdout(t *testing.T) {
	logPath := t.TempDir() + "/out.log"

	captured := captureStdoutDuring(t, func() {
		log, err := New(&config.LoggingConfig{
			Level: "info", Format: "json", Output: logPath, FileOnly: true,
		})
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}
		log.Info("file-only-marker")
		_ = log.Close()
	})

	if strings.Contains(captured, "file-only-marker") {
		t.Error("file_only output should not write to stdout")
	}

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	if !strings.Contains(string(content), "file-only-marker") {
		t.Error("log file should contain the logged message")
	}
}

func TestFileOutputTeesToStdoutByDefault(t *testing.T) {
	logPath := t.TempDir() + "/out.log"

	captured := captureStdoutDuring(t, func() {
		log, err := New(&config.LoggingConfig{
			Level: "info", Format: "json", Output: logPath,
		})
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}
		log.Info("tee-marker")
		_ = log.Close()
	})

	if !strings.Contains(captured, "tee-marker") {
		t.Error("file output without file_only should still write to stdout")
	}
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
