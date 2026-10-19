package cmd

import (
	"os"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestEffectiveJobLogging: the command line beats the job's logging block, which beats the
// global one; a key the job omits keeps the global value (M6-3).
func TestEffectiveJobLogging(t *testing.T) {
	global := config.LoggingConfig{Level: "warn", Format: "json", Output: "stdout"}
	fileOnly := true
	job := &jobspec.Spec{Logging: &jobspec.LoggingOverrides{
		Level: "debug", Format: "text", Output: "/opt/goarchive/ShipmentErrorLogs.log", FileOnly: &fileOnly,
	}}
	if got := effectiveJobLogging(global, job, CLIOverrides{}); got.Level != "debug" || got.Format != "text" ||
		got.Output != "/opt/goarchive/ShipmentErrorLogs.log" || !got.FileOnly {
		t.Errorf("job over global = %+v", got)
	}
	if got := effectiveJobLogging(global, job, CLIOverrides{LogLevel: "error", LogFormat: "json"}); got.Level != "error" ||
		got.Format != "json" || got.Output != "/opt/goarchive/ShipmentErrorLogs.log" {
		t.Errorf("flags over job = %+v", got)
	}
	if got := effectiveJobLogging(global, &jobspec.Spec{}, CLIOverrides{LogLevel: "debug"}); got.Level != "debug" || got.Output != "stdout" {
		t.Errorf("no job block = %+v, want global plus the flag", got)
	}
}

func TestNewJobLoggerTagsJobAndWritesFile(t *testing.T) {
	logPath := t.TempDir() + "/job.log"
	fileOnly := true
	doc := &jobspec.Document{Shared: config.DefaultConfig()}
	job := &jobspec.Spec{Logging: &jobspec.LoggingOverrides{Format: "json", Output: logPath, FileOnly: &fileOnly}}
	log, err := newJobLogger(doc, job, "archive_shipment_error_logs", rootCmd, false)
	if err != nil {
		t.Fatalf("newJobLogger failed: %v", err)
	}
	log.Info("job tag check")
	_ = log.Close()
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), `"job":"archive_shipment_error_logs"`) && !strings.Contains(string(content), `"job": "archive_shipment_error_logs"`) {
		t.Errorf("no job tag: %s", content)
	}
}
