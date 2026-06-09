package cmd

import (
	"os"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/internal/config"
)

func TestEffectiveJobLogging(t *testing.T) {
	baseCfg := func() *config.Config {
		cfg := config.DefaultConfig()
		cfg.Logging = config.LoggingConfig{Level: "warn", Format: "json", Output: "stdout"}
		return cfg
	}
	jobWithLogging := &config.JobConfig{
		Logging: &config.LoggingConfig{
			Level:    "debug",
			Format:   "text",
			Output:   "/opt/goarchive/ShipmentErrorLogs.log",
			FileOnly: true,
		},
	}

	t.Run("job overrides global", func(t *testing.T) {
		got := effectiveJobLogging(baseCfg(), jobWithLogging, CLIOverrides{})
		if got.Level != "debug" || got.Format != "text" ||
			got.Output != "/opt/goarchive/ShipmentErrorLogs.log" || !got.FileOnly {
			t.Errorf("expected job logging to override global, got %+v", got)
		}
	})

	t.Run("CLI flags override job", func(t *testing.T) {
		got := effectiveJobLogging(baseCfg(), jobWithLogging, CLIOverrides{LogLevel: "error", LogFormat: "json"})
		if got.Level != "error" {
			t.Errorf("expected CLI level error to win over job, got %s", got.Level)
		}
		if got.Format != "json" {
			t.Errorf("expected CLI format json to win over job, got %s", got.Format)
		}
		if got.Output != "/opt/goarchive/ShipmentErrorLogs.log" {
			t.Errorf("expected job output retained, got %s", got.Output)
		}
	})

	t.Run("job without logging block uses global", func(t *testing.T) {
		got := effectiveJobLogging(baseCfg(), &config.JobConfig{}, CLIOverrides{})
		if got.Level != "warn" || got.Output != "stdout" {
			t.Errorf("expected global logging, got %+v", got)
		}
	})

	t.Run("nil job uses global plus CLI", func(t *testing.T) {
		got := effectiveJobLogging(baseCfg(), nil, CLIOverrides{LogLevel: "debug"})
		if got.Level != "debug" || got.Output != "stdout" {
			t.Errorf("expected global+CLI logging, got %+v", got)
		}
	})
}

func TestNewJobLoggerTagsJobAndWritesFile(t *testing.T) {
	logPath := t.TempDir() + "/job.log"
	cfg := config.DefaultConfig()
	jobCfg := &config.JobConfig{
		Logging: &config.LoggingConfig{Format: "json", Output: logPath, FileOnly: true},
	}

	log, err := newJobLogger(cfg, jobCfg, "archive_shipment_error_logs")
	if err != nil {
		t.Fatalf("newJobLogger failed: %v", err)
	}
	log.Info("job tag check")
	_ = log.Close()

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read job log file: %v", err)
	}
	if !strings.Contains(string(content), `"job": "archive_shipment_error_logs"`) &&
		!strings.Contains(string(content), `"job":"archive_shipment_error_logs"`) {
		t.Errorf("expected job tag on log entries, got: %s", content)
	}
	if !strings.Contains(string(content), "job tag check") {
		t.Error("expected logged message in job log file")
	}
}
