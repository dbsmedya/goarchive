package config

import (
	"errors"
	"strings"
	"testing"
)

func validShared() *Config {
	c := DefaultConfig()
	c.Source.Host, c.Source.User, c.Source.Database = "src", "u", "sakila"
	c.Destination.Host, c.Destination.User, c.Destination.Database = "dst", "u", "archive"
	return c
}

// TestValidateSharedIgnoresJobs: the 3.0 job model validates its own jobs, so a config
// with no 2.x jobs passes ValidateShared while Validate still demands one.
func TestValidateSharedIgnoresJobs(t *testing.T) {
	c := validShared()
	if err := c.ValidateShared(); err != nil {
		t.Fatalf("ValidateShared() = %v, want nil", err)
	}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "at least one job") {
		t.Fatalf("Validate() = %v, want the at-least-one-job error", err)
	}
}

// TestValidateSharedReportsEverySharedBlock: one broken setting per block, each reported.
func TestValidateSharedReportsEverySharedBlock(t *testing.T) {
	c := validShared()
	c.Source.Host = ""
	c.Destination.Port = 0
	c.Replication.Enabled = true // with no servers
	c.Processing.BatchSize = 0
	c.Logging.Level = "loud"
	err := c.ValidateShared()
	var ve ValidationErrors
	if !errors.As(err, &ve) {
		t.Fatalf("ValidateShared() = %v, want ValidationErrors", err)
	}
	for _, field := range []string{"source.host", "destination.port", "replication", "processing.batch_size", "logging.level"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("ValidateShared() does not report %s:\n%v", field, err)
		}
	}
}

// TestValidateSharedLeavesVerificationToJobspec: 3.0's verification is one value that
// jobspec owns (R42), so the 2.x verification block is not judged here.
func TestValidateSharedLeavesVerificationToJobspec(t *testing.T) {
	c := validShared()
	c.Verification = VerificationConfig{}
	if err := c.ValidateShared(); err != nil {
		t.Errorf("ValidateShared() = %v, want no verification judgement", err)
	}
}

func TestExportedOverrideValidators(t *testing.T) {
	if errs := ValidateProcessingConfig("jobs.j.processing", ProcessingConfig{BatchSize: 0, BatchDeleteSize: 1}); len(errs) != 1 || errs[0].Field != "jobs.j.processing.batch_size" {
		t.Errorf("ValidateProcessingConfig = %v", errs)
	}
	if errs := ValidateLoggingConfig("jobs.j.logging", LoggingConfig{Format: "xml"}); len(errs) != 1 || errs[0].Field != "jobs.j.logging.format" {
		t.Errorf("ValidateLoggingConfig = %v", errs)
	}
}

func TestNormalizeAppliesReplicationServerDefaults(t *testing.T) {
	c := validShared()
	c.Replication.Servers = []ReplicationServerConfig{{Host: "r1"}}
	c.Normalize()
	if s := c.Replication.Servers[0]; s.Port != 3306 || s.TLS != "preferred" || s.Type != "async" {
		t.Fatalf("Normalize() left %+v", s)
	}
}
