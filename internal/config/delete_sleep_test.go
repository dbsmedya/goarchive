package config

import "testing"

// TestProcessingOverridesApply_DeleteSleepSeconds verifies that a job-level
// delete_sleep_seconds overrides the global value while other processing fields
// continue to inherit from the global config, and that a job without its own
// processing block inherits the global delete_sleep_seconds.
func TestProcessingOverridesApply_DeleteSleepSeconds(t *testing.T) {
	global := ProcessingConfig{
		BatchSize:          1000,
		BatchDeleteSize:    500,
		SleepSeconds:       1,
		DeleteSleepSeconds: 0,
	}

	deleteSleep := 2.5
	o := &ProcessingOverrides{DeleteSleepSeconds: &deleteSleep}
	got := o.Apply(global)
	if got.DeleteSleepSeconds != 2.5 {
		t.Errorf("expected job delete_sleep_seconds 2.5, got %v", got.DeleteSleepSeconds)
	}
	// Untouched fields inherit the global values.
	if got.BatchSize != 1000 || got.BatchDeleteSize != 500 || got.SleepSeconds != 1 {
		t.Errorf("expected other fields to inherit global, got %+v", got)
	}

	// A job with no processing block inherits the global delete_sleep_seconds.
	global.DeleteSleepSeconds = 3
	var none *ProcessingOverrides
	if got := none.Apply(global); got.DeleteSleepSeconds != 3 {
		t.Errorf("expected inherited delete_sleep_seconds 3, got %v", got.DeleteSleepSeconds)
	}
}

// TestValidateProcessing_DeleteSleepSeconds verifies that a negative
// delete_sleep_seconds is rejected and that zero (disabled) is accepted.
func TestValidateProcessing_DeleteSleepSeconds(t *testing.T) {
	bad := &Config{Processing: ProcessingConfig{
		BatchSize:          1,
		BatchDeleteSize:    1,
		DeleteSleepSeconds: -1,
	}}
	errs := bad.validateProcessing()
	found := false
	for _, e := range errs {
		if e.Field == "processing.delete_sleep_seconds" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a validation error for negative delete_sleep_seconds, got %v", errs)
	}

	ok := &Config{Processing: ProcessingConfig{
		BatchSize:          1,
		BatchDeleteSize:    1,
		DeleteSleepSeconds: 0,
	}}
	for _, e := range ok.validateProcessing() {
		if e.Field == "processing.delete_sleep_seconds" {
			t.Fatalf("did not expect delete_sleep_seconds error for zero value, got %v", e)
		}
	}
}
