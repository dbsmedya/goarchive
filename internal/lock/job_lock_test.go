// Package lock provides MySQL advisory locking functionality for GoArchive.
package lock

import (
	"strings"
	"testing"
)

// ============================================================================
// GenerateJobLockName Tests
// ============================================================================

func TestGenerateJobLockName_Format(t *testing.T) {
	tests := []struct {
		jobName  string
		expected string
	}{
		{"archive_orders", "goarchive:job:archive_orders"},
		{"job-123", "goarchive:job:job-123"},
		{"my_job_name", "goarchive:job:my_job_name"},
		{"UPPERCASE_JOB", "goarchive:job:UPPERCASE_JOB"},
		{"MixedCase_Job-123", "goarchive:job:MixedCase_Job-123"},
	}

	for _, tt := range tests {
		t.Run(tt.jobName, func(t *testing.T) {
			result := GenerateJobLockName(tt.jobName)
			if result != tt.expected {
				t.Errorf("GenerateJobLockName(%q) = %q, expected %q", tt.jobName, result, tt.expected)
			}
		})
	}
}

func TestGenerateJobLockName_Sanitization(t *testing.T) {
	tests := []struct {
		jobName  string
		expected string
	}{
		// Special characters should be replaced with underscores
		{"job.with.dots", "goarchive:job:job_with_dots"},
		{"job/with/slashes", "goarchive:job:job_with_slashes"},
		{"job@with@ats", "goarchive:job:job_with_ats"},
		{"job with spaces", "goarchive:job:job_with_spaces"},
		{"job#with#hash", "goarchive:job:job_with_hash"},
		{"job$with$dollar", "goarchive:job:job_with_dollar"},
		{"job%with%percent", "goarchive:job:job_with_percent"},
		{"job&ampersand", "goarchive:job:job_ampersand"},
		{"job*star*", "goarchive:job:job_star_"},
		{"job(paren)", "goarchive:job:job_paren_"},
		{"job+plus+", "goarchive:job:job_plus_"},
		{"job=equals=", "goarchive:job:job_equals_"},
		{"job?question?", "goarchive:job:job_question_"},
		{"job!exclaim!", "goarchive:job:job_exclaim_"},
		{"job[bracket]", "goarchive:job:job_bracket_"},
		{"job{brace}", "goarchive:job:job_brace_"},
		{"job:colon:", "goarchive:job:job_colon_"},
		{"job;semi;", "goarchive:job:job_semi_"},
		{"job'quote'", "goarchive:job:job_quote_"},
		{`job"double"`, "goarchive:job:job_double_"},
		{"job<less>", "goarchive:job:job_less_"},
		{"job>greater>", "goarchive:job:job_greater_"},
		{"job|pipe|", "goarchive:job:job_pipe_"},
		{"job^caret^", "goarchive:job:job_caret_"},
		{"job~tilde~", "goarchive:job:job_tilde_"},
		{"job`backtick`", "goarchive:job:job_backtick_"},
		// Multiple special characters
		{"job!@#$%", "goarchive:job:job_____"},
		// Mixed valid and invalid characters
		{"job_name.with-dots", "goarchive:job:job_name_with-dots"},
	}

	for _, tt := range tests {
		t.Run(tt.jobName, func(t *testing.T) {
			result := GenerateJobLockName(tt.jobName)
			if result != tt.expected {
				t.Errorf("GenerateJobLockName(%q) = %q, expected %q", tt.jobName, result, tt.expected)
			}
		})
	}
}

func TestGenerateJobLockName_EdgeCases(t *testing.T) {
	// Empty job name
	result := GenerateJobLockName("")
	if result != "goarchive:job:" {
		t.Errorf("Empty job name: got %q, expected %q", result, "goarchive:job:")
	}

	// Single character
	result = GenerateJobLockName("a")
	if result != "goarchive:job:a" {
		t.Errorf("Single char: got %q, expected %q", result, "goarchive:job:a")
	}

	// Only special characters (all become underscores)
	result = GenerateJobLockName("!@#$%")
	if result != "goarchive:job:_____" {
		t.Errorf("Only special chars: got %q, expected %q", result, "goarchive:job:_____")
	}

	// Long job name (should not be truncated by our function, but MySQL limits to 64 chars)
	longName := strings.Repeat("a", 100)
	result = GenerateJobLockName(longName)
	expectedPrefix := "goarchive:job:"
	if !strings.HasPrefix(result, expectedPrefix) {
		t.Errorf("Long name should have prefix %q, got %q", expectedPrefix, result)
	}
}

func TestGenerateJobLockName_Consistency(t *testing.T) {
	// Same input should always produce same output
	jobName := "my_test_job"
	result1 := GenerateJobLockName(jobName)
	result2 := GenerateJobLockName(jobName)
	result3 := GenerateJobLockName(jobName)

	if result1 != result2 || result2 != result3 {
		t.Error("GenerateJobLockName should be consistent for same input")
	}
}

// ============================================================================
// NewJobLock Tests
// ============================================================================

func TestNewJobLock(t *testing.T) {
	db := connectToTestDB(t)
	defer func() { _ = db.Close() }()

	jobName := "test_archive_job"
	lock := NewJobLock(db, jobName)

	if lock == nil {
		t.Fatal("NewJobLock returned nil")
	}

	expectedLockName := GenerateJobLockName(jobName)
	if lock.lockName != expectedLockName {
		t.Errorf("Lock name = %q, expected %q", lock.lockName, expectedLockName)
	}

	if lock.db != db {
		t.Error("Lock should store database connection")
	}

	if lock.held {
		t.Error("New lock should not be held")
	}
}

func TestNewJobLock_EmptyJobName(t *testing.T) {
	db := connectToTestDB(t)
	defer func() { _ = db.Close() }()

	lock := NewJobLock(db, "")
	if lock == nil {
		t.Fatal("NewJobLock should not return nil for empty job name")
	}

	expectedLockName := "goarchive:job:"
	if lock.lockName != expectedLockName {
		t.Errorf("Empty job lock name = %q, expected %q", lock.lockName, expectedLockName)
	}
}

func TestNewJobLock_SanitizedName(t *testing.T) {
	db := connectToTestDB(t)
	defer func() { _ = db.Close() }()

	// Job name with special characters that get sanitized
	jobName := "my.job@name"
	lock := NewJobLock(db, jobName)

	expectedLockName := "goarchive:job:my_job_name"
	if lock.lockName != expectedLockName {
		t.Errorf("Sanitized lock name = %q, expected %q", lock.lockName, expectedLockName)
	}
}
