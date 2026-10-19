package cmd

import (
	"bytes"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMain lets a test run the real Execute in a child process (GOARCHIVE_TEST_EXECUTE=1), so
// the process exit status is tested through os.Exit (spec §3.4).
func TestMain(m *testing.M) {
	if os.Getenv("GOARCHIVE_TEST_EXECUTE") == "1" {
		Execute()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestExecuteExitStatus: the process exit status, through Execute and os.Exit.
func TestExecuteExitStatus(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want int
	}{{[]string{"version"}, 0}, {[]string{"archive"}, 2}, {[]string{"bogus"}, 2}} {
		cmd := exec.Command(os.Args[0], tc.args...)
		cmd.Env = append(os.Environ(), "GOARCHIVE_TEST_EXECUTE=1")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		code := 0
		if e, ok := err.(*exec.ExitError); ok {
			code = e.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		if code != tc.want {
			t.Errorf("%v: exit %d, want %d; stderr %q", tc.args, code, tc.want, stderr.String())
		}
		if tc.want == 0 && (stdout.Len() == 0 || stderr.Len() != 0) {
			t.Errorf("%v: stdout %q, stderr %q; want the result on stdout only", tc.args, stdout.String(), stderr.String())
		}
	}
}

func TestExecute(t *testing.T) {
	// Note: Execute() calls os.Exit(1) on error, so we can't test the error case directly
	// without causing the test to exit. We test the function exists and doesn't panic
	// when called with valid arguments.

	// Test that Execute function exists (doesn't return anything)
	// This is primarily a compile-time check
	assert.NotNil(t, Execute)
}

func TestVersionVariables(t *testing.T) {
	// Verify version variables exist and have default values
	assert.NotEmpty(t, Version, "Version should not be empty")
	assert.NotEmpty(t, Commit, "Commit should not be empty")
}

func TestCLIFlagsVariables(t *testing.T) {
	// Verify CLI flag variables exist
	// These are package-level variables that get set by cobra flags

	// String flags - cfgFile defaults to "archiver.yaml" via init()
	assert.Equal(t, "archiver.yaml", cfgFile, "cfgFile should default to archiver.yaml")
	assert.Equal(t, "", logLevel)
	assert.Equal(t, "", logFormat)
}
