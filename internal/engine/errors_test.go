package engine

import "testing"

// TestJobRunningError: the typed refusal keeps today's two texts (startup.go), so only the
// exit code changes (spec §3.4).
func TestJobRunningError(t *testing.T) {
	for _, tc := range []struct {
		err  *JobRunningError
		want string
	}{
		{&JobRunningError{Job: "j"}, `engine: job "j" is already running: its advisory lock was held by another session a moment ago`},
		{&JobRunningError{Job: "j", Holder: 42}, `engine: job "j" is already running: its advisory lock is held by another session (destination connection 42)`},
	} {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("Error() = %q, want %q", got, tc.want)
		}
	}
}
