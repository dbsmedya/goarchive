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

// TestVerificationErrorJSONHint (JSN-1): a sha256 hash mismatch on a table with JSON columns
// names them and what a text copy can do to a JSON value; without JSON columns the text is
// today's.
func TestVerificationErrorJSONHint(t *testing.T) {
	base := VerificationError{Table: "jt", Method: "sha256", Requested: 2, Source: 2, Destination: 2,
		Detail: ", hashes differ (source 0123456789abcdef…, destination fedcba9876543210…)"}
	const prefix = "engine: sha256 verification of jt failed: 2 keys requested, 2 source rows, 2 destination rows, " +
		"hashes differ (source 0123456789abcdef…, destination fedcba9876543210…)"
	const tail = ": a JSON value copied as text can arrive changed (a DECIMAL such as 12.50 as 12.5; on an 8.0 or 8.4 destination, " +
		"some doubles), see docs/README_COMPAT.md → JSON values that do not survive a text copy; the batch was rolled back"
	for _, tc := range []struct {
		cols []string
		want string
	}{
		{nil, prefix + "; the batch was rolled back"},
		{[]string{"j"}, prefix + "; jt has JSON columns (j)" + tail},
		{[]string{"a", "b"}, prefix + "; jt has JSON columns (a, b)" + tail},
	} {
		e := base
		e.JSONColumns = tc.cols
		if got := e.Error(); got != tc.want {
			t.Errorf("JSONColumns %v:\n got %q\nwant %q", tc.cols, got, tc.want)
		}
	}
}
