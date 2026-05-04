package archiver

import (
	"errors"
	"testing"
)

func TestFinalJobStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want JobStatus
	}{
		{"nil error -> idle", nil, JobStatusIdle},
		{"non-nil error -> failed", errors.New("boom"), JobStatusFailed},
		{"wrapped error -> failed", &customErr{}, JobStatusFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := finalJobStatus(tc.err)
			if got != tc.want {
				t.Fatalf("finalJobStatus(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

type customErr struct{}

func (*customErr) Error() string { return "custom" }
