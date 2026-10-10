//go:build integration

package engine

import (
	"maps"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestMySQLReportVerifiedAndElapsed (M7b): the report says how the run's copies were
// verified and how long the run took. Verified holds the rows each table's verification
// passed for, so under sha256 and count it equals Copied, and it is empty when nothing was
// compared. Elapsed covers the run's own sleeps: two full batches of two roots each sleep
// sleep_seconds after them.
func TestMySQLReportVerifiedAndElapsed(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  jobspec.Type
		v    jobspec.Verification
	}{
		{"archive_sha256", jobspec.Archive, jobspec.VerifySHA256},
		{"archive_count", jobspec.Archive, jobspec.VerifyCount},
		{"archive_none", jobspec.Archive, jobspec.VerifyNone},
		{"copy_only_sha256", jobspec.CopyOnly, jobspec.VerifySHA256},
		{"purge", jobspec.Purge, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "report")
			f := keyFamilies[0] // INT keys: four eligible roots, two children each
			if tc.typ == jobspec.Purge {
				execAll(t, e.src, f.tables()...)
			} else {
				e.both(t, f.tables()...)
			}
			f.seed(t, e)
			cfg := e.config(t, job(t, tc.typ, keyJob))
			cfg.Verification = tc.v
			cfg.Processing.SleepSeconds = 0.3
			rep := e.mustRun(t, cfg)

			if rep.Verification != tc.v {
				t.Errorf("Verification = %q, want %q", rep.Verification, tc.v)
			}
			switch tc.v {
			case jobspec.VerifySHA256, jobspec.VerifyCount:
				if want := map[string]int64{"r": 4, "c": 8}; !maps.Equal(rep.Verified, want) || !maps.Equal(rep.Copied, want) {
					t.Errorf("Verified = %v, Copied = %v, want both %v", rep.Verified, rep.Copied, want)
				}
			default:
				if len(rep.Verified) != 0 {
					t.Errorf("Verified = %v, want none: nothing was compared", rep.Verified)
				}
			}
			if floor := 600 * time.Millisecond; rep.Elapsed < floor {
				t.Errorf("Elapsed = %v, below the run's own sleeps (%v)", rep.Elapsed, floor)
			}
		})
	}
}
