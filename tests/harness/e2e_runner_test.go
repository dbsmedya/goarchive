package harness

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestE2EDryRunTakesTheRunsTriggerFlag: run_archive_job's dry-run runs the job's
// preflight, so it accepts DELETE triggers exactly when the run does: archive and purge
// pass --force-triggers to validate, dry-run and the run; copy-only, which has no such
// flag, passes it to none of them.
func TestE2EDryRunTakesTheRunsTriggerFlag(t *testing.T) {
	for _, tc := range []struct {
		command string
		force   bool
	}{{"archive", true}, {"purge", true}, {"copy-only", false}} {
		t.Run(tc.command, func(t *testing.T) {
			dir := t.TempDir()
			writeFixture(t, filepath.Join(dir, "configs/t.yaml"), "jobs:\n  the-job:\n    job_type: "+tc.command+"\n")
			writeFixture(t, filepath.Join(dir, "goarchive"), "#!/bin/bash\nprintf '%s\\n' \"$*\" >> \""+filepath.Join(dir, "calls")+"\"\n")
			r := runLib(t, dir, `GOARCHIVE_BIN="$E2E_DIR/goarchive" GOARCHIVE_BIN_EXPLICIT=true PROJECT_ROOT="$E2E_DIR"
run_archive_job t.yaml `+tc.command)
			if r.rc != 0 {
				t.Fatalf("run_archive_job %s: rc=%d\n%s%s", tc.command, r.rc, r.stdout, r.stderr)
			}
			calls := strings.Split(strings.TrimSpace(readFixture(t, filepath.Join(dir, "calls"))), "\n")
			if len(calls) != 3 || !strings.HasPrefix(calls[0], "validate ") || !strings.HasPrefix(calls[1], "dry-run ") ||
				!strings.HasPrefix(calls[2], tc.command+" ") {
				t.Fatalf("calls = %q, want validate, dry-run, %s", calls, tc.command)
			}
			for _, c := range calls {
				if strings.Contains(c, "--force-triggers") != tc.force {
					t.Errorf("%s: %q, want --force-triggers=%t", tc.command, c, tc.force)
				}
			}
		})
	}
}
