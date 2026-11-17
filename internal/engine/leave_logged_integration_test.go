//go:build integration

package engine

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestMySQLLeaveLoggedReportsARefusalAsItself (Matrix verify run 44005058337, row partial_revokes_on): leaveLogged leaves
// a run's roots logged by cancelling it after its copy. A run that preflight refuses first leaves nothing logged, and the
// helper used to go on to read the log: the test then failed on `Error 1146: Table 'archiver_job' doesn't exist`, which
// names no check, so the row, whose expectation is that the failures cite the permission checks, could not tell the
// refusal from a defect. The helper now reports the run's own error, as interrupted does. The child process runs the
// helper on a run that is refused (the destination has none of the tables) and fails by design; this test reads how.
func TestMySQLLeaveLoggedReportsARefusalAsItself(t *testing.T) {
	if os.Getenv("GOARCHIVE_LEAVE_LOGGED_CHILD") == "1" {
		e := newEnv(t, "llref")
		d16Tables(t, e, jobspec.Purge) // the source's tables only: an archive into a destination without them is refused
		seedOrders(t, e, 3)
		leaveLogged(t, e, e.config(t, job(t, jobspec.Archive, orderJob)), 3)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMySQLLeaveLoggedReportsARefusalAsItself$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), "GOARCHIVE_LEAVE_LOGGED_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the helper passed on a run that preflight refuses:\n%s", out)
	}
	text := string(out)
	if strings.Contains(text, "1146") {
		t.Errorf("the helper read the log of a run that never started (a missing table is not the refusal):\n%s", text)
	}
	if !strings.Contains(text, "want it interrupted") || !strings.Contains(text, "Run() = ") {
		t.Errorf("the helper does not report the run's own error:\n%s", text)
	}
}
