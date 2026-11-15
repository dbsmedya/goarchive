package harness

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// setup-replication.sh talks to the estate only through mysql-query.sh, so its messages can be read with a stub of that
// script: the real script and topology.sh are copied beside a stub that answers `SHOW REPLICA STATUS` from the test's
// environment and remembers a START REPLICA. What the script says of the replica must be the topology's replica, db3 on
// the plain estate and gr-replica on the group estate, and of the source the one it attaches to (GR-C5-1).

const stubQuery = `#!/bin/bash
# $1 is the port, $2 the statement, as mysql-query.sh takes them.
attached="$STUB_DIR/attached"
case "$2" in
*"SHOW REPLICA STATUS"*)
    if [ "${STUB_ATTACHED:-0}" = 1 ] || [ -f "$attached" ]; then
        cat <<STATUS
        Source_Host: ${STUB_SOURCE_HOST}
        Source_Port: 3306
        Source_User: repl
        Replica_IO_Running: Yes
        Replica_SQL_Running: Yes
        SQL_Delay: 0
        Auto_Position: 1
        Seconds_Behind_Source: 0
STATUS
    fi ;;
"START REPLICA;") touch "$attached" ;;
esac
exit 0
`

// runSetup runs the real setup-replication.sh over the stub for a topology and returns its output and exit code.
func runSetup(t *testing.T, topology, sourceHost string, attached bool, positioning string) (stdout, stderr string, code int) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, name := range []string{"setup-replication.sh", "topology.sh"} {
		body, err := os.ReadFile(filepath.Join(root, "tests", "scripts", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "mysql-query.sh"), []byte(stubQuery), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", filepath.Join(dir, "setup-replication.sh"))
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "STUB_DIR=" + dir, "STUB_SOURCE_HOST=" + sourceHost,
		"MYSQL_ROOT_PASSWORD=x", "REPL_USER=repl", "REPL_PASSWORD=pw", "REPL_POSITIONING=" + positioning, "TOPOLOGY=" + topology}
	if topology == "group" {
		env = append(env, "REPL_SOURCE_HOST=gr1") // as tests/dot.env.group sets it
	}
	if attached {
		env = append(env, "STUB_ATTACHED=1")
	}
	cmd.Env = env
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("setup-replication.sh timed out: %v\n%s", ctx.Err(), errb.String())
	}
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	return out.String(), errb.String(), code
}

// TestSetupReplicationNamesTheTopologysReplica: every message the script prints about the replica names the topology's
// replica, and none names the other topology's.
func TestSetupReplicationNamesTheTopologysReplica(t *testing.T) {
	for _, tc := range []struct {
		topology, source, replica, other string
	}{
		{"plain", "db1", "db3", "gr-replica"},
		{"group", "gr1", "gr-replica", "db3"},
	} {
		t.Run(tc.topology, func(t *testing.T) {
			t.Run("already replicating and caught up", func(t *testing.T) {
				out, errOut, code := runSetup(t, tc.topology, tc.source, true, "gtid")
				want := "setup-replication.sh: " + tc.replica + " is already replicating from " + tc.source + ":3306 (gtid) and caught up — nothing to do."
				if code != 0 || !strings.Contains(out, want) {
					t.Errorf("exit %d, stdout:\n%s\nstderr:\n%s\nwant %q", code, out, errOut, want)
				}
				if strings.Contains(out+errOut, tc.other) {
					t.Errorf("the output names %s, the other topology's replica:\n%s%s", tc.other, out, errOut)
				}
			})
			t.Run("attaching a replica that has no replication", func(t *testing.T) {
				out, errOut, code := runSetup(t, tc.topology, tc.source, false, "gtid")
				for _, want := range []string{
					"setup-replication.sh: attaching " + tc.replica + " -> " + tc.source + ":3306 (gtid)...",
					"setup-replication.sh: " + tc.replica + " is replicating from " + tc.source + ":3306 (gtid); both threads running, caught up.",
				} {
					if !strings.Contains(out, want) {
						t.Errorf("stdout lacks %q:\n%s\nstderr:\n%s", want, out, errOut)
					}
				}
				if code != 0 || strings.Contains(out+errOut, tc.other) {
					t.Errorf("exit %d, and the output must not name %s:\n%s%s", code, tc.other, out, errOut)
				}
			})
			t.Run("a replication config that diverges", func(t *testing.T) {
				_, errOut, code := runSetup(t, tc.topology, "somewhere-else", true, "gtid")
				for _, want := range []string{
					"setup-replication.sh: " + tc.replica + " already has a replication config, and it DIVERGES",
					"(The remediation above discards " + tc.replica + "'s replication config. It does not touch",
				} {
					if !strings.Contains(errOut, want) {
						t.Errorf("stderr lacks %q:\n%s", want, errOut)
					}
				}
				if code != 1 || strings.Contains(errOut, tc.other) {
					t.Errorf("exit %d, and the message must not name %s:\n%s", code, tc.other, errOut)
				}
			})
			t.Run("a snapshot attach, which it refuses", func(t *testing.T) {
				_, errOut, code := runSetup(t, tc.topology, tc.source, false, "snapshot")
				for _, want := range []string{
					"REPL_POSITIONING=snapshot, but " + tc.replica + " has no replication",
					"must seed " + tc.replica + " from a " + tc.source + " dump",
				} {
					if !strings.Contains(errOut, want) {
						t.Errorf("stderr lacks %q:\n%s", want, errOut)
					}
				}
				if code != 1 || strings.Contains(errOut, tc.other) {
					t.Errorf("exit %d, and the message must not name %s:\n%s", code, tc.other, errOut)
				}
			})
		})
	}
}
