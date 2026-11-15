//go:build integration

package cmd

import (
	"context"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// These tests run spec §4.7's report block through the commands (GR C2): a member source gets the
// block in validate and dry-run, and a plain pair gets nothing. The member ones need a Group
// Replication source and skip on the plain estate; the group estate (TOPOLOGY=group) runs them.

// requireGroupMemberSource skips the test unless TEST_SOURCE is a Group Replication member: the
// plugin is ACTIVE and a group name is set.
func requireGroupMemberSource(t *testing.T) {
	t.Helper()
	g, err := dbsrepl.NewInspector(serverFor(t, "TEST_SOURCE").open(t, "")).GroupReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if g.PluginStatus != "ACTIVE" || g.GroupName == "" {
		t.Skip("the source is not a Group Replication member: this test runs on the group estate (TOPOLOGY=group)")
	}
}

// blockLines returns the Group Replication block of out, from its heading to its sizes line.
func blockLines(out string) string {
	start := strings.Index(out, "Group Replication:\n")
	if start < 0 {
		return ""
	}
	rest := out[start:]
	end := strings.Index(rest, "sizes:")
	if end < 0 {
		return rest
	}
	return rest[:end+strings.Index(rest[end:], "\n")]
}

func TestMySQLValidateReportsAGroupSource(t *testing.T) {
	requireGroupMemberSource(t)
	f := newFixture(t, "grpvalidate", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	out, err := execute(t, "validate", "-c", f.config, "--job", "archive_orders")
	if err != nil {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	block := blockLines(out)
	for _, want := range []string{"  source:      member of group ", ", ONLINE, ", "; transaction size limit ", " bytes\n",
		"  destination: Group Replication plugin not active\n", "  sizes:       batch_size 2, batch_delete_size 2 ("} {
		if !strings.Contains(block, want) {
			t.Errorf("validate's block %q lacks %q\n%s", block, want, out)
		}
	}
	if strings.Count(out, "Group Replication:") != 1 || strings.Index(out, "Group Replication:") > strings.Index(out, "Statement caps") {
		t.Errorf("validate printed the block %d times, or after the caps:\n%s", strings.Count(out, "Group Replication:"), out)
	}
}

func TestMySQLDryRunReportsAGroupSource(t *testing.T) {
	requireGroupMemberSource(t)
	f := newFixture(t, "grpdryrun", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)
	out, err := execute(t, "dry-run", "-c", f.config, "--job", "archive_orders")
	if err != nil {
		t.Fatalf("dry-run: %v\n%s", err, out)
	}
	if block := blockLines(out); !strings.Contains(block, "  source:      member of group ") || !strings.Contains(block, ", ONLINE, ") {
		t.Errorf("dry-run's block = %q\n%s", block, out)
	}
	if strings.Count(out, "Group Replication:") != 1 || strings.Index(out, "Group Replication:") > strings.Index(out, "Statement caps") {
		t.Errorf("dry-run printed the block %d times, or after the caps:\n%s", strings.Count(out, "Group Replication:"), out)
	}
}

// TestMySQLAPlainPairGetsNoGroupBlock: the destination server is a plain server on every estate;
// a pair of two of its schemas has no block in validate or dry-run.
func TestMySQLAPlainPairGetsNoGroupBlock(t *testing.T) {
	f := newFixtureOn(t, "TEST_DEST", "plainblock", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)
	for _, cmd := range []string{"validate", "dry-run"} {
		out, err := execute(t, cmd, "-c", f.config, "--job", "archive_orders")
		if err != nil {
			t.Fatalf("%s: %v\n%s", cmd, err, out)
		}
		if strings.Contains(out, "Group Replication") {
			t.Errorf("%s of a plain pair mentions Group Replication:\n%s", cmd, out)
		}
		if !strings.Contains(out, "Statement caps") {
			t.Errorf("%s printed no caps: the run did not reach the report\n%s", cmd, out)
		}
	}
}
