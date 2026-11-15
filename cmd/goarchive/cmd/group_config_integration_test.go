//go:build integration

package cmd

import (
	"fmt"
	"strings"
	"testing"
)

// `type: group` and the banner through the commands (spec §4.1, §8; GR C5). The plain estate's tests use its
// destination as the monitored "group server": a plain server, so the check refuses it, and reading it changes
// nothing. The group estate's use its second member.

// groupBlock is a replication block listing one server as `type: group` (and none else).
func groupBlock(s server) string {
	return fmt.Sprintf("replication:\n  enabled: true\n  check_interval: 1\n  cache_ttl: 0\n  group_applier_queue_within: 1000\n"+
		"  group_certifier_queue_within: 1000\n  servers:\n    - host: %s\n      port: %d\n      user: %s\n      password: %q\n      tls: disable\n      type: group\n",
		s.host, s.port, s.user, s.pass)
}

// TestMySQLAGroupServerOnAPlainServerIsRefused: validate and dry-run refuse a `type: group` server that is no group
// member with REPLICATION_TYPE_CHECK, naming it; the interim refusal of the gate does not decide for them.
func TestMySQLAGroupServerOnAPlainServerIsRefused(t *testing.T) {
	f := orderFixture(t, "grptype")
	plain := serverFor(t, "TEST_DEST")
	appendConfig(t, f, groupBlock(plain))
	out, errOut, code := mainStreams(t, "validate", "-c", f.config, "--job", "archive_orders")
	if code == 0 || !strings.Contains(out, "FAILED: REPLICATION_TYPE_CHECK") ||
		!strings.Contains(out, fmt.Sprintf("replication.servers[0] %s:%d is listed with type: group, but the Group Replication plugin is not installed on it", plain.host, plain.port)) {
		t.Errorf("validate: code %d, stdout:\n%s\nstderr:\n%s\nwant REPLICATION_TYPE_CHECK naming the server", code, out, errOut)
	}
	_, errOut, code = mainStreams(t, "dry-run", "-c", f.config, "--job", "archive_orders")
	if code == 0 || !strings.Contains(errOut, "REPLICATION_TYPE_CHECK") {
		t.Errorf("dry-run: code %d, stderr:\n%s\nwant REPLICATION_TYPE_CHECK", code, errOut)
	}
}

// TestMySQLARunWithAGroupServerIsRefusedByTheGate: the interim refusal at the gate's construction: exit 1 before
// preflight, so before anything is read of the job's tables or written to the tracking tables.
func TestMySQLARunWithAGroupServerIsRefusedByTheGate(t *testing.T) {
	f := orderFixture(t, "grpgate")
	appendConfig(t, f, groupBlock(serverFor(t, "TEST_DEST")))
	for _, args := range [][]string{{"archive", "-c", f.config, "--job", "archive_orders"}} {
		_, errOut, code := mainStreams(t, args...)
		if code != 1 || !strings.Contains(errOut, "is listed with type: group, which the replication gate does not probe yet (not built yet)") {
			t.Errorf("%v: code %d, stderr:\n%s\nwant exit 1 and the gate's refusal", args, code, errOut)
		}
	}
	if got := scalar(t, f.src, "SELECT COUNT(*) FROM orders"); got != 4 {
		t.Errorf("source orders = %d, want 4 untouched", got)
	}
	if got := scalar(t, f.dst, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'archiver_job'"); got != 0 {
		t.Errorf("the destination holds tracking tables, want none: the refusal comes before the run writes anything")
	}
}

// TestMySQLAGroupServerOfTheSourcesGroupPassesValidate: a member of the source's group listed with `type: group`
// passes validate (the group estate), and with only an asynchronous replica listed, a member source gets the banner.
func TestMySQLAGroupServerOfTheSourcesGroupPassesValidate(t *testing.T) {
	requireGroupMemberSource(t)
	f := orderFixture(t, "grpok")
	appendConfig(t, f, groupBlock(serverFor(t, "TEST_GROUP_MEMBER2")))
	out, errOut, code := mainStreams(t, "validate", "-c", f.config, "--job", "archive_orders")
	if code != 0 || !strings.Contains(out, "PASSED") {
		t.Errorf("validate with a member of the group as a group server: code %d, stdout:\n%s\nstderr:\n%s\nwant PASSED", code, out, errOut)
	}
	if strings.Contains(out+errOut, "no server of type group is monitored") {
		t.Errorf("the banner appeared although a group server is listed:\n%s%s", out, errOut)
	}
}

func TestMySQLAMemberSourceWithOnlyAnAsynchronousReplicaGetsTheBanner(t *testing.T) {
	requireGroupMemberSource(t)
	f := orderFixture(t, "grpbanner")
	appendConfig(t, f, replicationBlock(t, 30))
	_, errOut, code := mainStreams(t, "validate", "-c", f.config, "--job", "archive_orders")
	const want = "WARNING: replication: the source is a Group Replication member and no server of type group is monitored: the group's backlog does not hold this job"
	if code != 0 || strings.Count(errOut, want) != 1 {
		t.Errorf("validate: code %d, stderr:\n%s\nwant it to pass with the banner once", code, errOut)
	}
}

// TestMySQLAPlainSourceWithReplicationGetsNoBanner: replication enabled with only an asynchronous replica is as it
// was: no banner on a source that is no member. The source is on the destination's server, a plain server on
// every estate: TEST_SOURCE is a member on the group estate.
func TestMySQLAPlainSourceWithReplicationGetsNoBanner(t *testing.T) {
	f := newFixtureOn(t, "TEST_DEST", "plainnobanner", orderJobs)
	mustExec(t, f.src, orderDDL...)
	mustExec(t, f.dst, orderDDL...)
	seed(t, f)
	appendConfig(t, f, replicationBlock(t, 30))
	_, errOut, _ := mainStreams(t, "validate", "-c", f.config, "--job", "archive_orders")
	if strings.Contains(errOut, "no server of type group is monitored") {
		t.Errorf("a plain source got the banner:\n%s", errOut)
	}
}
