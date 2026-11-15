//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// REPLICATION_TYPE_CHECK against real servers (spec §8; GR C5): the pool of each `type: group` server is read
// through the library's GroupReplication fact, which needs no privilege.

// groupFleet is a replication block listing one `type: group` server for each pool, index-paired with them.
func groupFleet(pools ...*sql.DB) (config.ReplicationConfig, []*sql.DB) {
	bound := 1000
	cfg := config.ReplicationConfig{Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: 5,
		GroupApplierQueueWithin: &bound, GroupCertifierQueueWithin: &bound}
	for i := range pools {
		cfg.Servers = append(cfg.Servers, config.ReplicationServerConfig{Host: "r" + strconv.Itoa(i+1), Port: 3306, User: "m", TLS: "disable", Type: "group"})
	}
	return cfg, pools
}

// runWithFleet runs preflight over a fresh archive pair with the fleet, returning its error and the banners given.
func runWithFleet(t *testing.T, name string, pools ...*sql.DB) ([]string, error) {
	t.Helper()
	src, dst, spec := orderPair(t, name, jobspec.Archive)
	repl, ps := groupFleet(pools...)
	var banners []string
	_, err := Run(context.Background(), Config{Source: src, SourceDatabase: schemaOf(t, src), Destination: dst, DestinationDatabase: schemaOf(t, dst),
		JobSchema: schemaOf(t, dst), Spec: spec, Verification: jobspec.VerifyCount, Processing: defaultProcessing,
		Replication: repl, ReplicaPools: ps, Banner: func(b string) { banners = append(banners, b) }})
	return banners, err
}

// TestMySQLAGroupServerOnAPlainServerIsRefused: a `type: group` server that is no group member is refused, naming
// it, the type written and what was found. The plain estate's destination stands in for it.
func TestMySQLAGroupServerOnAPlainServerIsRefused(t *testing.T) {
	_, err := runWithFleet(t, "rtplain", openServer(t, "TEST_DEST", ""))
	e := refusedWith(t, err, "REPLICATION_TYPE_CHECK")
	const want = "replication.servers[0] r1:3306 is listed with type: group, but the Group Replication plugin is not installed on it"
	if !strings.Contains(e.Message, want) {
		t.Errorf("the refusal does not say %q:\n%s", want, e.Message)
	}
}

// TestMySQLAGroupServerOfTheSourcesGroupPasses: another member of the source's own group passes, and a member
// source that has a group server listed gets no banner.
func TestMySQLAGroupServerOfTheSourcesGroupPasses(t *testing.T) {
	requireMemberSource(t)
	banners, err := runWithFleet(t, "rtgroup", openServer(t, "TEST_GROUP_MEMBER2", ""))
	if err != nil {
		t.Fatalf("Run() = %v, want a member of the source's group to pass", err)
	}
	for _, b := range banners {
		if strings.Contains(b, "no server of type group is monitored") {
			t.Errorf("the banner was given although a group server is listed: %q", b)
		}
	}
}

// TestMySQLAnUnreadableGroupServerIsOnlyAWarning: a server whose read fails after its pool was opened is not
// judged: one warning, and the run goes on.
func TestMySQLAnUnreadableGroupServerIsOnlyAWarning(t *testing.T) {
	closed := openServer(t, "TEST_DEST", "")
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	banners, err := runWithFleet(t, "rtclosed", closed)
	if err != nil {
		t.Fatalf("Run() = %v, want success: an unreadable server is left to the gate", err)
	}
	var found bool
	for _, b := range banners {
		found = found || (strings.Contains(b, "WARNING: REPLICATION_TYPE_CHECK: replication.servers[0] r1:3306 is listed with type: group, and its Group Replication state could not be read (") &&
			strings.Contains(b, "): it is not judged here, and the replication gate decides"))
	}
	if !found {
		t.Errorf("banners = %q, want the warning that the server was not judged", banners)
	}
}
