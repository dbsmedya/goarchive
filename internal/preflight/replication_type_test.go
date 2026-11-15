package preflight

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// REPLICATION_TYPE_CHECK and the banner of a member source that no group server watches (spec §4.1, §8; GR C5).
// The check reads each `type: group` server's GroupReplication fact through the pool opened on it; the tests
// supply those reads.

const otherGroup = "9b9b9b9b-0000-4000-8000-0000000000bb"

func group(status, name string) dbsrepl.GroupReplication {
	return dbsrepl.GroupReplication{PluginStatus: status, GroupName: name, TransactionSizeLimit: 150000000, TransactionSizeLimitKnown: true}
}

// fleet makes the world's replication block, enabled, with one server per type and a fixed read for each
// `type: group` server; reads[i] is server i's answer. It returns the list of the servers read.
func (w *world) fleet(types []string, reads map[int]any) *[]int {
	var read []int
	srv := make([]config.ReplicationServerConfig, len(types))
	for i, typ := range types {
		srv[i] = config.ReplicationServerConfig{Host: fmt.Sprintf("r%d", i+1), Port: 3306, User: "m", TLS: "disable", Type: typ}
	}
	bound := 1000
	w.cfg.Replication = config.ReplicationConfig{Enabled: true, SecondsBehindSourceWithin: 10, CheckInterval: 5, Servers: srv,
		GroupApplierQueueWithin: &bound, GroupCertifierQueueWithin: &bound}
	w.cfg.groupRead = func(_ context.Context, i int) (dbsrepl.GroupReplication, error) {
		read = append(read, i)
		switch v := reads[i].(type) {
		case dbsrepl.GroupReplication:
			return v, nil
		case error:
			return dbsrepl.GroupReplication{}, v
		}
		return dbsrepl.GroupReplication{}, errors.New("a read nobody planned")
	}
	return &read
}

const typeAdvice = ". A server listed with type: group must be configured for the source's group; an asynchronous replica is listed without a type, or with type: async."

// TestReplicationTypeCheckRefusesWhatIsNotAGroupServer: a `type: group` server whose plugin is not ACTIVE with a
// group name, or that is configured for another group than the source's, is refused, and the message names the
// server, the type written and what was found.
func TestReplicationTypeCheckRefusesWhatIsNotAGroupServer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source bool // the source is a member of groupName
		read   dbsrepl.GroupReplication
		want   string
	}{
		{"no plugin", false, dbsrepl.GroupReplication{}, "replication.servers[0] r1:3306 is listed with type: group, but the Group Replication plugin is not installed on it"},
		{"plugin INACTIVE", false, group("INACTIVE", ""), "replication.servers[0] r1:3306 is listed with type: group, but the Group Replication plugin is INACTIVE on it"},
		{"plugin DISABLED", true, group("DISABLED", ""), "replication.servers[0] r1:3306 is listed with type: group, but the Group Replication plugin is DISABLED on it"},
		{"no group name", false, group("ACTIVE", ""), "replication.servers[0] r1:3306 is listed with type: group, but the Group Replication plugin is ACTIVE on it and no group name is set"},
		{"another group", true, group("ACTIVE", otherGroup), "replication.servers[0] r1:3306 is listed with type: group, but it is configured for group " + otherGroup + " and the source for group " + groupName},
	} {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
			t.Run(tc.name+"/"+string(typ), func(t *testing.T) {
				w := healthy(t, typ)
				if tc.source {
					w.asMember("ONLINE", "PRIMARY")
				}
				w.fleet([]string{"group"}, map[int]any{0: tc.read})
				e := w.mustFail("REPLICATION_TYPE_CHECK")
				if want := tc.want + typeAdvice; e.Message != want {
					t.Errorf("message:\n got %s\nwant %s", e.Message, want)
				}
			})
		}
	}
}

// TestReplicationTypeCheckNamesEveryOffendingServer: one refusal for the block, each wrong server in its place.
func TestReplicationTypeCheckNamesEveryOffendingServer(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.asMember("ONLINE", "PRIMARY")
	read := w.fleet([]string{"group", "async", "group", "group"}, map[int]any{
		0: group("ACTIVE", groupName), 2: group("ACTIVE", otherGroup), 3: dbsrepl.GroupReplication{},
	})
	e := w.mustFail("REPLICATION_TYPE_CHECK")
	for _, want := range []string{"replication.servers[2] r3:3306 is listed with type: group, but it is configured for group " + otherGroup,
		"replication.servers[3] r4:3306 is listed with type: group, but the Group Replication plugin is not installed on it"} {
		if !strings.Contains(e.Message, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, e.Message)
		}
	}
	if strings.Contains(e.Message, "r1:3306") || strings.Contains(e.Message, "r2:3306") {
		t.Errorf("the refusal names a server that passed or is not a group server:\n%s", e.Message)
	}
	if want := []int{0, 2, 3}; !slices.Equal(*read, want) {
		t.Errorf("servers read = %v, want only the type: group ones, %v", *read, want)
	}
}

// TestReplicationTypeCheckPasses: a group server of the source's group passes, and so does one when the source
// is no member at all (a destination that is a group, a monitored group beside a plain source: the names are
// compared only when the source is configured for a group).
func TestReplicationTypeCheckPasses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source bool
		read   dbsrepl.GroupReplication
	}{
		{"a group server of the source's group", true, group("ACTIVE", groupName)},
		{"a group server beside a plain source", false, group("ACTIVE", otherGroup)},
	} {
		w := healthy(t, jobspec.Archive)
		if tc.source {
			w.asMember("ONLINE", "PRIMARY")
		}
		w.fleet([]string{"group"}, map[int]any{0: tc.read})
		w.cfg.Banner = func(string) {}
		if _, err := w.run(); err != nil {
			t.Errorf("%s: run() = %v, want success", tc.name, err)
		}
	}
}

// TestReplicationTypeCheckAppliesOnlyWhereTheGateDoes: archive and purge with replication enabled, and not under
// a skip flag (a policy check); copy-only deletes nothing, and a disabled block has no gate.
func TestReplicationTypeCheckAppliesOnlyWhereTheGateDoes(t *testing.T) {
	bad := map[int]any{0: dbsrepl.GroupReplication{}}
	for _, tc := range []struct {
		name    string
		typ     jobspec.Type
		mutate  func(w *world)
		refused bool
	}{
		{"archive", jobspec.Archive, func(*world) {}, true},
		{"purge", jobspec.Purge, func(*world) {}, true},
		{"copy-only", jobspec.CopyOnly, func(*world) {}, false},
		{"replication disabled", jobspec.Archive, func(w *world) { w.cfg.Replication.Enabled = false }, false},
		{"a skip flag", jobspec.Archive, func(w *world) { w.cfg.SkipPolicy = true }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, tc.typ)
			read := w.fleet([]string{"group"}, bad)
			tc.mutate(w)
			if tc.refused {
				w.refused("REPLICATION_TYPE_CHECK")
				return
			}
			w.mustPass()
			if len(*read) != 0 {
				t.Errorf("the group server was read (%v), want no read where the check does not apply", *read)
			}
		})
	}
}

// TestReplicationTypeCheckLeavesAnUnreadableServerToTheGate: a server whose read fails after the connection was
// made is not judged. One warning says so, and the run goes on: the gate decides at its first probe.
func TestReplicationTypeCheckLeavesAnUnreadableServerToTheGate(t *testing.T) {
	for _, withBanner := range []bool{false, true} {
		t.Run(fmt.Sprintf("banner=%v", withBanner), func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			w.fleet([]string{"group", "group"}, map[int]any{0: errors.New("driver: bad connection"), 1: group("ACTIVE", groupName)})
			w.asMember("ONLINE", "PRIMARY")
			var banners []string
			if withBanner {
				w.cfg.Banner = func(b string) { banners = append(banners, b) }
			}
			w.mustPass()
			const want = "WARNING: REPLICATION_TYPE_CHECK: replication.servers[0] r1:3306 is listed with type: group, and its Group Replication state could not be read (driver: bad connection): it is not judged here, and the replication gate decides"
			if withBanner {
				if !slices.Contains(banners, want) {
					t.Errorf("banners = %q, want %q", banners, want)
				}
			} else if !w.warned(want) {
				t.Errorf("no warning %q in the log", want)
			}
		})
	}
}

// TestAMemberSourceWithNoGroupServerGetsABanner: an ONLINE member source, replication enabled and no `type: group`
// server listed gets one banner, for an archive and a purge, whatever is skipped; every other case gets none.
func TestAMemberSourceWithNoGroupServerGetsABanner(t *testing.T) {
	const want = "WARNING: replication: the source is a Group Replication member and no server of type group is monitored: the group's backlog does not hold this job"
	for _, tc := range []struct {
		name   string
		typ    jobspec.Type
		setup  func(w *world)
		banner bool
	}{
		{"an ONLINE member, archive", jobspec.Archive, func(w *world) { w.asMember("ONLINE", "PRIMARY") }, true},
		{"an ONLINE member, purge", jobspec.Purge, func(w *world) { w.asMember("ONLINE", "PRIMARY") }, true},
		{"an ONLINE secondary", jobspec.Archive, func(w *world) { w.asMember("ONLINE", "SECONDARY") }, true},
		{"under a skip flag", jobspec.Archive, func(w *world) { w.asMember("ONLINE", "PRIMARY"); w.cfg.SkipPolicy = true }, true},
		{"a group server is listed", jobspec.Archive, func(w *world) {
			w.asMember("ONLINE", "PRIMARY")
			w.fleet([]string{"async", "group"}, map[int]any{1: group("ACTIVE", groupName)})
		}, false},
		{"not an ONLINE member", jobspec.Archive, func(w *world) { w.asMember("RECOVERING", "SECONDARY") }, false},
		{"a plain source", jobspec.Archive, func(*world) {}, false},
		{"replication disabled", jobspec.Archive, func(w *world) { w.asMember("ONLINE", "PRIMARY"); w.cfg.Replication.Enabled = false }, false},
		{"copy-only", jobspec.CopyOnly, func(w *world) { w.asMember("ONLINE", "PRIMARY") }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, tc.typ)
			w.fleet([]string{"async"}, nil)
			tc.setup(w)
			var banners []string
			w.cfg.Banner = func(b string) { banners = append(banners, b) }
			w.mustPass()
			n := 0
			for _, b := range banners {
				if b == want {
					n++
				}
			}
			if tc.banner && n != 1 || !tc.banner && n != 0 {
				t.Errorf("the banner was given %d times (banners %q), want %v", n, banners, tc.banner)
			}
		})
	}
}

// TestReplicationStagesByJobType: the two stages are enabled for an archive and a purge with replication on, the
// check is a policy check and the banner is not, and neither is on for copy-only or a disabled block.
func TestReplicationStagesByJobType(t *testing.T) {
	on := config.ReplicationConfig{Enabled: true}
	has := func(cfg Config, stage string) bool { return slices.Contains(enabledStages(cfg), stage) }
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		cfg := Config{Spec: &jobspec.Spec{Type: typ}, Replication: on}
		if !has(cfg, "ReplicationType") || !has(cfg, "ReplicationBanner") {
			t.Errorf("%s with replication: stages %v, want both", typ, enabledStages(cfg))
		}
		if i, j := slices.Index(enabledStages(cfg), "GroupFacts"), slices.Index(enabledStages(cfg), "ReplicationType"); !(i >= 0 && j > i) {
			t.Errorf("%s: ReplicationType must come after GroupFacts, whose facts it compares with (stages %v)", typ, enabledStages(cfg))
		}
		cfg.SkipPolicy = true
		if has(cfg, "ReplicationType") || !has(cfg, "ReplicationBanner") {
			t.Errorf("%s under a skip flag: stages %v, want the banner and not the check", typ, enabledStages(cfg))
		}
		cfg = Config{Spec: &jobspec.Spec{Type: typ}}
		if has(cfg, "ReplicationType") || has(cfg, "ReplicationBanner") {
			t.Errorf("%s with replication off: stages %v, want neither", typ, enabledStages(cfg))
		}
	}
	cfg := Config{Spec: &jobspec.Spec{Type: jobspec.CopyOnly}, Replication: on}
	if has(cfg, "ReplicationType") || has(cfg, "ReplicationBanner") {
		t.Errorf("copy-only: stages %v, want neither", enabledStages(cfg))
	}
}
