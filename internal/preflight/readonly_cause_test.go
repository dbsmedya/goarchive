package preflight

import (
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// G9's cause (spec §4.5's four-row table) and copy-only's warning (GR C8). With v1.6.0 the plugin-state read passes a
// fence, so a refused read-only source can be asked why: the source's own row in its group's view says whether it is a
// secondary, or the primary that is still applying its backlog or was made read-only. The refusal is issued either way.

func groupRows(self dbsrepl.GroupMember, peers ...dbsrepl.GroupMember) []dbsrepl.GroupMember {
	return append([]dbsrepl.GroupMember{self}, peers...)
}

func member(id, host string, port int64, state, role string, self bool) dbsrepl.GroupMember {
	return dbsrepl.GroupMember{ChannelName: "group_replication_applier", MemberID: id, Host: host, Port: sql.NullInt64{Int64: port, Valid: port > 0},
		State: state, Role: role, Self: self}
}

const (
	secondaryCause = "The source is a SECONDARY of group " + groupName + "; the primary is gr2.internal:3306."
	primaryCause   = "The source is the PRIMARY of group " + groupName + " and is read-only: it was newly elected and is still applying its backlog, " +
		"or an operator made it read-only. Run again when it is writable."
)

// TestAReadOnlySourceIsToldItsCauseFromItsRole: the four rows of the table, whole.
func TestAReadOnlySourceIsToldItsCauseFromItsRole(t *testing.T) {
	const vars = "super_read_only is ON"
	base := "the source is read-only (" + vars + "). An archive or a purge deletes from the source, and its recovery replays deletes there, " +
		"so it is refused before anything is copied or replayed. "
	active := dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName, TransactionSizeLimit: 150000000, TransactionSizeLimitKnown: true}
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		for _, tc := range []struct {
			name     string
			group    dbsrepl.GroupReplication
			members  []dbsrepl.GroupMember
			err      error
			want     string
			viewRead bool
		}{
			{"a SECONDARY names the primary", active,
				groupRows(member(sourceUUID, "gr1.internal", 3306, "ONLINE", "SECONDARY", true), member(otherUUID, "gr2.internal", 3306, "ONLINE", "PRIMARY", false)),
				nil, base + secondaryCause, true},
			{"a PRIMARY that is read-only gets both explanations", active,
				groupRows(member(sourceUUID, "gr1.internal", 3306, "ONLINE", "PRIMARY", true), member(otherUUID, "gr2.internal", 3306, "ONLINE", "SECONDARY", false)),
				nil, base + primaryCause, true},
			{"a SECONDARY with no primary in its view still says what it is", active,
				groupRows(member(sourceUUID, "gr1.internal", 3306, "ONLINE", "SECONDARY", true)),
				nil, base + "The source is a SECONDARY of group " + groupName + ".", true},
			{"a primary whose port is NULL is named by its host", active,
				groupRows(member(sourceUUID, "gr1.internal", 3306, "ONLINE", "SECONDARY", true), member(otherUUID, "gr2.internal", 0, "ONLINE", "PRIMARY", false)),
				nil, base + "The source is a SECONDARY of group " + groupName + "; the primary is gr2.internal.", true},
			{"a plain read-only server gets the plain message and its view is not read", dbsrepl.GroupReplication{PluginStatus: ""},
				nil, nil, base + sourceGeneral, false},
			{"a plugin that is not ACTIVE gets the plain message", dbsrepl.GroupReplication{PluginStatus: "DISABLED"},
				nil, nil, base + sourceGeneral, false},
			{"a member that is not ONLINE has no role to name", active,
				groupRows(member(sourceUUID, "gr1.internal", 3306, "RECOVERING", "", true), member(otherUUID, "gr2.internal", 3306, "ONLINE", "PRIMARY", false)),
				nil, base + sourceGeneral, true},
			{"no row of its own in its view", active,
				groupRows(member(otherUUID, "gr2.internal", 3306, "ONLINE", "PRIMARY", false)),
				nil, base + sourceGeneral, true},
			{"a view that cannot be read for another reason names no cause", active, nil, errors.New("connection reset"),
				base + sourceGeneral, true},
		} {
			t.Run(string(typ)+"/"+tc.name, func(t *testing.T) {
				w := healthy(t, typ)
				w.cfg.SourceServerUUID = sourceUUID
				w.src.group, w.src.members, w.src.membersErr = tc.group, tc.members, tc.err
				w.src.config = dbsrepl.Config{SuperReadOnly: true}
				e := w.mustFail("SOURCE_READ_ONLY_CHECK")
				if e.Message != tc.want {
					t.Errorf("message:\n got %s\nwant %s", e.Message, tc.want)
				}
				if got := slices.Contains(w.src.calls, "GroupMembers"); got != tc.viewRead {
					t.Errorf("the view was read = %v, want %v (calls %v)", got, tc.viewRead, w.src.calls)
				}
			})
		}
	}
}

// TestTheViewGrantIsNamedWhenTheCauseCannotBeRead: the fourth row. The grant is named, no cause is, and the refusal is
// the read-only one.
func TestTheViewGrantIsNamedWhenTheCauseCannotBeRead(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.asMember("ONLINE", "SECONDARY")
	w.src.config = dbsrepl.Config{ReadOnly: true, SuperReadOnly: true}
	w.src.membersErr = deniedView()
	e := w.mustFail("SOURCE_READ_ONLY_CHECK")
	if !strings.Contains(e.Message, "GRANT SELECT ON performance_schema.replication_group_members TO <user>") ||
		!strings.Contains(e.Message, "read_only and super_read_only are ON") || !strings.Contains(e.Message, sourceGeneral) {
		t.Errorf("message = %s, want the variables, the general sentence and the grant", e.Message)
	}
}

// TestADestinationIsNeverToldACause (G13): its view is not read, whatever the destination is, and no grant is added for a message.
func TestADestinationIsNeverToldACause(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.dst.group = dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName}
	w.dst.config = dbsrepl.Config{SuperReadOnly: true}
	e := w.mustFail("DEST_READ_ONLY_CHECK")
	if e.Message != destinationMessage("super_read_only is ON") {
		t.Errorf("message = %s", e.Message)
	}
	if slices.Contains(w.dst.calls, "GroupMembers") || slices.Contains(w.dst.calls, "GroupReplication") {
		t.Errorf("the destination was asked for more than its configuration: %v", w.dst.calls)
	}
}

// Copy-only's warning: a run whose source is PRIMARY in its own view and read-only. The server may hold its reads until the
// backlog is applied, so the run says so and how it is stopped, before its first table read; it is not refused.

// TestCopyOnlyIsWarnedWhenItsSourceIsAReadOnlyPrimary: one banner, naming the group and the variables, what the server may do,
// and the stop sequence; given before any table read and under SkipPolicy too; the run passes.
func TestCopyOnlyIsWarnedWhenItsSourceIsAReadOnlyPrimary(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(map[bool]string{false: "policy", true: "skip"}[skip], func(t *testing.T) {
			w := healthy(t, jobspec.CopyOnly)
			w.cfg.SkipPolicy = skip
			w.asMember("ONLINE", "PRIMARY")
			w.src.config = dbsrepl.Config{ReadOnly: true, SuperReadOnly: true}
			order := w.ordered()
			var banners []string
			w.cfg.Banner = func(b string) { banners = append(banners, b); *order = append(*order, "banner") }
			w.mustPass()
			if len(banners) != 1 {
				t.Fatalf("banners = %q, want one", banners)
			}
			for _, want := range []string{"WARNING:", "PRIMARY", groupName, "read_only and super_read_only are ON", "newly elected", "backlog",
				"wait_timeout", "two signals", "stop"} {
				if !strings.Contains(banners[0], want) {
					t.Errorf("the warning %q does not contain %q", banners[0], want)
				}
			}
			// the first read that is not one of the group facts is the first table or key read
			at := slices.Index(*order, "banner")
			firstRead := slices.IndexFunc(*order, func(c string) bool { return c != "banner" && !slices.Contains(groupCalls, c) })
			if at < 0 || firstRead < 0 || at > firstRead {
				t.Errorf("the warning came at %d, the first table read at %d (%v): it must come before it", at, firstRead, *order)
			}
		})
	}
}

// TestCopyOnlyIsNotWarnedWhereNothingIsHeld: a secondary, a writable primary, a plain read-only server and a source that is
// not an ONLINE member get no warning, and none is a refusal; an archive on the read-only primary is refused and not warned.
func TestCopyOnlyIsNotWarnedWhereNothingIsHeld(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(w *world)
	}{
		{"a SECONDARY, read-only", func(w *world) { w.asMember("ONLINE", "SECONDARY"); w.src.config = dbsrepl.Config{SuperReadOnly: true} }},
		{"a PRIMARY, writable", func(w *world) { w.asMember("ONLINE", "PRIMARY") }},
		{"a plain read-only server", func(w *world) { w.src.config = dbsrepl.Config{SuperReadOnly: true} }},
		{"a primary row that is not ONLINE", func(w *world) {
			w.asMember("RECOVERING", "PRIMARY")
			w.src.config = dbsrepl.Config{SuperReadOnly: true}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.CopyOnly)
			tc.setup(w)
			var banners []string
			w.cfg.Banner = func(b string) { banners = append(banners, b) }
			w.mustPass()
			if len(banners) != 0 {
				t.Errorf("banners = %q, want none", banners)
			}
		})
	}
	t.Run("an archive on a read-only primary is refused, not warned", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.asMember("ONLINE", "PRIMARY")
		w.src.config = dbsrepl.Config{SuperReadOnly: true}
		var banners []string
		w.cfg.Banner = func(b string) { banners = append(banners, b) }
		w.refused("SOURCE_READ_ONLY_CHECK")
		if len(banners) != 0 {
			t.Errorf("banners = %q, want none", banners)
		}
	})
}
