package preflight

import (
	"slices"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// GROUP_MULTI_PRIMARY_CHECK (G10), spec §4.6 and §8's *Multi-primary, unit* (GR C7). The check judges the library's mode
// fact on a side whose plugin is ACTIVE with a group name, the source and the destination alike: ON passes; OFF, no value
// and any other text are refused for every job type, and --allow-multi-primary covers every refusal and says which case
// it covered. No role in the view is consulted, and no flag but the bypass skips it.

const (
	multiWhy = "GoArchive serializes runs with advisory locks, and an advisory lock is local to one member, so in multi-primary mode " +
		"two runs connected to different members are not serialized. GoArchive 3.1 supports single-primary groups."
	multiRemedy = "Run the job against a single-primary group, or accept this with --allow-multi-primary (command line only)."
	multiRetry  = "Run the job once the mode can be read, or accept this with --allow-multi-primary (command line only)."
)

// refusal is the message of a refused case: the case's words, then why, then the way out.
func refusal(what, remedy string) string { return what + ". " + multiWhy + " " + remedy }

// banner is the bypass's banner for a case.
func bannerOf(what string) string {
	return "WARNING: --allow-multi-primary: " + what + ". Advisory locks are local to one member, so two runs connected to " +
		"different members are not serialized; GoArchive 3.1 supports single-primary groups."
}

// The cases' words, by side.
func runningOff() string {
	return "the source is a member of Group Replication group " + groupName + ", and the group is in multi-primary mode (group_replication_single_primary_mode is OFF)"
}
func configuredOff(side string) string {
	return "the " + side + " is configured for multi-primary mode (group_replication_single_primary_mode is OFF)"
}
func unreadable(side string) string {
	return "the " + side + "'s group mode could not be read: the server returned no value for group_replication_single_primary_mode"
}
func otherText(side, text string) string {
	return "the " + side + "'s group mode could not be read: group_replication_single_primary_mode is \"" + text + "\", which is neither ON nor OFF"
}

// withMode sets a side's mode fact.
func withMode(g dbsrepl.GroupReplication, mode string, known bool) dbsrepl.GroupReplication {
	g.SinglePrimaryMode, g.SinglePrimaryModeKnown = mode, known
	return g
}

// destGroup is a destination configured for the group, in the given mode.
func destGroup(mode string, known bool) dbsrepl.GroupReplication {
	return withMode(dbsrepl.GroupReplication{PluginStatus: "ACTIVE", GroupName: groupName,
		TransactionSizeLimit: 64000000, TransactionSizeLimitKnown: true}, mode, known)
}

var jobTypes = []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly}

// TestMultiPrimaryTheFourOutcomes: the table of spec §4.6 from the fact's values, on the source and on the
// destination, for every job type. A mode that is known and is neither ON nor OFF is refused with the
// "could not be read" case, naming the text as the server spelled it.
func TestMultiPrimaryTheFourOutcomes(t *testing.T) {
	for _, typ := range jobTypes {
		for _, tc := range []struct {
			name   string
			setup  func(w *world)
			refuse string // the refusal's words, "" for a pass
		}{
			{"the source, known ON", func(w *world) { w.asMember("ONLINE", "PRIMARY") }, ""},
			{"the source, known OFF", func(w *world) { w.asMember("ONLINE", "PRIMARY"); w.src.group = withMode(w.src.group, "OFF", true) },
				refusal(runningOff(), multiRemedy)},
			{"the source, no value", func(w *world) { w.asMember("ONLINE", "PRIMARY"); w.src.group = withMode(w.src.group, "", false) },
				refusal(unreadable("source"), multiRetry)},
			{"the source, another text", func(w *world) { w.asMember("ONLINE", "PRIMARY"); w.src.group = withMode(w.src.group, "1", true) },
				refusal(otherText("source", "1"), multiRetry)},
			{"the source, the text in another case", func(w *world) { w.asMember("ONLINE", "PRIMARY"); w.src.group = withMode(w.src.group, "on", true) },
				refusal(otherText("source", "on"), multiRetry)},
			{"the source, an empty text that is known", func(w *world) { w.asMember("ONLINE", "PRIMARY"); w.src.group = withMode(w.src.group, "", true) },
				refusal(otherText("source", ""), multiRetry)},
			{"the destination, known ON", func(w *world) { w.dst.group = destGroup("ON", true) }, ""},
			{"the destination, known OFF", func(w *world) { w.dst.group = destGroup("OFF", true) },
				refusal(configuredOff("destination"), multiRemedy)},
			{"the destination, no value", func(w *world) { w.dst.group = destGroup("", false) },
				refusal(unreadable("destination"), multiRetry)},
			{"the destination, another text", func(w *world) { w.dst.group = destGroup("ENABLED", true) },
				refusal(otherText("destination", "ENABLED"), multiRetry)},
		} {
			t.Run(string(typ)+"/"+tc.name, func(t *testing.T) {
				w := healthy(t, typ)
				tc.setup(w)
				if tc.refuse == "" {
					w.mustPass()
					return
				}
				if e := w.mustFail("GROUP_MULTI_PRIMARY_CHECK"); e.Message != tc.refuse {
					t.Errorf("message:\n got %s\nwant %s", e.Message, tc.refuse)
				}
			})
		}
	}
}

// TestMultiPrimaryJudgesOnlyAPluginThatIsActiveWithAName: a side whose plugin is not ACTIVE, or has no group
// name, is not in a group whatever its mode variable says (the variable can be registered while a plugin
// installs), so nothing is judged and nothing is refused.
func TestMultiPrimaryJudgesOnlyAPluginThatIsActiveWithAName(t *testing.T) {
	for _, tc := range []struct {
		name string
		gr   dbsrepl.GroupReplication
	}{
		{"no plugin", dbsrepl.GroupReplication{}},
		{"disabled", dbsrepl.GroupReplication{PluginStatus: "DISABLED", SinglePrimaryMode: "OFF", SinglePrimaryModeKnown: true}},
		{"inactive with a name", dbsrepl.GroupReplication{PluginStatus: "INACTIVE", GroupName: groupName, SinglePrimaryMode: "OFF", SinglePrimaryModeKnown: true}},
		{"active with no name", dbsrepl.GroupReplication{PluginStatus: "ACTIVE", SinglePrimaryMode: "OFF", SinglePrimaryModeKnown: true}},
		{"active with no name and no mode", dbsrepl.GroupReplication{PluginStatus: "ACTIVE"}},
	} {
		for _, side := range []string{"source", "destination"} {
			t.Run(tc.name+"/"+side, func(t *testing.T) {
				w := healthy(t, jobspec.Archive)
				if side == "source" {
					w.src.group = tc.gr
				} else {
					w.dst.group = tc.gr
				}
				w.mustPass()
			})
		}
	}
}

// TestMultiPrimaryTheWordsNameWhatIsKnown (spec §4.6, rev 5.3): on a source whose own row is ONLINE the refusal
// says that the group is in multi-primary mode; on a source that is not a running member (a row of another state, or no
// row of its own) and on every destination it says that the server is configured for it, and no more.
func TestMultiPrimaryTheWordsNameWhatIsKnown(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(w *world)
		what  string
	}{
		{"an ONLINE own row", func(w *world) { w.asMember("ONLINE", "PRIMARY") }, runningOff()},
		{"an own row RECOVERING", func(w *world) { w.asMember("RECOVERING", "SECONDARY") }, configuredOff("source")},
		{"an own row OFFLINE", func(w *world) { w.asMember("OFFLINE", "") }, configuredOff("source")},
		{"an own row in ERROR", func(w *world) { w.asMember("ERROR", "") }, configuredOff("source")},
		{"no row of its own", func(w *world) { w.asMember("ONLINE", "PRIMARY"); w.src.members = w.src.members[1:] }, configuredOff("source")},
		{"an empty view", func(w *world) { w.asMember("ONLINE", "PRIMARY"); w.src.members = nil }, configuredOff("source")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, jobspec.Archive)
			tc.setup(w)
			w.src.group = withMode(w.src.group, "OFF", true)
			if e := w.mustFail("GROUP_MULTI_PRIMARY_CHECK"); e.Message != refusal(tc.what, multiRemedy) {
				t.Errorf("message:\n got %s\nwant %s", e.Message, refusal(tc.what, multiRemedy))
			}
		})
	}
}

// TestMultiPrimaryConsultsNoRole: the mode is the fact, never a count of primaries. A view with several PRIMARY rows
// and the mode ON passes (the roles of a peer returning mid-run change nothing), and a view with one PRIMARY and the
// mode OFF is refused (a multi-primary group with one member ONLINE and a peer recovering shows one primary).
func TestMultiPrimaryConsultsNoRole(t *testing.T) {
	two := func(w *world) {
		w.asMember("ONLINE", "PRIMARY")
		w.src.members = append(w.src.members, dbsrepl.GroupMember{MemberID: "33333333-3333-4333-8333-333333333333", Host: "gr3", State: "ONLINE", Role: "PRIMARY"})
		w.src.members[1].Role = "PRIMARY"
	}
	t.Run("several PRIMARY rows, mode ON", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		two(w)
		w.mustPass()
	})
	t.Run("a lone PRIMARY row, mode OFF", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.asMember("ONLINE", "PRIMARY")
		w.src.members = w.src.members[:1]
		w.src.group = withMode(w.src.group, "OFF", true)
		w.refused("GROUP_MULTI_PRIMARY_CHECK")
	})
	t.Run("a SECONDARY own row, mode OFF", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.asMember("ONLINE", "SECONDARY")
		w.src.group = withMode(w.src.group, "OFF", true)
		if e := w.mustFail("GROUP_MULTI_PRIMARY_CHECK"); e.Message != refusal(runningOff(), multiRemedy) {
			t.Errorf("message = %s", e.Message)
		}
	})
	t.Run("a PRIMARY own row, mode ON, one peer RECOVERING", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.asMember("ONLINE", "PRIMARY")
		w.src.members[1].State = "RECOVERING"
		w.mustPass()
	})
}

// TestMultiPrimaryIsJudgedUnderEverySkipPolicy: --skip-dangerously-validate-preflight-allow-experimental skips the
// policy checks, not this one. The one bypass is --allow-multi-primary, so a second silent opt-out does not exist.
func TestMultiPrimaryIsJudgedUnderEverySkipPolicy(t *testing.T) {
	for _, typ := range jobTypes {
		t.Run(string(typ), func(t *testing.T) {
			w := healthy(t, typ)
			w.cfg.SkipPolicy = true
			w.asMember("ONLINE", "PRIMARY")
			w.src.group = withMode(w.src.group, "OFF", true)
			w.refused("GROUP_MULTI_PRIMARY_CHECK")
		})
	}
}

// TestMultiPrimaryRefusesBeforeAnyTableRead: the check runs on the facts read first, so nothing but the group
// set is read before the refusal, and the source is judged before the destination.
func TestMultiPrimaryRefusesBeforeAnyTableRead(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.asMember("ONLINE", "PRIMARY")
	w.src.group = withMode(w.src.group, "OFF", true)
	w.dst.group = destGroup("OFF", true)
	order := w.ordered()
	e := w.mustFail("GROUP_MULTI_PRIMARY_CHECK")
	if e.Message != refusal(runningOff(), multiRemedy) {
		t.Errorf("both sides are multi-primary: the refusal names %q, want the source's first", e.Message)
	}
	if !slices.Equal(*order, groupCalls) {
		t.Errorf("reads = %v, want only the group set %v", *order, groupCalls)
	}
}

// TestMultiPrimaryTheBypassCoversEveryRefusalAndNamesTheCase: --allow-multi-primary passes each refused case, once
// per refused side, with a banner that says which case it covered; it says nothing when nothing was refused.
func TestMultiPrimaryTheBypassCoversEveryRefusalAndNamesTheCase(t *testing.T) {
	for _, typ := range jobTypes {
		for _, tc := range []struct {
			name  string
			setup func(w *world)
			want  []string
		}{
			{"a running member in multi-primary mode", func(w *world) {
				w.asMember("ONLINE", "PRIMARY")
				w.src.group = withMode(w.src.group, "OFF", true)
			}, []string{bannerOf(runningOff())}},
			{"a source configured multi-primary and not running", func(w *world) {
				w.asMember("OFFLINE", "")
				w.src.group = withMode(w.src.group, "OFF", true)
			}, []string{bannerOf(configuredOff("source"))}},
			{"a source whose mode has no value", func(w *world) {
				w.asMember("ONLINE", "PRIMARY")
				w.src.group = withMode(w.src.group, "", false)
			}, []string{bannerOf(unreadable("source"))}},
			{"a source whose mode is another text", func(w *world) {
				w.asMember("ONLINE", "PRIMARY")
				w.src.group = withMode(w.src.group, "2", true)
			}, []string{bannerOf(otherText("source", "2"))}},
			{"a destination configured multi-primary", func(w *world) { w.dst.group = destGroup("OFF", true) },
				[]string{bannerOf(configuredOff("destination"))}},
			{"a destination whose mode has no value", func(w *world) { w.dst.group = destGroup("", false) },
				[]string{bannerOf(unreadable("destination"))}},
			{"both sides", func(w *world) {
				w.asMember("ONLINE", "PRIMARY")
				w.src.group = withMode(w.src.group, "OFF", true)
				w.dst.group = destGroup("OFF", true)
			}, []string{bannerOf(runningOff()), bannerOf(configuredOff("destination"))}},
			{"a single-primary group, nothing refused", func(w *world) { w.asMember("ONLINE", "PRIMARY") }, nil},
			{"a plain pair, nothing refused", func(*world) {}, nil},
		} {
			t.Run(string(typ)+"/"+tc.name, func(t *testing.T) {
				w := healthy(t, typ)
				tc.setup(w)
				w.cfg.AllowMultiPrimary = true
				var got []string
				w.cfg.Banner = func(b string) { got = append(got, b) }
				w.mustPass()
				if !slices.Equal(got, tc.want) {
					t.Errorf("banners:\n got %q\nwant %q", got, tc.want)
				}
			})
		}
	}
}

// TestMultiPrimaryTheBypassWithoutABannerFuncLogsAWarning: a caller with no Banner (a test, an embedding) still gets the
// warning, in the log, as the other preflight warnings do.
func TestMultiPrimaryTheBypassWithoutABannerFuncLogsAWarning(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.asMember("ONLINE", "PRIMARY")
	w.src.group = withMode(w.src.group, "OFF", true)
	w.cfg.AllowMultiPrimary = true
	w.mustPass()
	if !w.warned(bannerOf(runningOff())) {
		t.Errorf("no warning %q was logged", bannerOf(runningOff()))
	}
}

// TestMultiPrimaryTheBypassIsNotARefusalOfAnythingElse: with the bypass the run still fails a check that has
// nothing to do with the mode, and the mode does not hide it.
func TestMultiPrimaryTheBypassIsNotARefusalOfAnythingElse(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.asMember("ONLINE", "PRIMARY")
	w.src.group = withMode(w.src.group, "OFF", true)
	w.cfg.AllowMultiPrimary = true
	w.src.config.ReadOnly = true
	if e := w.mustFail("SOURCE_READ_ONLY_CHECK"); !strings.Contains(e.Message, "read-only") {
		t.Errorf("message = %s", e.Message)
	}
}
