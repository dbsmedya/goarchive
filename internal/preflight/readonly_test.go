package preflight

import (
	"slices"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// SOURCE_READ_ONLY_CHECK (G9) and DEST_READ_ONLY_CHECK (G13), spec §4.5 (GR C4). Both are judged on a side's
// ReplicationConfig, its first statement, before the plugin-state read a fenced new primary holds and before
// recovery replays anything; no flag skips either; neither names a cause (the role is C8's).

const sourceGeneral = "On a Group Replication group a secondary is read-only, and so is a newly elected primary until it has applied its backlog."

func sourceMessage(vars string) string {
	return "the source is read-only (" + vars + "). An archive or a purge deletes from the source, and its recovery replays deletes there, " +
		"so it is refused before anything is copied or replayed. " + sourceGeneral
}

func destinationMessage(vars string) string {
	return "the destination is read-only (" + vars + "). Every job writes to the destination: its data, or its tracking tables, so it is refused " +
		"before anything is read from it. " + sourceGeneral
}

// TestSourceReadOnlyRefusesAnArchiveAndAPurge: either variable refuses the job before anything else is read,
// and the message says which was on. It is judged under SkipPolicy too.
func TestSourceReadOnlyRefusesAnArchiveAndAPurge(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		for _, tc := range []struct {
			name string
			cfg  dbsrepl.Config
			vars string
		}{
			{"read_only", dbsrepl.Config{ReadOnly: true}, "read_only is ON"},
			{"super_read_only", dbsrepl.Config{SuperReadOnly: true}, "super_read_only is ON"},
			{"both", dbsrepl.Config{ReadOnly: true, SuperReadOnly: true}, "read_only and super_read_only are ON"},
		} {
			for _, skip := range []bool{false, true} {
				t.Run(string(typ)+"/"+tc.name+"/skip="+map[bool]string{false: "no", true: "yes"}[skip], func(t *testing.T) {
					w := healthy(t, typ)
					w.cfg.SkipPolicy = skip
					w.src.config = tc.cfg
					order := w.ordered()
					e := w.mustFail("SOURCE_READ_ONLY_CHECK")
					if want := sourceMessage(tc.vars); e.Message != want {
						t.Errorf("message:\n got %s\nwant %s", e.Message, want)
					}
					if !slices.Equal(*order, []string{"src.ReplicationConfig"}) {
						t.Errorf("reads = %v, want only the source's configuration: the refusal comes before anything else is read", *order)
					}
				})
			}
		}
	}
}

// TestACopyOnlyRunMayReadAReadOnlySource: copy-only reads the source and never writes it, so a read-only source
// is no reason to refuse it (a secondary or a replica is a use the docs describe). Its facts are still read.
func TestACopyOnlyRunMayReadAReadOnlySource(t *testing.T) {
	w := healthy(t, jobspec.CopyOnly)
	w.src.config = dbsrepl.Config{ReadOnly: true, SuperReadOnly: true}
	res := w.mustPass()
	if !res.Group.SourceConfig.SuperReadOnly {
		t.Errorf("the source's configuration = %+v, want it returned as read", res.Group.SourceConfig)
	}
}

// TestDestinationReadOnlyRefusesEveryJobType: super_read_only on the destination refuses an archive, a purge
// and a copy-only run, before anything else is read from the destination; read_only alone stays as it is.
func TestDestinationReadOnlyRefusesEveryJobType(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		for _, skip := range []bool{false, true} {
			t.Run(string(typ)+"/skip="+map[bool]string{false: "no", true: "yes"}[skip], func(t *testing.T) {
				w := healthy(t, typ)
				w.cfg.SkipPolicy = skip
				w.dst.config = dbsrepl.Config{ReadOnly: true, SuperReadOnly: true}
				order := w.ordered()
				e := w.mustFail("DEST_READ_ONLY_CHECK")
				if want := destinationMessage("read_only and super_read_only are ON"); e.Message != want {
					t.Errorf("message:\n got %s\nwant %s", e.Message, want)
				}
				if want := []string{"src.ReplicationConfig", "dst.ReplicationConfig"}; !slices.Equal(*order, want) {
					t.Errorf("reads = %v, want %v: the destination's first statement is its configuration, and nothing else follows a refusal", *order, want)
				}
			})
		}
		t.Run(string(typ)+"/read_only alone is today's outcome", func(t *testing.T) {
			w := healthy(t, typ)
			w.dst.config = dbsrepl.Config{ReadOnly: true}
			w.mustPass()
		})
	}
	t.Run("only super_read_only is named when only it is on", func(t *testing.T) {
		w := healthy(t, jobspec.Archive)
		w.dst.config = dbsrepl.Config{SuperReadOnly: true}
		if e := w.mustFail("DEST_READ_ONLY_CHECK"); e.Message != destinationMessage("super_read_only is ON") {
			t.Errorf("message = %s", e.Message)
		}
	})
}

// TestTheSourceIsJudgedBeforeTheDestination: a pair that is read-only on both sides is refused for the source,
// and the destination's configuration is not read for a job that is already refused.
func TestTheSourceIsJudgedBeforeTheDestination(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.src.config = dbsrepl.Config{ReadOnly: true, SuperReadOnly: true}
	w.dst.config = dbsrepl.Config{ReadOnly: true, SuperReadOnly: true}
	order := w.ordered()
	w.refused("SOURCE_READ_ONLY_CHECK")
	if slices.Contains(*order, "dst.ReplicationConfig") {
		t.Errorf("reads = %v, want the destination left unread after the source's refusal", *order)
	}
}

// TestAReadOnlyMemberSourceWithoutTheViewGrantIsRefusedForBeingReadOnly (architect, after C2): G9 is judged on the
// first read, before the view is asked for, so a read-only member source whose account lacks the grant gets
// SOURCE_READ_ONLY_CHECK. The permission check is what a writable member source and a copy-only job get.
func TestAReadOnlyMemberSourceWithoutTheViewGrantIsRefusedForBeingReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typ      jobspec.Type
		readOnly bool
		want     string
	}{
		{"an archive on a read-only member", jobspec.Archive, true, "SOURCE_READ_ONLY_CHECK"},
		{"a purge on a read-only member", jobspec.Purge, true, "SOURCE_READ_ONLY_CHECK"},
		{"a copy-only job on a read-only member", jobspec.CopyOnly, true, "SOURCE_GROUP_VIEW_PERMISSION_CHECK"},
		{"an archive on a writable member", jobspec.Archive, false, "SOURCE_GROUP_VIEW_PERMISSION_CHECK"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := healthy(t, tc.typ)
			w.asMember("ONLINE", "SECONDARY")
			w.src.config = dbsrepl.Config{ReadOnly: tc.readOnly, SuperReadOnly: tc.readOnly}
			w.src.membersErr = deniedView()
			e := w.mustFail(tc.want)
			if tc.want == "SOURCE_READ_ONLY_CHECK" && slices.Contains(w.src.calls, "GroupMembers") {
				t.Errorf("the view was read before the read-only refusal (calls %v)", w.src.calls)
			}
			if strings.Contains(e.Message, "performance_schema") && tc.want == "SOURCE_READ_ONLY_CHECK" {
				t.Errorf("the read-only refusal names the grant: %s", e.Message)
			}
		})
	}
}

// TestReadOnlyMessagesNameNoCause: the messages say which variable is on and repeat the general sentence; they
// do not say whether the server is a secondary, a newly elected primary or an operator's choice (the cause from
// the role is C8's).
func TestReadOnlyMessagesNameNoCause(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.asMember("ONLINE", "SECONDARY")
	w.src.config = dbsrepl.Config{ReadOnly: true, SuperReadOnly: true}
	e := w.mustFail("SOURCE_READ_ONLY_CHECK")
	for _, claim := range []string{"is a SECONDARY", "is the PRIMARY", "the primary is ", "was newly elected", "an operator made"} {
		if strings.Contains(e.Message, claim) {
			t.Errorf("the message names a cause (%q): %s", claim, e.Message)
		}
	}
	if !strings.Contains(e.Message, sourceGeneral) {
		t.Errorf("the message lacks the general sentence: %s", e.Message)
	}
}
