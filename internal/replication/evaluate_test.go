package replication

import (
	"database/sql"
	"reflect"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
)

// ch builds one channel snapshot entry. The empty name is the default channel,
// which is a name rather than an absence.
func ch(name, io, sqlr string, lag int64, lagValid bool) dbsrepl.ChannelStatus {
	return dbsrepl.ChannelStatus{
		ChannelName:         name,
		IORunning:           io,
		SQLRunning:          sqlr,
		SecondsBehindSource: sql.NullInt64{Int64: lag, Valid: lagValid},
	}
}

// problemChecks reduces a result to its check IDs, in order, so a case can pin
// both membership and assembly order.
func problemChecks(ps []problem) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.check)
	}

	return out
}

func TestEvaluate(t *testing.T) {
	healthy := ch("", "Yes", "Yes", 0, true)

	cases := []struct {
		name      string
		snapshot  []dbsrepl.ChannelStatus
		selected  []string
		tolerance int64
		want      []string
	}{
		{
			name:      "1 unconfigured server fails closed",
			snapshot:  nil,
			selected:  nil,
			tolerance: 10,
			want:      []string{dbsrepl.IDReplicationConfigured},
		},
		{
			name:      "2 one healthy channel passes",
			snapshot:  []dbsrepl.ChannelStatus{healthy},
			selected:  nil,
			tolerance: 10,
			want:      []string{},
		},
		{
			name: "3 all-channel gating catches the stopped one",
			snapshot: []dbsrepl.ChannelStatus{
				ch("main", "Yes", "Yes", 0, true),
				ch("billing", "Yes", "No", 0, true),
			},
			selected:  nil,
			tolerance: 10,
			want:      []string{dbsrepl.IDReplicationChannelsRunning},
		},
		{
			name: "4 selection excludes the lagging channel",
			snapshot: []dbsrepl.ChannelStatus{
				ch("main", "Yes", "Yes", 0, true),
				ch("billing", "Yes", "Yes", 100, true),
			},
			selected:  []string{"main"},
			tolerance: 10,
			want:      []string{},
		},
		{
			name:      "5 one configured name absent",
			snapshot:  []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 0, true)},
			selected:  []string{"main", "billing"},
			tolerance: 10,
			want:      []string{checkChannelNotFound},
		},
		{
			name:      "6 sole configured name absent emits both",
			snapshot:  []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 0, true)},
			selected:  []string{"typo"},
			tolerance: 10,
			want:      []string{checkChannelNotFound, dbsrepl.IDReplicationConfigured},
		},
		{
			name: "7 default channel selected, named one stopped and ignored",
			snapshot: []dbsrepl.ChannelStatus{
				ch("", "Yes", "Yes", 0, true),
				ch("named", "Yes", "No", 0, true),
			},
			selected:  []string{""},
			tolerance: 10,
			want:      []string{},
		},
		{
			name:      "8a lag equal to tolerance passes",
			snapshot:  []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 10, true)},
			selected:  nil,
			tolerance: 10,
			want:      []string{},
		},
		{
			name:      "8b lag above tolerance fails",
			snapshot:  []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 11, true)},
			selected:  nil,
			tolerance: 10,
			want:      []string{dbsrepl.IDSecondsBehindSourceWithin},
		},
		{
			name:      "9a zero tolerance is exact, lag 0 passes",
			snapshot:  []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 0, true)},
			selected:  nil,
			tolerance: 0,
			want:      []string{},
		},
		{
			name:      "9b zero tolerance is exact, lag 1 fails",
			snapshot:  []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 1, true)},
			selected:  nil,
			tolerance: 0,
			want:      []string{dbsrepl.IDSecondsBehindSourceWithin},
		},
		{
			name:      "10 unknown lag fails closed even with running threads",
			snapshot:  []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 0, false)},
			selected:  nil,
			tolerance: 10,
			want:      []string{dbsrepl.IDSecondsBehindSourceWithin},
		},
		{
			name:      "11 Connecting is a failing receiver state",
			snapshot:  []dbsrepl.ChannelStatus{ch("", "Connecting", "Yes", 0, true)},
			selected:  nil,
			tolerance: 10,
			want:      []string{dbsrepl.IDReplicationChannelsRunning},
		},
		{
			name:      "12 every absent name, then the configured check",
			snapshot:  []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 0, true)},
			selected:  []string{"x", "y"},
			tolerance: 10,
			want: []string{
				checkChannelNotFound,
				checkChannelNotFound,
				dbsrepl.IDReplicationConfigured,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := problemChecks(evaluate(tc.snapshot, tc.selected, tc.tolerance))
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("evaluate checks = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// Case 3's pin (#21): the finding names the channel that is actually stopped,
// not the first one in the snapshot.
func TestEvaluateNamesTheStoppedChannel(t *testing.T) {
	snapshot := []dbsrepl.ChannelStatus{
		ch("main", "Yes", "Yes", 0, true),
		ch("billing", "Yes", "No", 0, true),
	}

	ps := evaluate(snapshot, nil, 10)

	if len(ps) != 1 {
		t.Fatalf("evaluate returned %d problems, want 1", len(ps))
	}
	if len(ps[0].channels) != 1 || ps[0].channels[0] != "billing" {
		t.Errorf("channels = %#v, want [\"billing\"]", ps[0].channels)
	}
}

// Case 5's pin: the missing-name problem comes first, carries the requested
// name and everything the server observed, and the library checks still run on
// the surviving channel.
func TestEvaluateChannelNotFoundFacts(t *testing.T) {
	snapshot := []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 0, true)}

	ps := evaluate(snapshot, []string{"main", "billing"}, 10)

	if len(ps) != 1 {
		t.Fatalf("evaluate returned %d problems, want 1", len(ps))
	}
	if ps[0].check != checkChannelNotFound {
		t.Fatalf("first check = %q, want %q", ps[0].check, checkChannelNotFound)
	}
	facts, ok := ps[0].facts.(channelNotFoundFacts)
	if !ok {
		t.Fatalf("facts type = %T, want channelNotFoundFacts", ps[0].facts)
	}
	if facts.configured != "billing" {
		t.Errorf("facts.configured = %q, want %q", facts.configured, "billing")
	}
	if !reflect.DeepEqual(facts.observed, []string{"main"}) {
		t.Errorf("facts.observed = %#v, want [\"main\"]", facts.observed)
	}
}

// Case 12's pin: absent names are reported in `selected` order.
func TestEvaluateMissingNamesKeepSelectionOrder(t *testing.T) {
	snapshot := []dbsrepl.ChannelStatus{ch("main", "Yes", "Yes", 0, true)}

	ps := evaluate(snapshot, []string{"x", "y"}, 10)

	if len(ps) != 3 {
		t.Fatalf("evaluate returned %d problems, want 3", len(ps))
	}
	for i, want := range []string{"x", "y"} {
		facts, ok := ps[i].facts.(channelNotFoundFacts)
		if !ok {
			t.Fatalf("problem %d facts type = %T, want channelNotFoundFacts", i, ps[i].facts)
		}
		if facts.configured != want {
			t.Errorf("problem %d configured = %q, want %q", i, facts.configured, want)
		}
	}
}

// Case 13: the typed ChannelStatus payload survives conversion, so the
// renderer can show the last-error data the library captured.
func TestEvaluatePreservesChannelStatusFacts(t *testing.T) {
	stopped := ch("main", "Yes", "No", 0, true)
	stopped.LastSQLErrno = 1062
	stopped.LastSQLError = "dup"

	ps := evaluate([]dbsrepl.ChannelStatus{stopped}, nil, 10)

	if len(ps) != 1 {
		t.Fatalf("evaluate returned %d problems, want 1", len(ps))
	}
	facts, ok := ps[0].facts.(dbsrepl.ChannelStatus)
	if !ok {
		t.Fatalf("facts type = %T, want dbsrepl.ChannelStatus", ps[0].facts)
	}
	if facts.LastSQLError != "dup" {
		t.Errorf("facts.LastSQLError = %q, want %q", facts.LastSQLError, "dup")
	}
	if facts.LastSQLErrno != 1062 {
		t.Errorf("facts.LastSQLErrno = %d, want 1062", facts.LastSQLErrno)
	}
}
