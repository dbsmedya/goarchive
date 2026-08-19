package replication

import (
	"database/sql"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"

	"github.com/dbsmedya/goarchive/internal/config"
)

// Case 1: fromFinding is lossless — Check, Message, Channels and the typed
// Facts payload all survive the conversion.
func TestProblemFromFindingPreservesFields(t *testing.T) {
	cs := dbsrepl.ChannelStatus{
		ChannelName:         "",
		IORunning:           "Yes",
		SQLRunning:          "Yes",
		SecondsBehindSource: sql.NullInt64{Int64: 42, Valid: true},
	}
	f := dbsrepl.Finding{
		Check:    dbsrepl.IDSecondsBehindSourceWithin,
		Message:  "channel lags",
		Channels: []string{""},
		Facts:    cs,
	}

	p := fromFinding(f)

	if p.check != dbsrepl.IDSecondsBehindSourceWithin {
		t.Errorf("check = %q, want %q", p.check, dbsrepl.IDSecondsBehindSourceWithin)
	}
	if p.message != "channel lags" {
		t.Errorf("message = %q, want %q", p.message, "channel lags")
	}
	if len(p.channels) != 1 || p.channels[0] != "" {
		t.Errorf("channels = %#v, want [\"\"]", p.channels)
	}
	got, ok := p.facts.(dbsrepl.ChannelStatus)
	if !ok {
		t.Fatalf("facts type = %T, want dbsrepl.ChannelStatus", p.facts)
	}
	if got != cs {
		t.Errorf("facts = %#v, want %#v", got, cs)
	}
}

// Case 2: the default channel is never rendered as an invisible empty string.
func TestProblemRenderChannel(t *testing.T) {
	if got := renderChannel(""); got != "<default>" {
		t.Errorf("renderChannel(\"\") = %q, want %q", got, "<default>")
	}
	if got := renderChannel("main"); got != "main" {
		t.Errorf("renderChannel(\"main\") = %q, want %q", got, "main")
	}
}

// A channel name is server-supplied or operator-supplied text like any other,
// and validation permits arbitrary content in it. It must not be able to split
// one log line into several.
func TestProblemRenderChannelNameSanitized(t *testing.T) {
	// The name carries both a newline and a carriage return.
	const forged = "billing\nforged\rsecond"
	const clean = "billing forged second"

	t.Run("renderChannel", func(t *testing.T) {
		if got := renderChannel(forged); got != clean {
			t.Errorf("renderChannel(%q) = %q, want %q", forged, got, clean)
		}
	})

	t.Run("running failure", func(t *testing.T) {
		p := problem{
			check: dbsrepl.IDReplicationChannelsRunning,
			facts: dbsrepl.ChannelStatus{ChannelName: forged, IORunning: "Yes", SQLRunning: "No"},
		}

		got := renderProblems([]problem{p}, 10)

		want := "channel " + clean + ": io=Yes sql=No"
		if got != want {
			t.Errorf("rendered %q, want %q", got, want)
		}
		if strings.ContainsAny(got, "\n\r") {
			t.Errorf("rendered %q, want a single physical line", got)
		}
	})

	t.Run("lag failure", func(t *testing.T) {
		p := problem{
			check: dbsrepl.IDSecondsBehindSourceWithin,
			facts: dbsrepl.ChannelStatus{
				ChannelName:         forged,
				SecondsBehindSource: sql.NullInt64{Int64: 99, Valid: true},
			},
		}

		got := renderProblems([]problem{p}, 10)

		want := "channel " + clean + ": lag=99s tolerance=10s"
		if got != want {
			t.Errorf("rendered %q, want %q", got, want)
		}
		if strings.ContainsAny(got, "\n\r") {
			t.Errorf("rendered %q, want a single physical line", got)
		}
	})

	// Both name sources at once: configured is the operator's, observed is the
	// server's.
	t.Run("missing channel, configured and observed", func(t *testing.T) {
		const forgedObserved = "main\r\nalso-forged"
		const cleanObserved = "main  also-forged"

		got := renderProblems([]problem{newChannelNotFound(forged, []string{forgedObserved})}, 10)

		want := "channel " + clean + " not found (server reports: " + cleanObserved + ")"
		if got != want {
			t.Errorf("rendered %q, want %q", got, want)
		}
		if strings.ContainsAny(got, "\n\r") {
			t.Errorf("rendered %q, want a single physical line", got)
		}
	})
}

// Case 3: a running failure renders receiver/applier states plus the non-empty
// last-error data, and omits the error pair that carries nothing.
func TestProblemRenderRunningFailure(t *testing.T) {
	cs := dbsrepl.ChannelStatus{
		ChannelName:  "main",
		IORunning:    "Yes",
		SQLRunning:   "No",
		LastSQLErrno: 1062,
		LastSQLError: "dup",
	}
	p := problem{check: dbsrepl.IDReplicationChannelsRunning, message: "not running", facts: cs}

	got := renderProblems([]problem{p}, 10)

	for _, want := range []string{"channel main:", "io=Yes", "sql=No", "last_sql_err=1062", `"dup"`} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered %q, want it to contain %q", got, want)
		}
	}
	if strings.Contains(got, "last_io_err") {
		t.Errorf("rendered %q, want no last_io_err pair (errno 0, empty message)", got)
	}
}

// Case 4: lag rendering carries the reported value or NULL, plus the
// configured tolerance.
func TestProblemRenderLag(t *testing.T) {
	valued := problem{
		check: dbsrepl.IDSecondsBehindSourceWithin,
		facts: dbsrepl.ChannelStatus{
			ChannelName:         "main",
			SecondsBehindSource: sql.NullInt64{Int64: 100, Valid: true},
		},
	}
	if got := renderProblems([]problem{valued}, 10); got != "channel main: lag=100s tolerance=10s" {
		t.Errorf("rendered %q, want %q", got, "channel main: lag=100s tolerance=10s")
	}

	null := problem{
		check: dbsrepl.IDSecondsBehindSourceWithin,
		facts: dbsrepl.ChannelStatus{
			ChannelName:         "",
			SecondsBehindSource: sql.NullInt64{Valid: false},
		},
	}
	if got := renderProblems([]problem{null}, 0); got != "channel <default>: lag=NULL tolerance=0s" {
		t.Errorf("rendered %q, want %q", got, "channel <default>: lag=NULL tolerance=0s")
	}
}

// Case 5: a missing channel renders what was requested and what the server
// actually reports, both through renderChannel.
func TestProblemRenderChannelNotFound(t *testing.T) {
	p := newChannelNotFound("billing", []string{"", "main"})

	if p.check != checkChannelNotFound {
		t.Errorf("check = %q, want %q", p.check, checkChannelNotFound)
	}
	facts, ok := p.facts.(channelNotFoundFacts)
	if !ok {
		t.Fatalf("facts type = %T, want channelNotFoundFacts", p.facts)
	}
	if facts.configured != "billing" {
		t.Errorf("facts.configured = %q, want %q", facts.configured, "billing")
	}
	if len(facts.observed) != 2 || facts.observed[0] != "" || facts.observed[1] != "main" {
		t.Errorf("facts.observed = %#v, want [\"\", \"main\"]", facts.observed)
	}

	got := renderProblems([]problem{p}, 10)
	want := "channel billing not found (server reports: <default>, main)"
	if got != want {
		t.Errorf("rendered %q, want %q", got, want)
	}

	// The default channel itself can be the missing one.
	got = renderProblems([]problem{newChannelNotFound("", []string{"main"})}, 10)
	want = "channel <default> not found (server reports: main)"
	if got != want {
		t.Errorf("rendered %q, want %q", got, want)
	}
}

// Case 6: several problems join into ONE physical line, and embedded newlines
// or carriage returns never survive.
func TestProblemRenderProblemsSingleLine(t *testing.T) {
	ps := []problem{
		{check: dbsrepl.IDReplicationConfigured, message: "a\nb"},
		{
			check: dbsrepl.IDReplicationChannelsRunning,
			facts: dbsrepl.ChannelStatus{
				ChannelName:  "main",
				IORunning:    "No",
				SQLRunning:   "No",
				LastIOErrno:  2003,
				LastIOError:  "line one\r\nline two",
				LastSQLErrno: 0,
			},
		},
	}

	got := renderProblems(ps, 10)

	if strings.ContainsAny(got, "\n\r") {
		t.Errorf("rendered %q, want a single physical line", got)
	}
	if !strings.Contains(got, "; ") {
		t.Errorf("rendered %q, want problems joined with %q", got, "; ")
	}
	if !strings.HasPrefix(got, dbsrepl.IDReplicationConfigured+": a b") {
		t.Errorf("rendered %q, want the sanitized default branch first", got)
	}
}

// Case 7: no credential can reach a rendered line — the renderer's inputs are
// channel names and ChannelStatus fields only.
func TestProblemRenderNoCredentials(t *testing.T) {
	fleet := config.ReplicationConfig{
		Enabled: true,
		Servers: []config.ReplicationServerConfig{{
			Host:     "replica1.example.com",
			Port:     3306,
			User:     "monitor",
			Password: "sekrit-pw",
			Channels: []string{"", "billing"},
		}},
	}

	var ps []problem
	for _, name := range fleet.Servers[0].Channels {
		ps = append(ps, newChannelNotFound(name, []string{"main"}))
	}
	ps = append(ps, problem{
		check: dbsrepl.IDReplicationChannelsRunning,
		facts: dbsrepl.ChannelStatus{ChannelName: "main", IORunning: "Connecting", SQLRunning: "Yes"},
	})

	got := renderProblems(ps, 10)

	if strings.Contains(got, "sekrit-pw") {
		t.Errorf("rendered %q, want no password", got)
	}
}

// Case 8: REPLICATION_CONFIGURED — no channels, an empty Facts slice — renders
// through the default branch without panicking and without an empty line.
func TestProblemRenderDefaultBranch(t *testing.T) {
	findings := dbsrepl.CheckReplicationConfigured(nil)
	if len(findings) != 1 {
		t.Fatalf("CheckReplicationConfigured(nil) returned %d findings, want 1", len(findings))
	}
	p := fromFinding(findings[0])
	if len(p.channels) != 0 {
		t.Errorf("channels = %#v, want none for a server-scoped check", p.channels)
	}

	got := renderProblems([]problem{p}, 10)

	if !strings.HasPrefix(got, dbsrepl.IDReplicationConfigured+": ") {
		t.Errorf("rendered %q, want the %q default branch", got, dbsrepl.IDReplicationConfigured)
	}
	if got == dbsrepl.IDReplicationConfigured+": " {
		t.Errorf("rendered %q, want a non-empty message", got)
	}
	if strings.ContainsAny(got, "\n\r") {
		t.Errorf("rendered %q, want a single physical line", got)
	}
}
