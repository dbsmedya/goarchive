package control

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The command table over the store (spec §4.4): what each request does, which reason word a
// failure gets, and the text of the replies. Driven on execute, with no connection; the framing
// of a connection is protocol_test.go's and the server's.

var testView = fakeView{lines: []string{"job (archive): running"}, json: `{"protocol":1}`}

func newCmds(store *tuning.Store, sentinel string) *commands {
	return newCommands(store, testView, sentinel)
}

func (c *commands) do(line string) reply { return c.execute(line, testPeer) }

func wantErr(t *testing.T, r reply, reason string) {
	t.Helper()
	if r.ok || string(r.reason) != reason {
		t.Errorf("reply = ok %v, reason %q %q, want ERR %s", r.ok, r.reason, r.message, reason)
	}
}

func wantOK(t *testing.T, r reply) {
	t.Helper()
	if !r.ok {
		t.Errorf("reply = ERR %s %q, want OK", r.reason, r.message)
	}
}

// TestCommandReplyFraming: every command and every setter of a run that uses all of them gets a
// well-framed reply, whatever it answers: OK and a body whose every line ends with LF, or one ERR
// line.
func TestCommandReplyFraming(t *testing.T) {
	store, _ := newTestStore(t, archiveApplies)
	c := newCmds(store, "")
	lines := []string{"help", "status", "status-json", "pause", "pause", "resume", "resume", "stop", "stop"}
	for _, k := range tuning.Keys() {
		lines = append(lines, string(k)+"=?", string(k)+"="+store.Load().Text(k), string(k)+"=1")
	}
	lines = append(lines, "frobnicate", "batch_size=0", "batch_size=5000", "seconds_behind_source_within=600", "Batch=1")
	for _, line := range lines {
		wire := string(c.do(line).wire())
		if !strings.HasSuffix(wire, "\n") {
			t.Errorf("%q: reply %q does not end with LF", line, wire)
		}
		if strings.HasPrefix(wire, "ERR ") && strings.Count(wire, "\n") != 1 {
			t.Errorf("%q: reply %q is an ERR of more than one line", line, wire)
		}
		if !strings.HasPrefix(wire, "ERR ") && !strings.HasPrefix(wire, "OK\n") {
			t.Errorf("%q: reply %q is neither OK nor ERR", line, wire)
		}
	}
}

// TestProtocolVersion (spec §7 row 8, the help half; status-json's is the status tests'): the
// first body line of help is `protocol 1`.
func TestProtocolVersion(t *testing.T) {
	store, _ := newTestStore(t, archiveApplies)
	r := newCmds(store, "").do("help")
	wantOK(t, r)
	if len(r.body) == 0 || r.body[0] != "protocol 1" {
		t.Errorf("help body = %q, want protocol 1 first", r.body)
	}
}

// TestHelpListsWhatTheRunAccepts (spec §4.4): the actions always, and only the setters the run
// uses.
func TestHelpListsWhatTheRunAccepts(t *testing.T) {
	names := func(r reply) map[string]bool {
		out := map[string]bool{}
		for _, l := range r.body[1:] {
			out[strings.Fields(l)[0]] = true
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		applies tuning.Applicability
		want    []string
		not     []string
	}{
		{"all", archiveApplies, []string{"help", "status", "status-json", "pause", "resume", "stop", "batch_size=<n>", "batch_delete_size=<n>", "sleep_seconds=<s>", "delete_sleep_seconds=<s>", "seconds_behind_source_within=<n>", "check_interval=<n>"}, nil},
		{"no gate", noGateApplies, []string{"batch_size=<n>", "batch_delete_size=<n>", "delete_sleep_seconds=<s>"}, []string{"seconds_behind_source_within=<n>", "check_interval=<n>"}},
		{"copy-only", copyOnlyApplies, []string{"help", "pause", "resume", "stop", "batch_size=<n>", "sleep_seconds=<s>"}, []string{"batch_delete_size=<n>", "delete_sleep_seconds=<s>", "seconds_behind_source_within=<n>", "check_interval=<n>"}},
	} {
		store, _ := newTestStore(t, tc.applies)
		got := names(newCmds(store, "").do("help"))
		for _, w := range tc.want {
			if !got[w] {
				t.Errorf("%s: help does not list %s: %v", tc.name, w, got)
			}
		}
		for _, n := range tc.not {
			if got[n] {
				t.Errorf("%s: help lists %s, which this run does not use", tc.name, n)
			}
		}
	}
}

// TestUnknownCommand (spec §7 row 13): a well-formed request for a name the protocol does not
// have is `unknown`, and the message names help. So is a setting used as an action, and an
// action used as a setting.
func TestUnknownCommand(t *testing.T) {
	store, _ := newTestStore(t, archiveApplies)
	c := newCmds(store, "")
	for _, line := range []string{"frobnicate", "batch_size", "pause=5", "status=?", "help=1", "batch_sizes=5", "frobnicate=?"} {
		r := c.do(line)
		wantErr(t, r, "unknown")
		if !strings.Contains(r.message, "help") {
			t.Errorf("%q: message %q does not name help", line, r.message)
		}
	}
	if got := store.Load().Revision; got != 0 {
		t.Errorf("revision = %d after unknown requests, want 0", got)
	}
}

// TestCacheTTLNotASetting (spec §7 row 39): cache_ttl is startup configuration. Setting it or
// reading it is an unknown key, and nothing changes.
func TestCacheTTLNotASetting(t *testing.T) {
	store, _ := newTestStore(t, archiveApplies)
	c := newCmds(store, "")
	for _, line := range []string{"cache_ttl=5", "cache_ttl=?", "cache_ttl"} {
		wantErr(t, c.do(line), "unknown")
	}
	if store.Load().Revision != 0 || len(store.Terminal().Settings) != 0 {
		t.Error("a cache_ttl request changed the store")
	}
	if r := c.do("help"); strings.Contains(strings.Join(r.body, "\n"), "cache_ttl") {
		t.Errorf("help lists cache_ttl: %q", r.body)
	}
}

// TestApplicability (spec §7 row 40): on a copy-only run the two delete setters and the two gate
// setters are refused with their messages, their getters too, and nothing enters the summary; on
// an archive run without replication the gate setters are refused.
func TestApplicability(t *testing.T) {
	const copyOnly, noGate = "a copy-only job deletes nothing", "this run does not use the replication gate"
	for _, tc := range []struct {
		name    string
		applies tuning.Applicability
		key     string
		message string
	}{
		{"copy-only batch_delete_size", copyOnlyApplies, "batch_delete_size", copyOnly},
		{"copy-only delete_sleep_seconds", copyOnlyApplies, "delete_sleep_seconds", copyOnly},
		{"copy-only seconds_behind_source_within", copyOnlyApplies, "seconds_behind_source_within", noGate},
		{"copy-only check_interval", copyOnlyApplies, "check_interval", noGate},
		{"no gate seconds_behind_source_within", noGateApplies, "seconds_behind_source_within", noGate},
		{"no gate check_interval", noGateApplies, "check_interval", noGate},
	} {
		store, _ := newTestStore(t, tc.applies)
		c := newCmds(store, "")
		for _, line := range []string{tc.key + "=1", tc.key + "=?"} {
			r := c.do(line)
			wantErr(t, r, "inapplicable")
			if r.message != tc.message {
				t.Errorf("%s %q: message %q, want %q", tc.name, line, r.message, tc.message)
			}
		}
		if store.Load().Revision != 0 || len(store.Terminal().Settings) != 0 {
			t.Errorf("%s: a refused request changed the store", tc.name)
		}
	}
	// A setting the run does use still works, on the same run.
	store, _ := newTestStore(t, copyOnlyApplies)
	wantOK(t, newCmds(store, "").do("batch_size=10"))
}

// TestReasonWords (spec §7 row 9): every row of §4.3's example table gives its word, and for a
// request that fails in two ways the earlier word in the table's order wins.
func TestReasonWords(t *testing.T) {
	for _, tc := range []struct {
		line    string
		applies tuning.Applicability
		word    string
	}{
		{"batch_size=-5", archiveApplies, "malformed"},
		{"sleep_seconds=.nan", archiveApplies, "malformed"},
		{"sleep_seconds=1e3", archiveApplies, "malformed"},
		{"Batch_Size=5", archiveApplies, "malformed"},
		{"batch_size = 5", archiveApplies, "malformed"},
		{"batch_sizes=5", archiveApplies, "unknown"},
		{"frobnicate", archiveApplies, "unknown"},
		{"cache_ttl=5", archiveApplies, "unknown"},
		{"batch_delete_size=5", copyOnlyApplies, "inapplicable"},
		{"batch_size=1.5", archiveApplies, "invalid"},
		{"batch_size=99999999999999999999", archiveApplies, "invalid"},
		{"batch_size=0", archiveApplies, "invalid"},
		{"sleep_seconds=9223372037", archiveApplies, "invalid"},
		{"batch_size=5000", archiveApplies, "ceiling"},
		{"seconds_behind_source_within=600", archiveApplies, "step"},
		// Two failures: the earlier word wins.
		{"frobnicate=-5", archiveApplies, "malformed"},
		{"frobnicate=1e3", archiveApplies, "malformed"},
		{"batch_sizes=1.5", archiveApplies, "unknown"},
		{"batch_delete_size=99999999999999999999", copyOnlyApplies, "inapplicable"},
		{"batch_delete_size=0", copyOnlyApplies, "inapplicable"},
		{"seconds_behind_source_within=1.5", noGateApplies, "inapplicable"},
	} {
		store, _ := newTestStore(t, tc.applies)
		r := newCmds(store, "").do(tc.line)
		if r.ok || string(r.reason) != tc.word {
			t.Errorf("%q: reply = ok %v, %s %q, want ERR %s", tc.line, r.ok, r.reason, r.message, tc.word)
		}
	}

	// After the terminal transition an invalid value is `ending`, which comes before `invalid`;
	// status and the getters are never `ending`.
	store, _ := newTestStore(t, archiveApplies)
	c := newCmds(store, "")
	store.Terminal()
	for _, line := range []string{"batch_size=0", "batch_size=10", "pause", "resume", "stop"} {
		wantErr(t, c.do(line), "ending")
	}
	for _, line := range []string{"status", "status-json", "help", "batch_size=?"} {
		wantOK(t, c.do(line))
	}

	// busy comes last: a full audit queue, with nothing consuming it.
	store, _ = newTestStore(t, archiveApplies, func(o *tuning.Options) { o.AuditSlots = 1 })
	c = newCmds(store, "")
	wantOK(t, c.do("batch_size=10"))
	wantErr(t, c.do("batch_size=20"), "busy")
	wantErr(t, c.do("pause"), "busy")
	wantErr(t, c.do("stop"), "busy")
	wantOK(t, c.do("batch_size=?"))
}

// TestSetterBodies: a setter's body is one line `key: <old> -> <new>, <when it takes effect>`, or
// `key: <value> (unchanged)`; a getter's body is `key=<value>`.
func TestSetterBodies(t *testing.T) {
	store, _ := newTestStore(t, archiveApplies)
	c := newCmds(store, "")
	for _, tc := range []struct{ line, body string }{
		{"batch_delete_size=100", "batch_delete_size: 500 -> 100, from the next DELETE"},
		{"batch_delete_size=100", "batch_delete_size: 100 (unchanged)"},
		{"batch_size=250", "batch_size: 1000 -> 250, from the next batch or recovery page"},
		{"sleep_seconds=0.25", "sleep_seconds: 1 -> 0.25, at once"},
		{"seconds_behind_source_within=3", "seconds_behind_source_within: 10 -> 3, from the next gate decision"},
		{"seconds_behind_source_within=?", "seconds_behind_source_within=3"},
		{"sleep_seconds=?", "sleep_seconds=0.25"},
	} {
		r := c.do(tc.line)
		wantOK(t, r)
		if len(r.body) != 1 || r.body[0] != tc.body {
			t.Errorf("%q: body %q, want one line %q", tc.line, r.body, tc.body)
		}
	}
}

// TestGetterRoundTrip (spec §7 row 7): for each setting and for values across its range, the
// getter's body is a line the server accepts as a setter and that sets the same value; no body
// carries an exponent.
func TestGetterRoundTrip(t *testing.T) {
	largest := math.Ldexp(1, 63) / float64(time.Second)
	for config.ValidateSleepSeconds(largest) != nil {
		largest = math.Nextafter(largest, 0)
	}
	for _, key := range []tuning.Key{tuning.SleepSeconds, tuning.DeleteSleepSeconds} {
		for _, v := range []float64{0, 0.000000001, 0.1, 1.0 / 3, 1, 3600, largest} {
			start := testStart
			if key == tuning.SleepSeconds {
				start.SleepSeconds = v
			} else {
				start.DeleteSleepSeconds = v
			}
			store, _ := newTestStore(t, archiveApplies, func(o *tuning.Options) { o.Start = start })
			c := newCmds(store, "")
			roundTrip(t, c, string(key))
		}
	}
	for _, key := range []tuning.Key{tuning.BatchSize, tuning.BatchDeleteSize, tuning.SecondsBehindSourceWithin, tuning.CheckInterval} {
		for _, v := range []int{1, 1000, 12345, math.MaxInt32, math.MaxInt32 + 1} {
			if key == tuning.CheckInterval && v <= 0 {
				continue
			}
			start := testStart
			switch key {
			case tuning.BatchSize:
				start.BatchSize = v
			case tuning.BatchDeleteSize:
				start.BatchDeleteSize = v
			case tuning.SecondsBehindSourceWithin:
				start.SecondsBehindSourceWithin = v
			case tuning.CheckInterval:
				start.CheckInterval = v
			}
			store, _ := newTestStore(t, archiveApplies, func(o *tuning.Options) { o.Start = start })
			roundTrip(t, newCmds(store, ""), string(key))
		}
	}
	// A tolerance of 0 is a value too.
	start := testStart
	start.SecondsBehindSourceWithin = 0
	store, _ := newTestStore(t, archiveApplies, func(o *tuning.Options) { o.Start = start })
	roundTrip(t, newCmds(store, ""), "seconds_behind_source_within")
}

func roundTrip(t *testing.T, c *commands, key string) {
	t.Helper()
	got := c.do(key + "=?")
	if !got.ok || len(got.body) != 1 || !strings.HasPrefix(got.body[0], key+"=") {
		t.Errorf("%s=?: %+v, want one line %s=<value>", key, got, key)
		return
	}
	if strings.ContainsAny(strings.TrimPrefix(got.body[0], key+"="), "eE+") {
		t.Errorf("%s: body %q carries an exponent or a sign", key, got.body[0])
	}
	set := c.do(got.body[0])
	if !set.ok || len(set.body) != 1 || !strings.HasSuffix(set.body[0], "(unchanged)") {
		t.Errorf("%q set back: %+v, want OK and unchanged", got.body[0], set)
	}
}

// TestPauseResumeReplies (spec §4.7): pause says since when it was already set; resume says when a
// sentinel file still holds the run, and when there was no socket pause to clear.
func TestPauseResumeReplies(t *testing.T) {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	store, _ := newTestStore(t, archiveApplies, func(o *tuning.Options) { o.Now = func() time.Time { return at } })
	sentinel := filepath.Join(t.TempDir(), "pause")
	c := newCmds(store, sentinel)

	r := c.do("resume")
	wantOK(t, r)
	if !strings.Contains(strings.Join(r.body, " "), "no socket pause") {
		t.Errorf("resume with no pause: %q, want it to say there was none", r.body)
	}

	wantOK(t, c.do("pause"))
	r = c.do("pause")
	wantOK(t, r)
	if !strings.Contains(strings.Join(r.body, " "), "2026-11-15T12:00:00Z") || !strings.Contains(strings.Join(r.body, " "), "already") {
		t.Errorf("pause again: %q, want it to say since when", r.body)
	}
	r = c.do("resume")
	wantOK(t, r)
	if strings.Contains(strings.Join(r.body, " "), "still paused") {
		t.Errorf("resume with no sentinel: %q, want no sentinel remark", r.body)
	}

	// With the sentinel file present the run does not resume silently against it.
	if err := os.WriteFile(sentinel, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	wantOK(t, c.do("pause"))
	r = c.do("resume")
	wantOK(t, r)
	// The reply names the file quoted (%q), as every message here does; on Windows that doubles the backslashes.
	want := "socket pause cleared; still paused: sentinel file " + strconv.Quote(sentinel) + " is present"
	if len(r.body) != 1 || r.body[0] != want {
		t.Errorf("resume under a sentinel: %q, want %q", r.body, want)
	}
	// With no socket pause either, the reply still tells that the file holds the run.
	r = c.do("resume")
	if !strings.Contains(strings.Join(r.body, " "), "no socket pause") || !strings.Contains(strings.Join(r.body, " "), strconv.Quote(sentinel)) {
		t.Errorf("resume with only the sentinel: %q, want both facts", r.body)
	}
}

// TestStopCallsRequestStopOnce (plan §3 C5): the first stop calls the store's RequestStop once; a
// second says it is already stopping and calls nothing; after the terminal transition a stop is
// `ending` and calls nothing.
func TestStopCallsRequestStopOnce(t *testing.T) {
	store, stops := newTestStore(t, archiveApplies)
	c := newCmds(store, "")
	r := c.do("stop")
	wantOK(t, r)
	if stops.calls() != 1 {
		t.Fatalf("RequestStop calls after the first stop = %d, want 1", stops.calls())
	}
	r = c.do("stop")
	wantOK(t, r)
	if !strings.Contains(strings.Join(r.body, " "), "already stopping") {
		t.Errorf("second stop: %q, want it to say already stopping", r.body)
	}
	if stops.calls() != 1 {
		t.Errorf("RequestStop calls after a second stop = %d, want 1", stops.calls())
	}

	store, stops = newTestStore(t, archiveApplies)
	c = newCmds(store, "")
	store.Terminal()
	wantErr(t, c.do("stop"), "ending")
	if stops.calls() != 0 {
		t.Errorf("RequestStop calls after the terminal transition = %d, want 0", stops.calls())
	}
}

// TestStatusActions: status and status-json answer with the view's body; neither writes to the
// store.
func TestStatusActions(t *testing.T) {
	store, _ := newTestStore(t, archiveApplies)
	c := newCommands(store, fakeView{lines: []string{"a", "b"}, json: `{"x":1}`}, "")
	if r := c.do("status"); !r.ok || strings.Join(r.body, "|") != "a|b" {
		t.Errorf("status = %+v, want the view's lines", r)
	}
	if r := c.do("status-json"); !r.ok || strings.Join(r.body, "|") != `{"x":1}` {
		t.Errorf("status-json = %+v, want the view's one line", r)
	}
	if store.Load().Revision != 0 {
		t.Error("a status read changed the store")
	}
}
