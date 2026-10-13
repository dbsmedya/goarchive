//go:build integration

package tracking

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// TestMySQLBlockers (unfinished-work guard spec §3–§5): Blockers lists the other jobs on the
// same source data that share a table with the incoming tree and are marked running or hold
// copied or deleting roots, whatever their status; it replaces Running, the same-root list.
func TestMySQLBlockers(t *testing.T) {
	ctx := context.Background()
	schema, db := scratch(t, "blockers")
	exec(t, db, ordersDDL, "CREATE TABLE other (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB")
	s := mustOpen(t, db, schema)
	const here = "11111111-1111-1111-1111-111111111111"
	same := func(uuid string) bool { return uuid == here }
	// toLines is other → lines: a job on another root whose tree shares lines.
	toLines := func(table string) func(*Binding) {
		return func(b *Binding) {
			b.Fingerprint.Relations = []Edge{{Parent: "other", Table: table, Pairs: [][2]string{{"other_id", "id"}}}}
		}
	}
	add := func(name, root string, st JobStatus, edit ...func(*Binding)) *Job {
		t.Helper()
		b := binding("1=1")
		for _, f := range edit {
			f(&b)
		}
		j, err := s.Attach(ctx, JobInput{Name: name, Type: jobspec.Archive, RootKey: rootKey(t, db, schema, root), Binding: b})
		if err != nil {
			t.Fatal(err)
		}
		if err := j.SetStatus(ctx, st); err != nil {
			t.Fatal(err)
		}
		return j
	}
	logged := func(j *Job, st Status) { // one root of an "other"-rooted job
		t.Helper()
		exec(t, db, fmt.Sprintf("INSERT INTO archiver_job_log_%d (k1, ga_status) VALUES (1, %d)", j.ID, st))
	}
	byName := func(bs []Blocker) map[string]Blocker {
		m := map[string]Blocker{}
		for _, b := range bs {
			m[b.Name] = b
		}
		return m
	}
	incoming := []string{"Orders", "lines"}

	add("me", "orders", JobRunning)
	crashed := add("crashed", "orders", JobRunning) // the same root, marked running, empty log
	if err := crashed.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	logged(add("failed_pending", "other", JobFailed, toLines("lines")), StatusCopied) // a non-root overlap, failed
	logged(add("idle_pending", "other", JobIdle, toLines("LINES")), StatusDeleting)   // idle, folded
	add("idle_clean", "orders", JobIdle)
	logged(add("completed_only", "other", JobFailed, toLines("lines")), StatusCompleted) // copy-only's resume record
	add("disjoint", "other", JobRunning)                                                 // shares no table
	add("otherdb", "orders", JobRunning, func(b *Binding) { b.SourceDatabase = "src_other" })
	add("foreign", "orders", JobRunning, func(b *Binding) { b.SourceServerUUID = "22222222-2222-2222-2222-222222222222" })
	logged(add("unreadable", "other", JobIdle), StatusCopied)
	add("unreadable_clean", "other", JobIdle)
	exec(t, db, "UPDATE archiver_job SET config_fingerprint = 'not json' WHERE job_name IN ('unreadable', 'unreadable_clean')")
	add("unregistered", "orders", JobRunning)
	exec(t, db, "UPDATE archiver_job SET root_key_columns = '' WHERE job_name = 'unregistered'")

	got, err := s.Blockers(ctx, "SRC", same, incoming, "me")
	if err != nil {
		t.Fatal(err)
	}
	m := byName(got)
	var names []string
	for _, b := range got { // ordered by job_name
		names = append(names, b.Name)
	}
	if want := []string{"crashed", "failed_pending", "idle_pending", "unreadable", "unregistered"}; !slices.Equal(names, want) {
		t.Fatalf("Blockers = %v, want %v", names, want)
	}
	for _, c := range []struct {
		name    string
		pending bool
		shared  []string
	}{
		{"crashed", false, []string{"orders"}},
		{"failed_pending", true, []string{"lines"}},
		{"idle_pending", true, []string{"lines"}},
		{"unreadable", true, nil},
		{"unregistered", false, []string{"orders"}},
	} {
		b := m[c.name]
		if b.Pending != c.pending || !slices.Equal(b.Shared, c.shared) {
			t.Errorf("%s = %+v, want pending %v, shared %v", c.name, b, c.pending, c.shared)
		}
	}
	if m["unregistered"].Registered || !m["crashed"].Registered || !m["unreadable"].Registered {
		t.Errorf("Registered: unregistered %v, crashed %v, unreadable %v; want false, true, true",
			m["unregistered"].Registered, m["crashed"].Registered, m["unreadable"].Registered)
	}
	if b := m["crashed"]; !b.Beat || b.HeartbeatAge < 0 || b.HeartbeatAge > 5*time.Second || b.Status != JobRunning {
		t.Errorf("crashed = %+v, want running with a fresh heartbeat", b)
	}
	if b := m["unreadable"]; b.Unreadable == nil || b.Status != JobIdle {
		t.Errorf("unreadable = %+v, want an idle job with its fingerprint fault", b)
	}
	if m["failed_pending"].SourceServerUUID != here {
		t.Errorf("failed_pending's uuid = %q", m["failed_pending"].SourceServerUUID)
	}

	// The same-data predicate decides the uuid half of the scope (spec §3a).
	all := func(string) bool { return true }
	if got, err := s.Blockers(ctx, "src", all, incoming, "me"); err != nil || !slices.ContainsFunc(got, func(b Blocker) bool { return b.Name == "foreign" }) {
		t.Errorf("Blockers(all data) = %+v, %v; want foreign listed", got, err)
	}
	// A fingerprint that cannot be read overlaps every tree while its job has unfinished roots.
	if got, err := s.Blockers(ctx, "src", same, []string{"missing"}, "me"); err != nil || len(got) != 1 || got[0].Name != "unreadable" {
		t.Errorf("Blockers(missing) = %+v, %v; want only unreadable", got, err)
	}

	// A registered overlapping job whose log was dropped refuses (spec §5), with its way out.
	exec(t, db, fmt.Sprintf("DROP TABLE archiver_job_log_%d", crashed.ID))
	_, err = s.Blockers(ctx, "src", same, incoming, "me")
	var lu *LogUnreadableError
	if !errors.As(err, &lu) || !lu.Missing || lu.Job != "crashed" || !strings.Contains(err.Error(), "Retiring a job completely") {
		t.Errorf("Blockers with a dropped log = %v, want a missing-log refusal naming crashed", err)
	}
}
