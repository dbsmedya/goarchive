//go:build integration

package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// The unfinished-work guard tests (spec .ayder/superpowers_20261012/specs/
// 2026-10-12-unfinished-work-guard-design.md §11). Job j archives orders → order_lines →
// line_notes (orderJob); job notes purges line_notes (purgeNotesJob): the trees share
// line_notes, a non-root table of j's.

// guardEnv is an env with the order tables seeded, and the two configs.
func guardEnv(t *testing.T, name string) (e env, archive, purge Config) {
	t.Helper()
	e = newEnv(t, name)
	e.both(t, orderTables(cs4)...)
	seedOrders(t, e, 3)
	archive = e.config(t, job(t, jobspec.Archive, orderJob))
	purge = e.config(t, job(t, jobspec.Purge, purgeNotesJob))
	purge.Name = "notes"
	return e, archive, purge
}

// srcRows counts every row of the order tree on the source.
func srcRows(t *testing.T, e env) int64 {
	return count(t, e.src, "SELECT (SELECT COUNT(*) FROM orders) + (SELECT COUNT(*) FROM order_lines) + (SELECT COUNT(*) FROM line_notes)")
}

// setJob runs one UPDATE on job name's registry row, as an operator would.
func setJob(t *testing.T, e env, name, set string) {
	t.Helper()
	execAll(t, e.dst, "UPDATE archiver_job SET "+set+" WHERE job_name = '"+name+"'")
}

// refusedBy checks that err is an OverlapError naming each of blockers, that the refused run
// left the source as it was, and that it registered nothing under its own name.
func refusedBy(t *testing.T, e env, err error, refused string, before int64, blockers ...string) *OverlapError {
	t.Helper()
	oe := wantErr[*OverlapError](t, err)
	for _, b := range blockers {
		if !strings.Contains(oe.Error(), "\n  "+b+": ") {
			t.Errorf("the refusal does not name %s:\n%s", b, oe)
		}
	}
	if got := srcRows(t, e); got != before {
		t.Errorf("the refused run changed the source: %d rows, want %d", got, before)
	}
	if got := count(t, e.dst, "SELECT COUNT(*) FROM archiver_job WHERE job_name = ?", refused); got != 0 {
		t.Errorf("the refused run %s registered itself", refused)
	}
	return oe
}

// TestMySQLGuardCrashedArchiveBlocksLeafPurge (I1, G1, gap 1): an archive killed with copied
// roots, left marked running, refuses a purge rooted at its leaf, naming the shared table.
func TestMySQLGuardCrashedArchiveBlocksLeafPurge(t *testing.T) {
	e, _, purge := guardEnv(t, "g_crash")
	e.killAt(t, jobspec.Archive, orderJob, atCommitted, 1)
	before := srcRows(t, e)
	_, err := e.run(t, purge)
	oe := refusedBy(t, e, err, "notes", before, "j")
	if !strings.Contains(oe.Error(), "j: marked running") || !strings.Contains(oe.Error(), "unfinished roots in its log; shares line_notes") {
		t.Errorf("refusal:\n%s", oe)
	}
	// dry-run takes no lock and runs no guard (spec §6): a blocked job still shows its plans.
	if _, err := Explain(context.Background(), purge); err != nil {
		t.Errorf("Explain of a blocked job = %v", err)
	}
}

// TestMySQLGuardFailedPurgeBlocksArchive (I2, gap 2): a purge of line_notes that failed with
// deleting roots refuses an archive whose tree contains line_notes.
func TestMySQLGuardFailedPurgeBlocksArchive(t *testing.T) {
	e, archive, purge := guardEnv(t, "g_failed")
	e.interrupted(t, purge, atLogged)
	before := srcRows(t, e)
	_, err := e.run(t, archive)
	oe := refusedBy(t, e, err, "j", before, "notes")
	if !strings.Contains(oe.Error(), "notes: failed, unfinished roots in its log; shares line_notes") {
		t.Errorf("refusal:\n%s", oe)
	}
}

// TestMySQLGuardIdleWithWorkBlocks (I3, I4, G3): marking a job with copied roots idle no
// longer lets an overlapping job pass; the job's own rerun finishes its work, and then the
// other job runs.
func TestMySQLGuardIdleWithWorkBlocks(t *testing.T) {
	e, archive, purge := guardEnv(t, "g_idle")
	e.interrupted(t, archive, atCommitted)
	setJob(t, e, "j", "job_status = 0")
	before := srcRows(t, e)
	_, err := e.run(t, purge)
	oe := refusedBy(t, e, err, "notes", before, "j")
	if strings.Contains(oe.Error(), "UPDATE") {
		t.Errorf("the refusal offers to mark a job with unfinished roots idle:\n%s", oe)
	}
	if rep := e.mustRun(t, archive); rep.Recovered != 2 {
		t.Errorf("j's own rerun recovered %d roots, want 2", rep.Recovered)
	}
	e.mustRun(t, purge)
}

// TestMySQLGuardScope (I5): a stopped job with work does not block a job whose tree it does
// not share, nor one on a source database of another name.
func TestMySQLGuardScope(t *testing.T) {
	e, archive, purge := guardEnv(t, "g_scope")
	execAll(t, e.src, "CREATE TABLE r (id INT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB", "INSERT INTO r VALUES (1, 1)")
	e.interrupted(t, archive, atCommitted)
	other := e.config(t, job(t, jobspec.Purge, "\n    root_table: r\n    primary_key: id\n    where: \"id > 0\"\n"))
	other.Name = "other"
	e.mustRun(t, other) // disjoint tree
	setJob(t, e, "j", "source_database = 'elsewhere'")
	e.mustRun(t, purge) // another source database
}

// TestMySQLGuardCopyOnly (I6): a failed copy-only job's completed roots are a resume record,
// not unfinished work; marked running, the job blocks.
func TestMySQLGuardCopyOnly(t *testing.T) {
	e, _, purge := guardEnv(t, "g_copy")
	copyOnly := e.config(t, job(t, jobspec.CopyOnly, orderJob))
	e.interrupted(t, copyOnly, atCommitted)
	if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] == 0 {
		t.Fatalf("log = %v, want completed roots", got)
	}
	setJob(t, e, "j", "job_status = 1")
	before := srcRows(t, e)
	_, err := e.run(t, purge)
	refusedBy(t, e, err, "notes", before, "j")
	setJob(t, e, "j", "job_status = 3")
	e.mustRun(t, purge)
}

// TestMySQLGuardRunningEmptyLog (I7): a job marked running with no unfinished roots blocks,
// and its refusal gives the existing remedy; marked idle, it no longer blocks.
func TestMySQLGuardRunningEmptyLog(t *testing.T) {
	e, archive, purge := guardEnv(t, "g_empty")
	e.mustRun(t, archive)
	// A crash an hour ago: marked running, its heartbeat stopped.
	setJob(t, e, "j", "job_status = 1, last_heartbeat_at = UTC_TIMESTAMP() - INTERVAL 1 HOUR")
	before := srcRows(t, e)
	_, err := e.run(t, purge)
	oe := refusedBy(t, e, err, "notes", before, "j")
	if !strings.Contains(oe.Error(), "no unfinished roots in its log") || !strings.Contains(oe.Error(), "WHERE job_name = 'j'") {
		t.Errorf("refusal:\n%s", oe)
	}
	// With a recent heartbeat the job may be live elsewhere: no "mark it idle".
	setJob(t, e, "j", "last_heartbeat_at = UTC_TIMESTAMP()")
	_, err = e.run(t, purge)
	if oe := refusedBy(t, e, err, "notes", before, "j"); strings.Contains(oe.Error(), "UPDATE") || !strings.Contains(oe.Error(), "recent heartbeat") {
		t.Errorf("refusal for a recent heartbeat:\n%s", oe)
	}
	setJob(t, e, "j", "job_status = 0")
	e.mustRun(t, purge)
}

// guardTestUUID is the one foreign uuid the tests put into the source's gtid_executed (probe
// L11): fixed, so a rerun adds nothing (the server skips an executed GTID silently).
const guardTestUUID = "f11ed000-57a1-4c0d-9e5e-000000000001"

// absentTestUUID is never put into any set.
const absentTestUUID = "f11ed000-57a1-4c0d-9e5e-0000000000ff"

// sourceDataFacts reads the source's facts the way the engine does, through the library.
func sourceDataFacts(t *testing.T, db *sql.DB) sourceFacts {
	t.Helper()
	f, err := readSourceData(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// injectGTID commits one empty transaction under guardTestUUID on one pinned admin session,
// discarding the session if a step fails (a session left with gtid_next assigned refuses
// later transactions), then asserts membership: never that the injection did something.
func injectGTID(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	admin := openServer(t, "TEST_SOURCE", "", nil)
	conn, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range []string{"SET gtid_next = '" + guardTestUUID + ":1'", "BEGIN", "COMMIT", "SET gtid_next = 'AUTOMATIC'"} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if f := sourceDataFacts(t, admin); !strings.Contains(strings.ToLower(f.gtid.Executed), guardTestUUID) {
		t.Fatalf("gtid_executed %q lacks %s after the injection", f.gtid.Executed, guardTestUUID)
	}
}

// TestMySQLGuardSourceData (I8, I8s, I8n, IG): a stopped job recorded on another source
// server blocks only when its work is in this source's data (spec §3a). The test reads the
// facts it runs under and asserts the matching branch: on the estate (gtid_mode ON, no group)
// a uuid in gtid_executed blocks and an absent one does not; under the matrix's
// gtid_mode_on_permissive row and on its Group Replication row both block.
func TestMySQLGuardSourceData(t *testing.T) {
	e, archive, purge := guardEnv(t, "g_data")
	e.interrupted(t, archive, atCommitted)
	facts := sourceDataFacts(t, e.src)
	precise := facts.gtid.Mode == "ON" && facts.group.GroupName == ""

	if precise {
		injectGTID(t)
	}
	setJob(t, e, "j", "source_server_uuid = '"+guardTestUUID+"'")
	before := srcRows(t, e)
	_, err := e.run(t, purge)
	oe := refusedBy(t, e, err, "notes", before, "j")
	if !strings.Contains(oe.Error(), "recorded on source server "+guardTestUUID) {
		t.Errorf("the refusal does not say why another server's job counts:\n%s", oe)
	}

	setJob(t, e, "j", "source_server_uuid = '"+absentTestUUID+"'")
	_, err = e.run(t, purge)
	switch {
	case precise && err != nil:
		t.Errorf("gtid_mode ON, no group: a job on a server whose data this source does not hold blocked the run: %v", err)
	case !precise:
		oe := refusedBy(t, e, err, "notes", before, "j")
		want := "gtid_mode is " + facts.gtid.Mode
		if facts.group.GroupName != "" {
			want = "Group Replication member"
		}
		if !strings.Contains(oe.Error(), want) {
			t.Errorf("the refusal does not give the fallback %q:\n%s", want, oe)
		}
	}
}

// TestMySQLGuardUnreadableFingerprint (I9): a fingerprint that cannot be read blocks every
// tree while its job has unfinished roots, and is skipped when it has none.
func TestMySQLGuardUnreadableFingerprint(t *testing.T) {
	e, archive, purge := guardEnv(t, "g_unread")
	e.interrupted(t, archive, atCommitted)
	setJob(t, e, "j", "config_fingerprint = 'not json'")
	before := srcRows(t, e)
	_, err := e.run(t, purge)
	oe := refusedBy(t, e, err, "notes", before, "j")
	if !strings.Contains(oe.Error(), "its config_fingerprint cannot be read") || !strings.Contains(oe.Error(), "Abandoning logged roots") {
		t.Errorf("refusal:\n%s", oe)
	}
	execAll(t, e.dst, fmt.Sprintf("DELETE FROM archiver_job_log_%d WHERE ga_status = 1", logID(t, e, "j")))
	e.mustRun(t, purge)
}

// logID is job name's id, which names its log.
func logID(t *testing.T, e env, name string) int64 {
	t.Helper()
	return count(t, e.dst, "SELECT id FROM archiver_job WHERE job_name = ?", name)
}

// TestMySQLGuardMissingLog (I10): an overlapping registered job whose log was dropped refuses
// the run, naming the log and the way out; the job's own rerun recreates the log empty.
func TestMySQLGuardMissingLog(t *testing.T) {
	e, archive, purge := guardEnv(t, "g_nolog")
	e.mustRun(t, archive)
	execAll(t, e.dst, fmt.Sprintf("DROP TABLE archiver_job_log_%d", logID(t, e, "j")))
	_, err := e.run(t, purge)
	lu := wantErr[*tracking.LogUnreadableError](t, err)
	if !lu.Missing || lu.Job != "j" || !strings.Contains(err.Error(), "Retiring a job completely") {
		t.Errorf("refusal = %v", err)
	}
	e.mustRun(t, archive)
	e.mustRun(t, purge)
}

// TestMySQLGuardMutualBlock (I11, spec §8): two jobs each holding unfinished roots on a
// shared table refuse each other; abandoning one's roots lets the other finish. The guard
// forbids building notes' state by running notes, so it is seeded through the tracking store,
// as an earlier release could have left it.
func TestMySQLGuardMutualBlock(t *testing.T) {
	e, archive, purge := guardEnv(t, "g_mutual")
	e.interrupted(t, archive, atCommitted)
	ctx := context.Background()
	store, err := tracking.Open(ctx, e.dst, e.dstSchema)
	if err != nil {
		t.Fatal(err)
	}
	res, err := preflight.Run(ctx, preflight.Config{Source: e.src, SourceDatabase: e.srcSchema, Destination: e.dst,
		DestinationDatabase: e.dstSchema, JobSchema: e.dstSchema, Spec: purge.Spec, Processing: purge.Processing})
	if err != nil {
		t.Fatal(err)
	}
	uuid := strings.Split(e.storedBinding(t, e.dstSchema), "|")[0]
	notes, err := store.Attach(ctx, tracking.JobInput{Name: "notes", Type: jobspec.Purge, RootKey: res.Keys["line_notes"],
		Binding: tracking.Binding{SourceServerUUID: uuid, SourceDatabase: e.srcSchema, DestinationDatabase: e.dstSchema,
			Fingerprint: tracking.FingerprintOf(purge.Spec)}})
	if err != nil {
		t.Fatal(err)
	}
	execAll(t, e.dst, fmt.Sprintf("INSERT INTO archiver_job_log_%d (k1, ga_status) VALUES (1, %d)", notes.ID, tracking.StatusDeleting))
	if err := notes.SetStatus(ctx, tracking.JobFailed); err != nil {
		t.Fatal(err)
	}
	before := srcRows(t, e)
	_, err = e.run(t, archive)
	refusedBy(t, e, err, "", before, "notes")
	_, err = e.run(t, purge)
	refusedBy(t, e, err, "", before, "j")
	// The documented abandonment of notes' roots (README_JOBS_SCHEMA, "Abandoning logged roots").
	execAll(t, e.dst, fmt.Sprintf("DELETE FROM archiver_job_log_%d WHERE ga_status = 4", notes.ID))
	e.mustRun(t, archive)
	e.mustRun(t, purge)
}

// TestMySQLGuardSourceFactsUnreadable (I12): a run whose source facts cannot be read is
// refused, with both lock sets released (the leak guard) and nothing written.
func TestMySQLGuardSourceFactsUnreadable(t *testing.T) {
	e, archive, _ := guardEnv(t, "g_facts")
	saved := readSourceData
	readSourceData = func(context.Context, *sql.DB) (sourceFacts, error) { return sourceFacts{}, errors.New("proxy says no") }
	t.Cleanup(func() { readSourceData = saved })
	before := srcRows(t, e)
	_, err := e.run(t, archive)
	if err == nil || !strings.Contains(err.Error(), "cannot read the source's gtid_mode") || !strings.Contains(err.Error(), "proxy says no") {
		t.Fatalf("Run = %v, want the source-facts refusal", err)
	}
	if srcRows(t, e) != before || count(t, e.dst, "SELECT COUNT(*) FROM archiver_job") != 0 {
		t.Error("the refused run wrote something")
	}
}
