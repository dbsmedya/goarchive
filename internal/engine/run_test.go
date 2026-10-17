package engine

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
	"github.com/go-sql-driver/mysql"
)

// TestChunkSizes: a key-addressed statement carries at most its index's cap (M5-1) and the
// configured size; reads and verification also stay under the destination cap, so both
// servers see the same chunks.
func TestChunkSizes(t *testing.T) {
	res := &preflight.Result{Caps: []preflight.Cap{
		{Table: "orders", Server: "source", Statement: "by key", Tuples: 8000},
		{Table: "orders", Server: "destination", Statement: "by key", Tuples: 300},
		{Table: "lines", Server: "source", Statement: "by key", Tuples: 50},
		{Table: "lines", Server: "source", Statement: "discovery", Tuples: 70},
		{Table: "lines", Server: "destination", Statement: "by key", Tuples: 9000},
	}}
	r := &run{caps: capsOf(res), cfg: Config{Processing: config.ProcessingConfig{BatchSize: 1000, BatchDeleteSize: 500}}}
	for name, tc := range map[string]struct{ got, want int }{
		"orders by key: the destination cap": {r.byKeyChunk("orders"), 300},
		"lines by key: the source cap":       {r.byKeyChunk("lines"), 50},
		"lines discovery: its own cap":       {r.discoveryChunk("lines"), 70},
		"orders delete: batch_delete_size":   {r.deleteChunk("orders"), 500},
		"lines delete: the source cap":       {r.deleteChunk("lines"), 50},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", name, tc.got, tc.want)
		}
	}
	r.cfg.Processing.BatchSize = 10
	if got := r.byKeyChunk("orders"); got != 10 {
		t.Errorf("byKeyChunk under batch_size 10 = %d", got)
	}
	r.caps.destination = map[string]int{} // purge: no destination statements
	if got := r.byKeyChunk("lines"); got != 10 {
		t.Errorf("purge byKeyChunk = %d", got)
	}
}

// TestChunkSizesStayWithinTheBatch (Fable M7c-2 review F4): preflight's
// MAX_ALLOWED_PACKET_CHECK bounds each statement at min(cap, batch_size), and the source's
// by-key statements at the larger of batch_size and batch_delete_size, on the promise that
// the engine never sends more keys than that. Across caps above and below the batch sizes,
// every chunk stays within its batch size and its cap; each case where the batch binds has a
// neighbour where the cap binds.
func TestChunkSizesStayWithinTheBatch(t *testing.T) {
	for _, capTuples := range []int{1, 7, 999, 1000, 1001, 16000, 65535} {
		for _, p := range []config.ProcessingConfig{{BatchSize: 1, BatchDeleteSize: 1}, {BatchSize: 1000, BatchDeleteSize: 500},
			{BatchSize: 10, BatchDeleteSize: 16000}, {BatchSize: 65535, BatchDeleteSize: 65535}} {
			res := &preflight.Result{Caps: []preflight.Cap{
				{Table: "t", Server: "source", Statement: "by key", Tuples: capTuples},
				{Table: "t", Server: "source", Statement: "discovery", Tuples: capTuples},
				{Table: "t", Server: "destination", Statement: "by key", Tuples: capTuples},
			}}
			r := &run{caps: capsOf(res), cfg: Config{Processing: p}}
			for name, tc := range map[string]struct{ got, batch int }{
				"byKeyChunk":     {r.byKeyChunk("t"), p.BatchSize},
				"discoveryChunk": {r.discoveryChunk("t"), p.BatchSize},
				"deleteChunk":    {r.deleteChunk("t"), p.BatchDeleteSize},
			} {
				if want := min(tc.batch, capTuples); tc.got != want {
					t.Errorf("cap %d, %+v: %s = %d, want min(batch %d, cap %d) = %d", capTuples, p, name, tc.got, tc.batch, capTuples, want)
				}
			}
		}
	}
}

func TestConfigValidate(t *testing.T) {
	good := func() Config {
		return Config{Name: "j", Spec: &jobspec.Spec{Type: jobspec.Archive}, Source: &sql.DB{}, Destination: &sql.DB{}, JobSchema: "s",
			Processing: config.ProcessingConfig{BatchSize: 1, BatchDeleteSize: 1}, Verification: jobspec.VerifySHA256}
	}
	c := good()
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	purge := good()
	purge.Spec = &jobspec.Spec{Type: jobspec.Purge}
	purge.Verification = ""
	if err := purge.validate(); err != nil {
		t.Errorf("a purge job verifies nothing: %v", err)
	}
	for name, edit := range map[string]func(*Config){
		"no job":              func(c *Config) { c.Spec = nil },
		"no type":             func(c *Config) { c.Spec = &jobspec.Spec{} },
		"no source":           func(c *Config) { c.Source = nil },
		"no destination":      func(c *Config) { c.Destination = nil },
		"no job schema":       func(c *Config) { c.JobSchema = "" },
		"batch_size 0":        func(c *Config) { c.Processing.BatchSize = 0 },
		"batch_delete_size 0": func(c *Config) { c.Processing.BatchDeleteSize = 0 },
		"no verification":     func(c *Config) { c.Verification = "" },
	} {
		c := good()
		edit(&c)
		if err := c.validate(); err == nil {
			t.Errorf("%s: passed", name)
		}
	}
}

// TestOverlapError (U2, unfinished-work guard spec §7): every blocker is listed with its
// state and shared tables, each state gets its remedy, and no job with unfinished roots is
// ever offered "mark it idle".
func TestOverlapError(t *testing.T) {
	e := &OverlapError{Job: "purge_logs", Database: "shop", Registry: "`goarchive`.`archiver_job`",
		Blockers: []tracking.Blocker{
			{Name: "archive_orders", Status: tracking.JobFailed, Registered: true, Pending: true, Shared: []string{"payment_logs"}},
			{Name: "sweep", Status: tracking.JobRunning, Registered: true, Beat: true, HeartbeatAge: time.Hour, Pending: true,
				Shared: []string{"order_items", "payment_logs"}, SourceServerUUID: "22222222-2222-2222-2222-222222222222"},
			{ID: 3, Name: "killed", Status: tracking.JobRunning, Registered: true, Shared: []string{"payments"}},
			{Name: "fresh", Status: tracking.JobRunning, Registered: true, Beat: true, HeartbeatAge: 5 * time.Second, Shared: []string{"payments"}},
			{Name: "broken", Status: tracking.JobIdle, Registered: true, Pending: true, Unreadable: errors.New("bad json")},
			{ID: 6, Name: "half", Status: tracking.JobRunning, Beat: true, HeartbeatAge: time.Hour, Shared: []string{"payments"}},
			{ID: 7, Name: "garbled", Status: tracking.JobRunning, Registered: true, Unreadable: errors.New("bad json")},
			// Job names may hold ' and \ (printable ASCII): the remedy names the row by id (Codex N1).
			{ID: 8, Name: `it's\old`, Status: tracking.JobRunning, Registered: true, Shared: []string{"payments"}},
		},
		Why: map[string]string{"sweep": "whose data this server holds (its uuid is in this server's gtid_executed)"}}
	msg := e.Error()
	for _, want := range []string{
		"job \"purge_logs\" shares source tables in `shop`", "registry `goarchive`.`archiver_job`", "This run did nothing.",
		"archive_orders: failed, unfinished roots in its log; shares payment_logs",
		"sweep: marked running, heartbeat 1h0m0s ago, unfinished roots in its log; shares order_items, payment_logs; " +
			"recorded on source server 22222222-2222-2222-2222-222222222222, whose data this server holds",
		"killed: marked running, never beat, no unfinished roots in its log; shares payments",
		"fresh: marked running, heartbeat 5s ago, no unfinished roots in its log",
		"broken: idle, unfinished roots in its log; its config_fingerprint cannot be read (bad json)",
		"half: marked running, heartbeat 1h0m0s ago, unregistered, no log yet; shares payments",
		"garbled: marked running, never beat, no unfinished roots in its log; its config_fingerprint cannot be read (bad json)",
		"half has no unfinished roots: once its process is gone, mark it idle",
		"garbled has no unfinished roots: once its process is gone, mark it idle",
		"Rerun each job with unfinished roots", "Clearing a stale running job",
		"killed has no unfinished roots: once its process is gone, mark it idle with UPDATE `goarchive`.`archiver_job` SET job_status = 0 WHERE id = 3.",
		"it's\\old has no unfinished roots: once its process is gone, mark it idle with UPDATE `goarchive`.`archiver_job` SET job_status = 0 WHERE id = 8.",
		"a live process on another server", "Abandoning logged roots",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal lacks %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "WHERE job_name") {
		t.Errorf("the remedy quotes a job name in SQL:\n%s", msg)
	}
	if n := strings.Count(msg, "SET job_status = 0 WHERE id = "); n != 4 {
		t.Errorf("%d mark-idle remedies, want 4 (killed, half, garbled, it's\\old):\n%s", n, msg)
	}
	// One job with unfinished roots only: no heartbeat or idle remedy at all.
	one := &OverlapError{Job: "b", Database: "s", Registry: "r",
		Blockers: []tracking.Blocker{{Name: "a", Status: tracking.JobIdle, Pending: true, Shared: []string{"t"}}}}
	if msg := one.Error(); strings.Contains(msg, "UPDATE") || strings.Contains(msg, "heartbeat") {
		t.Errorf("an idle job with unfinished roots: %s", msg)
	}
	// A job that cannot be rerun has its way out in the abandonment procedure (Fable final M2a).
	if msg := one.Error(); !strings.Contains(msg, "A job that cannot be rerun") || !strings.Contains(msg, "Abandoning logged roots") {
		t.Errorf("the pending remedy does not cite the abandonment procedure: %s", msg)
	}
	// An unreadable fingerprint with nothing unfinished gets the idle remedy only (Fable final M2b).
	clean := &OverlapError{Job: "b", Database: "s", Registry: "r",
		Blockers: []tracking.Blocker{{Name: "g", Status: tracking.JobRunning, Registered: true, Unreadable: errors.New("bad json")}}}
	if msg := clean.Error(); strings.Contains(msg, "restore it from a known-good record") || !strings.Contains(msg, "g has no unfinished roots") {
		t.Errorf("an unreadable job with nothing unfinished: %s", msg)
	}
}

func TestDuplicateError(t *testing.T) {
	if msg := (&DuplicateError{Table: "t", Key: "7", CopyOnly: true}).Error(); !strings.Contains(msg, "resume: true") {
		t.Errorf("a copy-only duplicate does not suggest resume: true: %s", msg)
	}
	if msg := (&DuplicateError{Table: "t", Key: "7"}).Error(); strings.Contains(msg, "resume") {
		t.Errorf("an archive duplicate suggests resume: %s", msg)
	}
	for msg, want := range map[string]string{
		"Duplicate entry 'a-1' for key 't.PRIMARY'":        "a-1",
		"Duplicate entry 'O'Reilly' for key 't.PRIMARY'":   "O'Reilly", // MySQL does not escape the value
		"Duplicate entry 'O'Reilly-5' for key 't.PRIMARY'": "O'Reilly-5",
		"Duplicate entry '' for key 't.uk'":                "",
		"not a duplicate-entry message":                    "not a duplicate-entry message",
	} {
		if got := duplicateKey(msg); got != want {
			t.Errorf("duplicateKey(%q) = %q, want %q", msg, got, want)
		}
	}
}

// TestRecurringRootError (RKP-2): the log collision names the root, the job and its type's
// remedy, and keeps MySQL's error.
func TestRecurringRootError(t *testing.T) {
	me := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry '7' for key 'archiver_job_log_3.PRIMARY'"}
	const contract = `docs/README_LIMITATIONS.md, "No DDL and no concurrent writes during a run (contract)"`
	const remedy = `docs/README_JOBS_SCHEMA.md, "Maintenance: what is safe to delete"`
	for _, tc := range []struct {
		typ       jobspec.Type
		want, not []string
	}{
		{jobspec.Archive, []string{"the root '7'", `job "j"`, "archived before", contract,
			"nothing was logged or deleted", "To archive it again", remedy, "Duplicate entry '7'"}, []string{"resume"}},
		{jobspec.Purge, []string{"the root '7'", `job "j"`, "purged before", contract,
			"nothing was logged or deleted", "To purge it again", remedy, "Duplicate entry '7'"}, []string{"resume"}},
		{jobspec.CopyOnly, []string{"the root '7'", `job "j"`, "copied it before",
			"nothing was copied or logged", "set resume: true on the job", "Duplicate entry '7'"}, []string{contract, remedy}},
	} {
		err := error(&RecurringRootError{Job: "j", Type: tc.typ, Root: "7", Err: me})
		msg := err.Error()
		for _, w := range tc.want {
			if !strings.Contains(msg, w) {
				t.Errorf("%s: lacks %q:\n%s", tc.typ, w, msg)
			}
		}
		for _, w := range tc.not {
			if strings.Contains(msg, w) {
				t.Errorf("%s: holds %q:\n%s", tc.typ, w, msg)
			}
		}
		var got *mysql.MySQLError
		if !errors.As(err, &got) || got != me {
			t.Errorf("%s: errors.As does not reach the MySQL error", tc.typ)
		}
	}
}

// TestLogInsertError (RKP-2): a 1062 on this job's log primary key becomes a RecurringRootError
// for every job type, naming the root as MySQL reports it; any other error passes through
// unchanged, a 1062 on another key or table included (final review R1).
func TestLogInsertError(t *testing.T) {
	job := &tracking.Job{ID: 3, Name: "j"}
	for msg, root := range map[string]string{
		"Duplicate entry '5-7' for key 'archiver_job_log_3.PRIMARY'":        "5-7",
		"Duplicate entry 'O'Reilly-5' for key 'archiver_job_log_3.PRIMARY'": "O'Reilly-5",
	} {
		me := &mysql.MySQLError{Number: 1062, Message: msg}
		wrapped := fmt.Errorf("tracking: log 2 root(s) as deleting for job %q: %w", "j", me)
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
			r := &run{typ: typ, job: job}
			var rr *RecurringRootError
			if err := r.logInsertError(wrapped); !errors.As(err, &rr) || rr.Root != root || rr.Type != typ || rr.Job != "j" || rr.Err != me {
				t.Errorf("%s: %v, want a RecurringRootError for the root %s", typ, err, root)
			}
		}
	}
	for _, other := range []error{
		fmt.Errorf("x: %w", &mysql.MySQLError{Number: 1213, Message: "Deadlock found"}),
		fmt.Errorf("x: %w", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry '4' for key 'archiver_job_log_3.accidental_status'"}),
		fmt.Errorf("x: %w", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry '7' for key 'archiver_job_log_4.PRIMARY'"}),
		fmt.Errorf("x: %w", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry '7' for key 'audit.PRIMARY'"}),
		fmt.Errorf("x: %w", &mysql.MySQLError{Number: 1062, Message: "Duplicate entry '7'"}),
	} {
		if err := (&run{typ: jobspec.Purge, job: job}).logInsertError(other); err != other {
			t.Errorf("%v changed: %v", other, err)
		}
	}
	if err := (&run{typ: jobspec.Purge, job: job}).logInsertError(nil); err != nil {
		t.Errorf("nil became %v", err)
	}
}
