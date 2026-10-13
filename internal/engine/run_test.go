package engine

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
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
			{Name: "killed", Status: tracking.JobRunning, Registered: true, Shared: []string{"payments"}},
			{Name: "fresh", Status: tracking.JobRunning, Registered: true, Beat: true, HeartbeatAge: 5 * time.Second, Shared: []string{"payments"}},
			{Name: "broken", Status: tracking.JobIdle, Registered: true, Pending: true, Unreadable: errors.New("bad json")},
			{Name: "half", Status: tracking.JobRunning, Beat: true, HeartbeatAge: time.Hour, Shared: []string{"payments"}},
			{Name: "garbled", Status: tracking.JobRunning, Registered: true, Unreadable: errors.New("bad json")},
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
		"killed has no unfinished roots: once its process is gone, mark it idle with UPDATE `goarchive`.`archiver_job` SET job_status = 0 WHERE job_name = 'killed'.",
		"a live process on another server", "Abandoning logged roots",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal lacks %q:\n%s", want, msg)
		}
	}
	for _, name := range []string{"archive_orders", "sweep", "broken", "fresh"} {
		if strings.Contains(msg, "WHERE job_name = '"+name+"'") {
			t.Errorf("the refusal offers to mark %s idle:\n%s", name, msg)
		}
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
	if got := duplicateKey("Duplicate entry 'a-1' for key 't.PRIMARY'"); got != "a-1" {
		t.Errorf("duplicateKey = %q", got)
	}
}
