package engine

import (
	"database/sql"
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

func TestConcurrentError(t *testing.T) {
	live := &ConcurrentError{RootTable: "orders", JobTable: "`t`.archiver_job", Jobs: []tracking.RunningJob{
		{Name: "a", Beat: true, HeartbeatAge: 5 * time.Second}, {Name: "b", Beat: true, HeartbeatAge: time.Hour}}}
	if msg := live.Error(); !strings.Contains(msg, "another job is running") || !strings.Contains(msg, "a") || strings.Contains(msg, "UPDATE") {
		t.Errorf("a live job: %s", msg)
	}
	stale := &ConcurrentError{RootTable: "orders", JobTable: "`t`.archiver_job", Jobs: []tracking.RunningJob{
		{Name: "b", Beat: true, HeartbeatAge: time.Hour}, {Name: "c"}}}
	msg := stale.Error()
	for _, want := range []string{"b (heartbeat 1h0m0s ago)", "c (no heartbeat)", "UPDATE `t`.archiver_job SET job_status = 0"} {
		if !strings.Contains(msg, want) {
			t.Errorf("stale jobs: %q lacks %q", msg, want)
		}
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
