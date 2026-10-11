package preflight

import (
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

func TestTablesExist(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	delete(w.src.specs, "line_notes")
	if e := w.mustFail("TABLE_EXISTENCE_CHECK"); !hasTable(e, "line_notes") {
		t.Errorf("tables = %v", e.Tables)
	}

	w = healthy(t, jobspec.Archive)
	w.src.types = map[string]string{"order_lines": "VIEW"}
	if e := w.mustFail("TABLE_EXISTENCE_CHECK"); !hasTable(e, "order_lines(VIEW)") {
		t.Errorf("tables = %v, want the view named with its type", e.Tables)
	}

	w = healthy(t, jobspec.Archive)
	delete(w.dst.specs, "orders")
	w.refused("DEST_TABLE_EXISTENCE_CHECK")

	w = healthy(t, jobspec.CopyOnly)
	w.dst.types = map[string]string{"orders": "VIEW"}
	w.refused("DEST_TABLE_EXISTENCE_CHECK")
}

func TestStorageEngine(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.src.engines = map[string]string{"orders": "MyISAM", "line_notes": ""}
	e := w.mustFail("STORAGE_ENGINE_CHECK")
	if !hasTable(e, "orders(MyISAM)") || !hasTable(e, "line_notes(<unknown>)") {
		t.Errorf("tables = %v", e.Tables)
	}
}

// TestPermissions: each privilege check passes only on a provable grant, for the job types
// that need it.
func TestPermissions(t *testing.T) {
	for _, tc := range []struct {
		typ   jobspec.Type
		dst   bool
		priv  validations.Privilege
		table string
		check string
	}{
		{jobspec.Archive, false, validations.PrivilegeSelect, "order_lines", "SOURCE_SELECT_PERMISSION_CHECK"},
		{jobspec.CopyOnly, false, validations.PrivilegeSelect, "orders", "SOURCE_SELECT_PERMISSION_CHECK"},
		{jobspec.Archive, false, validations.PrivilegeDelete, "line_notes", "SOURCE_DELETE_PERMISSION_CHECK"},
		{jobspec.Purge, false, validations.PrivilegeDelete, "orders", "SOURCE_DELETE_PERMISSION_CHECK"},
		{jobspec.Archive, true, validations.PrivilegeInsert, "orders", "DEST_WRITE_PERMISSION_CHECK"},
		{jobspec.CopyOnly, true, validations.PrivilegeInsert, "order_lines", "DEST_WRITE_PERMISSION_CHECK"},
	} {
		t.Run(tc.check+"/"+string(tc.typ), func(t *testing.T) {
			w := healthy(t, tc.typ)
			s := w.src
			if tc.dst {
				s = w.dst
			}
			s.lacking = map[validations.Privilege][]string{tc.priv: {tc.table}}
			if e := w.mustFail(tc.check); !hasTable(e, tc.table+"(absent)") {
				t.Errorf("tables = %v", e.Tables)
			}
		})
	}
	t.Run("purge writes nothing to destination tables", func(t *testing.T) {
		w := healthy(t, jobspec.Purge)
		w.dst.lacking = map[validations.Privilege][]string{validations.PrivilegeInsert: {"orders"}}
		w.mustPass()
	})
}

func TestJobSchemaPermissions(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		w := healthy(t, typ)
		w.dst.lacking = map[validations.Privilege][]string{validations.PrivilegeCreate: {"*"}, validations.PrivilegeUpdate: {"*"}}
		e := w.mustFail("JOB_SCHEMA_PERMISSION_CHECK")
		if !strings.Contains(e.Message, "CREATE DATABASE `dst`; GRANT CREATE, UPDATE ON `dst`.* TO <user>") {
			t.Errorf("%s: message = %s", typ, e.Message)
		}
	}
}

func TestTriggers(t *testing.T) {
	w := healthy(t, jobspec.Archive)
	w.src.triggers = []validations.TriggerInfo{{Table: "order_lines", Name: "trg_del", Event: "DELETE", Timing: "BEFORE"}}
	if e := w.mustFail("DELETE_TRIGGER_CHECK"); !hasTable(e, "order_lines(trg_del)") {
		t.Errorf("tables = %v", e.Tables)
	}
	w.cfg.ForceTriggers = true
	w.mustPass()
	if !w.warned("--force-triggers") {
		t.Error("forced triggers were not reported")
	}

	// --force-triggers never covers a destination INSERT trigger.
	w.dst.triggers = []validations.TriggerInfo{{Table: "orders", Name: "trg_ins", Event: "INSERT", Timing: "AFTER"}}
	if e := w.mustFail("DEST_INSERT_TRIGGER_CHECK"); !hasTable(e, "orders(trg_ins)") {
		t.Errorf("tables = %v", e.Tables)
	}
}

func TestCascadeRulesWarn(t *testing.T) {
	w := healthy(t, jobspec.Purge)
	w.src.fks[1].OnDelete = "CASCADE"
	w.mustPass()
	// The constraint has both ends in the job, so the incoming and the outgoing selection
	// both hold it; it is reported once.
	if !w.warned("ON DELETE CASCADE rules detected (1)") ||
		!w.warned("src.line_notes(order_id,line_no)->src.order_lines(order_id,line_no)") {
		t.Errorf("no cascade warning naming the composite constraint once; logs: %v", w.logs.All())
	}

	// Two constraints declaring the same edge, here in another column order, print it once;
	// the duplicate advisory names both.
	w = healthy(t, jobspec.Purge)
	w.src.fks[1].OnDelete = "CASCADE"
	again := w.src.fks[1]
	again.ConstraintName = "fk_notes_line_again"
	again.ChildColumns, again.ParentColumns = []string{"line_no", "order_id"}, []string{"line_no", "order_id"}
	w.src.fks = append(w.src.fks, again)
	w.mustPass()
	if !w.warned("ON DELETE CASCADE rules detected (1)") {
		t.Errorf("a duplicate CASCADE constraint was reported twice; logs: %v", w.logs.All())
	}
}

// TestForeignKeyCoverage: a table outside the job referencing a job table is fatal, for
// every job type and ON DELETE rule.
func TestForeignKeyCoverage(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		w := healthy(t, typ)
		ext := fk("fk_refund", "refunds", []string{"order_id"}, "orders", []string{"id"})
		ext.OnDelete = "SET NULL"
		w.src.fks = append(w.src.fks, ext)
		e := w.mustFail("FK_COVERAGE_CHECK")
		if !strings.Contains(e.Message, "orders is referenced by: [src.refunds (ON DELETE SET NULL)]") {
			t.Errorf("%s: message = %s", typ, e.Message)
		}
	}
}

// TestStatementCaps: the cap of each statement is key.MaxTuples over the columns of the
// index it addresses: the table's key for statements by key, the child's foreign_key
// columns for discovery, and the destination's own key for destination statements.
func TestStatementCaps(t *testing.T) {
	w := stringKeyWorld(t, "utf8mb3 utf8mb3_general_ci", "utf8mb4 utf8mb4_general_ci")
	// The child's foreign_key column is wider than the parent key it references, so the
	// discovery cap differs from the parent's; the destination has its own budget.
	setColumn(w.src, "order_lines", col("order_id", "varchar(255)", "utf8mb3 utf8mb3_general_ci"))
	setColumn(w.dst, "order_lines", col("order_id", "varchar(255)", "utf8mb4 utf8mb4_general_ci"))
	w.dst.mem = 16 << 20
	res := w.mustPass()
	got := map[string]int{}
	for _, c := range res.Caps {
		got[c.Server+" "+c.Statement+" "+c.Table] = c.Tuples
	}
	src := mustColumn(t, "varchar(36)", "utf8mb3 utf8mb3_general_ci")
	srcFK := mustColumn(t, "varchar(255)", "utf8mb3 utf8mb3_general_ci")
	dst := mustColumn(t, "varchar(36)", "utf8mb4 utf8mb4_general_ci")
	dstFK := mustColumn(t, "varchar(255)", "utf8mb4 utf8mb4_general_ci")
	small := mustColumn(t, "smallint", "")
	const srcMem, dstMem = 8 << 20, 16 << 20
	want := map[string]int{
		"source by key orders":           key.MaxTuples([]key.Column{src}, srcMem),
		"source by key order_lines":      key.MaxTuples([]key.Column{srcFK, small}, srcMem),
		"source discovery order_lines":   key.MaxTuples([]key.Column{srcFK}, srcMem),
		"destination by key orders":      key.MaxTuples([]key.Column{dst}, dstMem),
		"destination by key order_lines": key.MaxTuples([]key.Column{dstFK, small}, dstMem),
	}
	if len(got) != len(want) {
		t.Errorf("caps = %v, want %v", got, want)
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("%s = %d, want %d", k, got[k], n)
		}
	}
	if got["source discovery order_lines"] >= got["source by key orders"] {
		t.Errorf("discovery addresses the wider foreign_key index, so its cap must be lower: %v", got)
	}

	// A purge job addresses no destination rows. A budget of 0 means unlimited, which
	// leaves only the 65,535-placeholder bound.
	w = healthy(t, jobspec.Purge)
	w.src.mem = 0
	for _, c := range w.mustPass().Caps {
		if c.Server != "source" || c.Tuples != 65535/len(c.Columns) {
			t.Errorf("cap %+v, want a source cap of 65535/%d", c, len(c.Columns))
		}
	}
}

// TestMaxAllowedPacket (M7c F3): a key-addressed statement at its cap must fit each
// server's max_allowed_packet, or preflight refuses before any row is read; the run would
// otherwise fail mid-batch with error 1153. Pair: the healthy world's 64 MiB passes, the
// same world with 16 KiB on one server is refused, naming that server. Purge is checked on
// the destination too, where its tracking log statements run. It is a policy check.
func TestMaxAllowedPacket(t *testing.T) {
	// These cases describe statements at their caps (M7c-1): batch sizes above every cap
	// keep the bound there (M7c-F4, TestMaxAllowedPacketBatchBound).
	atCaps := func(t *testing.T, typ jobspec.Type) *world {
		w := healthy(t, typ)
		w.cfg.Processing = config.ProcessingConfig{BatchSize: 1 << 20, BatchDeleteSize: 1 << 20}
		return w
	}
	atCaps(t, jobspec.Archive).mustPass()
	for _, side := range []string{"source", "destination"} {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
			w := atCaps(t, typ)
			if side == "source" {
				w.src.packet = 16 << 10
			} else {
				w.dst.packet = 16 << 10
			}
			e := w.mustFail("MAX_ALLOWED_PACKET_CHECK")
			for _, want := range []string{side + " max_allowed_packet is 16384 bytes", "key tuples", "Raise max_allowed_packet"} {
				if !strings.Contains(e.Message, want) {
					t.Errorf("%s %s: message %q lacks %q", side, typ, e.Message, want)
				}
			}
			if len(e.Tables) != 1 {
				t.Errorf("%s %s: tables = %v, want the one table with the largest statement", side, typ, e.Tables)
			}
		}
	}
	w := atCaps(t, jobspec.Archive)
	w.src.packet, w.cfg.SkipPolicy = 16<<10, true
	w.mustPass()

	// The tracking log's INSERT binds a status beside each root key, in VALUES text: at the
	// healthy world's 16,000-root cap of a BIGINT UNSIGNED key it sends about 324 KB, more
	// than its key lookups (Fable M7c-1 review I1). 320,000 bytes on the destination must be
	// refused, naming that statement.
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		w := atCaps(t, typ)
		w.dst.packet = 320000
		if e := w.mustFail("MAX_ALLOWED_PACKET_CHECK"); !strings.Contains(e.Message, "a tracking log insert statement on orders") {
			t.Errorf("%s: message %q does not name the tracking log's INSERT", typ, e.Message)
		}
	}

	// Above the driver's own 64 MiB, the client's limit decides: a VARCHAR(255) utf8mb4 key
	// at an unlimited range budget (the 65,535-placeholder cap) needs about 67 MB, and the
	// remedy is to shrink the caps, not to raise the server's packet.
	w = atCaps(t, jobspec.Archive)
	w.jobYAML = withoutNotes
	w.src.fks = w.src.fks[:1]
	for _, s := range []*fakeServer{w.src, w.dst} {
		setColumn(s, "orders", col("id", "varchar(255)", "utf8mb4 utf8mb4_0900_ai_ci"))
		setColumn(s, "order_lines", col("order_id", "varchar(255)", "utf8mb4 utf8mb4_0900_ai_ci"))
	}
	w.dst.mem, w.dst.packet = 0, 1<<30
	if e := w.mustFail("MAX_ALLOWED_PACKET_CHECK"); !strings.Contains(e.Message, "client's 64 MiB packet limit") {
		t.Errorf("message %q does not name the client's limit", e.Message)
	} else if !strings.Contains(e.Message, "set range_optimizer_max_mem_size on the destination") || !strings.Contains(e.Message, "batch_size") {
		// Fable M7c-1 review M4(b): at an unlimited range budget there is nothing to lower;
		// the DBA must set a limit, or the job must send fewer keys.
		t.Errorf("message %q: at range_optimizer_max_mem_size = 0 the remedy must be to set a limit or lower batch_size", e.Message)
	}

	// Codex M7c-1 review B1: a latin1 VARCHAR(255) key is read and bound in utf8mb4, up to
	// four bytes a character, so a purge's log INSERT at 8,272 roots needs about 8.5 MB, not
	// the 2.2 MB its stored bytes suggest. 4 MiB on the destination is refused; 16 MiB passes.
	latin1 := atCaps(t, jobspec.Purge)
	latin1.jobYAML = withoutNotes
	latin1.src.fks = latin1.src.fks[:1]
	for _, s := range []*fakeServer{latin1.src, latin1.dst} {
		setColumn(s, "orders", col("id", "varchar(255)", "latin1 latin1_bin"))
		setColumn(s, "order_lines", col("order_id", "varchar(255)", "latin1 latin1_bin"))
	}
	latin1.dst.packet = 4 << 20
	if e := latin1.mustFail("MAX_ALLOWED_PACKET_CHECK"); !strings.Contains(e.Message, "destination max_allowed_packet") {
		t.Errorf("latin1 key: message %q does not name the destination", e.Message)
	}
	latin1.dst.packet = 16 << 20
	latin1.mustPass()

	// Codex M7c-1 review B2: discovery binds the parent's key values, projected into
	// references order, against the child's foreign_key index, so the parent's widths bound
	// it. Each case has a wide parent key over a narrow foreign key: the narrow index caps
	// discovery high, every tuple is a wide parent value, and discovery is the source's
	// largest statement. The smaller source packet is refused naming discovery; the larger
	// one passes.
	for _, tc := range []struct {
		name, parent, child string
		cs                  string
		refused, passes     uint64
	}{
		// 16,000 tuples of 1,020-byte values: about 16 MB.
		{"VARCHAR(255) over VARCHAR(1)", "varchar(255)", "varchar(1)", "utf8mb4 utf8mb4_bin", 4 << 20, 32 << 20},
		// 16,000 tuples of 67-character values: about 1.1 MB, above the root's own lookups.
		{"DECIMAL(65,0) over DECIMAL(10,0)", "decimal(65,0)", "decimal(10,0)", "", 768 << 10, 2 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := atCaps(t, jobspec.Archive)
			w.jobYAML = withoutNotes
			w.src.fks = w.src.fks[:1]
			for _, s := range []*fakeServer{w.src, w.dst} {
				setColumn(s, "orders", col("id", tc.parent, tc.cs))
				setColumn(s, "order_lines", col("order_id", tc.child, tc.cs))
			}
			w.src.packet = tc.refused
			if e := w.mustFail("MAX_ALLOWED_PACKET_CHECK"); !strings.Contains(e.Message, "a discovery statement on order_lines") {
				t.Errorf("message %q does not name discovery on order_lines", e.Message)
			}
			w.src.packet = tc.passes
			w.mustPass()
		})
	}
}

// TestMaxAllowedPacketBatchBound (M7c-2, M7c-F4): the engine never sends more keys in one
// statement than batch_size, or batch_delete_size for the source's deletes (engine
// run.go byKeyChunk, discoveryChunk, deleteChunk; the tracking log sees one batch of
// roots), so the check bounds each statement there, not at its cap.
// Pair: a 2 MiB destination with a VARCHAR(255) utf8mb4 key passes at batch_size 1000
// (about 1 MB a statement) and is refused at 16000 (the cap), offering batch_size. A
// batch_delete_size above batch_size is the source's by-key bound, and is named.
func TestMaxAllowedPacketBatchBound(t *testing.T) {
	setup := func(batch, deletes int) *world {
		w := healthy(t, jobspec.Archive)
		w.jobYAML = withoutNotes
		w.src.fks = w.src.fks[:1]
		for _, s := range []*fakeServer{w.src, w.dst} {
			setColumn(s, "orders", col("id", "varchar(255)", "utf8mb4 utf8mb4_0900_ai_ci"))
			setColumn(s, "order_lines", col("order_id", "varchar(255)", "utf8mb4 utf8mb4_0900_ai_ci"))
		}
		w.cfg.Processing = config.ProcessingConfig{BatchSize: batch, BatchDeleteSize: deletes}
		return w
	}

	w := setup(1000, 500)
	w.dst.packet = 2 << 20
	w.mustPass()

	w = setup(16000, 500)
	w.dst.packet = 2 << 20
	e := w.mustFail("MAX_ALLOWED_PACKET_CHECK")
	for _, want := range []string{"destination max_allowed_packet is 2097152 bytes", "Raise max_allowed_packet on the destination", "lower batch_size to at most"} {
		if !strings.Contains(e.Message, want) {
			t.Errorf("batch_size 16000: message %q lacks %q", e.Message, want)
		}
	}

	// Deletes: batch_delete_size bounds the source's by-key statements when it is larger.
	w = setup(10, 16000)
	w.src.packet = 2 << 20
	e = w.mustFail("MAX_ALLOWED_PACKET_CHECK")
	if !strings.Contains(e.Message, "lower batch_delete_size to at most") || strings.Contains(e.Message, "lower batch_size") {
		t.Errorf("batch_delete_size 16000: message %q must offer batch_delete_size only (batch_size 10 fits)", e.Message)
	}
	w = setup(10, 1000)
	w.src.packet = 2 << 20
	w.mustPass()

	// Fable M7c-2 review F3: copy-only never deletes, so batch_delete_size bounds none of
	// its statements. The archive refused above with batch_delete_size 16000 passes as a
	// copy-only job.
	w = setup(10, 16000)
	w.jobType = jobspec.CopyOnly
	w.src.packet = 2 << 20
	w.mustPass()

	// A size below 1 is a configuration error, never a bound of 0.
	for _, p := range []config.ProcessingConfig{{BatchSize: 0, BatchDeleteSize: 500}, {BatchSize: 1000, BatchDeleteSize: 0}} {
		w := healthy(t, jobspec.Archive)
		w.cfg.Processing = p
		if _, err := w.run(); err == nil || !strings.Contains(err.Error(), "batch") {
			t.Errorf("run() with %+v = %v, want a configuration error naming the batch size", p, err)
		}
	}
}
