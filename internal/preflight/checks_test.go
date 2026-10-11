package preflight

import (
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

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
	healthy(t, jobspec.Archive).mustPass()
	for _, side := range []string{"source", "destination"} {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
			w := healthy(t, typ)
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
	w := healthy(t, jobspec.Archive)
	w.src.packet, w.cfg.SkipPolicy = 16<<10, true
	w.mustPass()

	// The tracking log's INSERT binds a status beside each root key, in VALUES text: at the
	// healthy world's 16,000-root cap of a BIGINT UNSIGNED key it sends about 324 KB, more
	// than its key lookups (Fable M7c-1 review I1). 320,000 bytes on the destination must be
	// refused, naming that statement.
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		w := healthy(t, typ)
		w.dst.packet = 320000
		if e := w.mustFail("MAX_ALLOWED_PACKET_CHECK"); !strings.Contains(e.Message, "a tracking log insert statement on orders") {
			t.Errorf("%s: message %q does not name the tracking log's INSERT", typ, e.Message)
		}
	}

	// Above the driver's own 64 MiB, the client's limit decides: a VARCHAR(255) utf8mb4 key
	// at an unlimited range budget (the 65,535-placeholder cap) needs about 67 MB, and the
	// remedy is to shrink the caps, not to raise the server's packet.
	w = healthy(t, jobspec.Archive)
	w.jobYAML = withoutNotes
	w.src.fks = w.src.fks[:1]
	for _, s := range []*fakeServer{w.src, w.dst} {
		setColumn(s, "orders", col("id", "varchar(255)", "utf8mb4 utf8mb4_0900_ai_ci"))
		setColumn(s, "order_lines", col("order_id", "varchar(255)", "utf8mb4 utf8mb4_0900_ai_ci"))
	}
	w.dst.mem, w.dst.packet = 0, 1<<30
	if e := w.mustFail("MAX_ALLOWED_PACKET_CHECK"); !strings.Contains(e.Message, "client's 64 MiB packet limit") {
		t.Errorf("message %q does not name the client's limit", e.Message)
	}
}
