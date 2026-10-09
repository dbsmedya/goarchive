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
