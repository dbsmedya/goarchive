package preflight

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
)

// fakeServer serves dbsgomysql's typed facts for a set of tables. It never sees SQL.
type fakeServer struct {
	schema   string
	specs    map[string]validations.TableSpec // drives Tables, PrimaryKeys and TableSpec
	types    map[string]string                // TABLE_TYPE overrides; default BASE TABLE
	engines  map[string]string                // engine overrides; default InnoDB
	triggers []validations.TriggerInfo
	fks      []validations.ForeignKey // every constraint touching the schema
	partial  bool                     // foreign-key metadata not proven complete
	lacking  map[validations.Privilege][]string
	mem      uint64
	calls    []string
}

func (s *fakeServer) call(name string) { s.calls = append(s.calls, name) }

func (s *fakeServer) Tables(_ context.Context, tables []string) ([]validations.TableInfo, error) {
	s.call("Tables")
	var out []validations.TableInfo
	for _, t := range tables {
		if _, ok := s.specs[t]; !ok {
			continue
		}
		typ, engine := baseTableType, "InnoDB"
		if v, ok := s.types[t]; ok {
			typ = v
		}
		if v, ok := s.engines[t]; ok {
			engine = v
		}
		out = append(out, validations.TableInfo{Table: t, Type: typ, Engine: engine})
	}
	return out, nil
}

func (s *fakeServer) PrimaryKeys(_ context.Context, tables []string) ([]validations.PKInfo, error) {
	s.call("PrimaryKeys")
	var out []validations.PKInfo
	for _, t := range tables {
		spec, ok := s.specs[t]
		if !ok {
			continue
		}
		pk := validations.PKInfo{Table: t, Kind: validations.PKNone}
		for _, idx := range spec.Indexes {
			if idx.Name == "PRIMARY" {
				for _, p := range idx.Parts {
					pk.Columns = append(pk.Columns, p.Column)
				}
				pk.Kind = validations.PKSingle
				if len(pk.Columns) > 1 {
					pk.Kind = validations.PKComposite
				}
			}
		}
		out = append(out, pk)
	}
	return out, nil
}

func (s *fakeServer) Triggers(_ context.Context, tables []string, event validations.TriggerEvent) ([]validations.TriggerInfo, error) {
	s.call("Triggers")
	var out []validations.TriggerInfo
	for _, tr := range s.triggers {
		if slices.Contains(tables, tr.Table) && strings.EqualFold(tr.Event, event.String()) {
			out = append(out, tr)
		}
	}
	return out, nil
}

func (s *fakeServer) ForeignKeys(_ context.Context, sel fkSelect, tables []string) (validations.ForeignKeyResult, error) {
	s.call("ForeignKeys")
	res := validations.ForeignKeyResult{Visibility: validations.VisibilityComplete}
	if s.partial {
		res.Visibility = validations.VisibilityUnconfirmed
	}
	in := func(schema, t string) bool { return schema == s.schema && slices.Contains(tables, t) }
	for _, fk := range s.fks {
		child, parent := in(fk.ChildSchema, fk.ChildTable), in(fk.ParentSchema, fk.ParentTable)
		if (sel == fkIncoming && parent) || (sel == fkOutgoing && child) || (sel == fkWithin && child && parent) {
			res.Keys = append(res.Keys, fk)
		}
	}
	return res, nil
}

func (s *fakeServer) TableSpec(_ context.Context, table string) (validations.TableSpec, error) {
	s.call("TableSpec")
	spec, ok := s.specs[table]
	if !ok {
		return validations.TableSpec{}, fmt.Errorf("table %s.%s does not exist", s.schema, table)
	}
	return spec, nil
}

func (s *fakeServer) Privileges(context.Context) (privileges, error) {
	s.call("Privileges")
	return fakePrivileges{s.lacking}, nil
}

func (s *fakeServer) RangeOptimizerMaxMemSize(context.Context) (uint64, error) {
	s.call("RangeOptimizerMaxMemSize")
	return s.mem, nil
}

// fakePrivileges holds every privilege except those listed: by table, or "*" for the
// schema.
type fakePrivileges struct {
	lacking map[validations.Privilege][]string
}

func (p fakePrivileges) Tables(schema string, tables []string, priv validations.Privilege) []validations.Finding {
	var out []validations.Finding
	for _, t := range tables {
		if slices.Contains(p.lacking[priv], t) {
			out = append(out, validations.Finding{Check: validations.IDTablePrivileges, Tables: []string{t},
				Facts: validations.PrivilegeFact{Schema: schema, Table: t, Privilege: priv, State: validations.GrantAbsent}})
		}
	}
	return out
}

func (p fakePrivileges) Schema(schema string, privs []validations.Privilege) []validations.Finding {
	var out []validations.Finding
	for _, priv := range privs {
		if slices.Contains(p.lacking[priv], "*") {
			out = append(out, validations.Finding{Check: validations.IDSchemaPrivileges,
				Facts: validations.PrivilegeFact{Schema: schema, Privilege: priv, State: validations.GrantAbsent}})
		}
	}
	return out
}

// col is a column fact. A character type gets charset and collation from cs, "charset
// collation"; other types take cs = "".
func col(name, typ, cs string) validations.ColumnSpec {
	c := validations.ColumnSpec{Name: name, Type: typ, NormalizedType: typ, Generated: validations.GeneratedNone}
	if cs != "" {
		c.Charset, c.Collation, _ = strings.Cut(cs, " ")
	}
	return c
}

// index is an index over whole columns.
func index(name string, unique bool, cols ...string) validations.IndexSpec {
	idx := validations.IndexSpec{Name: name, Unique: unique, Visible: true, Type: "BTREE"}
	for _, c := range cols {
		idx.Parts = append(idx.Parts, validations.IndexPart{Column: c})
	}
	return idx
}

// table is a table fact captured with its indexes; pk lists the PRIMARY KEY columns.
func table(name string, cols []validations.ColumnSpec, pk []string, indexes ...validations.IndexSpec) validations.TableSpec {
	spec := validations.TableSpec{Schema: "s", Table: name, Engine: "InnoDB", Columns: cols, Captured: validations.SectionIndexes}
	for i := range spec.Columns {
		spec.Columns[i].Ordinal = i + 1
	}
	if len(pk) > 0 {
		spec.Indexes = append(spec.Indexes, index("PRIMARY", true, pk...))
	}
	spec.Indexes = append(spec.Indexes, indexes...)
	return spec
}

// fk is a declared constraint in schema "src".
func fk(name, child string, childCols []string, parent string, parentCols []string) validations.ForeignKey {
	return validations.ForeignKey{ConstraintName: name, ChildSchema: "src", ChildTable: child, ChildColumns: childCols,
		ParentSchema: "src", ParentTable: parent, ParentColumns: parentCols, OnDelete: "RESTRICT", OnUpdate: "RESTRICT", Indexed: true}
}

// world is one job and its two servers. healthy returns a world every check passes.
type world struct {
	t       *testing.T
	jobYAML string
	jobType jobspec.Type
	src     *fakeServer
	dst     *fakeServer
	cfg     Config
	logs    *observer.ObservedLogs
}

// The job of healthy: orders, its lines on a composite key, and notes on a composite
// foreign key to the lines.
const healthyJob = `
    root_table: orders
    primary_key: id
    where: "1=1"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
        relations:
          - table: line_notes
            primary_key: note_id
            foreign_key: [order_id, line_no]
            references: [order_id, line_no]
`

// withoutNotes is healthyJob without line_notes, for tests that change order_lines'
// documented key: line_notes' references must name that key.
const withoutNotes = `
    root_table: orders
    primary_key: id
    where: "1=1"
    relations:
      - table: order_lines
        primary_key: [order_id, line_no]
        foreign_key: order_id
`

func healthySpecs() map[string]validations.TableSpec {
	return map[string]validations.TableSpec{
		"orders": table("orders", []validations.ColumnSpec{
			col("id", "bigint unsigned", ""), col("code", "varchar(20)", "utf8mb4 utf8mb4_0900_ai_ci"),
		}, []string{"id"}, index("uk_code", true, "code")),
		"order_lines": table("order_lines", []validations.ColumnSpec{
			col("order_id", "bigint unsigned", ""), col("line_no", "smallint", ""), col("sku", "varchar(32)", "utf8mb4 utf8mb4_0900_ai_ci"),
		}, []string{"order_id", "line_no"}),
		"line_notes": table("line_notes", []validations.ColumnSpec{
			col("note_id", "int", ""), col("order_id", "bigint unsigned", ""), col("line_no", "smallint", ""),
		}, []string{"note_id"}, index("ix_line", false, "order_id", "line_no")),
	}
}

func healthy(t *testing.T, typ jobspec.Type) *world {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	w := &world{
		t: t, jobYAML: healthyJob, jobType: typ, logs: logs,
		src: &fakeServer{schema: "src", specs: healthySpecs(), mem: 8 << 20, fks: []validations.ForeignKey{
			fk("fk_lines_order", "order_lines", []string{"order_id"}, "orders", []string{"id"}),
			fk("fk_notes_line", "line_notes", []string{"order_id", "line_no"}, "order_lines", []string{"order_id", "line_no"}),
		}},
		dst: &fakeServer{schema: "dst", specs: healthySpecs(), mem: 8 << 20},
	}
	w.cfg = Config{SourceDatabase: "src", DestinationDatabase: "dst", JobSchema: "dst",
		Verification: jobspec.VerifyCount,
		Logger:       &logger.Logger{SugaredLogger: zap.New(core).Sugar()}}
	return w
}

// spec loads the world's job through jobspec, as the commands do.
func (w *world) spec() *jobspec.Spec {
	w.t.Helper()
	doc, err := jobspec.Parse([]byte("source: {host: s, user: u, database: src}\n" +
		"destination: {host: d, user: u, database: dst}\njobs:\n  j:\n    job_type: " + string(w.jobType) + w.jobYAML))
	if err != nil {
		w.t.Fatalf("jobspec.Parse: %v", err)
	}
	s, err := doc.Job("j", "")
	if err != nil {
		w.t.Fatal(err)
	}
	return s
}

func (w *world) run() (*Result, error) {
	w.t.Helper()
	cfg := w.cfg
	cfg.Spec = w.spec()
	return run(context.Background(), cfg, w.src, w.dst)
}

// mustPass runs the world and fails the test on any error.
func (w *world) mustPass() *Result {
	w.t.Helper()
	res, err := w.run()
	if err != nil {
		w.t.Fatalf("run() = %v, want success", err)
	}
	return res
}

// mustFail runs the world and returns its *Error, which must carry check.
func (w *world) mustFail(check string) *Error {
	w.t.Helper()
	_, err := w.run()
	var pe *Error
	if !errors.As(err, &pe) {
		w.t.Fatalf("run() = %v (%T), want *Error %s", err, err, check)
	}
	if pe.Check != check {
		w.t.Fatalf("run() = %v, want check %s", pe, check)
	}
	return pe
}

// refused runs the world and requires it to fail with check.
func (w *world) refused(check string) {
	w.t.Helper()
	_ = w.mustFail(check)
}

// warned reports whether a warning containing s was logged.
func (w *world) warned(s string) bool {
	for _, e := range w.logs.All() {
		if e.Level == zapcore.WarnLevel && strings.Contains(e.Message, s) {
			return true
		}
	}
	return false
}

// hasTable reports whether one of e's tables contains s.
func hasTable(e *Error, s string) bool {
	return slices.ContainsFunc(e.Tables, func(t string) bool { return strings.Contains(t, s) })
}

// setColumn replaces column name of table t on server s.
func setColumn(s *fakeServer, t string, c validations.ColumnSpec) {
	spec := s.specs[t]
	cols := slices.Clone(spec.Columns)
	for i := range cols {
		if cols[i].Name == c.Name {
			c.Ordinal = cols[i].Ordinal
			cols[i] = c
		}
	}
	spec.Columns = cols
	s.specs[t] = spec
}

// setColumns replaces a column on both servers.
func setColumns(w *world, t string, c validations.ColumnSpec) {
	setColumn(w.src, t, c)
	setColumn(w.dst, t, c)
}
