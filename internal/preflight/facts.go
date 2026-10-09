package preflight

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// fkSelect names which foreign keys of the job's tables a fact covers.
type fkSelect int

const (
	fkIncoming fkSelect = iota // constraints whose parent is a job table
	fkOutgoing                 // constraints whose child is a job table
	fkWithin                   // constraints with both ends in the job
)

// server is one connection's facts. Every schema fact comes from dbsgomysql; the
// privilege answer and the range optimizer budget need the connection itself.
type server interface {
	Tables(ctx context.Context, tables []string) ([]validations.TableInfo, error)
	PrimaryKeys(ctx context.Context, tables []string) ([]validations.PKInfo, error)
	Triggers(ctx context.Context, tables []string, event validations.TriggerEvent) ([]validations.TriggerInfo, error)
	ForeignKeys(ctx context.Context, sel fkSelect, tables []string) (validations.ForeignKeyResult, error)
	// TableSpec captures one table of the server's database with its indexes.
	TableSpec(ctx context.Context, table string) (validations.TableSpec, error)
	Privileges(ctx context.Context) (privileges, error)
	// RangeOptimizerMaxMemSize is the session's range_optimizer_max_mem_size.
	RangeOptimizerMaxMemSize(ctx context.Context) (uint64, error)
}

// privileges judges the account's privileges. The library emits a finding for every
// privilege that is not GrantPresent.
type privileges interface {
	Tables(schema string, tables []string, p validations.Privilege) []validations.Finding
	Schema(schema string, privs []validations.Privilege) []validations.Finding
}

type mysqlServer struct {
	db     *sql.DB
	schema string
	insp   *validations.Inspector
}

func newMySQLServer(db *sql.DB, schema string) *mysqlServer {
	return &mysqlServer{db: db, schema: schema, insp: validations.NewInspector(db, schema)}
}

func (s *mysqlServer) Tables(ctx context.Context, tables []string) ([]validations.TableInfo, error) {
	return s.insp.Tables(ctx, tables)
}

func (s *mysqlServer) PrimaryKeys(ctx context.Context, tables []string) ([]validations.PKInfo, error) {
	return s.insp.PrimaryKeys(ctx, tables)
}

func (s *mysqlServer) Triggers(ctx context.Context, tables []string, event validations.TriggerEvent) ([]validations.TriggerInfo, error) {
	return s.insp.Triggers(ctx, tables, event)
}

func (s *mysqlServer) ForeignKeys(ctx context.Context, sel fkSelect, tables []string) (validations.ForeignKeyResult, error) {
	switch sel {
	case fkIncoming:
		return s.insp.ForeignKeys(ctx, validations.IncomingTo(tables...))
	case fkOutgoing:
		return s.insp.ForeignKeys(ctx, validations.OutgoingFrom(tables...))
	default:
		return s.insp.ForeignKeys(ctx, validations.Within(tables...))
	}
}

func (s *mysqlServer) TableSpec(ctx context.Context, table string) (validations.TableSpec, error) {
	return s.insp.TableSpec(ctx, validations.Ref(s.schema, table), validations.WithIndexes())
}

// Privileges reads the grants through a dedicated connection and releases it at once.
// The library's strongest answers need an exact *sql.Conn, and holding it across another
// query would deadlock a pool limited to one connection (max_connections: 1).
func (s *mysqlServer) Privileges(ctx context.Context) (privileges, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for privilege inspection: %w", err)
	}
	grants, gerr := validations.NewInspector(conn, s.schema).Grants(ctx)
	if cerr := conn.Close(); cerr != nil && gerr == nil {
		return nil, fmt.Errorf("release privilege-inspection connection: %w", cerr)
	}
	if gerr != nil {
		return nil, gerr
	}
	return grantsJudge{grants}, nil
}

func (s *mysqlServer) RangeOptimizerMaxMemSize(ctx context.Context) (uint64, error) {
	var v uint64
	if err := s.db.QueryRowContext(ctx, "SELECT @@SESSION.range_optimizer_max_mem_size").Scan(&v); err != nil {
		return 0, fmt.Errorf("read range_optimizer_max_mem_size: %w", err)
	}
	return v, nil
}

type grantsJudge struct{ g validations.Grants }

func (j grantsJudge) Tables(schema string, tables []string, p validations.Privilege) []validations.Finding {
	return validations.CheckTablePrivileges(j.g, schema, tables, p)
}

func (j grantsJudge) Schema(schema string, privs []validations.Privilege) []validations.Finding {
	return validations.CheckSchemaPrivileges(j.g, schema, privs)
}

// memo is one fact, read on first use. The error is kept too: one broken connection must
// not give two verdicts in a run.
type memo[T any] struct {
	v      T
	err    error
	loaded bool
}

func (m *memo[T]) get(load func() (T, error)) (T, error) {
	if !m.loaded {
		m.v, m.err = load()
		m.loaded = true
	}
	return m.v, m.err
}

// facts is everything one run reads from the servers. Each fact is read by the first
// stage that needs it, at that stage's place in the order, so a later stage's query error
// never surfaces ahead of an earlier stage's finding. It holds values only, never a
// connection. A run is used by one goroutine.
type facts struct {
	cfg    Config
	tree   *jobspec.Tree
	tables []string // copy order
	src    server
	dst    server

	srcTables, dstTables         memo[[]validations.TableInfo]
	srcPKs, dstPKs               memo[[]validations.PKInfo]
	srcSpecs, dstSpecs           memo[map[string]validations.TableSpec]
	srcPrivileges, dstPrivileges memo[privileges]
	srcDeleteTriggers            memo[[]validations.TriggerInfo]
	dstInsertTriggers            memo[[]validations.TriggerInfo]
	fkIn, fkOut, fkWithin        memo[validations.ForeignKeyResult]

	// Results of earlier stages.
	keys    map[string]key.Def // source keys, by job table
	dstKeys map[string]key.Def // destination keys, set by checkDestinationKeys
	caps    []Cap
}

func newFacts(cfg Config, src, dst server) *facts {
	tree := jobspec.NewTree(cfg.Spec)
	var tables []string
	for _, n := range tree.CopyOrder() {
		tables = append(tables, n.Table)
	}
	return &facts{cfg: cfg, tree: tree, tables: tables, src: src, dst: dst}
}

func (f *facts) sourceTables(ctx context.Context) ([]validations.TableInfo, error) {
	return f.srcTables.get(func() ([]validations.TableInfo, error) { return f.src.Tables(ctx, f.tables) })
}

func (f *facts) destTables(ctx context.Context) ([]validations.TableInfo, error) {
	return f.dstTables.get(func() ([]validations.TableInfo, error) { return f.dst.Tables(ctx, f.tables) })
}

func (f *facts) sourcePKs(ctx context.Context) ([]validations.PKInfo, error) {
	return f.srcPKs.get(func() ([]validations.PKInfo, error) { return f.src.PrimaryKeys(ctx, f.tables) })
}

func (f *facts) destPKs(ctx context.Context) ([]validations.PKInfo, error) {
	return f.dstPKs.get(func() ([]validations.PKInfo, error) { return f.dst.PrimaryKeys(ctx, f.tables) })
}

func (f *facts) sourceSpecs(ctx context.Context) (map[string]validations.TableSpec, error) {
	return f.srcSpecs.get(func() (map[string]validations.TableSpec, error) { return specs(ctx, f.src, "source", f.tables) })
}

func (f *facts) destSpecs(ctx context.Context) (map[string]validations.TableSpec, error) {
	return f.dstSpecs.get(func() (map[string]validations.TableSpec, error) { return specs(ctx, f.dst, "destination", f.tables) })
}

func specs(ctx context.Context, s server, side string, tables []string) (map[string]validations.TableSpec, error) {
	out := make(map[string]validations.TableSpec, len(tables))
	for _, t := range tables {
		spec, err := s.TableSpec(ctx, t)
		if err != nil {
			return nil, fmt.Errorf("%s table %s: %w", side, t, err)
		}
		if !spec.Captured.Has(validations.SectionIndexes) {
			return nil, fmt.Errorf("PREFLIGHT_INSPECTION_INTEGRITY: the %s spec of table %q did not capture "+
				"its indexes, so its key cannot be judged. Preflight fails closed", side, t)
		}
		out[t] = spec
	}
	return out, nil
}

func (f *facts) sourcePrivileges(ctx context.Context) (privileges, error) {
	return f.srcPrivileges.get(func() (privileges, error) { return f.src.Privileges(ctx) })
}

func (f *facts) destPrivileges(ctx context.Context) (privileges, error) {
	return f.dstPrivileges.get(func() (privileges, error) { return f.dst.Privileges(ctx) })
}

func (f *facts) sourceDeleteTriggers(ctx context.Context) ([]validations.TriggerInfo, error) {
	return f.srcDeleteTriggers.get(func() ([]validations.TriggerInfo, error) {
		return f.src.Triggers(ctx, f.tables, validations.TriggerDelete)
	})
}

func (f *facts) destInsertTriggers(ctx context.Context) ([]validations.TriggerInfo, error) {
	return f.dstInsertTriggers.get(func() ([]validations.TriggerInfo, error) {
		return f.dst.Triggers(ctx, f.tables, validations.TriggerInsert)
	})
}

// foreignKeys returns one selection of the job tables' constraints. The incoming
// selection also carries the proof that metadata discovery was complete, so visibility
// and coverage read facts and proof from the same result.
func (f *facts) foreignKeys(ctx context.Context, sel fkSelect) (validations.ForeignKeyResult, error) {
	m := map[fkSelect]*memo[validations.ForeignKeyResult]{fkIncoming: &f.fkIn, fkOutgoing: &f.fkOut, fkWithin: &f.fkWithin}[sel]
	return m.get(func() (validations.ForeignKeyResult, error) { return f.src.ForeignKeys(ctx, sel, f.tables) })
}
