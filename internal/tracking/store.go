package tracking

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// facts are the library's table facts for job_schema. Every schema fact comes from
// dbsgomysql; GoArchive reads only its own tables with its own SELECTs.
type facts interface {
	Tables(ctx context.Context, tables []string) ([]validations.TableInfo, error)
	TableSpec(ctx context.Context, table string) (validations.TableSpec, error)
}

type inspectorFacts struct {
	schema string
	insp   *validations.Inspector
}

func (f inspectorFacts) Tables(ctx context.Context, tables []string) ([]validations.TableInfo, error) {
	return f.insp.Tables(ctx, tables)
}

func (f inspectorFacts) TableSpec(ctx context.Context, table string) (validations.TableSpec, error) {
	return f.insp.TableSpec(ctx, validations.Ref(f.schema, table), validations.WithIndexes())
}

// Store is the tracking schema on the destination server.
type Store struct {
	db        *sql.DB
	schema    string
	jobTable  string // quoted, qualified
	metaTable string
	facts     facts
	budget    uint64 // the destination's range_optimizer_max_mem_size
}

// Open checks the tracking schema in jobSchema on the destination pool db, refusing any
// label but 3.0 and any 2.x schema (spec §6.1, R37), and creates and stamps a new one. It
// is idempotent and safe for concurrent runs. db's sessions must be pinned to UTC
// (spec §4.5), as every GoArchive pool is.
func Open(ctx context.Context, db *sql.DB, jobSchema string) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("tracking: no destination connection")
	}
	return open(ctx, db, jobSchema, inspectorFacts{schema: jobSchema, insp: validations.NewInspector(db, jobSchema)})
}

func open(ctx context.Context, db *sql.DB, jobSchema string, f facts) (*Store, error) {
	if !sqlutil.IsSimpleIdentifier(jobSchema) {
		return nil, fmt.Errorf("tracking: invalid job_schema %q: only letters, digits and underscores", jobSchema)
	}
	q := sqlutil.QuoteIdentifier(jobSchema)
	s := &Store{db: db, schema: jobSchema, facts: f,
		jobTable:  q + "." + sqlutil.QuoteIdentifier(jobTableName),
		metaTable: q + "." + sqlutil.QuoteIdentifier(metaTableName)}
	if err := s.ensureSchema(ctx); err != nil {
		return nil, err
	}
	if err := db.QueryRowContext(ctx, "SELECT @@SESSION.range_optimizer_max_mem_size").Scan(&s.budget); err != nil {
		return nil, fmt.Errorf("tracking: read range_optimizer_max_mem_size: %w", err)
	}
	return s, nil
}

func (s *Store) logTable(id int64) (name, qualified string) {
	name = fmt.Sprintf("archiver_job_log_%d", id)
	return name, sqlutil.QuoteIdentifier(s.schema) + "." + sqlutil.QuoteIdentifier(name)
}
