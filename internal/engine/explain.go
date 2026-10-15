package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// Explanation is dry-run's report (spec §7.2, §3.8 of the console-output spec): how much the
// job covers, the first batch's rows per table, and the plans MySQL chooses for the
// statements a run sends most often. No rule acts on them; they are for the operator to read.
type Explanation struct {
	// Preflight is the job's preflight result, under its type's profile (spec §8.1).
	Preflight *preflight.Result
	// Count is the where range's size, counted now on Explain's own source connection.
	Count int64
	// Roots is how many root keys the first batch holds.
	Roots int
	// Rows holds the first batch's rows by table, the root's included (Roots).
	Rows map[string]int
	// Fetch is the root fetch's plan. Discovery holds one plan per child table, in copy
	// order, each bound with the first chunk of its parent's keys from the first batch; a
	// table whose parent has no rows in the first batch has Skipped set and no plan.
	Fetch     Plan
	Discovery []Plan
}

// Plan is one statement's EXPLAIN FORMAT=TRADITIONAL.
type Plan struct {
	Table   string
	Query   string // the EXPLAIN statement as sent; key values are bound parameters
	Display Display
	Rows    []ExplainRow
	// Notes are remarks on this statement, for example a fetch that sorts every batch.
	Notes []string
	// Skipped says why the table has no plan (its parent has no rows in the first batch).
	Skipped string
}

// ExplainRow is one row of EXPLAIN FORMAT=TRADITIONAL. Each field holds the column's text,
// empty for NULL.
type ExplainRow struct {
	ID, SelectType, Table, Partitions, Type, PossibleKeys, Key, KeyLen, Ref, Rows, Filtered, Extra string
}

// explainFormat is always named: 9.7 defaults to TREE, which has no type, key or rows column.
const explainFormat = "EXPLAIN FORMAT=TRADITIONAL "

// Explain runs dry-run for one job: the same checks as Run before its first lock (identity,
// the UTC pin, preflight), then the first batch's root keys and the plans of the root fetch
// and of each child's discovery (spec §7.2). It takes no lock, writes no tracking row and
// changes no data. The discovery statements also run, to give each grandchild its keys.
func Explain(ctx context.Context, cfg Config) (*Explanation, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.NewDefault()
	}
	srcID, _, res, err := prepare(ctx, cfg)
	if err != nil {
		return nil, err
	}
	r, err := newRun(ctx, cfg, res, nil, srcID.Schema)
	if err != nil {
		return nil, err
	}
	src, err := cfg.Source.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = src.Close() }()

	ex := &Explanation{Preflight: res, Rows: map[string]int{}}
	if ex.Fetch, err = explainOne(ctx, src, r.root.Table, fetchSQL(r.root, cfg.Spec.Where, false), cfg.Processing.BatchSize); err != nil {
		return nil, err
	}
	ex.Fetch.Display = fetchDisplay(r.root, cfg.Spec.Where, cfg.Processing.BatchSize)
	for _, row := range ex.Fetch.Rows {
		if strings.Contains(row.Extra, "Using filesort") {
			ex.Fetch.Notes = append(ex.Fetch.Notes, fmt.Sprintf("The root fetch on %s sorts its matching rows on every batch (Using filesort). "+
				"A descending part in the primary key, or a where clause served by a secondary index, causes this.", r.root.Table))
			break
		}
	}
	// The where range's size, on the connection Explain holds: a source pool of one
	// connection must not wait for a second.
	if err := src.QueryRowContext(ctx, rangeCountSQL(r.root, cfg.Spec.Where)).Scan(&ex.Count); err != nil {
		return nil, fmt.Errorf("engine: dry-run count on %s: %w", r.root.Table, err)
	}
	roots, err := r.fetch(ctx, src, nil)
	if err != nil {
		return nil, err
	}
	ex.Roots = len(roots)
	ex.Rows[r.root.Table] = len(roots)
	if len(roots) == 0 {
		return ex, nil
	}
	keys := map[string][]key.Key{r.root.Table: roots}
	for _, n := range r.tree.CopyOrder() {
		if n.Parent == nil {
			continue
		}
		parents := keys[n.Parent.Table]
		if len(parents) == 0 {
			ex.Discovery = append(ex.Discovery, Plan{Table: n.Table, Skipped: fmt.Sprintf("the first batch has no %s rows", n.Parent.Table)})
			continue
		}
		stmts, err := r.discoveryStatements(n, parents)
		if err != nil {
			return nil, err
		}
		p, err := explainOne(ctx, src, n.Table, stmts[0].query, stmts[0].args...)
		if err != nil {
			return nil, err
		}
		p.Display = stmts[0].display
		ex.Discovery = append(ex.Discovery, p)
		if keys[n.Table], err = r.discoverChild(ctx, src, n, parents); err != nil {
			return nil, err
		}
		ex.Rows[n.Table] = len(keys[n.Table])
	}
	return ex, nil
}

// explainOne runs EXPLAIN FORMAT=TRADITIONAL of query on q.
func explainOne(ctx context.Context, q queryer, table, query string, args ...any) (Plan, error) {
	p := Plan{Table: table, Query: explainFormat + query}
	rows, err := q.QueryContext(ctx, p.Query, args...)
	if err != nil {
		return p, fmt.Errorf("engine: EXPLAIN on %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return p, err
	}
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range vals {
			dest[i] = &vals[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return p, fmt.Errorf("engine: EXPLAIN on %s: %w", table, err)
		}
		var row ExplainRow
		fields := map[string]*string{
			"id": &row.ID, "select_type": &row.SelectType, "table": &row.Table, "partitions": &row.Partitions,
			"type": &row.Type, "possible_keys": &row.PossibleKeys, "key": &row.Key, "key_len": &row.KeyLen,
			"ref": &row.Ref, "rows": &row.Rows, "filtered": &row.Filtered, "Extra": &row.Extra,
		}
		for i, c := range cols {
			if f, ok := fields[c]; ok {
				*f = vals[i].String
			}
		}
		p.Rows = append(p.Rows, row)
	}
	return p, rows.Err()
}
