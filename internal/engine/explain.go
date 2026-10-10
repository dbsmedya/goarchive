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

// Explanation is dry-run's report (spec §7.2): the plans MySQL chooses for the statements a
// run sends most often. No rule acts on them; they are for the operator to read.
type Explanation struct {
	// Preflight is the job's preflight result, under its type's profile (spec §8.1).
	Preflight *preflight.Result
	// Roots is how many root keys the first batch holds.
	Roots int
	// Fetch is the root fetch's plan. Discovery holds one plan per child table, in copy
	// order, each bound with the first chunk of its parent's keys from the first batch.
	Fetch     Plan
	Discovery []Plan
	// Notes are remarks on the plans, for example a fetch that sorts every batch.
	Notes []string
}

// Plan is one statement's EXPLAIN FORMAT=TRADITIONAL.
type Plan struct {
	Table string
	Query string // the EXPLAIN statement as sent; key values are bound parameters
	Rows  []ExplainRow
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

	ex := &Explanation{Preflight: res}
	if ex.Fetch, err = explainOne(ctx, src, r.root.Table, fetchSQL(r.root, cfg.Spec.Where, false), cfg.Processing.BatchSize); err != nil {
		return nil, err
	}
	for _, row := range ex.Fetch.Rows {
		if strings.Contains(row.Extra, "Using filesort") {
			ex.Notes = append(ex.Notes, fmt.Sprintf("The root fetch on %s sorts its matching rows on every batch (Using filesort). "+
				"A descending part in the primary key, or a where clause served by a secondary index, causes this.", r.root.Table))
			break
		}
	}
	roots, err := r.fetch(ctx, src, nil)
	if err != nil {
		return nil, err
	}
	ex.Roots = len(roots)
	if len(roots) == 0 {
		ex.Notes = append(ex.Notes, "No rows match the where clause, so there are no keys to bind: discovery was not explained.")
		return ex, nil
	}
	keys := map[string][]key.Key{r.root.Table: roots}
	for _, n := range r.tree.CopyOrder() {
		if n.Parent == nil {
			continue
		}
		parents := keys[n.Parent.Table]
		if len(parents) == 0 {
			ex.Notes = append(ex.Notes, fmt.Sprintf("The first batch has no %s rows, so %s's discovery was not explained.", n.Parent.Table, n.Table))
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
		ex.Discovery = append(ex.Discovery, p)
		if keys[n.Table], err = r.discoverChild(ctx, src, n, parents); err != nil {
			return nil, err
		}
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
