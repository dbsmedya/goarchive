package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/types"
)

// Every key-addressed statement of a batch is built here, from key definitions MySQL
// reported (internal/key), with every key value a bound parameter (spec §4.2 rule 3). Each
// carries a membership list only, so key.MaxTuples budgets all of its placeholders (M5-2);
// the root fetch carries the continuation and the LIMIT instead.

// op names a kind of key-addressed statement, as errors and the fault seam see it.
type op string

const (
	opDiscover op = "discovery"
	opRead     op = "copy read"
	opCount    op = "count verification"
	opHash     op = "sha256 verification"
	opDelete   op = "delete"
	opPresent  op = "recovery read"
	opHeld     op = "recovery destination read"
)

// statement is one key-addressed statement before it runs.
type statement struct {
	op          op
	destination bool // runs on the destination; otherwise on the source
	table       string
	query       string
	args        []any
	display     Display // dry-run's view of the statement (discovery only)
}

// fetchSQL is the root fetch (spec §7.2): the next batch of root keys in ascending key
// order, after the last key of the previous batch when continued. where is the job's text
// between parentheses, unmodified; the newline ends a trailing "--" comment before the
// parenthesis that closes it.
func fetchSQL(root key.Def, where string, continued bool) string {
	var b strings.Builder
	b.WriteString("SELECT " + key.Projection(root) + " FROM " + sqlutil.QuoteIdentifier(root.Table))
	b.WriteString(" WHERE (" + where + "\n)")
	if continued {
		b.WriteString(" AND " + key.After(root))
	}
	b.WriteString(" ORDER BY " + key.OrderBy(root) + " LIMIT ?")
	return b.String()
}

// Display is a statement as dry-run shows it (spec §3.8), built beside its SQL from the same
// parts: no part is parsed back out of SQL, and the job's where is kept as written.
type Display struct {
	Select    string // "SELECT <projection> FROM <table>"
	Where     string // the job's where, verbatim (the fetch)
	Predicate string // the key predicate, key.InDisplay (a discovery)
	Tail      string // "ORDER BY <key> LIMIT <n>" (the fetch)
	Tuples    int    // the parent tuples this statement binds (a discovery)
	Of        int    // the parent's tuples in the first batch (a discovery)
	Parent    string // the parent table (a discovery)
}

// fetchDisplay is the first batch's root fetch, as fetchSQL builds it, with limit bound.
func fetchDisplay(root key.Def, where string, limit int) Display {
	return Display{
		Select: "SELECT " + key.Projection(root) + " FROM " + sqlutil.QuoteIdentifier(root.Table),
		Where:  where,
		Tail:   "ORDER BY " + key.OrderBy(root) + " LIMIT " + strconv.Itoa(limit),
	}
}

// discoveryDisplay is discoverySQL for tuples of the parent's of first-batch tuples.
func discoveryDisplay(child key.Def, n *jobspec.Node, bind []key.Column, tuples, of int) Display {
	return Display{
		Select:    "SELECT " + key.Projection(child) + " FROM " + sqlutil.QuoteIdentifier(child.Table),
		Predicate: key.InDisplay(n.ForeignKey, bind, fmt.Sprintf("%d %s tuples", tuples, n.Parent.Table)),
		Tuples:    tuples, Of: of, Parent: n.Parent.Table,
	}
}

// rangeCountSQL is the where range's count: dry-run runs it, and --progress reads the
// optimizer's estimate of it with EXPLAIN (PRC-1). The where text sits between parentheses
// exactly as in the fetch.
func rangeCountSQL(root key.Def, where string) string {
	return "SELECT COUNT(*) FROM " + sqlutil.QuoteIdentifier(root.Table) + " WHERE (" + where + "\n)"
}

// discoverySQL reads the keys of child's rows whose foreign_key columns hold one of tuples
// parent key tuples (spec §7.3). The columns are listed in config order and each is bound
// like the parent column it pairs with (jobspec.ProjectColumns); the caller binds every
// parent tuple projected into references order (jobspec.Project).
func discoverySQL(child key.Def, n *jobspec.Node, bind []key.Column, tuples int) string {
	return "SELECT " + key.Projection(child) + " FROM " + sqlutil.QuoteIdentifier(child.Table) +
		" WHERE " + key.In(n.ForeignKey, bind, tuples)
}

// byKey is the membership predicate on d's own key.
func byKey(d key.Def, tuples int) string { return key.In(d.Names(), d.Columns, tuples) }

// readSQL is the copy read (spec §7.4): the key projection first, for the identity check,
// then every column of the table, temporal ones as text.
func readSQL(d key.Def, meta types.ColumnMetadata, tuples int) (string, error) {
	cols, err := meta.Projection()
	if err != nil {
		return "", err
	}
	return "SELECT " + key.Projection(d) + ", " + cols + " FROM " + sqlutil.QuoteIdentifier(d.Table) + " WHERE " + byKey(d, tuples), nil
}

// countSQL is count verification of one chunk.
func countSQL(d key.Def, tuples int) string {
	return "SELECT COUNT(*) FROM " + sqlutil.QuoteIdentifier(d.Table) + " WHERE " + byKey(d, tuples)
}

// hashSQL reads one chunk's rows for sha256 verification, in key order on both servers.
func hashSQL(d key.Def, meta types.ColumnMetadata, tuples int) (string, error) {
	cols, err := meta.Projection()
	if err != nil {
		return "", err
	}
	return "SELECT " + cols + " FROM " + sqlutil.QuoteIdentifier(d.Table) + " WHERE " + byKey(d, tuples) + " ORDER BY " + key.OrderBy(d), nil
}

// deleteSQL deletes one chunk by key (spec §7.5). There is never a range delete.
func deleteSQL(d key.Def, tuples int) string {
	return "DELETE FROM " + sqlutil.QuoteIdentifier(d.Table) + " WHERE " + byKey(d, tuples)
}

// byKeySQL reads which of the bound keys a table holds: recovery's read of the logged roots
// in the source, and its check of every rediscovered key in the destination (GH #7).
func byKeySQL(d key.Def, tuples int) string {
	return "SELECT " + key.Projection(d) + " FROM " + sqlutil.QuoteIdentifier(d.Table) + " WHERE " + byKey(d, tuples)
}

// insertSQL is a strict multi-row INSERT of rows rows. INSERT IGNORE does not exist in 3.0
// (decision 8).
func insertSQL(table string, columns []string, rows int) string {
	tuple := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(columns)), ", ") + ")"
	values := strings.TrimSuffix(strings.Repeat(tuple+", ", rows), ", ")
	return "INSERT INTO " + sqlutil.QuoteIdentifier(table) + " (" + quotedColumnList(columns) + ") VALUES " + values
}
