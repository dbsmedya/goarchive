package tracking

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// Bookkeeping column names of a log table. Key columns are k1…kN, so a root key column
// named ga_status cannot collide with them (spec §6.2, R27 B4).
const (
	colStatus    = "ga_status"
	colUpdatedAt = "ga_updated_at"
)

// logColumnName is the generated name of the log's key column at position i (0-based).
func logColumnName(i int) string { return "k" + strconv.Itoa(i+1) }

// logDef is the log table's key: the root key's columns renamed k1…kN, with TIMESTAMP
// stored as DATETIME of the same precision, so no session time zone can shift it (spec
// §6.2). Values bind and read back exactly as the root's: key.Placeholder casts both
// families to DATETIME(n), and key.Projection reads both as text.
func logDef(table string, root key.Def) key.Def {
	d := key.Def{Table: table, Columns: make([]key.Column, len(root.Columns))}
	for i, c := range root.Columns {
		c.Name = logColumnName(i)
		if c.Family == key.FamilyTimestamp {
			c.Family, c.Type = key.FamilyDateTime, temporalType("datetime", c.Fsp)
		}
		d.Columns[i] = c
	}
	return d
}

func temporalType(base string, fsp int) string {
	if fsp == 0 {
		return base
	}
	return fmt.Sprintf("%s(%d)", base, fsp)
}

// typeText is the shape of a key column's NormalizedType that may be written into DDL.
// MySQL reports it; this guard keeps anything else, however it arrived, out of a CREATE.
var typeText = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|integer|bigint|decimal|char|varchar|binary|varbinary|date|datetime|time|year)(\([0-9]+(,[0-9]+)?\))?( unsigned)?( zerofill)?$`)

// columnDDL is one log key column's definition, with the root column's source name in
// its COMMENT for DBAs. GoArchive never reads the comment back (R28).
func columnDDL(log, root key.Column) (string, error) {
	if !typeText.MatchString(log.Type) {
		return "", fmt.Errorf("tracking: key column %s has type %q, which GoArchive cannot write into a log table", root.Name, log.Type)
	}
	def := sqlutil.QuoteIdentifier(log.Name) + " " + log.Type
	if log.Family == key.FamilyString {
		if !sqlutil.IsSimpleIdentifier(log.Charset) || !sqlutil.IsSimpleIdentifier(log.Collation) {
			return "", fmt.Errorf("tracking: key column %s has charset %q, collation %q", root.Name, log.Charset, log.Collation)
		}
		def += " CHARACTER SET " + log.Charset + " COLLATE " + log.Collation
	}
	return def + " NOT NULL COMMENT " + commentLiteral(root.Name), nil
}

// commentLiteral quotes s as a string literal for a COMMENT clause. A column name may
// hold any character; the comment is for people only.
func commentLiteral(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(s) + "'"
}

// createLogSQL is the DDL of archiver_job_log_<id> for root (spec §6.2).
func createLogSQL(qualified string, root key.Def) (string, error) {
	ld := logDef("", root)
	cols := make([]string, 0, len(root.Columns)+4)
	for i := range root.Columns {
		def, err := columnDDL(ld.Columns[i], root.Columns[i])
		if err != nil {
			return "", err
		}
		cols = append(cols, "  "+def)
	}
	cols = append(cols,
		"  "+colStatus+" TINYINT NOT NULL",
		"  "+colUpdatedAt+" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
		"  PRIMARY KEY ("+key.OrderBy(ld)+")",
		"  KEY idx_ga_status ("+colStatus+")")
	return "CREATE TABLE IF NOT EXISTS " + qualified + " (\n" + strings.Join(cols, ",\n") + "\n) ENGINE=InnoDB", nil
}

// encodeKeyColumns is archiver_job.root_key_columns: the root key's source column names,
// in key order. JSON keeps a name holding a comma or a quote apart from its neighbours.
func encodeKeyColumns(names []string) string {
	b, err := json.Marshal(names)
	if err != nil {
		panic(fmt.Sprintf("tracking: marshal key columns: %v", err))
	}
	return string(b)
}

func decodeKeyColumns(text string) ([]string, error) {
	var names []string
	if err := json.Unmarshal([]byte(text), &names); err != nil || len(names) == 0 {
		return nil, fmt.Errorf("tracking: unreadable root_key_columns %q", text)
	}
	return names, nil
}

// LogKeyError refuses a job whose root key changed since the job was created (spec §6.2):
// the log's typed columns are fixed for the job's life.
type LogKeyError struct {
	Job    string
	Reason string
}

func (e *LogKeyError) Error() string {
	return fmt.Sprintf("job %q: the root table's key changed since the job was created: %s. "+
		"The job's log is typed by the key it was created with, so it cannot record this key. "+
		"Retire the job (docs/README_JOBS_SCHEMA.md, \"Retiring a job completely\") or use a new job name",
		e.Job, e.Reason)
}

// checkKeyNames is the log-key check's first half: the root's current PRIMARY KEY column
// names, in order, must equal root_key_columns. It detects a renamed, reordered, added or
// removed key column.
func checkKeyNames(job string, stored []string, root key.Def) error {
	if current := root.Names(); !slices.Equal(stored, current) {
		return &LogKeyError{Job: job, Reason: fmt.Sprintf("it was (%s) and is now (%s)",
			strings.Join(stored, ", "), strings.Join(current, ", "))}
	}
	return nil
}

// checkLogColumns is the log-key check's second half: the log's key columns k1…kN, as
// the library reads them from job_schema, must match the root's current key columns by
// position in type, charset and collation. TIMESTAMP → DATETIME(n) is the one allowed
// mapping. Bookkeeping columns aside, the log must hold exactly k1…kN.
func checkLogColumns(job string, spec validations.TableSpec, root key.Def) error {
	want := logDef(spec.Table, root)
	var got []validations.ColumnSpec
	for _, c := range spec.Columns {
		if c.Name != colStatus && c.Name != colUpdatedAt {
			got = append(got, c)
		}
	}
	if len(got) != len(want.Columns) {
		return &LogKeyError{Job: job, Reason: fmt.Sprintf("the log %s has %d key columns and the root key has %d",
			spec.Table, len(got), len(want.Columns))}
	}
	for i, c := range got {
		w := want.Columns[i]
		r := root.Columns[i]
		gotType := strings.ToLower(strings.TrimSpace(c.NormalizedType))
		switch {
		case c.Name != w.Name:
			return &LogKeyError{Job: job, Reason: fmt.Sprintf("the log %s has column %s at position %d, expected %s",
				spec.Table, c.Name, i+1, w.Name)}
		case gotType != w.Type:
			return &LogKeyError{Job: job, Reason: fmt.Sprintf("key column %s is %s, and the log stores %s",
				r.Name, r.Type, gotType)}
		case c.Charset != w.Charset || c.Collation != w.Collation:
			return &LogKeyError{Job: job, Reason: fmt.Sprintf("key column %s is %s %s, and the log stores %s %s",
				r.Name, r.Charset, r.Collation, c.Charset, c.Collation)}
		}
	}
	return nil
}
