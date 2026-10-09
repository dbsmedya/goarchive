package key

import (
	"fmt"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// Def is a table's primary key: its columns in PRIMARY KEY order, as MySQL reports them.
// MySQL is authoritative (spec §4.1); a Def is never built from config.
type Def struct {
	Table   string
	Columns []Column
}

// Names returns the key's column names in key order.
func (d Def) Names() []string {
	out := make([]string, len(d.Columns))
	for i, c := range d.Columns {
		out[i] = c.Name
	}
	return out
}

// NoPrimaryKeyError reports a participating table without a PRIMARY KEY (PRIMARY_KEY_CHECK).
type NoPrimaryKeyError struct{ Table string }

func (e *NoPrimaryKeyError) Error() string {
	return fmt.Sprintf("PRIMARY_KEY_CHECK: %s has no PRIMARY KEY; a table without one cannot be addressed by key", e.Table)
}

// FromFacts builds a Def from dbsgomysql facts: pk from Inspector.PrimaryKeys and spec
// from Inspector.TableSpec captured WithIndexes(). It refuses a missing key, a prefix
// or functional key part, and every type outside the §4.4 allowlist.
func FromFacts(pk validations.PKInfo, spec validations.TableSpec) (Def, error) {
	table := spec.Table
	if pk.Kind == validations.PKNone || len(pk.Columns) == 0 {
		return Def{}, &NoPrimaryKeyError{Table: table}
	}
	if !spec.Captured.Has(validations.SectionIndexes) {
		return Def{}, fmt.Errorf("key: TableSpec for %s was captured without WithIndexes", table)
	}
	var primary *validations.IndexSpec
	for i := range spec.Indexes {
		if spec.Indexes[i].Name == "PRIMARY" {
			primary = &spec.Indexes[i]
			break
		}
	}
	if primary == nil {
		return Def{}, &NoPrimaryKeyError{Table: table}
	}
	if len(primary.Parts) != len(pk.Columns) {
		return Def{}, fmt.Errorf("key: %s: PRIMARY index has %d parts but PrimaryKeys reports %d columns",
			table, len(primary.Parts), len(pk.Columns))
	}
	columns := make(map[string]validations.ColumnSpec, len(spec.Columns))
	for _, c := range spec.Columns {
		columns[c.Name] = c
	}
	def := Def{Table: table, Columns: make([]Column, 0, len(pk.Columns))}
	for i, part := range primary.Parts {
		if part.Expression != "" || part.Column == "" {
			return Def{}, &UnsupportedError{Table: table, Column: part.Expression, Type: "functional key part",
				Reason: "a functional key part does not name a column"}
		}
		if part.Column != pk.Columns[i] {
			return Def{}, fmt.Errorf("key: %s: PRIMARY part %d is %q but PrimaryKeys reports %q",
				table, i+1, part.Column, pk.Columns[i])
		}
		spec, ok := columns[part.Column]
		if !ok {
			return Def{}, fmt.Errorf("key: %s: key column %q is missing from TableSpec", table, part.Column)
		}
		if part.SubPart != 0 {
			return Def{}, &UnsupportedError{Table: table, Column: part.Column, Type: spec.Type,
				Reason: fmt.Sprintf("prefix key part (%d) does not identify the full value", part.SubPart)}
		}
		col, err := ColumnFromSpec(table, spec)
		if err != nil {
			return Def{}, err
		}
		def.Columns = append(def.Columns, col)
	}
	return def, nil
}
