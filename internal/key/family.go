// Package key is GoArchive 3.0's primary-key model: a table's key definition read from
// MySQL, typed key values, and the SQL shapes that address rows by key
// (spec 2026-10-07-3.0-core-design.md §4).
package key

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// Family classifies a key column by how its values are read, held in Go and bound.
type Family uint8

const (
	FamilyInvalid   Family = iota
	FamilySigned           // TINYINT … BIGINT; held as int64
	FamilyUnsigned         // TINYINT … BIGINT UNSIGNED; held as uint64
	FamilyDecimal          // DECIMAL(p,s), signed or unsigned; held as exact text
	FamilyString           // CHAR(n), VARCHAR(n); held as the bytes MySQL returned
	FamilyBinary           // BINARY(n), VARBINARY(n); held as the bytes MySQL returned
	FamilyDate             // DATE; held as SQL text
	FamilyDateTime         // DATETIME(fsp); held as SQL text
	FamilyTimestamp        // TIMESTAMP(fsp); held as SQL text (UTC session, spec §4.5)
	FamilyTime             // TIME(fsp); held as SQL text
	FamilyYear             // YEAR; held as int64
)

var familyNames = [...]string{"invalid", "signed", "unsigned", "decimal", "string", "binary",
	"date", "datetime", "timestamp", "time", "year"}

func (f Family) String() string {
	if int(f) < len(familyNames) {
		return familyNames[f]
	}
	return "Family(" + strconv.Itoa(int(f)) + ")"
}

// temporal reports whether values of f are projected and held as SQL text.
func (f Family) temporal() bool {
	return f == FamilyDate || f == FamilyDateTime || f == FamilyTimestamp || f == FamilyTime
}

// Column is one primary-key column as MySQL reports it.
type Column struct {
	Name      string // exact server-side spelling
	Type      string // NormalizedType from dbsgomysql, lower case
	Family    Family
	Precision int // DECIMAL precision
	Scale     int // DECIMAL scale
	Fsp       int // fractional-seconds precision of DATETIME, TIMESTAMP, TIME
	Length    int // CHAR, VARCHAR, BINARY, VARBINARY length
	Charset   string
	Collation string
}

// UnsupportedError reports a key column whose type 3.0 refuses (spec §4.4). The message
// carries the preflight code KEY_TYPE_UNSUPPORTED.
type UnsupportedError struct {
	Table, Column, Type, Reason string
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("KEY_TYPE_UNSUPPORTED: %s.%s (%s): %s", e.Table, e.Column, e.Type, e.Reason)
}

// ColumnFromSpec classifies one column fact against the key allowlist (spec §4.4). table
// is used only in errors. Preflight also uses it for a child's foreign_key columns, which
// pair with the parent's key columns (spec §8.3).
func ColumnFromSpec(table string, c validations.ColumnSpec) (Column, error) {
	t := strings.ToLower(strings.TrimSpace(c.NormalizedType))
	if t == "" {
		t = strings.ToLower(strings.TrimSpace(c.Type))
	}
	col := Column{Name: c.Name, Type: t, Charset: c.Charset, Collation: c.Collation}
	refuse := func(reason string) (Column, error) {
		return Column{}, &UnsupportedError{Table: table, Column: c.Name, Type: c.Type, Reason: reason}
	}
	base, args, rest := splitType(t)
	nums, numsOK := parseInts(args)
	switch base {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		col.Family = FamilySigned
		if strings.Contains(rest, "unsigned") {
			col.Family = FamilyUnsigned
		}
	case "decimal":
		if !numsOK || len(nums) != 2 {
			return refuse("DECIMAL without an explicit precision and scale")
		}
		col.Family, col.Precision, col.Scale = FamilyDecimal, nums[0], nums[1]
	case "char", "varchar":
		if !numsOK || len(nums) != 1 {
			return refuse("character type without a length")
		}
		if c.Charset == "" || c.Collation == "" {
			return refuse("character type without a charset and collation")
		}
		col.Family, col.Length = FamilyString, nums[0]
	case "binary", "varbinary":
		if !numsOK || len(nums) != 1 {
			return refuse("binary type without a length")
		}
		col.Family, col.Length = FamilyBinary, nums[0]
	case "date":
		col.Family = FamilyDate
	case "datetime", "timestamp", "time":
		fsp := 0
		if args != "" {
			if !numsOK || len(nums) != 1 {
				return refuse("unreadable fractional-seconds precision")
			}
			fsp = nums[0]
		}
		col.Fsp = fsp
		col.Family = map[string]Family{"datetime": FamilyDateTime, "timestamp": FamilyTimestamp, "time": FamilyTime}[base]
	case "year":
		col.Family = FamilyYear
	case "float", "double", "real":
		return refuse("approximate values cannot identify a row")
	case "enum", "set":
		return refuse("sorts by its internal index, not by its text, so a key-order scan could skip rows")
	case "bit":
		return refuse("BIT keys are not supported")
	default:
		return refuse("not an allowed key type (spec §4.4)")
	}
	return col, nil
}

// splitType splits "decimal(30,10) unsigned" into ("decimal", "30,10", " unsigned") and
// "int unsigned" into ("int", "", " unsigned").
func splitType(t string) (base, args, rest string) {
	if open := strings.IndexByte(t, '('); open >= 0 {
		if closeAt := strings.LastIndexByte(t, ')'); closeAt > open {
			return t[:open], t[open+1 : closeAt], t[closeAt+1:]
		}
	}
	if sp := strings.IndexByte(t, ' '); sp >= 0 {
		return t[:sp], "", t[sp:]
	}
	return t, "", ""
}

// parseInts parses "30,10" into [30 10]. An empty string yields no numbers and ok=true.
func parseInts(args string) ([]int, bool) {
	if args == "" {
		return nil, true
	}
	parts := strings.Split(args, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
}
