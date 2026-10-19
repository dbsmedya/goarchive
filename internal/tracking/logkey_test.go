package tracking

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/key"
)

// orders is a root key of a VARCHAR, a TIMESTAMP(3) and an INT UNSIGNED, the first named
// ga_status to show that generated names cannot collide with the bookkeeping columns.
func orders() key.Def {
	return key.Def{Table: "orders", Columns: []key.Column{
		{Name: "ga_status", Type: "varchar(8)", Family: key.FamilyString, Length: 8, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
		{Name: "placed", Type: "timestamp(3)", Family: key.FamilyTimestamp, Fsp: 3},
		{Name: "it's", Type: "int unsigned", Family: key.FamilyUnsigned},
	}}
}

func TestLogDef(t *testing.T) {
	d := logDef("archiver_job_log_7", orders())
	if !slices.Equal(d.Names(), []string{"k1", "k2", "k3"}) || d.Table != "archiver_job_log_7" {
		t.Fatalf("logDef = %+v", d)
	}
	if c := d.Columns[1]; c.Family != key.FamilyDateTime || c.Type != "datetime(3)" || c.Fsp != 3 {
		t.Errorf("TIMESTAMP(3) maps to %+v, want DATETIME(3)", c)
	}
	if c := d.Columns[0]; c.Family != key.FamilyString || c.Charset != "utf8mb4" || c.Collation != "utf8mb4_0900_ai_ci" {
		t.Errorf("VARCHAR maps to %+v", c)
	}
	if c := logDef("", key.Def{Columns: []key.Column{{Name: "t", Type: "timestamp", Family: key.FamilyTimestamp}}}).Columns[0]; c.Type != "datetime" {
		t.Errorf("TIMESTAMP maps to %q, want datetime", c.Type)
	}
	if orders().Columns[1].Family != key.FamilyTimestamp {
		t.Error("logDef changed its argument")
	}
}

func TestCreateLogSQL(t *testing.T) {
	ddl, err := createLogSQL("`s`.`archiver_job_log_7`", orders())
	if err != nil {
		t.Fatal(err)
	}
	const want = "CREATE TABLE IF NOT EXISTS `s`.`archiver_job_log_7` (\n" +
		"  `k1` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT 'ga_status',\n" +
		"  `k2` datetime(3) NOT NULL COMMENT 'placed',\n" +
		"  `k3` int unsigned NOT NULL COMMENT 'it''s',\n" +
		"  ga_status TINYINT NOT NULL,\n" +
		"  ga_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
		"  PRIMARY KEY (`k1`, `k2`, `k3`),\n" +
		"  KEY idx_ga_status (ga_status)\n" +
		") ENGINE=InnoDB"
	if ddl != want {
		t.Errorf("createLogSQL =\n%s\nwant\n%s", ddl, want)
	}
}

// TestColumnDDLRefusesUnwritableTypes: only a key type's known shape reaches a CREATE.
func TestColumnDDLRefusesUnwritableTypes(t *testing.T) {
	for _, c := range []key.Column{
		{Name: "a", Type: "int) ENGINE=MyISAM; DROP TABLE x; (", Family: key.FamilySigned},
		{Name: "b", Type: "enum('a','b')", Family: key.FamilyString, Charset: "utf8mb4", Collation: "utf8mb4_bin"},
		{Name: "c", Type: "varchar(8)", Family: key.FamilyString, Charset: "utf8mb4 COLLATE x", Collation: "utf8mb4_bin"},
		{Name: "d", Type: "varchar(8)", Family: key.FamilyString, Charset: "utf8mb4", Collation: ""},
	} {
		if ddl, err := columnDDL(c, c); err == nil {
			t.Errorf("columnDDL(%+v) = %q", c, ddl)
		}
	}
	for _, typ := range []string{"bigint", "tinyint(1)", "int(5) unsigned zerofill", "decimal(10,2)", "binary(16)", "date", "time(2)", "year"} {
		if _, err := columnDDL(key.Column{Name: "x", Type: typ}, key.Column{Name: "x"}); err != nil {
			t.Errorf("columnDDL(%s) = %v", typ, err)
		}
	}
	if got := commentLiteral(`a'b\c`); got != `'a''b\\c'` {
		t.Errorf("commentLiteral = %s", got)
	}
}

func TestKeyColumnsRoundTrip(t *testing.T) {
	names := []string{"a,b", `q"`, "région"}
	got, err := decodeKeyColumns(encodeKeyColumns(names))
	if err != nil || !slices.Equal(got, names) {
		t.Fatalf("round trip = %v, %v", got, err)
	}
	for _, text := range []string{"", "[]", "a,b", "null"} {
		if _, err := decodeKeyColumns(text); err == nil {
			t.Errorf("decodeKeyColumns(%q) passed", text)
		}
	}
}

// TestCheckKeyNames is the log-key check's first half: a renamed, reordered, added or
// removed key column is refused, and the message shows both keys (spec §6.2).
func TestCheckKeyNames(t *testing.T) {
	stored := []string{"ga_status", "placed", "it's"}
	if err := checkKeyNames("j", stored, orders()); err != nil {
		t.Fatalf("same key: %v", err)
	}
	for name, edit := range map[string]func(d *key.Def){
		"renamed":   func(d *key.Def) { d.Columns[1].Name = "placed_at" },
		"reordered": func(d *key.Def) { d.Columns[0], d.Columns[1] = d.Columns[1], d.Columns[0] },
		"added": func(d *key.Def) {
			d.Columns = append(d.Columns, key.Column{Name: "x", Type: "int", Family: key.FamilySigned})
		},
		"removed":   func(d *key.Def) { d.Columns = d.Columns[:2] },
		"case only": func(d *key.Def) { d.Columns[1].Name = "Placed" },
	} {
		d := orders()
		edit(&d)
		err := checkKeyNames("j", stored, d)
		var le *LogKeyError
		if !errors.As(err, &le) || !strings.Contains(err.Error(), "it was (ga_status, placed, it's) and is now") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func logSpec(cols ...validations.ColumnSpec) validations.TableSpec {
	return validations.TableSpec{Table: "archiver_job_log_7", Engine: "InnoDB", Captured: validations.SectionIndexes,
		Columns: append(cols,
			validations.ColumnSpec{Name: "ga_status", NormalizedType: "tinyint"},
			validations.ColumnSpec{Name: "ga_updated_at", NormalizedType: "timestamp", OnUpdate: true}),
		Indexes: []validations.IndexSpec{
			{Name: "PRIMARY", Unique: true, Parts: []validations.IndexPart{{Column: "k1"}, {Column: "k2"}, {Column: "k3"}}},
			{Name: "idx_ga_status", Parts: []validations.IndexPart{{Column: "ga_status"}}},
		}}
}

// TestCheckLogTable is the log's table check: a status write must roll back with the
// work it records, and a logged key must fail a second insert (spec §6.2, §6.3). So the
// log is InnoDB, its primary key is exactly k1…kN, and its bookkeeping columns are as
// created. idx_ga_status is an access path only.
func TestCheckLogTable(t *testing.T) {
	healthy := func() validations.TableSpec { return logSpec(healthyLog()...) }
	if got := checkLogTable(healthy(), 3); got != "" {
		t.Fatalf("healthy log: %s", got)
	}
	noStatusIndex := healthy()
	noStatusIndex.Indexes = noStatusIndex.Indexes[:1]
	if got := checkLogTable(noStatusIndex, 3); got != "" {
		t.Errorf("a log without idx_ga_status: %s", got)
	}
	column := func(s validations.TableSpec, name string) *validations.ColumnSpec {
		for i := range s.Columns {
			if s.Columns[i].Name == name {
				return &s.Columns[i]
			}
		}
		t.Fatalf("no column %s", name)
		return nil
	}
	drop := func(s *validations.TableSpec, name string) {
		s.Columns = slices.DeleteFunc(s.Columns, func(c validations.ColumnSpec) bool { return c.Name == name })
	}
	for _, tc := range []struct {
		name string
		edit func(s *validations.TableSpec)
		want string
	}{
		{"MyISAM", func(s *validations.TableSpec) { s.Engine = "MyISAM" }, "its engine is MyISAM, not InnoDB"},
		{"indexes not read", func(s *validations.TableSpec) { s.Captured, s.Indexes = 0, nil }, "its indexes were not read"},
		{"no primary key", func(s *validations.TableSpec) { s.Indexes = s.Indexes[1:] }, "its primary key is not (k1, k2, k3)"},
		{"a reordered primary key", func(s *validations.TableSpec) {
			s.Indexes[0].Parts[0], s.Indexes[0].Parts[1] = s.Indexes[0].Parts[1], s.Indexes[0].Parts[0]
		},
			"its primary key is not (k1, k2, k3)"},
		{"a shorter primary key", func(s *validations.TableSpec) { s.Indexes[0].Parts = s.Indexes[0].Parts[:2] }, "its primary key is not (k1, k2, k3)"},
		{"a prefix part", func(s *validations.TableSpec) { s.Indexes[0].Parts[0].SubPart = 4 }, "its primary key is not (k1, k2, k3)"},
		{"a descending part", func(s *validations.TableSpec) { s.Indexes[0].Parts[2].Descending = true }, "its primary key is not (k1, k2, k3)"},
		{"no ga_status", func(s *validations.TableSpec) { drop(s, "ga_status") }, "column ga_status is missing or not tinyint NOT NULL"},
		{"a nullable ga_status", func(s *validations.TableSpec) { column(*s, "ga_status").Nullable = true }, "column ga_status is missing or not tinyint NOT NULL"},
		{"an int ga_status", func(s *validations.TableSpec) { column(*s, "ga_status").NormalizedType = "int" }, "column ga_status is missing or not tinyint NOT NULL"},
		{"no ga_updated_at", func(s *validations.TableSpec) { drop(s, "ga_updated_at") }, "column ga_updated_at is missing or not timestamp NOT NULL"},
		{"a datetime ga_updated_at", func(s *validations.TableSpec) { column(*s, "ga_updated_at").NormalizedType = "datetime" },
			"column ga_updated_at is missing or not timestamp NOT NULL"},
	} {
		s := healthy()
		tc.edit(&s)
		if got := checkLogTable(s, 3); !strings.Contains(got, tc.want) {
			t.Errorf("%s: checkLogTable() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func healthyLog() []validations.ColumnSpec {
	return []validations.ColumnSpec{
		{Name: "k1", NormalizedType: "varchar(8)", Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
		{Name: "k2", NormalizedType: "datetime(3)"},
		{Name: "k3", NormalizedType: "int unsigned"},
	}
}

// TestCheckLogColumns is the log-key check's second half: the log's k1…kN must match the
// root key by position in type, charset and collation, TIMESTAMP → DATETIME(n) aside.
func TestCheckLogColumns(t *testing.T) {
	if err := checkLogColumns("j", logSpec(healthyLog()...), orders()); err != nil {
		t.Fatalf("healthy log: %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(cols []validations.ColumnSpec) []validations.ColumnSpec
		want string
	}{
		{"a wider type", func(c []validations.ColumnSpec) []validations.ColumnSpec {
			c[2].NormalizedType = "bigint unsigned"
			return c
		},
			"key column it's is int unsigned, and the log stores bigint unsigned"},
		{"another precision", func(c []validations.ColumnSpec) []validations.ColumnSpec {
			c[1].NormalizedType = "datetime(6)"
			return c
		},
			"key column placed is timestamp(3), and the log stores datetime(6)"},
		{"a TIMESTAMP log column", func(c []validations.ColumnSpec) []validations.ColumnSpec {
			c[1].NormalizedType = "timestamp(3)"
			return c
		},
			"and the log stores timestamp(3)"},
		{"a collation", func(c []validations.ColumnSpec) []validations.ColumnSpec { c[0].Collation = "utf8mb4_bin"; return c },
			"key column ga_status is utf8mb4 utf8mb4_0900_ai_ci, and the log stores utf8mb4 utf8mb4_bin"},
		{"a charset", func(c []validations.ColumnSpec) []validations.ColumnSpec { c[0].Charset = "utf8mb3"; return c },
			"and the log stores utf8mb3 utf8mb4_0900_ai_ci"},
		{"a missing column", func(c []validations.ColumnSpec) []validations.ColumnSpec { return c[:2] },
			"has 2 key columns and the root key has 3"},
		{"an extra column", func(c []validations.ColumnSpec) []validations.ColumnSpec {
			return append(c, validations.ColumnSpec{Name: "k4", NormalizedType: "int"})
		}, "has 4 key columns and the root key has 3"},
		{"a renamed column", func(c []validations.ColumnSpec) []validations.ColumnSpec { c[0].Name = "region"; return c },
			"has column region at position 1, expected k1"},
	} {
		err := checkLogColumns("j", logSpec(tc.edit(healthyLog())...), orders())
		var le *LogKeyError
		if !errors.As(err, &le) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %q", tc.name, err, tc.want)
		}
	}
}
