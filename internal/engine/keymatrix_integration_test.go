//go:build integration

package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// keyFamily is a root keyed by one column of type typ, with the spec §10 regression
// witnesses as its eligible keys: values 2.x's text tracking could confuse.
type keyFamily struct {
	name, typ string
	keys      []string // SQL expressions of the eligible root keys
	other     string   // an ineligible root key
}

var keyFamilies = []keyFamily{
	{"int", "INT", []string{"-2147483648", "0", "1", "2147483647"}, "7"},
	{"bigint_unsigned", "BIGINT UNSIGNED", []string{"9007199254740992", "9007199254740993", "9223372036854775807",
		"9223372036854775808", "9223372036854775809", "18446744073709551615"}, "5"}, // above 2^53 and 2^63
	{"decimal", "DECIMAL(30,10)", []string{"12345678901234567890.0000000001", "12345678901234567890.0000000002", // equal as doubles
		"-0.0000000001", "0.0000000000"}, "3.5"},
	{"uuid", "CHAR(36) CHARACTER SET ascii COLLATE ascii_bin", []string{"'0b9e1d3c-6f2a-4c55-9a4e-2f3c8d7b1a01'", "'0b9e1d3c-6f2a-4c55-9a4e-2f3c8d7b1a02'"}, "'ffffffff-0000-0000-0000-000000000000'"},
	{"binary16", "BINARY(16)", []string{"UNHEX('FF00FE01FD02FC03FB04FA05F906F807')", "UNHEX('00FF00FF00FF00FF00FF00FF00FF00FF')", "UNHEX('C0C1F5F6F7F8F9FAFBFCFDFEFF000000')"}, "UNHEX('11111111111111111111111111111111')"}, // not UTF-8
	{"varbinary", "VARBINARY(8)", []string{"X''", "X'00'", "X'0000'", "X'FF'"}, "X'AA'"},
	{"varchar_bin", "VARCHAR(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", []string{"'abc'", "'ABC'", "REPEAT('k', 300)", "'é'", "''"}, "'zzz'"}, // > 255 characters
	{"varchar_nopad", "VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin", []string{"'a'", "'a '", "'a  '"}, "'q'"},                        // NO PAD
	{"varchar_ci", "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", []string{"'alpha'", "'Beta'", "'gämma'", "'Ω-1'"}, "'zeta'"},     // _ci: the pair of varchar_bin
	{"char_nopad", "CHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", []string{"'ab'", "'x'"}, "'q'"},                                     // shorter than n (probe P5)
	{"latin1", "VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci", []string{"'Ñandú'", "'Åsa'", "'naïve'"}, "'q'"},                        // non-ASCII
	{"date", "DATE", []string{"'1000-01-01'", "'2019-12-31'", "'9999-12-31'"}, "'2025-01-01'"},
	{"datetime", "DATETIME(6)", []string{"'2019-01-01 00:00:00.000001'", "'2019-01-01 00:00:00.000002'", "'1000-01-01 00:00:00'"}, "'2025-01-01 00:00:00'"},
	{"timestamp", "TIMESTAMP(3)", []string{"'2019-03-31 01:30:00.001'", "'2019-10-27 00:30:00'", "'2019-10-27 01:30:00'"}, "'2025-01-01 00:00:00'"},
	{"time", "TIME(2)", []string{"'-838:59:59'", "'00:00:00.01'", "'838:59:59'", "'10:00:00.01'", "'03:03:00'", "'01:09:13.10'"}, "'12:00:00'"},
	{"year", "YEAR", []string{"1901", "2019", "2155"}, "2000"},
}

func (f keyFamily) tables() []string {
	return []string{
		"CREATE TABLE r (k " + f.typ + " NOT NULL, eligible TINYINT NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB",
		"CREATE TABLE c (id INT NOT NULL, rk " + f.typ + " NULL, PRIMARY KEY (id), KEY ix_rk (rk), " +
			"CONSTRAINT fk_c_r FOREIGN KEY (rk) REFERENCES r (k)) ENGINE=InnoDB",
	}
}

const keyJob = `
    root_table: r
    primary_key: k
    where: "eligible = 1"
    relations:
      - table: c
        primary_key: id
        foreign_key: rk
`

func (f keyFamily) seed(t *testing.T, e env) {
	t.Helper()
	id := 0
	add := func(k string, eligible int) {
		execAll(t, e.src, fmt.Sprintf("INSERT INTO r VALUES (%s, %d)", k, eligible))
		for range 2 {
			id++
			execAll(t, e.src, fmt.Sprintf("INSERT INTO c VALUES (%d, %s)", id, k))
		}
	}
	for _, k := range f.keys {
		add(k, 1)
	}
	add(f.other, 0)
}

// TestMySQLKeyMatrix runs each key family as archive, purge and copy-only, three roots per
// batch, and archive once more interrupted after its first copy and recovered, and checks
// every row byte for byte: no witness is lost, merged or replaced.
func TestMySQLKeyMatrix(t *testing.T) {
	for _, f := range keyFamilies {
		for _, c := range []struct {
			typ       jobspec.Type
			recovered bool
		}{{jobspec.Archive, false}, {jobspec.Archive, true}, {jobspec.Purge, false}, {jobspec.CopyOnly, false}} {
			typ := c.typ
			name := f.name + "/" + string(typ)
			if c.recovered {
				name += "/recovered"
			}
			t.Run(name, func(t *testing.T) {
				e := newEnv(t, "km")
				if typ == jobspec.Purge {
					execAll(t, e.src, f.tables()...)
				} else {
					e.both(t, f.tables()...)
				}
				f.seed(t, e)
				eligibleR := rows(t, e.src, "r", "k")
				eligibleC := rows(t, e.src, "c", "id")
				// HEX(eligible) ends each root row: 1 eligible, 0 not. Child ids follow the
				// seed order, eligible roots first.
				keepR := filter(eligibleR, func(r string) bool { return strings.HasSuffix(r, "|0") })
				wantR := filter(eligibleR, func(r string) bool { return strings.HasSuffix(r, "|1") })
				wantC, keepC := eligibleC[:2*len(f.keys)], eligibleC[2*len(f.keys):]

				cfg := e.config(t, job(t, typ, keyJob))
				cfg.Processing.BatchSize = 3
				var first int64
				if c.recovered {
					// Interrupted after its first batch's copy commits: the rerun's recovery reads
					// every key of that batch back from the destination (GH #7) before deleting it.
					e.interrupted(t, cfg, atCommitted)
					first = int64(min(3, len(f.keys)))
				}
				rep := e.mustRun(t, cfg)
				if rep.Recovered != first || rep.Roots != int64(len(f.keys))-first {
					t.Errorf("recovered = %d, roots = %d, want %d and %d", rep.Recovered, rep.Roots, first, int64(len(f.keys))-first)
				}
				if typ != jobspec.CopyOnly {
					if got := rows(t, e.src, "r", "k"); !equal(got, keepR) {
						t.Errorf("source r = %v, want %v", got, keepR)
					}
					if got := rows(t, e.src, "c", "id"); !equal(got, keepC) {
						t.Errorf("source c = %v, want %v", got, keepC)
					}
				}
				if typ != jobspec.Purge {
					if got := rows(t, e.dst, "r", "k"); !equal(got, wantR) {
						t.Errorf("destination r = %v, want %v", got, wantR)
					}
					if got := rows(t, e.dst, "c", "id"); !equal(got, wantC) {
						t.Errorf("destination c = %v, want %v", got, wantC)
					}
				}
				if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCompleted)] != int64(len(f.keys)) || len(got) != 1 {
					t.Errorf("log = %v, want %d completed", got, len(f.keys))
				}
			})
		}
	}
}

func filter(in []string, keep func(string) bool) []string {
	var out []string
	for _, s := range in {
		if keep(s) {
			out = append(out, s)
		}
	}
	return out
}
