//go:build integration

package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// The spec §10 witnesses that M7b's survey found uncovered (plan reference
// m7b-e2e/evidence/spec10-survey.md). Each is behaviour that already exists, so each is
// a pair (R56): the condition, and the same setup with one fact changed.

// compositeTree: a root keyed (order_id, line_no), items on the root's key plus item_no,
// and notes on a three-column foreign key to the items.
var compositeTree = []string{
	"CREATE TABLE r (order_id INT NOT NULL, line_no SMALLINT NOT NULL, eligible TINYINT NOT NULL, " +
		"PRIMARY KEY (order_id, line_no)) ENGINE=InnoDB",
	"CREATE TABLE c (order_id INT NOT NULL, line_no SMALLINT NOT NULL, item_no TINYINT NOT NULL, " +
		"PRIMARY KEY (order_id, line_no, item_no), " +
		"CONSTRAINT fk_c_r FOREIGN KEY (order_id, line_no) REFERENCES r (order_id, line_no)) ENGINE=InnoDB",
	"CREATE TABLE g (note_id INT NOT NULL, order_id INT NOT NULL, line_no SMALLINT NOT NULL, item_no TINYINT NOT NULL, " +
		"PRIMARY KEY (note_id), KEY ix_g_c (order_id, line_no, item_no), " +
		"CONSTRAINT fk_g_c FOREIGN KEY (order_id, line_no, item_no) REFERENCES c (order_id, line_no, item_no)) ENGINE=InnoDB",
}

const compositeTreeJob = `
    root_table: r
    primary_key: [order_id, line_no]
    where: "eligible = 1"
    relations:
      - table: c
        primary_key: [order_id, line_no, item_no]
        foreign_key: [order_id, line_no]
        references: [order_id, line_no]
        relations:
          - table: g
            primary_key: note_id
            foreign_key: [order_id, line_no, item_no]
            references: [order_id, line_no, item_no]
`

// compositeOrder is each table's ORDER BY, and compositeRoot the position of the root key
// (order_id, line_no) among the table's columns, as rows() returns them.
var compositeOrder = map[string]string{"r": "order_id, line_no", "c": "order_id, line_no, item_no", "g": "note_id"}
var compositeRoot = map[string]int{"r": 0, "c": 0, "g": 1}

// TestMySQLCompositeRootTree (spec §10 key matrix, KM-8): a composite root with a child
// and a grandchild on composite keys, as archive, purge and copy-only. The pair is in the
// data: orders 3 and 4 each have an eligible line 1 and an ineligible line 2, so a
// binding by order_id alone would take line 2's tree too.
func TestMySQLCompositeRootTree(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			e := newEnv(t, "ctree")
			if typ == jobspec.Purge {
				execAll(t, e.src, compositeTree...)
			} else {
				e.both(t, compositeTree...)
			}
			note := 0
			for o := 1; o <= 4; o++ {
				for l := 1; l <= 2; l++ {
					eligible := 0
					if l == 1 || o <= 2 {
						eligible = 1
					}
					execAll(t, e.src, fmt.Sprintf("INSERT INTO r VALUES (%d, %d, %d)", o, l, eligible))
					for i := 1; i <= 2; i++ {
						note++
						execAll(t, e.src, fmt.Sprintf("INSERT INTO c VALUES (%d, %d, %d)", o, l, i),
							fmt.Sprintf("INSERT INTO g VALUES (%d, %d, %d, %d)", note, o, l, i))
					}
				}
			}
			keep, want := map[string][]string{}, map[string][]string{}
			for table, by := range compositeOrder {
				at := compositeRoot[table]
				ineligible := func(r string) bool {
					f := strings.Split(r, "|")
					return (f[at] == "3" || f[at] == "4") && f[at+1] == "2"
				}
				all := rows(t, e.src, table, by)
				keep[table] = filter(all, ineligible)
				want[table] = filter(all, func(r string) bool { return !ineligible(r) })
			}

			rep := e.mustRun(t, e.config(t, job(t, typ, compositeTreeJob)))
			if rep.Roots != 6 || rep.Batches != 3 {
				t.Errorf("report = %+v, want 6 roots in 3 batches", rep)
			}
			for table, by := range compositeOrder {
				if typ != jobspec.CopyOnly {
					if got := rows(t, e.src, table, by); !equal(got, keep[table]) {
						t.Errorf("source %s = %v, want only the ineligible trees %v", table, got, keep[table])
					}
				}
				if typ != jobspec.Purge {
					if got := rows(t, e.dst, table, by); !equal(got, want[table]) {
						t.Errorf("destination %s = %v, want %v", table, got, want[table])
					}
				}
			}
			if got := statuses(t, e.dst, e.dstSchema); fmt.Sprint(got) != "map[2:6]" {
				t.Errorf("log = %v, want 6 completed", got)
			}
		})
	}
}

// TestMySQLKeyCharsetTwins (spec §10 charset tests, R32; CS-1 to CS-4): a utf8mb3_general_ci
// key archived into utf8mb4_general_ci runs and verifies under sha256, over keys whose
// order differs under other collations ('a' < 'B' < '_' < 'é' here; 'B' < '_' < 'a' < 'é'
// by bytes; '_' first under 0900_ai_ci). The same run into utf8mb4_0900_ai_ci, utf8mb4
// into utf8mb3 and latin1 into utf8mb4 are each refused by preflight, with the source
// unchanged. Changing the destination's collation is the pair's one fact.
func TestMySQLKeyCharsetTwins(t *testing.T) {
	const (
		mb3  = "CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci"
		mb4  = "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
		ai   = "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
		lat1 = "CHARACTER SET latin1 COLLATE latin1_swedish_ci"
	)
	for _, tc := range []struct {
		name, src, dst string
		refused        bool
	}{
		{"utf8mb3_general_ci into utf8mb4_general_ci runs", mb3, mb4, false},
		{"utf8mb3_general_ci into utf8mb4_0900_ai_ci is refused", mb3, ai, true},
		{"utf8mb4 into utf8mb3 is refused", mb4, mb3, true},
		{"latin1 into utf8mb4 is refused", lat1, mb4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "twin")
			execAll(t, e.src, orderTables(tc.src)...)
			execAll(t, e.dst, orderTables(tc.dst)...)
			note := 0
			for i, id := range []string{"a", "B", "_", "é", "new"} {
				placed := "2019-06-01"
				if id == "new" {
					placed = "2021-06-01"
				}
				execAll(t, e.src, fmt.Sprintf("INSERT INTO orders VALUES ('%s', '%s', 'code-%d')", id, placed, i))
				for line := 1; line <= 2; line++ {
					note++
					execAll(t, e.src,
						fmt.Sprintf("INSERT INTO order_lines VALUES ('%s', %d, 'sku', '2019-06-01 10:00:00.000001')", id, line),
						fmt.Sprintf("INSERT INTO line_notes VALUES (%d, '%s', %d, 'n')", note, id, line))
				}
			}
			old := oldRows(t, e)
			before := rows(t, e.src, "orders", "id")

			rep, err := e.run(t, e.config(t, job(t, jobspec.Archive, orderJob)))
			if tc.refused {
				pe := wantErr[*preflight.Error](t, err)
				if pe.Check != "DEST_KEY_IDENTITY_CHECK" {
					t.Errorf("refused by %s, want DEST_KEY_IDENTITY_CHECK: %v", pe.Check, err)
				}
				if got := rows(t, e.src, "orders", "id"); !equal(got, before) {
					t.Errorf("source orders changed: %v, want %v", got, before)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if rep.Roots != 4 || rep.Verification != jobspec.VerifySHA256 || rep.Verified["orders"] != 4 {
				t.Errorf("report = %+v, want 4 roots verified by sha256", rep)
			}
			for table, by := range orderOrder {
				if got := rows(t, e.dst, table, by); !equal(got, old[table]) {
					t.Errorf("destination %s =\n%v\nwant\n%v", table, got, old[table])
				}
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM orders"); got != 1 {
				t.Errorf("source orders = %d, want only the new one", got)
			}
		})
	}
}

// TestMySQLReplayKeepsKeyBytes (spec §10 regression witnesses RW-1, RW-5, RW-6): an archive
// killed after a destination commit leaves that batch's two roots logged copied, read back
// from the log by the restart's recovery. A key longer than 255 characters, BINARY(16)
// bytes that are not UTF-8 and a latin1 key with non-ASCII characters come back whole: the
// replay deletes exactly their trees, and the result equals an uninterrupted run. Each
// case kills at the commit whose batch holds its witness key, and asserts the key is
// among the logged copied roots before the restart. The INT family is the pair's
// control: the same kill on a key with nothing to lose.
func TestMySQLReplayKeepsKeyBytes(t *testing.T) {
	families := map[string]keyFamily{}
	for _, f := range keyFamilies {
		families[f.name] = f
	}
	for _, tc := range []struct {
		family  string
		nth     int    // the destination commit to kill at: batch nth holds the witness
		witness string // a predicate on the log's k1 that one copied root satisfies
	}{
		{"int", 1, "k1 = -2147483648"},
		{"varchar_bin", 2, "CHAR_LENGTH(k1) = 300"},                       // '' 'ABC' | 'abc' 'k'x300 | 'é'
		{"binary16", 1, "k1 = UNHEX('C0C1F5F6F7F8F9FAFBFCFDFEFF000000')"}, // not UTF-8
		{"latin1", 1, "k1 = 'Ñandú'"},                                     // latin1_swedish_ci: naïve, Ñandú | Åsa
	} {
		f := families[tc.family]
		t.Run(tc.family, func(t *testing.T) {
			e := newEnv(t, "replay")
			e.both(t, f.tables()...)
			f.seed(t, e)
			all := rows(t, e.src, "r", "k")
			keepR := filter(all, func(r string) bool { return strings.HasSuffix(r, "|0") })
			wantR := filter(all, func(r string) bool { return strings.HasSuffix(r, "|1") })

			e.killAt(t, jobspec.Archive, keyJob, atCommitted, tc.nth)
			wantLog := map[int]int64{1: 2}
			if tc.nth > 1 {
				wantLog[2] = int64(2 * (tc.nth - 1))
			}
			if got := statuses(t, e.dst, e.dstSchema); fmt.Sprint(got) != fmt.Sprint(wantLog) {
				t.Fatalf("log after the kill = %v, want %v", got, wantLog)
			}
			id := count(t, e.dst, "SELECT id FROM archiver_job WHERE job_name = 'j'")
			if n := count(t, e.dst, fmt.Sprintf("SELECT COUNT(*) FROM archiver_job_log_%d WHERE ga_status = 1 AND %s", id, tc.witness)); n != 1 {
				t.Fatalf("the witness key (%s) is not among the copied roots the restart replays", tc.witness)
			}
			rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, keyJob)))
			if want := int64(len(f.keys) - 2*tc.nth); rep.Recovered != 2 || rep.Roots != want {
				t.Errorf("report = %+v, want 2 recovered and %d archived", rep, want)
			}
			if got := rows(t, e.src, "r", "k"); !equal(got, keepR) {
				t.Errorf("source r = %v, want %v", got, keepR)
			}
			if got := rows(t, e.dst, "r", "k"); !equal(got, wantR) {
				t.Errorf("destination r = %v, want %v", got, wantR)
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM c"); got != 2 {
				t.Errorf("source c = %d rows, want only the ineligible root's 2", got)
			}
			if got := statuses(t, e.dst, e.dstSchema); fmt.Sprint(got) != fmt.Sprintf("map[2:%d]", len(f.keys)) {
				t.Errorf("log = %v, want %d completed", got, len(f.keys))
			}
		})
	}
}

// TestMySQLLogKeyCheckUnderSkip (spec §10 binding tests, BD-4): --skip-dangerously-validate-preflight-allow-experimental
// skips preflight's policy checks, not the log-key check. A root key widened from INT to
// BIGINT since the job's first run is refused before anything is touched; the same second
// run without the change archives.
func TestMySQLLogKeyCheckUnderSkip(t *testing.T) {
	ddl := "CREATE TABLE r (k INT NOT NULL, eligible TINYINT NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB"
	const body = `
    root_table: r
    primary_key: k
    where: "eligible = 1"
`
	for _, widened := range []bool{true, false} {
		t.Run(fmt.Sprintf("widened=%t", widened), func(t *testing.T) {
			e := newEnv(t, "lkskip")
			e.both(t, ddl)
			execAll(t, e.src, "INSERT INTO r VALUES (1, 1), (2, 0)")
			e.mustRun(t, e.config(t, job(t, jobspec.Archive, body)))
			if widened {
				for _, db := range []string{"src", "dst"} {
					q := "ALTER TABLE r MODIFY k BIGINT NOT NULL"
					if db == "src" {
						execAll(t, e.src, q)
					} else {
						execAll(t, e.dst, q)
					}
				}
			}
			execAll(t, e.src, "INSERT INTO r VALUES (3, 1)")
			cfg := e.config(t, job(t, jobspec.Archive, body))
			cfg.SkipPolicy = true
			_, err := e.run(t, cfg)
			if widened {
				_ = wantErr[*tracking.LogKeyError](t, err)
				if got := count(t, e.src, "SELECT COUNT(*) FROM r WHERE k = 3"); got != 1 {
					t.Error("the refused run deleted root 3")
				}
				return
			}
			if err != nil {
				t.Fatalf("the unchanged key under skip: %v", err)
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM r"); got != 1 {
				t.Errorf("source r = %d rows, want only the ineligible root", got)
			}
		})
	}
}
