//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/types"
)

// invalidDates is a source pool whose sessions accept invalid dates, to seed rows the
// server's default mode refuses.
func invalidDates(t *testing.T, e env) *sql.DB {
	t.Helper()
	return openServer(t, "TEST_SOURCE", e.srcSchema, map[string]string{"sql_mode": "'ALLOW_INVALID_DATES'"})
}

// texts reads one text column of q's rows, in its order.
func texts(t *testing.T, db *sql.DB, q string) []string {
	t.Helper()
	r, err := db.QueryContext(context.Background(), q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer func() { _ = r.Close() }()
	var out []string
	for r.Next() {
		var s string
		if err := r.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestMySQLInvalidTemporalPayload (M7-3, G22): a DATE payload the source holds under
// ALLOW_INVALID_DATES, copied as SQL text into a destination whose session is strict (the
// server default). Archive and copy-only, under count and none.
// Pair: d = '2020-02-31' → the INSERT fails with MySQL 1292, the source keeps the row, the
// destination is empty and nothing is logged; d = '2020-02-29', the one change → copied as
// that text (archive also deletes the source row).
func TestMySQLInvalidTemporalPayload(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.CopyOnly} {
		for _, v := range []jobspec.Verification{jobspec.VerifyCount, jobspec.VerifyNone} {
			for _, d := range []string{"2020-02-31", "2020-02-29"} {
				t.Run(string(typ)+"/"+string(v)+"/"+d, func(t *testing.T) {
					e := newEnv(t, "tpay")
					e.both(t, "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, d DATE) ENGINE=InnoDB")
					execAll(t, invalidDates(t, e), "INSERT INTO r VALUES (1, '"+d+"')")
					if got := texts(t, e.src, "SELECT CAST(d AS CHAR) FROM r"); len(got) != 1 || got[0] != d {
						t.Fatalf("source d = %v, want %s", got, d)
					}
					if mode := texts(t, e.dst, "SELECT @@SESSION.sql_mode"); !strings.Contains(mode[0], "STRICT_TRANS_TABLES") {
						t.Fatalf("destination sql_mode = %q, want a strict session", mode[0])
					}
					cfg := e.config(t, job(t, typ, "\n    root_table: r\n    primary_key: id\n    where: \"1=1\"\n"))
					cfg.Verification = v
					_, err := e.run(t, cfg)
					if d == "2020-02-31" {
						var me *mysql.MySQLError
						if !errors.As(err, &me) || me.Number != 1292 {
							t.Fatalf("Run = %v, want MySQL error 1292", err)
						}
						untouched(t, e, "r", 1)
						return
					}
					if err != nil {
						t.Fatalf("Run: %v", err)
					}
					if got := texts(t, e.dst, "SELECT CAST(d AS CHAR) FROM r"); len(got) != 1 || got[0] != d {
						t.Errorf("destination d = %v, want %s", got, d)
					}
					want := int64(1)
					if typ == jobspec.Archive {
						want = 0
					}
					if got := count(t, e.src, "SELECT COUNT(*) FROM r"); got != want {
						t.Errorf("source r = %d rows, want %d", got, want)
					}
				})
			}
		}
	}
}

// TestMySQLInvalidTemporalKey (M7-3, G23): the 2.x collision fixture. Root 1's child
// vp_keys is keyed by DATE d, a grandchild vp_leaves references d, and root 2's child holds
// '2020-03-02', the day MySQL normalizes '2020-02-31' to. The job selects root 1 only.
// Every type, under count (purge has no verification).
// Pair: root 1's key '2020-02-31' → discovery refuses the key with TEMPORAL_READ_CONTRACT
// (operation key) on vp_keys.d before any copy, log or delete: every source row kept, the
// destination empty, nothing logged; the key '2020-02-29', the one change → exactly root
// 1's tree is archived, purged or copied, and root 2's '2020-03-02' rows survive in the
// source.
func TestMySQLInvalidTemporalKey(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		for _, d := range []string{"2020-02-31", "2020-02-29"} {
			t.Run(string(typ)+"/"+d, func(t *testing.T) {
				e := newEnv(t, "tkey")
				e.both(t,
					"CREATE TABLE vp_roots (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
					"CREATE TABLE vp_keys (d DATE NOT NULL PRIMARY KEY, root_id BIGINT NOT NULL, x INT, KEY ix_root (root_id), "+
						"CONSTRAINT fk_keys_root FOREIGN KEY (root_id) REFERENCES vp_roots (id)) ENGINE=InnoDB",
					"CREATE TABLE vp_leaves (id BIGINT NOT NULL PRIMARY KEY, parent_d DATE NOT NULL, KEY ix_parent (parent_d), "+
						"CONSTRAINT fk_leaves_key FOREIGN KEY (parent_d) REFERENCES vp_keys (d)) ENGINE=InnoDB")
				execAll(t, invalidDates(t, e),
					"INSERT INTO vp_roots VALUES (1), (2)",
					"INSERT INTO vp_keys VALUES ('"+d+"', 1, 1), ('2020-03-02', 2, 2)",
					"INSERT INTO vp_leaves VALUES (11, '"+d+"'), (22, '2020-03-02')")
				cfg := e.config(t, job(t, typ, `
    root_table: vp_roots
    primary_key: id
    where: "id = 1"
    relations:
      - table: vp_keys
        primary_key: d
        foreign_key: root_id
        relations:
          - table: vp_leaves
            primary_key: id
            foreign_key: parent_d
`))
				if typ != jobspec.Purge {
					cfg.Verification = jobspec.VerifyCount
				}
				_, err := e.run(t, cfg)
				all := map[string][]string{
					"SELECT CAST(id AS CHAR) FROM vp_roots ORDER BY id":                    {"1", "2"},
					"SELECT CONCAT(CAST(d AS CHAR), '/', x) FROM vp_keys ORDER BY root_id": {d + "/1", "2020-03-02/2"},
					"SELECT CAST(id AS CHAR) FROM vp_leaves ORDER BY id":                   {"11", "22"},
				}
				if d == "2020-02-31" {
					te := wantErr[*types.TemporalReadContractError](t, err)
					if te.Operation != "key" || !strings.Contains(err.Error(), "discovery on vp_keys") || !strings.Contains(err.Error(), "vp_keys.d") {
						t.Errorf("refusal = %v, want TEMPORAL_READ_CONTRACT operation key in vp_keys's discovery", err)
					}
					for q, want := range all {
						if got := texts(t, e.src, q); !equal(got, want) {
							t.Errorf("source %s = %v, want %v kept", q, got, want)
						}
					}
					for _, table := range []string{"vp_roots", "vp_keys", "vp_leaves"} {
						if got := count(t, e.dst, "SELECT COUNT(*) FROM "+table); got != 0 {
							t.Errorf("destination %s = %d rows, want 0", table, got)
						}
					}
					if st := statuses(t, e.dst, e.dstSchema); len(st) != 0 {
						t.Errorf("log = %v, want nothing logged", st)
					}
					return
				}
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				root1 := map[string][]string{
					"SELECT CAST(id AS CHAR) FROM vp_roots ORDER BY id":                    {"1"},
					"SELECT CONCAT(CAST(d AS CHAR), '/', x) FROM vp_keys ORDER BY root_id": {d + "/1"},
					"SELECT CAST(id AS CHAR) FROM vp_leaves ORDER BY id":                   {"11"},
				}
				root2 := map[string][]string{
					"SELECT CAST(id AS CHAR) FROM vp_roots ORDER BY id":                    {"2"},
					"SELECT CONCAT(CAST(d AS CHAR), '/', x) FROM vp_keys ORDER BY root_id": {"2020-03-02/2"},
					"SELECT CAST(id AS CHAR) FROM vp_leaves ORDER BY id":                   {"22"},
				}
				wantSrc, wantDst := root2, root1
				switch typ {
				case jobspec.Purge:
					wantDst = nil
				case jobspec.CopyOnly:
					wantSrc = all
				}
				for q, want := range wantSrc {
					if got := texts(t, e.src, q); !equal(got, want) {
						t.Errorf("source %s = %v, want %v", q, got, want)
					}
				}
				for q := range all {
					if got := texts(t, e.dst, q); !equal(got, wantDst[q]) {
						t.Errorf("destination %s = %v, want %v", q, got, wantDst[q])
					}
				}
			})
		}
	}
}
