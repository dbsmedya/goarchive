//go:build integration

package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// The key-set tests of spec §10: a fault seam changes a key-addressed statement so that it
// really selects another set of rows, and the statement's own check must refuse it.
//
//   - over-match: a DECIMAL key bound as a double, without its cast, on a fixture whose
//     neighbouring keys are equal as doubles, so one bound key reaches two rows;
//   - under-match: the last key dropped from the bound list.

const decimalTables = "CREATE TABLE r (k DECIMAL(30,10) NOT NULL, eligible TINYINT NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB"

const decimalJob = `
    root_table: r
    primary_key: k
    where: "eligible = 1"
`

// seedNeighbours writes two keys a double cannot tell apart; only the first is eligible.
func seedNeighbours(t *testing.T, e env) {
	t.Helper()
	execAll(t, e.src, "INSERT INTO r VALUES (12345678901234567890.0000000001, 1), (12345678901234567890.0000000002, 0)")
}

// asDouble rewrites st, when it is of kind o on the root table, to bind every key as a
// float64 compared without its cast.
func asDouble(o op) func(*statement) {
	return func(st *statement) {
		if st.op != o || st.table != "r" {
			return
		}
		st.query = strings.ReplaceAll(st.query, "CAST(? AS DECIMAL(30,10))", "?")
		for i, a := range st.args {
			if s, ok := a.(string); ok {
				f, _ := strconv.ParseFloat(s, 64)
				st.args[i] = f
			}
		}
	}
}

// dropLast removes the last key of a one-column membership list, when st is of kind o on r.
func dropLast(o op) func(*statement) {
	return func(st *statement) {
		if st.op != o || st.table != "r" || len(st.args) < 2 {
			return
		}
		i := strings.LastIndex(st.query, ",?)")
		st.query = st.query[:i] + ")" + st.query[i+3:]
		st.args = st.args[:len(st.args)-1]
	}
}

func TestMySQLOverMatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  jobspec.Type
		op   op
		v    jobspec.Verification
		want func(error) bool
	}{
		{"copy read", jobspec.Archive, opRead, jobspec.VerifySHA256, isKeySet(opRead)},
		{"count verification", jobspec.Archive, opCount, jobspec.VerifyCount, isVerification("count")},
		{"delete", jobspec.Purge, opDelete, "", isKeySet(opDelete)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "over")
			if tc.typ == jobspec.Purge {
				execAll(t, e.src, decimalTables)
			} else {
				e.both(t, decimalTables)
			}
			seedNeighbours(t, e)
			before := rows(t, e.src, "r", "k")
			cfg := e.config(t, job(t, tc.typ, decimalJob))
			cfg.Verification = tc.v
			cfg.fault = asDouble(tc.op)
			_, err := e.run(t, cfg)
			if !tc.want(err) {
				t.Fatalf("Run() = %v", err)
			}
			if got := rows(t, e.src, "r", "k"); !equal(got, before) {
				t.Errorf("source changed: %v, want %v", got, before)
			}
			if tc.typ != jobspec.Purge {
				if n := count(t, e.dst, "SELECT COUNT(*) FROM r"); n != 0 {
					t.Errorf("destination holds %d rows; the transaction must roll back", n)
				}
				if got := statuses(t, e.dst, e.dstSchema); len(got) != 0 {
					t.Errorf("log = %v, want empty", got)
				}
			}
		})
	}
}

const intTables = "CREATE TABLE r (k INT NOT NULL, eligible TINYINT NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB"

func TestMySQLUnderMatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  jobspec.Type
		op   op
		v    jobspec.Verification
		want func(error) bool
	}{
		{"copy read", jobspec.CopyOnly, opRead, jobspec.VerifySHA256, isKeySet(opRead)},
		{"count verification", jobspec.Archive, opCount, jobspec.VerifyCount, isVerification("count")},
		{"sha256 verification", jobspec.Archive, opHash, jobspec.VerifySHA256, isVerification("sha256")},
		{"delete", jobspec.Archive, opDelete, jobspec.VerifySHA256, isKeySet(opDelete)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, "under")
			e.both(t, intTables)
			execAll(t, e.src, "INSERT INTO r VALUES (1, 1), (2, 1), (3, 0)")
			before := rows(t, e.src, "r", "k")
			cfg := e.config(t, job(t, tc.typ, decimalJob))
			cfg.Verification = tc.v
			cfg.fault = dropLast(tc.op)
			_, err := e.run(t, cfg)
			if !tc.want(err) {
				t.Fatalf("Run() = %v", err)
			}
			if got := rows(t, e.src, "r", "k"); !equal(got, before) {
				t.Errorf("source changed: %v, want %v", got, before)
			}
			wantDst := int64(0)
			if tc.op == opDelete {
				wantDst = 2 // the copy committed before the delete was refused
			}
			if n := count(t, e.dst, "SELECT COUNT(*) FROM r"); n != wantDst {
				t.Errorf("destination holds %d rows, want %d", n, wantDst)
			}
			if tc.op == opDelete {
				// The copied roots stay logged as copied, and a clean run finishes them.
				if got := statuses(t, e.dst, e.dstSchema); got[int(tracking.StatusCopied)] != 2 {
					t.Fatalf("log = %v, want 2 copied", got)
				}
				cfg.fault = nil
				rep := e.mustRun(t, cfg)
				if rep.Recovered != 2 || count(t, e.src, "SELECT COUNT(*) FROM r") != 1 {
					t.Errorf("recovery: report %+v, source rows %d", rep, count(t, e.src, "SELECT COUNT(*) FROM r"))
				}
			}
		})
	}
}

func isKeySet(o op) func(error) bool {
	return func(err error) bool {
		var ke *KeySetError
		return errors.As(err, &ke) && ke.Statement == string(o)
	}
}

func isVerification(method string) func(error) bool {
	return func(err error) bool {
		var ve *VerificationError
		return errors.As(err, &ve) && ve.Method == method
	}
}

// TestMySQLRangeWarningFailsClosed: a statement past the range optimizer's budget raises
// warning 3170 and fails its batch (spec §7.1a). The session budget is 1 MiB, and
// preflight's caps are raised to 5,000 tuples so the engine sends a statement too large
// for it. A copy read rolls the destination back (verification: none, so no later read
// raises it first); a delete chunk rolls the source back.
func TestMySQLRangeWarningFailsClosed(t *testing.T) {
	const n = 5000
	for _, typ := range []jobspec.Type{jobspec.CopyOnly, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			e := newEnv(t, "r3170")
			ddl := "CREATE TABLE r (a INT NOT NULL, b INT NOT NULL, eligible TINYINT NOT NULL DEFAULT 1, PRIMARY KEY (a, b)) ENGINE=InnoDB"
			if typ == jobspec.Purge {
				execAll(t, e.src, ddl)
			} else {
				e.both(t, ddl)
			}
			execAll(t, e.src, "SET SESSION cte_max_recursion_depth = 20000", fmt.Sprintf(`INSERT INTO r (a, b)
				WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < %d) SELECT i DIV 100, i %% 100 FROM s`, n-1))
			small := openServer(t, "TEST_SOURCE", e.srcSchema, map[string]string{"range_optimizer_max_mem_size": "1048576"})
			cfg := e.config(t, job(t, typ, "\n    root_table: r\n    primary_key: [a, b]\n    where: \"eligible = 1\"\n"))
			cfg.Source = small
			cfg.Processing.BatchSize, cfg.Processing.BatchDeleteSize = n, n
			if typ == jobspec.CopyOnly {
				// Verification reads the same keys and would raise 3170 too; without it the
				// copy read's own check is the one that must fail the batch.
				cfg.Verification = jobspec.VerifyNone
			}
			cfg.preflighted = func(res *preflight.Result) {
				for i := range res.Caps {
					res.Caps[i].Tuples = n
				}
			}
			_, err := Run(context.Background(), cfg)
			if !errors.Is(err, key.ErrRangeScan) {
				t.Fatalf("Run() = %v, want key.ErrRangeScan", err)
			}
			if got := count(t, e.src, "SELECT COUNT(*) FROM r"); got != n {
				t.Errorf("source rows = %d, want %d", got, n)
			}
			if typ == jobspec.CopyOnly {
				if got := count(t, e.dst, "SELECT COUNT(*) FROM r"); got != 0 {
					t.Errorf("destination rows = %d, want 0", got)
				}
			}
		})
	}
}

// TestMySQLTimeKeyPurge is the R46 witness (docs/README_COMPAT.md): a purge of one eligible
// TIME root beside a decoy at the value CAST(? AS TIME(2)) turns it into. The purge must
// delete the eligible row and never the decoy; under the 3.0 core's first binding, a
// one-key delete chunk removed the decoy with the right row count.
func TestMySQLTimeKeyPurge(t *testing.T) {
	e := newEnv(t, "timepurge")
	execAll(t, e.src,
		"CREATE TABLE r (k TIME(2) NOT NULL, eligible TINYINT NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB",
		"CREATE TABLE c (id INT NOT NULL, rk TIME(2) NULL, PRIMARY KEY (id), KEY ix_rk (rk), CONSTRAINT fk_c_r FOREIGN KEY (rk) REFERENCES r (k)) ENGINE=InnoDB",
		"INSERT INTO r VALUES ('10:00:00.01', 1), ('01:00:00.00', 0)",
		"INSERT INTO c VALUES (1, '10:00:00.01'), (2, '01:00:00.00')")
	cfg := e.config(t, job(t, jobspec.Purge, keyJob))
	cfg.Processing.BatchSize = 1
	e.mustRun(t, cfg)
	if got := rows(t, e.src, "r", "k"); len(got) != 1 || !strings.HasPrefix(got[0], fmt.Sprintf("%X", "01:00:00.00")) {
		t.Errorf("source r = %v, want only the decoy 01:00:00.00", got)
	}
	if got := count(t, e.src, "SELECT COUNT(*) FROM c WHERE id = 2"); got != 1 {
		t.Error("the decoy's child was deleted")
	}
}
