//go:build integration

package engine

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/key"
	"github.com/dbsmedya/goarchive/v3/internal/lock"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
	"github.com/dbsmedya/goarchive/v3/internal/tracking"
)

// TestMySQLFourColumnKey (Review Focus 2): a root keyed (a INT, b DATETIME(6), c VARCHAR
// _ci, d INT) with a composite child, walked three roots per batch over ties in every
// leading column. The walk is exact, and the continued root fetch uses range access.
func TestMySQLFourColumnKey(t *testing.T) {
	e := newEnv(t, "four")
	ci := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	e.both(t,
		"CREATE TABLE r (a INT NOT NULL, b DATETIME(6) NOT NULL, c VARCHAR(20) "+ci+" NOT NULL, d INT NOT NULL, old TINYINT NOT NULL, "+
			"PRIMARY KEY (a, b, c, d)) ENGINE=InnoDB",
		"CREATE TABLE ch (id INT NOT NULL, a INT NOT NULL, b DATETIME(6) NOT NULL, c VARCHAR(20) "+ci+" NOT NULL, d INT NOT NULL, "+
			"PRIMARY KEY (id), KEY ix_r (a, b, c, d), CONSTRAINT fk_ch_r FOREIGN KEY (a, b, c, d) REFERENCES r (a, b, c, d)) ENGINE=InnoDB")
	id := 0
	for a := 0; a < 2; a++ {
		for _, b := range []string{"2019-01-01 00:00:00.000001", "2019-01-01 00:00:00.000002"} {
			for _, c := range []string{"x", "Y", "z"} {
				for d := 0; d < 2; d++ {
					execAll(t, e.src, fmt.Sprintf("INSERT INTO r VALUES (%d, '%s', '%s', %d, 1)", a, b, c, d))
					id++
					execAll(t, e.src, fmt.Sprintf("INSERT INTO ch VALUES (%d, %d, '%s', '%s', %d)", id, a, b, c, d))
				}
			}
		}
	}
	execAll(t, e.src, "INSERT INTO r VALUES (9, '2019-01-01', 'q', 0, 0)")
	want := filter(rows(t, e.src, "r", "a, b, c, d"), func(r string) bool { return strings.HasSuffix(r, "|1") })
	cfg := e.config(t, job(t, jobspec.Archive, `
    root_table: r
    primary_key: [a, b, c, d]
    where: "old = 1"
    relations:
      - table: ch
        primary_key: id
        foreign_key: [a, b, c, d]
        references: [a, b, c, d]
`))
	cfg.Processing.BatchSize = 3
	rep := e.mustRun(t, cfg)
	if rep.Roots != 24 || rep.Batches != 8 {
		t.Errorf("report = %+v, want 24 roots in 8 batches", rep)
	}
	if got := rows(t, e.dst, "r", "a, b, c, d"); !equal(got, want) {
		t.Errorf("destination r =\n%v\nwant\n%v", got, want)
	}
	if count(t, e.src, "SELECT COUNT(*) FROM r") != 1 || count(t, e.src, "SELECT COUNT(*) FROM ch") != 0 {
		t.Error("the source still holds archived rows")
	}

	// The continued fetch, EXPLAINed with a real last key: range access on PRIMARY.
	d := key.Def{Table: "r", Columns: []key.Column{
		{Name: "a", Family: key.FamilySigned}, {Name: "b", Family: key.FamilyDateTime, Fsp: 6},
		{Name: "c", Family: key.FamilyString}, {Name: "d", Family: key.FamilySigned}}}
	execAll(t, e.src, "INSERT INTO r WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 999) "+
		"SELECT i, '2019-01-01', 'x', i, 1 FROM n")
	execAll(t, e.src, "ANALYZE TABLE r")
	args := append(key.AfterArgs(key.Key{int64(500), "2019-01-01 00:00:00.000000", []byte("x"), int64(500)}), 3)
	r, err := e.src.QueryContext(context.Background(), "EXPLAIN FORMAT=TRADITIONAL "+fetchSQL(d, "old = 1", true), args...)
	if err != nil {
		t.Fatal(err)
	}
	cols, _ := r.Columns()
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	r.Next()
	if err := r.Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	plan := map[string]string{}
	for i, c := range cols {
		if b, ok := vals[i].([]byte); ok {
			plan[c] = string(b)
		}
	}
	if plan["type"] != "range" || plan["key"] != "PRIMARY" {
		t.Errorf("continued fetch plan = %v, want range on PRIMARY", plan)
	}
}

// berlinPool opens the source raw, its sessions in Europe/Berlin time: the engine is handed it
// so that its own UTC check refuses it, which database.OpenPool's UTC probe would do first.
func berlinPool(t *testing.T, schema string) *sql.DB {
	t.Helper()
	params := map[string]string{"time_zone": "'Europe/Berlin'"}
	port, _ := strconv.Atoi(os.Getenv("TEST_SOURCE_PORT"))
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.DBName = os.Getenv("TEST_SOURCE_USER"), os.Getenv("TEST_SOURCE_PASSWORD"), "tcp", schema
	cfg.Addr = fmt.Sprintf("%s:%d", os.Getenv("TEST_SOURCE_HOST"), port)
	cfg.Params = map[string]string{"time_zone": "'+00:00'", "sql_notes": "1", "autocommit": "1", "sql_select_limit": "18446744073709551615"}
	maps.Copy(cfg.Params, params)
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestMySQLTimestampFold (Review Focus 3): TIMESTAMP keys at the two UTC instants that are
// both 02:30 in Europe/Berlin on the night summer time ends. Under the UTC pin they are two
// roots and both are archived; the destination server's global zone is +03:00. A pool whose
// sessions run in Berlin time is refused before anything is read.
func TestMySQLTimestampFold(t *testing.T) {
	e := newEnv(t, "fold")
	e.both(t, "CREATE TABLE r (k TIMESTAMP(0) NOT NULL, old TINYINT NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB")
	execAll(t, e.src, "INSERT INTO r VALUES ('2026-10-25 00:30:00', 1), ('2026-10-25 01:30:00', 1), ('2026-10-26 00:00:00', 0)")
	berlin := berlinPool(t, e.srcSchema)
	var local string
	if err := berlin.QueryRowContext(context.Background(), "SELECT GROUP_CONCAT(CAST(k AS CHAR) ORDER BY k) FROM r WHERE old = 1").Scan(&local); err != nil {
		t.Fatal(err)
	}
	if local != "2026-10-25 02:30:00,2026-10-25 02:30:00" {
		t.Fatalf("Berlin reads the fold as %q; the fixture is not in the fold", local)
	}
	cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: r\n    primary_key: k\n    where: \"old = 1\"\n"))
	bad := cfg
	bad.Source = berlin
	if _, err := e.run(t, bad); err == nil || !strings.Contains(err.Error(), "not UTC") {
		t.Fatalf("a Berlin-time source = %v, want the UTC refusal", err)
	}
	rep := e.mustRun(t, cfg)
	if rep.Roots != 2 {
		t.Errorf("roots = %d, want both fold instants", rep.Roots)
	}
	if got := rows(t, e.dst, "r", "k"); len(got) != 2 {
		t.Errorf("destination = %v", got)
	}
}

// TestMySQLNullForeignKey (Review Focus 4): a child whose composite foreign key holds a NULL
// in one column belongs to no parent. It is not discovered, copied or deleted, and the
// parent's delete passes, because MySQL does not enforce a partly-NULL foreign key.
func TestMySQLNullForeignKey(t *testing.T) {
	e := newEnv(t, "nullfk")
	e.both(t,
		"CREATE TABLE p (a INT NOT NULL, b INT NOT NULL, old TINYINT NOT NULL, PRIMARY KEY (a, b)) ENGINE=InnoDB",
		"CREATE TABLE c (id INT NOT NULL, a INT NULL, b INT NULL, PRIMARY KEY (id), KEY ix_p (a, b), "+
			"CONSTRAINT fk_c_p FOREIGN KEY (a, b) REFERENCES p (a, b)) ENGINE=InnoDB")
	execAll(t, e.src, "INSERT INTO p VALUES (1, 1, 1)", "INSERT INTO c VALUES (1, 1, 1), (2, 1, NULL), (3, NULL, 1)")
	e.mustRun(t, e.config(t, job(t, jobspec.Archive, `
    root_table: p
    primary_key: [a, b]
    where: "old = 1"
    relations:
      - table: c
        primary_key: id
        foreign_key: [a, b]
        references: [a, b]
`)))
	if got := count(t, e.src, "SELECT COUNT(*) FROM c"); got != 2 {
		t.Errorf("source children = %d, want the two partly-NULL ones", got)
	}
	if got := count(t, e.dst, "SELECT COUNT(*) FROM c"); got != 1 {
		t.Errorf("destination children = %d, want 1", got)
	}
	if count(t, e.src, "SELECT COUNT(*) FROM p") != 0 {
		t.Error("the parent was not deleted")
	}
}

// TestMySQLCollationAgreement (Review Focus 5): under utf8mb4_0900_ai_ci, 'Résumé' and its
// children spelled 'resume' and 'RESUME' are one root. Discovery, the copy read, the delete
// and the log agree on it, and 'Ñu' finds its child 'ñu'.
func TestMySQLCollationAgreement(t *testing.T) {
	e := newEnv(t, "coll")
	ai := "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
	e.both(t,
		"CREATE TABLE r (k VARCHAR(20) "+ai+" NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB",
		"CREATE TABLE c (id INT NOT NULL, rk VARCHAR(20) "+ai+" NOT NULL, PRIMARY KEY (id), KEY ix_rk (rk), "+
			"CONSTRAINT fk_c_r FOREIGN KEY (rk) REFERENCES r (k)) ENGINE=InnoDB")
	execAll(t, e.src, "INSERT INTO r VALUES ('Résumé'), ('Ñu')",
		"INSERT INTO c VALUES (1, 'resume'), (2, 'RESUME'), (3, 'Résumé'), (4, 'ñu')")
	before := rows(t, e.src, "c", "id")
	rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, keyJob[:0]+`
    root_table: r
    primary_key: k
    where: "1=1"
    relations:
      - table: c
        primary_key: id
        foreign_key: rk
`)))
	if rep.Roots != 2 || rep.Copied["c"] != 4 || rep.Deleted["c"] != 4 {
		t.Errorf("report = %+v", rep)
	}
	if got := rows(t, e.dst, "c", "id"); !equal(got, before) {
		t.Errorf("destination children = %v, want %v", got, before)
	}
	if count(t, e.src, "SELECT COUNT(*) FROM r") != 0 {
		t.Error("the source still holds a root")
	}
}

// TestMySQLPermutedReferences: a composite relationship with no declared foreign key and a
// permuted references, over parents (1,2) and (2,1) of which only (1,2) is selected. Only
// its children move (spec §7.3, review counterexample B3).
func TestMySQLPermutedReferences(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
		t.Run(string(typ), func(t *testing.T) {
			e := newEnv(t, "perm")
			e.both(t,
				"CREATE TABLE p (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b)) ENGINE=InnoDB",
				"CREATE TABLE c (id INT NOT NULL, pb INT NOT NULL, pa INT NOT NULL, PRIMARY KEY (id), KEY ix_p (pb, pa)) ENGINE=InnoDB")
			execAll(t, e.src, "INSERT INTO p VALUES (1, 2), (2, 1)",
				"INSERT INTO c VALUES (1, 2, 1), (2, 2, 1), (3, 1, 2)") // 1, 2: children of (1,2); 3: of (2,1)
			e.mustRun(t, e.config(t, job(t, typ, `
    root_table: p
    primary_key: [a, b]
    where: "a = 1"
    relations:
      - table: c
        primary_key: id
        foreign_key: [pb, pa]
        references: [b, a]
`)))
			if got := count(t, e.src, "SELECT GROUP_CONCAT(id) FROM c"); got != 3 {
				t.Errorf("source children left = %d, want only 3", got)
			}
			if typ == jobspec.Archive {
				if got := count(t, e.dst, "SELECT COUNT(*) FROM c WHERE id IN (1, 2)"); got != 2 || count(t, e.dst, "SELECT COUNT(*) FROM c") != 2 {
					t.Errorf("destination children = %v", rows(t, e.dst, "c", "id"))
				}
			}
		})
	}
}

// TestMySQLBookkeepingNames: root keys named like the log's bookkeeping columns get k1…kN in
// the log, so the run completes (review counterexample B4).
func TestMySQLBookkeepingNames(t *testing.T) {
	for name, ddl := range map[string]string{
		"ga_status":               "CREATE TABLE r (ga_status INT NOT NULL, PRIMARY KEY (ga_status)) ENGINE=InnoDB",
		"ga_status_ga_updated_at": "CREATE TABLE r (ga_status INT NOT NULL, ga_updated_at DATETIME NOT NULL, PRIMARY KEY (ga_status, ga_updated_at)) ENGINE=InnoDB",
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, "bk")
			e.both(t, ddl)
			pk := "ga_status"
			if strings.Contains(ddl, "ga_updated_at DATETIME") {
				pk = "[ga_status, ga_updated_at]"
				execAll(t, e.src, "INSERT INTO r VALUES (1, '2019-01-01'), (2, '2019-01-02'), (3, '2019-01-03')")
			} else {
				execAll(t, e.src, "INSERT INTO r VALUES (1), (2), (3)")
			}
			rep := e.mustRun(t, e.config(t, job(t, jobspec.Archive, "\n    root_table: r\n    primary_key: "+pk+"\n    where: \"1=1\"\n")))
			if rep.Roots != 3 || statuses(t, e.dst, e.dstSchema)[int(tracking.StatusCompleted)] != 3 {
				t.Errorf("report = %+v, log = %v", rep, statuses(t, e.dst, e.dstSchema))
			}
		})
	}
}

// TestMySQLConcurrency: the job's own lock, held by another session, refuses the run;
// another job marked running on the same source data and root table refuses it too
// (unfinished-work guard spec §3); one on a same-named root in another source database does
// not (source-table-locks spec G3).
func TestMySQLConcurrency(t *testing.T) {
	e := newEnv(t, "conc")
	e.both(t, intTables)
	execAll(t, e.src, "INSERT INTO r VALUES (1, 1)")
	cfg := e.config(t, job(t, jobspec.Archive, decimalJob))

	held := lock.NewSet(e.dst, lock.JobLockName(e.dstSchema, "j"))
	if err := held.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire = %v", err)
	}
	if _, err := e.run(t, cfg); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Errorf("a held job lock = %v", err)
	}
	_ = held.Release(context.Background())

	store, err := tracking.Open(context.Background(), e.dst, e.dstSchema)
	if err != nil {
		t.Fatal(err)
	}
	res, err := preflight.Run(context.Background(), preflight.Config{Source: e.src, SourceDatabase: e.srcSchema, Destination: e.dst,
		DestinationDatabase: e.dstSchema, JobSchema: e.dstSchema, Spec: cfg.Spec, Verification: cfg.Verification,
		Processing: cfg.Processing})
	if err != nil {
		t.Fatal(err)
	}
	running := func(name, sourceDatabase string) {
		t.Helper()
		other, err := store.Attach(context.Background(), tracking.JobInput{Name: name, Type: jobspec.Purge, RootKey: res.Keys["r"],
			Binding: tracking.Binding{SourceServerUUID: "u", SourceDatabase: sourceDatabase, DestinationDatabase: "d"}})
		if err != nil {
			t.Fatal(err)
		}
		if err := other.SetStatus(context.Background(), tracking.JobRunning); err != nil {
			t.Fatal(err)
		}
		if err := other.Heartbeat(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	running("elsewhere", e.srcSchema+"_other")
	if _, err := e.run(t, cfg); err != nil {
		t.Fatalf("a job running on r in another source database refused the run: %v", err)
	}
	execAll(t, e.src, "INSERT INTO r VALUES (2, 1)")
	// The same database in another case, recorded under "u", which is not a well-formed uuid:
	// the guard counts it as this source's data (unfinished-work guard spec §3a, I2), and the
	// refusal says on what grounds (on a Group Replication source, membership comes first).
	running("other", strings.ToUpper(e.srcSchema))
	_, err = e.run(t, cfg)
	oe := wantErr[*OverlapError](t, err)
	if !strings.Contains(oe.Error(), "other: marked running") || !strings.Contains(oe.Error(), "recorded on source server u, ") {
		t.Errorf("a job marked running on the root: %v", oe)
	}
	if count(t, e.src, "SELECT COUNT(*) FROM r") != 1 {
		t.Error("the source changed")
	}
}

// TestMySQLLocksHeldForTheRun: at the running point the run's own sessions hold the job
// lock on the destination and the source table lock on the source; after the run both are
// free (source-table-locks spec §5.1). It replaces TestMySQLRootLockHeldUntilRunning, whose
// lock is retired.
func TestMySQLLocksHeldForTheRun(t *testing.T) {
	e := newEnv(t, "lockheld")
	e.both(t, intTables)
	execAll(t, e.src, "INSERT INTO r VALUES (1, 1)")
	cfg := e.config(t, job(t, jobspec.Archive, decimalJob))
	srcAdmin, dstAdmin := openServer(t, "TEST_SOURCE", "", nil), openServer(t, "TEST_DEST", "", nil)
	table, jobName := lock.TableLockName(e.srcSchema, "r"), lock.JobLockName(e.dstSchema, "j")
	held := func(db *sql.DB, name string) bool {
		t.Helper()
		id, err := lockHolder(context.Background(), db, name)
		if err != nil {
			t.Fatal(err)
		}
		return id != 0
	}
	reached := false
	cfg.at = func(p point) {
		if p != atRunning {
			return
		}
		reached = true
		if !held(srcAdmin, table) || !held(dstAdmin, jobName) {
			t.Errorf("at the running point: table lock held = %v, job lock held = %v; want both", held(srcAdmin, table), held(dstAdmin, jobName))
		}
	}
	e.mustRun(t, cfg)
	if !reached {
		t.Fatal("the running point was never reached")
	}
	if held(srcAdmin, table) || held(dstAdmin, jobName) {
		t.Error("a lock is still held after the run")
	}
}

// TestMySQLVerificationDetectsAChangedValue: a destination trigger changes a non-key value
// on insert. Preflight refuses destination INSERT triggers, so the run skips its policy
// checks (--skip-dangerously-validate-preflight-allow-experimental), the one way such a trigger reaches a copy. sha256
// refuses the batch and rolls it back; count, which proves only which keys landed, passes
// (R41).
func TestMySQLVerificationDetectsAChangedValue(t *testing.T) {
	for _, v := range []jobspec.Verification{jobspec.VerifySHA256, jobspec.VerifyCount} {
		t.Run(string(v), func(t *testing.T) {
			e := newEnv(t, "verify")
			e.both(t, "CREATE TABLE r (k INT NOT NULL, note VARCHAR(10) NOT NULL, PRIMARY KEY (k)) ENGINE=InnoDB")
			execAll(t, e.dst, "CREATE TRIGGER tr BEFORE INSERT ON r FOR EACH ROW SET NEW.note = 'changed'")
			execAll(t, e.src, "INSERT INTO r VALUES (1, 'kept'), (2, 'kept')")
			cfg := e.config(t, job(t, jobspec.Archive, "\n    root_table: r\n    primary_key: k\n    where: \"1=1\"\n"))
			cfg.Verification = v
			cfg.SkipPolicy = true
			_, err := e.run(t, cfg)
			if v == jobspec.VerifyCount {
				if err != nil {
					t.Fatalf("count: %v", err)
				}
				return
			}
			if ve := wantErr[*VerificationError](t, err); !strings.Contains(ve.Error(), "hashes differ") {
				t.Errorf("sha256: %v", ve)
			}
			if count(t, e.dst, "SELECT COUNT(*) FROM r") != 0 || count(t, e.src, "SELECT COUNT(*) FROM r") != 2 {
				t.Error("the failed batch was not rolled back")
			}
		})
	}
}
