//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
)

// These tests run preflight's permission and visibility checks as real, least-privileged
// accounts. Each one creates its accounts (ga7p_<pid>_<suffix>@'%') and roles on the
// server that needs them and drops them afterwards; the side of the pair that the account
// does not concern runs as the TEST_* account. Every side gets its own account, because a
// global privilege granted to a connected account does not reach its open sessions.

// acctTables is the fixture every permission test shares, created on both servers: a
// root and one child on a declared foreign key.
var acctTables = []string{
	"CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, placed DATE) ENGINE=InnoDB",
	"CREATE TABLE order_lines (id INT NOT NULL PRIMARY KEY, order_id INT NOT NULL, KEY ix_order (order_id), " +
		"CONSTRAINT fk_lines_order FOREIGN KEY (order_id) REFERENCES orders (id)) ENGINE=InnoDB",
}

const acctJob = `
    root_table: orders
    primary_key: id
    where: "1=1"
    relations:
      - table: order_lines
        primary_key: id
        foreign_key: order_id
`

// acctTypes are the three job types preflight profiles.
var acctTypes = []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly}

// acctFixture creates acctTables in fresh schemas on both servers and returns the
// TEST_* pools on them and their names.
func acctFixture(t *testing.T, name string) (src, dst *sql.DB, srcSchema, dstSchema string) {
	t.Helper()
	src = scratch(t, "TEST_SOURCE", name+"src")
	dst = scratch(t, "TEST_DEST", name+"dst")
	execAll(t, src, acctTables...)
	execAll(t, dst, acctTables...)
	return src, dst, schemaOf(t, src), schemaOf(t, dst)
}

// acctOpen opens a pool on one test server as user, with the session pins openServer
// uses.
func acctOpen(t *testing.T, prefix, schema, user, pass string) *sql.DB {
	t.Helper()
	host := os.Getenv(prefix + "_HOST")
	port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
	if host == "" || port == 0 {
		t.Fatal("integration environment not loaded: set -a; source tests/.env; set +a")
	}
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName = user, pass, "tcp", fmt.Sprintf("%s:%d", host, port), schema
	cfg.Params = map[string]string{"time_zone": "'+00:00'", "autocommit": "1"}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// acctName is the account or role name for suffix: ga7p_<pid>_<suffix>.
func acctName(suffix string) string {
	return fmt.Sprintf("ga7p_%d_%s", os.Getpid(), suffix)
}

// acctCreate creates the account for suffix on the server behind admin (prefix names that
// server), applies grants with {{A}} standing for the quoted account, drops it after the
// test, and returns a pool on schema logged in as it.
func acctCreate(t *testing.T, prefix string, admin *sql.DB, suffix, schema string, grants ...string) *sql.DB {
	t.Helper()
	user := acctName(suffix)
	account := "'" + user + "'@'%'"
	pass := fmt.Sprintf("ga7p-%d-%d", os.Getpid(), time.Now().UnixNano())
	execAll(t, admin, "DROP USER IF EXISTS "+account, "CREATE USER "+account+" IDENTIFIED BY '"+pass+"'")
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account) })
	for _, g := range grants {
		execAll(t, admin, strings.ReplaceAll(g, "{{A}}", account))
	}
	return acctOpen(t, prefix, schema, user, pass)
}

// acctRole creates the role for suffix on the server behind admin with grants, {{R}}
// standing for the quoted role, drops it after the test, and returns the quoted role.
func acctRole(t *testing.T, admin *sql.DB, suffix string, grants ...string) string {
	t.Helper()
	role := "'" + acctName(suffix) + "'@'%'"
	execAll(t, admin, "DROP ROLE IF EXISTS "+role, "CREATE ROLE "+role)
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role) })
	for _, g := range grants {
		execAll(t, admin, strings.ReplaceAll(g, "{{R}}", role))
	}
	return role
}

// acctRun runs preflight for typ over the acctJob, with the tracking tables in jobSchema
// on the destination server. The schema names are passed, not read, so a pool whose
// connection the test holds is not queried before Run.
func acctRun(ctx context.Context, t *testing.T, src, dst *sql.DB, srcSchema, dstSchema, jobSchema string, spec *jobspec.Spec) error {
	t.Helper()
	_, err := Run(ctx, Config{
		Source: src, SourceDatabase: srcSchema,
		Destination: dst, DestinationDatabase: dstSchema, JobSchema: jobSchema,
		Spec: spec, Verification: jobspec.VerifyCount,
	})
	return err
}

// acctMessage asserts e's message names every one of want.
func acctMessage(t *testing.T, e *Error, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(e.Message, w) {
			t.Errorf("%s message = %q, want it to contain %q", e.Check, e.Message, w)
		}
	}
}

// acctTablesAre asserts e names exactly want, in order.
func acctTablesAre(t *testing.T, e *Error, want ...string) {
	t.Helper()
	if !slices.Equal(e.Tables, want) {
		t.Errorf("%s tables = %v, want %v", e.Check, e.Tables, want)
	}
}

// acctSeesGrants is the grant that lets an account prove a privilege absent: the library
// reports "absent" only to an account that can read other accounts' privilege rows, a
// direct SELECT on the mysql schema, and "unconfirmed" otherwise.
const acctSeesGrants = "GRANT SELECT ON `mysql`.* TO {{A}}"

// TestMySQLJobSchemaPermission (M7-3, G1): the tracking schema needs CREATE, SELECT,
// INSERT and UPDATE at schema scope for every job type, purge included.
// Pair: a destination account with SELECT, INSERT, UPDATE on the job schema and no CREATE
// → JOB_SCHEMA_PERMISSION_CHECK for archive, purge and copy-only, naming no tables and
// CREATE(unconfirmed), or CREATE(absent) when the account can read mysql's grant rows;
// the same grant with CREATE added → every type passes.
// Second pair: CREATE, SELECT, UPDATE on the schema with INSERT held only at table scope on
// a pre-created archiver_job → refused naming INSERT(absent), since the check reads global
// and schema scope only; INSERT at schema scope instead → passes.
func TestMySQLJobSchemaPermission(t *testing.T) {
	src, dst, srcSchema, dstSchema := acctFixture(t, "pjs")
	ctx := context.Background()
	schemaGrant := func(privs string) string { return "GRANT " + privs + " ON `" + dstSchema + "`.* TO {{A}}" }

	for _, tc := range []struct {
		suffix, state string
		grants        []string
	}{
		{"jsnocreate", "CREATE(unconfirmed)", []string{schemaGrant("SELECT, INSERT, UPDATE")}},
		{"jsnocreatevis", "CREATE(absent)", []string{schemaGrant("SELECT, INSERT, UPDATE"), acctSeesGrants}},
	} {
		t.Run("no CREATE is refused: "+tc.state, func(t *testing.T) {
			as := acctCreate(t, "TEST_DEST", dst, tc.suffix, dstSchema, tc.grants...)
			for _, typ := range acctTypes {
				err := acctRun(ctx, t, src, as, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob))
				e := refusedWith(t, err, "JOB_SCHEMA_PERMISSION_CHECK")
				acctTablesAre(t, e)
				acctMessage(t, e, "(states: "+tc.state+")", fmt.Sprintf("lacks provable CREATE on tracking schema %q", dstSchema),
					"GRANT CREATE ON `"+dstSchema+"`.* TO <user>")
			}
		})
	}
	t.Run("with CREATE it passes", func(t *testing.T) {
		as := acctCreate(t, "TEST_DEST", dst, "jscreate", dstSchema, schemaGrant("CREATE, SELECT, INSERT, UPDATE"))
		for _, typ := range acctTypes {
			if err := acctRun(ctx, t, src, as, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob)); err != nil {
				t.Errorf("%s: Run() = %v, want success", typ, err)
			}
		}
	})

	execAll(t, dst, "CREATE TABLE archiver_job (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB")
	t.Run("table-scope INSERT is refused", func(t *testing.T) {
		as := acctCreate(t, "TEST_DEST", dst, "jstableins", dstSchema, schemaGrant("CREATE, SELECT, UPDATE"),
			"GRANT INSERT ON `"+dstSchema+"`.`archiver_job` TO {{A}}", acctSeesGrants)
		err := acctRun(ctx, t, src, as, srcSchema, dstSchema, dstSchema, job(t, jobspec.Purge, acctJob))
		e := refusedWith(t, err, "JOB_SCHEMA_PERMISSION_CHECK")
		acctTablesAre(t, e)
		acctMessage(t, e, "(states: INSERT(absent))")
	})
	t.Run("schema-scope INSERT passes", func(t *testing.T) {
		as := acctCreate(t, "TEST_DEST", dst, "jsschemains", dstSchema, schemaGrant("CREATE, SELECT, UPDATE"),
			schemaGrant("INSERT"), acctSeesGrants)
		if err := acctRun(ctx, t, src, as, srcSchema, dstSchema, dstSchema, job(t, jobspec.Purge, acctJob)); err != nil {
			t.Errorf("Run() = %v, want success", err)
		}
	})
}

// TestMySQLDestinationWritePermission (M7-3, G2): archive and copy-only need INSERT on
// every destination table; purge copies nothing and does not ask.
// Pair: an account that can write its separate job schema and only SELECT the data schema
// → DEST_WRITE_PERMISSION_CHECK for archive and copy-only naming orders(absent) and
// order_lines(absent) in copy order, and purge passes; the same account with INSERT
// granted on each of the two tables → archive and copy-only pass.
func TestMySQLDestinationWritePermission(t *testing.T) {
	src, dst, srcSchema, dstSchema := acctFixture(t, "pdw")
	jobSchema := schemaOf(t, scratch(t, "TEST_DEST", "pdwjob"))
	ctx := context.Background()
	base := []string{"GRANT CREATE, SELECT, INSERT, UPDATE ON `" + jobSchema + "`.* TO {{A}}",
		"GRANT SELECT ON `" + dstSchema + "`.* TO {{A}}", acctSeesGrants}

	t.Run("no INSERT on the data schema", func(t *testing.T) {
		as := acctCreate(t, "TEST_DEST", dst, "dwnoinsert", dstSchema, base...)
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.CopyOnly} {
			err := acctRun(ctx, t, src, as, srcSchema, dstSchema, jobSchema, job(t, typ, acctJob))
			acctTablesAre(t, refusedWith(t, err, "DEST_WRITE_PERMISSION_CHECK"), "orders(absent)", "order_lines(absent)")
		}
		if err := acctRun(ctx, t, src, as, srcSchema, dstSchema, jobSchema, job(t, jobspec.Purge, acctJob)); err != nil {
			t.Errorf("purge: Run() = %v, want success", err)
		}
	})
	t.Run("table-scope INSERT passes", func(t *testing.T) {
		as := acctCreate(t, "TEST_DEST", dst, "dwtableins", dstSchema, append(base,
			"GRANT INSERT ON `"+dstSchema+"`.`orders` TO {{A}}", "GRANT INSERT ON `"+dstSchema+"`.`order_lines` TO {{A}}")...)
		for _, typ := range acctTypes {
			if err := acctRun(ctx, t, src, as, srcSchema, dstSchema, jobSchema, job(t, typ, acctJob)); err != nil {
				t.Errorf("%s: Run() = %v, want success", typ, err)
			}
		}
	})
}

// acctOnTables grants privs on each fixture table of schema, so the account sees the
// tables without a schema grant.
func acctOnTables(privs, schema string, tables ...string) []string {
	var out []string
	for _, table := range tables {
		out = append(out, "GRANT "+privs+" ON `"+schema+"`.`"+table+"` TO {{A}}")
	}
	return out
}

// TestMySQLSourceSelectPermission (M7-3, G3): every job type reads the source, so each
// needs SELECT on every job table.
// Pair: a source account with PROCESS and table-scope DELETE and INSERT, and SELECT nowhere
// → SOURCE_SELECT_PERMISSION_CHECK for archive, purge and copy-only naming orders(absent)
// and order_lines(absent); the same account with SELECT on the schema → every type passes.
// Order pair (stages: SourceSelectPermissions before SourceDeletePermissions): SELECT and
// DELETE both withheld → archive reports SOURCE_SELECT_PERMISSION_CHECK; SELECT granted,
// DELETE still withheld → SOURCE_DELETE_PERMISSION_CHECK.
// Order pair (InternalForeignKeys before SourceSelectPermissions): SELECT withheld and a
// declared line_notes → order_lines foreign key the job configures under orders instead →
// INTERNAL_FK_COVERAGE; the job nesting line_notes under order_lines →
// SOURCE_SELECT_PERMISSION_CHECK naming all three tables.
func TestMySQLSourceSelectPermission(t *testing.T) {
	src, dst, srcSchema, dstSchema := acctFixture(t, "pss")
	ctx := context.Background()
	tables := acctOnTables("DELETE, INSERT", srcSchema, "orders", "order_lines")
	withProcess := func(grants ...string) []string {
		return append([]string{"GRANT PROCESS ON *.* TO {{A}}", acctSeesGrants}, grants...)
	}

	t.Run("no SELECT is refused", func(t *testing.T) {
		as := acctCreate(t, "TEST_SOURCE", src, "ssnoselect", srcSchema, withProcess(tables...)...)
		for _, typ := range acctTypes {
			err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob))
			acctTablesAre(t, refusedWith(t, err, "SOURCE_SELECT_PERMISSION_CHECK"), "orders(absent)", "order_lines(absent)")
		}
	})
	t.Run("with SELECT it passes", func(t *testing.T) {
		as := acctCreate(t, "TEST_SOURCE", src, "ssselect", srcSchema,
			withProcess(append(tables, "GRANT SELECT ON `"+srcSchema+"`.* TO {{A}}")...)...)
		for _, typ := range acctTypes {
			if err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob)); err != nil {
				t.Errorf("%s: Run() = %v, want success", typ, err)
			}
		}
	})

	insertOnly := acctOnTables("INSERT", srcSchema, "orders", "order_lines")
	t.Run("SELECT is reported before DELETE", func(t *testing.T) {
		as := acctCreate(t, "TEST_SOURCE", src, "ssinsonly", srcSchema, withProcess(insertOnly...)...)
		err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, jobspec.Archive, acctJob))
		acctTablesAre(t, refusedWith(t, err, "SOURCE_SELECT_PERMISSION_CHECK"), "orders(absent)", "order_lines(absent)")
	})
	t.Run("with SELECT, DELETE is reported", func(t *testing.T) {
		as := acctCreate(t, "TEST_SOURCE", src, "ssnodelete", srcSchema,
			withProcess(append(insertOnly, "GRANT SELECT ON `"+srcSchema+"`.* TO {{A}}")...)...)
		err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, jobspec.Archive, acctJob))
		acctTablesAre(t, refusedWith(t, err, "SOURCE_DELETE_PERMISSION_CHECK"), "orders(absent)", "order_lines(absent)")
	})

	const notes = "CREATE TABLE line_notes (id INT NOT NULL PRIMARY KEY, order_id INT NOT NULL, line_id INT NOT NULL, " +
		"KEY ix_order (order_id), KEY ix_line (line_id), " +
		"CONSTRAINT fk_notes_line FOREIGN KEY (line_id) REFERENCES order_lines (id)) ENGINE=InnoDB"
	execAll(t, src, notes)
	execAll(t, dst, notes)
	noSelect := acctCreate(t, "TEST_SOURCE", src, "ssnoselectfk", srcSchema,
		withProcess(acctOnTables("DELETE, INSERT", srcSchema, "orders", "order_lines", "line_notes")...)...)
	t.Run("an uncovered internal foreign key is reported before SELECT", func(t *testing.T) {
		err := acctRun(ctx, t, noSelect, dst, srcSchema, dstSchema, dstSchema, job(t, jobspec.Archive, acctJob+`
      - table: line_notes
        primary_key: id
        foreign_key: order_id
`))
		e := refusedWith(t, err, "INTERNAL_FK_COVERAGE")
		acctMessage(t, e, "line_notes(line_id) -> order_lines(id) (constraint fk_notes_line) "+
			"[no relation: line_notes is not configured under order_lines]")
	})
	t.Run("covered, SELECT is reported", func(t *testing.T) {
		err := acctRun(ctx, t, noSelect, dst, srcSchema, dstSchema, dstSchema, job(t, jobspec.Archive, acctJob+`
        relations:
          - table: line_notes
            primary_key: id
            foreign_key: line_id
`))
		acctTablesAre(t, refusedWith(t, err, "SOURCE_SELECT_PERMISSION_CHECK"),
			"orders(absent)", "order_lines(absent)", "line_notes(absent)")
	})
}

// TestMySQLSourceDeletePermission (M7-3, G4): archive and purge delete from the source;
// copy-only does not and is not asked.
// Pair: a source account with global SELECT and PROCESS and no DELETE → archive and purge
// refused with SOURCE_DELETE_PERMISSION_CHECK naming orders(absent) and
// order_lines(absent), and copy-only passes; the same account with a global DELETE (with
// @@global.partial_revokes OFF, which the test asserts) → archive and purge pass.
func TestMySQLSourceDeletePermission(t *testing.T) {
	src, dst, srcSchema, dstSchema := acctFixture(t, "psd")
	ctx := context.Background()
	var partial int
	if err := src.QueryRowContext(ctx, "SELECT @@global.partial_revokes").Scan(&partial); err != nil || partial != 0 {
		t.Fatalf("@@global.partial_revokes = %d, %v; this test needs it OFF", partial, err)
	}
	base := []string{"GRANT SELECT, PROCESS ON *.* TO {{A}}"}

	t.Run("no DELETE is refused", func(t *testing.T) {
		as := acctCreate(t, "TEST_SOURCE", src, "sdnodelete", srcSchema, base...)
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
			err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob))
			acctTablesAre(t, refusedWith(t, err, "SOURCE_DELETE_PERMISSION_CHECK"), "orders(absent)", "order_lines(absent)")
		}
		if err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, jobspec.CopyOnly, acctJob)); err != nil {
			t.Errorf("copy-only: Run() = %v, want success", err)
		}
	})
	t.Run("a global DELETE passes", func(t *testing.T) {
		as := acctCreate(t, "TEST_SOURCE", src, "sddelete", srcSchema, append(base, "GRANT DELETE ON *.* TO {{A}}")...)
		for _, typ := range acctTypes {
			if err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob)); err != nil {
				t.Errorf("%s: Run() = %v, want success", typ, err)
			}
		}
	})
}

// acctRoleActive asserts the account behind db has role active and that the role's
// grant is effective: it can count orders.
func acctRoleActive(t *testing.T, db *sql.DB, role string) {
	t.Helper()
	var active string
	var n int
	if err := db.QueryRow("SELECT CURRENT_ROLE()").Scan(&active); err != nil || active != strings.ReplaceAll(role, "'", "`") {
		t.Fatalf("CURRENT_ROLE() = %q, %v; want %s active", active, err, role)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM orders").Scan(&n); err != nil {
		t.Fatalf("the role's grant is not effective: %v", err)
	}
}

// TestMySQLRoleHeldGrantsFailClosed (M7-3, G5, deviation D1): a schema grant held only
// through an active default role is effective, but MySQL does not show a role's grant rows
// to the account, so the privilege is unconfirmed and the check fails closed.
// Pair (source): SELECT on the source schema held through a default role, with PROCESS and
// DELETE direct → archive refused with SOURCE_SELECT_PERMISSION_CHECK naming
// orders(unconfirmed) and order_lines(unconfirmed); the same SELECT granted directly →
// archive passes.
// Pair (destination): CREATE, SELECT, INSERT, UPDATE on the job schema held through a
// default role → JOB_SCHEMA_PERMISSION_CHECK with all four unconfirmed and the advice to
// grant directly; the same four granted directly → archive passes.
// The partial_revokes half of D1 is TestMySQLPartialRevokes (M7c).
func TestMySQLRoleHeldGrantsFailClosed(t *testing.T) {
	src, dst, srcSchema, dstSchema := acctFixture(t, "prole")
	ctx := context.Background()
	srcSelect := "GRANT SELECT ON `" + srcSchema + "`.* TO "
	direct := []string{"GRANT PROCESS ON *.* TO {{A}}", "GRANT DELETE ON `" + srcSchema + "`.* TO {{A}}", acctSeesGrants}
	dstAll := "GRANT CREATE, SELECT, INSERT, UPDATE ON `" + dstSchema + "`.* TO "

	t.Run("source SELECT through a role is refused", func(t *testing.T) {
		role := acctRole(t, src, "rsrc", srcSelect+"{{R}}")
		as := acctCreate(t, "TEST_SOURCE", src, "rlsrcrole", srcSchema,
			append(direct, "GRANT "+role+" TO {{A}}", "SET DEFAULT ROLE ALL TO {{A}}")...)
		acctRoleActive(t, as, role)
		err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, jobspec.Archive, acctJob))
		acctTablesAre(t, refusedWith(t, err, "SOURCE_SELECT_PERMISSION_CHECK"), "orders(unconfirmed)", "order_lines(unconfirmed)")
	})
	t.Run("source SELECT granted directly passes", func(t *testing.T) {
		as := acctCreate(t, "TEST_SOURCE", src, "rlsrcdirect", srcSchema, append(direct, srcSelect+"{{A}}")...)
		if err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, jobspec.Archive, acctJob)); err != nil {
			t.Errorf("Run() = %v, want success", err)
		}
	})

	t.Run("job schema grants through a role are refused", func(t *testing.T) {
		role := acctRole(t, dst, "rdst", dstAll+"{{R}}")
		as := acctCreate(t, "TEST_DEST", dst, "rldstrole", dstSchema,
			acctSeesGrants, "GRANT "+role+" TO {{A}}", "SET DEFAULT ROLE ALL TO {{A}}")
		acctRoleActive(t, as, role)
		err := acctRun(ctx, t, src, as, srcSchema, dstSchema, dstSchema, job(t, jobspec.Archive, acctJob))
		e := refusedWith(t, err, "JOB_SCHEMA_PERMISSION_CHECK")
		acctTablesAre(t, e)
		acctMessage(t, e, "(states: CREATE(unconfirmed), SELECT(unconfirmed), INSERT(unconfirmed), UPDATE(unconfirmed))",
			"Grant each missing privilege directly to the account at schema scope")
	})
	t.Run("job schema grants held directly pass", func(t *testing.T) {
		as := acctCreate(t, "TEST_DEST", dst, "rldstdirect", dstSchema, acctSeesGrants, dstAll+"{{A}}")
		if err := acctRun(ctx, t, src, as, srcSchema, dstSchema, dstSchema, job(t, jobspec.Archive, acctJob)); err != nil {
			t.Errorf("Run() = %v, want success", err)
		}
	})
}

// TestMySQLForeignKeyVisibility (M7-3, G8, deviation D2): archive and purge need proof
// that foreign-key discovery saw every schema, which is the PROCESS-gated InnoDB registry
// read; copy-only deletes nothing and is exempt.
// Pair: a source account with SELECT and DELETE on the source schema and no PROCESS →
// archive and purge refused with FK_COVERAGE_VISIBILITY_CHECK (state unconfirmed, the
// query-stage diagnostic), copy-only passes; PROCESS granted directly → every type
// passes; PROCESS held only through a default role → archive and purge pass too, because
// the proof is the read's success.
// Pair (stage order, ForeignKeyMetadataVisibility before ForeignKeyCoverage): an external
// ON DELETE CASCADE child of orders in another schema plus no PROCESS → archive reports
// FK_COVERAGE_VISIBILITY_CHECK; with PROCESS → archive and copy-only report
// FK_COVERAGE_CHECK naming the external child.
func TestMySQLForeignKeyVisibility(t *testing.T) {
	src, dst, srcSchema, dstSchema := acctFixture(t, "pvis")
	ctx := context.Background()
	base := []string{"GRANT SELECT, DELETE ON `" + srcSchema + "`.* TO {{A}}", acctSeesGrants}
	noProcess := acctCreate(t, "TEST_SOURCE", src, "visnoproc", srcSchema, base...)
	process := acctCreate(t, "TEST_SOURCE", src, "visproc", srcSchema, append(base, "GRANT PROCESS ON *.* TO {{A}}")...)
	invisible := func(t *testing.T, err error) {
		t.Helper()
		e := refusedWith(t, err, "FK_COVERAGE_VISIBILITY_CHECK")
		acctTablesAre(t, e)
		acctMessage(t, e, "(state: unconfirmed)", "The primary InnoDB metadata query failed", "GRANT PROCESS ON *.* TO <user>")
	}

	t.Run("no PROCESS is refused except for copy-only", func(t *testing.T) {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
			invisible(t, acctRun(ctx, t, noProcess, dst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob)))
		}
		if err := acctRun(ctx, t, noProcess, dst, srcSchema, dstSchema, dstSchema, job(t, jobspec.CopyOnly, acctJob)); err != nil {
			t.Errorf("copy-only: Run() = %v, want success", err)
		}
	})
	t.Run("direct PROCESS passes", func(t *testing.T) {
		for _, typ := range acctTypes {
			if err := acctRun(ctx, t, process, dst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob)); err != nil {
				t.Errorf("%s: Run() = %v, want success", typ, err)
			}
		}
	})
	t.Run("role-held PROCESS passes", func(t *testing.T) {
		role := acctRole(t, src, "rproc", "GRANT PROCESS ON *.* TO {{R}}")
		as := acctCreate(t, "TEST_SOURCE", src, "visroleproc", srcSchema,
			append(base, "GRANT "+role+" TO {{A}}", "SET DEFAULT ROLE ALL TO {{A}}")...)
		acctRoleActive(t, as, role)
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge} {
			if err := acctRun(ctx, t, as, dst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob)); err != nil {
				t.Errorf("%s: Run() = %v, want success", typ, err)
			}
		}
	})

	// Created after the source schema, so its cleanup drops it first (Error 3730).
	ext := scratch(t, "TEST_SOURCE", "pvisext")
	execAll(t, ext, "CREATE TABLE audit (id INT NOT NULL PRIMARY KEY, order_id INT, KEY ix_order (order_id), "+
		"CONSTRAINT fk_audit_order FOREIGN KEY (order_id) REFERENCES `"+srcSchema+"`.orders (id) ON DELETE CASCADE) ENGINE=InnoDB")
	external := schemaOf(t, ext) + ".audit (ON DELETE CASCADE)"
	t.Run("an external child and no PROCESS report visibility", func(t *testing.T) {
		invisible(t, acctRun(ctx, t, noProcess, dst, srcSchema, dstSchema, dstSchema, job(t, jobspec.Archive, acctJob)))
	})
	t.Run("an external child with PROCESS reports coverage", func(t *testing.T) {
		for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.CopyOnly} {
			err := acctRun(ctx, t, process, dst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob))
			e := refusedWith(t, err, "FK_COVERAGE_CHECK")
			acctMessage(t, e, "orders is referenced by: ["+external+"]")
		}
	})
}

// acctWatchdog is how long a preflight run may take before the test calls it blocked.
const acctWatchdog = 30 * time.Second

// acctStart runs preflight for an archive acctJob in a goroutine and returns the channel
// its result arrives on.
func acctStart(t *testing.T, src, dst *sql.DB, srcSchema, dstSchema string) <-chan error {
	t.Helper()
	spec := job(t, jobspec.Archive, acctJob)
	done := make(chan error, 1)
	go func() {
		_, err := Run(context.Background(), Config{
			Source: src, SourceDatabase: srcSchema,
			Destination: dst, DestinationDatabase: dstSchema, JobSchema: dstSchema,
			Spec: spec, Verification: jobspec.VerifyCount,
		})
		done <- err
	}()
	return done
}

// TestMySQLPrivilegeReadOnAPoolOfOne (M7-5, G6): mysqlServer.Privileges takes a
// dedicated connection for the grant read and releases it before the next query, so a
// pool limited to one connection (max_connections: 1) never waits on itself.
// Pair: the source pool, then separately the destination pool, limited to one connection
// → an archive preflight passes inside the watchdog; the same pool of one while the test
// holds its only connection → the run is still blocked after two seconds, which is what
// the watchdog detects, and once the test releases the connection the same run passes.
func TestMySQLPrivilegeReadOnAPoolOfOne(t *testing.T) {
	_, _, srcSchema, dstSchema := acctFixture(t, "ppool")
	for _, side := range []string{"source", "destination"} {
		t.Run(side, func(t *testing.T) {
			src, dst := openServer(t, "TEST_SOURCE", srcSchema), openServer(t, "TEST_DEST", dstSchema)
			one := src
			if side == "destination" {
				one = dst
			}
			one.SetMaxOpenConns(1)

			t.Run("free pool of one passes", func(t *testing.T) {
				select {
				case err := <-acctStart(t, src, dst, srcSchema, dstSchema):
					if err != nil {
						t.Fatalf("Run() = %v, want success", err)
					}
				case <-time.After(acctWatchdog):
					t.Fatalf("Run() did not return within %s on a %s pool of one", acctWatchdog, side)
				}
			})
			t.Run("held pool of one blocks until released", func(t *testing.T) {
				conn, err := one.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				done := acctStart(t, src, dst, srcSchema, dstSchema)
				select {
				case err := <-done:
					_ = conn.Close()
					t.Fatalf("Run() = %v while the test held the only %s connection, want it blocked", err, side)
				case <-time.After(2 * time.Second):
				}
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("Run() after release = %v, want success", err)
					}
				case <-time.After(acctWatchdog):
					t.Fatalf("Run() did not return within %s after the %s connection was released", acctWatchdog, side)
				}
			})
		})
	}
}

// TestMySQLPartialRevokes (M7c, G5's partial_revokes half, deviation D1): while
// @@global.partial_revokes is ON, dbsgomysql proves a privilege only from a direct schema
// or table grant; a global grant is unconfirmed, and the permission check fails closed.
// Environment pair (R56): the estate runs with it OFF; the M7c environment matrix turns it
// ON with SET GLOBAL on a throwaway pair. On both, accounts built from
// docs/README_PERMISSIONS.md's grant recipes pass for every job type. Accounts holding the
// same privileges only as global grants pass with it OFF, and with it ON are refused by
// the first permission check on a server that has it: JOB_SCHEMA_PERMISSION_CHECK on the
// destination, else SOURCE_SELECT_PERMISSION_CHECK, every state unconfirmed.
func TestMySQLPartialRevokes(t *testing.T) {
	src, dst, srcSchema, dstSchema := acctFixture(t, "prev")
	ctx := context.Background()
	on := func(db *sql.DB) bool {
		var v int
		if err := db.QueryRowContext(ctx, "SELECT @@global.partial_revokes").Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v != 0
	}
	srcOn, dstOn := on(src), on(dst)
	t.Logf("partial_revokes: source %v, destination %v", srcOn, dstOn)

	recipeSrc := acctCreate(t, "TEST_SOURCE", src, "prvrsrc", srcSchema,
		"GRANT SELECT, DELETE ON `"+srcSchema+"`.* TO {{A}}", "GRANT PROCESS ON *.* TO {{A}}")
	recipeDst := acctCreate(t, "TEST_DEST", dst, "prvrdst", dstSchema,
		"GRANT SELECT, INSERT, CREATE, UPDATE ON `"+dstSchema+"`.* TO {{A}}")
	globalSrc := acctCreate(t, "TEST_SOURCE", src, "prvgsrc", srcSchema, "GRANT SELECT, DELETE, PROCESS ON *.* TO {{A}}")
	globalDst := acctCreate(t, "TEST_DEST", dst, "prvgdst", dstSchema, "GRANT SELECT, INSERT, CREATE, UPDATE ON *.* TO {{A}}")

	for _, typ := range acctTypes {
		if err := acctRun(ctx, t, recipeSrc, recipeDst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob)); err != nil {
			t.Errorf("grant recipes, %s: Run() = %v, want success", typ, err)
		}
		err := acctRun(ctx, t, globalSrc, globalDst, srcSchema, dstSchema, dstSchema, job(t, typ, acctJob))
		switch {
		case dstOn:
			acctMessage(t, refusedWith(t, err, "JOB_SCHEMA_PERMISSION_CHECK"),
				"CREATE(unconfirmed)", "SELECT(unconfirmed)", "INSERT(unconfirmed)", "UPDATE(unconfirmed)")
		case srcOn:
			acctTablesAre(t, refusedWith(t, err, "SOURCE_SELECT_PERMISSION_CHECK"), "orders(unconfirmed)", "order_lines(unconfirmed)")
		case err != nil:
			t.Errorf("global grants, %s: Run() = %v with partial_revokes OFF, want success", typ, err)
		}
	}
}
