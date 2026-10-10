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
	cfg.Params = map[string]string{"time_zone": "'+00:00'"}
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
