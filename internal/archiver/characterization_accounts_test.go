//go:build integration

package archiver

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/internal/graph"
)

// ============================================================================
// Disposable MySQL account support for the characterization suite.
//
// Permission checks cannot be characterized as root: a root connection holds
// every privilege at global scope, so every global row resolves to GrantPresent and
// every check trivially passes. These helpers create precisely-scoped throwaway
// accounts and open *sql.DB handles as them.
//
// SAFETY: unlike the chr_* schemas, a MySQL ACCOUNT is server-global and is NOT
// removed by dropping a database. Every account created here is dropped by
// t.Cleanup on the server it was created on, and account names embed the test
// process id so a leftover from an interrupted run can never be reused or
// collided with by a later run.
// ============================================================================

// chrAccount is a disposable MySQL account created for one characterization or
// privilege test. Accounts always use host '%' so the test client can connect
// from whatever address the container sees.
type chrAccount struct {
	User     string
	Password string
	Host     string // MySQL host part of the account identity, always "%"
	Addr     string // host:port of the server the account lives on
}

// Identity renders the account as MySQL's quoted 'user'@'host' identity, which
// is also the GRANTEE spelling used by the information_schema privilege tables
// (see formatGrantee, preflight.go:1219).
func (a chrAccount) Identity() string {
	return "'" + a.User + "'@'" + a.Host + "'"
}

// chrAccountSeq makes account names unique within a package test run; the pid
// makes them unique ACROSS runs.
var chrAccountSeq int

// chrJobSchemaSeq numbers the throwaway tracking schemas.
var chrJobSchemaSeq int

// chrServerAddr returns host:port for the named side ("source" or
// "destination") from the integration configuration.
//
// DEVIATION from the plan, which declared chrServerAddr(t, setup, side) and made
// callers thread an *IntegrationTestSetup through. The published contract for
// chrCreateAccount takes the SIDE NAME, not an address, so the lookup is done
// from the integration config directly — the same config SetupIntegrationTest
// loads — and no call site needs the setup value.
func chrServerAddr(t *testing.T, side string) string {
	t.Helper()
	cfg, err := LoadIntegrationConfig()
	if err != nil {
		t.Fatalf("load integration config: %v", err)
	}
	for _, dbCfg := range cfg.Databases {
		if dbCfg.Name == side {
			return net.JoinHostPort(dbCfg.Host, strconv.Itoa(dbCfg.Port))
		}
	}
	t.Fatalf("no integration database configured with name %q", side)
	return ""
}

// chrCreateAccount creates a fresh account on the server reachable through admin
// and applies each GRANT statement verbatim. side names the server the account
// lives on ("source" or "destination") and MUST match admin — it only decides
// the address chrOpenAs dials.
//
// Every statement in grants must contain the literal token {{ACCOUNT}}, which is
// replaced by the quoted account identity.
//
// Example:
//
//	acct := chrCreateAccount(t, ctx, f.DestDB, "destination",
//	    "GRANT SELECT, INSERT ON `"+f.DestSchema+"`.* TO {{ACCOUNT}}")
//
// The account is dropped by t.Cleanup, including on failure.
func chrCreateAccount(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	side string,
	grants ...string,
) chrAccount {
	t.Helper()

	chrAccountSeq++
	acct := chrAccount{
		User:     fmt.Sprintf("chr_u_%d_%d", os.Getpid(), chrAccountSeq),
		Password: "chr_pw_Str0ng!",
		Host:     "%",
		Addr:     chrServerAddr(t, side),
	}
	identity := acct.Identity()

	if _, err := admin.ExecContext(ctx, "DROP USER IF EXISTS "+identity); err != nil {
		t.Fatalf("drop user %s: %v", identity, err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s'", identity, acct.Password)); err != nil {
		t.Fatalf("create user %s: %v", identity, err)
	}
	// Registered immediately after CREATE so a failing GRANT below still drops
	// the account. context.Background() is deliberate: cleanup must survive a
	// cancelled test context.
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+identity); err != nil {
			t.Errorf("cleanup drop user %s: %v", identity, err)
		}
	})

	for _, stmt := range grants {
		if !strings.Contains(stmt, chrAccountToken) {
			t.Fatalf("grant statement %q does not contain %s", stmt, chrAccountToken)
		}
		expanded := strings.ReplaceAll(stmt, chrAccountToken, identity)
		if _, err := admin.ExecContext(ctx, expanded); err != nil {
			t.Fatalf("grant %q: %v", expanded, err)
		}
	}
	// Redundant for GRANT (MySQL reloads the in-memory grant tables itself), but
	// cheap and it removes a class of flaky-test question.
	if _, err := admin.ExecContext(ctx, "FLUSH PRIVILEGES"); err != nil {
		t.Fatalf("flush privileges: %v", err)
	}

	return acct
}

// chrAccountToken is the placeholder chrCreateAccount substitutes in each GRANT.
const chrAccountToken = "{{ACCOUNT}}"

// chrOpenAs opens a *sql.DB connected as acct, defaulting to schema. The pool is
// closed by t.Cleanup. MaxOpenConns is left at the driver default here; phase
// 019's max_connections:1 fixture sets it explicitly.
func chrOpenAs(t *testing.T, acct chrAccount, schema string) *sql.DB {
	t.Helper()

	cfg := mysql.NewConfig()
	cfg.User = acct.User
	cfg.Passwd = acct.Password
	cfg.Net = "tcp"
	cfg.Addr = acct.Addr
	cfg.DBName = schema
	cfg.ParseTime = true
	cfg.MultiStatements = true
	cfg.Timeout = 30 * time.Second
	cfg.TLSConfig = "preferred"

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("open as %s: %v", acct.User, err)
	}
	t.Cleanup(func() {
		if cerr := db.Close(); cerr != nil {
			t.Logf("close pool for %s: %v", acct.User, cerr)
		}
	})
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping as %s: %v", acct.User, err)
	}
	return db
}

// chrTrackingSchema creates a throwaway tracking schema (chr_job_<n>) on the
// server behind admin and drops it via t.Cleanup.
//
// It exists because the default fixture sets jobSchema == DestSchema, which makes
// JOB_SCHEMA_PERMISSION_CHECK and DEST_WRITE_PERMISSION_CHECK impossible to
// isolate: JOB_SCHEMA requires INSERT at SCHEMA scope on the tracking schema and
// has no per-table fallback, so any grant that clears JOB_SCHEMA also clears
// DEST_WRITE. Separating the two schemas is the only way a DEST_WRITE fixture can
// prove anything.
func chrTrackingSchema(t *testing.T, ctx context.Context, admin *sql.DB) string {
	t.Helper()

	chrJobSchemaSeq++
	schema := fmt.Sprintf("chr_job_%d", chrJobSchemaSeq)

	if _, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+schema+"`"); err != nil {
		t.Fatalf("drop tracking schema %s: %v", schema, err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+schema+"`"); err != nil {
		t.Fatalf("create tracking schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+schema+"`"); err != nil {
			t.Errorf("cleanup drop tracking schema %s: %v", schema, err)
		}
	})
	return schema
}

// chrGrantScopeCounts reports how many privilege rows name priv for acct at each
// of the three scopes preflight consults, as seen by the privileged admin pool:
//
//	global — information_schema.USER_PRIVILEGES   (server-wide by definition)
//	schema — information_schema.SCHEMA_PRIVILEGES restricted to schema
//	table  — information_schema.TABLE_PRIVILEGES  restricted to schema
//
// A pass-asserting permission test proves nothing on its own — removing the
// defect cannot make it fail. These counts pin WHICH scope satisfied the check,
// so a fixture that accidentally holds the privilege somewhere else is caught.
func chrGrantScopeCounts(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	acct chrAccount,
	priv string,
	schema string,
) (global, schemaScope, tableScope int) {
	t.Helper()
	grantee := acct.Identity()

	scan := func(query string, args ...interface{}) int {
		var n int
		if err := admin.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
			t.Fatalf("count %s privileges for %s: %v", priv, grantee, err)
		}
		return n
	}

	global = scan(`SELECT COUNT(*) FROM information_schema.USER_PRIVILEGES
		WHERE GRANTEE = ? AND PRIVILEGE_TYPE = ?`, grantee, priv)
	schemaScope = scan(`SELECT COUNT(*) FROM information_schema.SCHEMA_PRIVILEGES
		WHERE GRANTEE = ? AND PRIVILEGE_TYPE = ? AND TABLE_SCHEMA = ?`, grantee, priv, schema)
	tableScope = scan(`SELECT COUNT(*) FROM information_schema.TABLE_PRIVILEGES
		WHERE GRANTEE = ? AND PRIVILEGE_TYPE = ? AND TABLE_SCHEMA = ?`, grantee, priv, schema)
	return global, schemaScope, tableScope
}

// chrAssertGrantScopes asserts the exact (global, schema, table) row counts for
// priv on schema. Pass -1 for a scope that is not being pinned.
func chrAssertGrantScopes(
	t *testing.T,
	ctx context.Context,
	admin *sql.DB,
	acct chrAccount,
	priv string,
	schema string,
	wantGlobal, wantSchema, wantTable int,
) {
	t.Helper()
	global, schemaScope, tableScope := chrGrantScopeCounts(t, ctx, admin, acct, priv, schema)
	for _, spec := range []struct {
		scope string
		got   int
		want  int
	}{
		{"global", global, wantGlobal},
		{"schema", schemaScope, wantSchema},
		{"table", tableScope, wantTable},
	} {
		if spec.want >= 0 && spec.got != spec.want {
			t.Fatalf("%s: %s privilege rows at %s scope on %q = %d, want %d",
				acct.Identity(), priv, spec.scope, schema, spec.got, spec.want)
		}
	}
}

// CheckerAs builds a PreflightChecker over the fixture's schemas but with
// caller-supplied connection pools, so preflight runs as a restricted account
// while the fixture's own admin pools are still used for DDL. The tracking schema
// equals the destination schema, exactly as chrFixture.Checker does.
func (f *chrFixture) CheckerAs(t *testing.T, g *graph.Graph, sourceDB, destDB *sql.DB) *PreflightChecker {
	t.Helper()
	return f.CheckerAsWithJobSchema(t, g, sourceDB, destDB, f.DestSchema)
}

// CheckerAsWithJobSchema is CheckerAs with an explicit tracking schema, so
// JOB_SCHEMA_PERMISSION_CHECK and the destination data-table checks can be
// granted independently.
func (f *chrFixture) CheckerAsWithJobSchema(
	t *testing.T,
	g *graph.Graph,
	sourceDB, destDB *sql.DB,
	jobSchema string,
) *PreflightChecker {
	t.Helper()
	c, err := NewPreflightChecker(sourceDB, f.SourceSchema, g, f.log)
	if err != nil {
		t.Fatalf("NewPreflightChecker: %v", err)
	}
	if err := c.ConfigureDestination(destDB, f.DestSchema, jobSchema); err != nil {
		t.Fatalf("ConfigureDestination: %v", err)
	}
	return c
}
