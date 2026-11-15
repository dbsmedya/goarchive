//go:build integration

package preflight

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/testestate"
)

// These tests run the group facts of spec §4.2 against real servers (GR C2). The recording ones
// run on every estate: on a plain server the first statement of a run is still the
// configuration read. The ones about a member need a Group Replication source and skip on the
// plain estate; the group estate (TOPOLOGY=group) runs them.

// statementLog is the statements a pool sent to its server, in order.
type statementLog struct {
	mu    sync.Mutex
	stmts []string
}

func (l *statementLog) add(q string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stmts = append(l.stmts, q)
}

func (l *statementLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.stmts)
}

// recConnector wraps the driver's connector so that every statement a pool sends is recorded
// before the server sees it. A statement with arguments is recorded once, when it is prepared.
type recConnector struct {
	base driver.Connector
	log  *statementLog
}

func (c recConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &recConn{Conn: conn, log: c.log}, nil
}

func (c recConnector) Driver() driver.Driver { return c.base.Driver() }

type recConn struct {
	driver.Conn
	log *statementLog
}

func (c *recConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	qc, ok := c.Conn.(driver.QueryerContext)
	if !ok || len(args) > 0 {
		return nil, driver.ErrSkip
	}
	c.log.add(q)
	return qc.QueryContext(ctx, q, args)
}

func (c *recConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	ec, ok := c.Conn.(driver.ExecerContext)
	if !ok || len(args) > 0 {
		return nil, driver.ErrSkip
	}
	c.log.add(q)
	return ec.ExecContext(ctx, q, args)
}

func (c *recConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	c.log.add(q)
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, q)
}

func (c *recConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *recConn) Ping(ctx context.Context) error { return c.Conn.(driver.Pinger).Ping(ctx) }

func (c *recConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *recConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }

func (c *recConn) CheckNamedValue(nv *driver.NamedValue) error {
	return c.Conn.(driver.NamedValueChecker).CheckNamedValue(nv)
}

// recorded opens a pool on one test server, as openServer does but without the connector's
// session checks, and records every statement it sends in log.
func recorded(t *testing.T, prefix, schema string, log *statementLog) *sql.DB {
	t.Helper()
	if prefix == "TEST_REPLICA" {
		testestate.LockReplica(t)
	}
	port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
	cfg := config.DatabaseConfig{Host: os.Getenv(prefix + "_HOST"), Port: port, User: os.Getenv(prefix + "_USER"),
		Password: os.Getenv(prefix + "_PASSWORD"), Database: schema, TLS: "disable"}
	if cfg.Host == "" || cfg.User == "" || port == 0 {
		t.Fatal("integration environment not loaded: set -a; source tests/.env; set +a")
	}
	mc, err := mysql.ParseDSN(database.BuildDSN(&cfg))
	if err != nil {
		t.Fatal(err)
	}
	base, err := mysql.NewConnector(mc)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(recConnector{base: base, log: log})
	db.SetMaxOpenConns(0)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// requireMemberSource skips the test unless TEST_SOURCE is a Group Replication member: the
// plugin is ACTIVE and a group name is set. The group estate is.
func requireMemberSource(t *testing.T) {
	t.Helper()
	g, err := dbsrepl.NewInspector(openServer(t, "TEST_SOURCE", "")).GroupReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if g.PluginStatus != "ACTIVE" || g.GroupName == "" {
		t.Skip("the source is not a Group Replication member: this test runs on the group estate (TOPOLOGY=group)")
	}
	testestate.RequireGroupBase(t) // a group a killed test left changed is not built on
}

// orderPair is a source and a destination with the order tables and the job's spec.
func orderPair(t *testing.T, name string, typ jobspec.Type) (src, dst *sql.DB, spec *jobspec.Spec) {
	t.Helper()
	const cs = "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
	src, dst = scratch(t, "TEST_SOURCE", name+"src"), scratch(t, "TEST_DEST", name+"dst")
	execAll(t, src, orderTables(cs)...)
	execAll(t, dst, orderTables(cs)...)
	return src, dst, job(t, typ, orderJob)
}

// TestMySQLTheFirstStatementIsTheConfigurationRead is spec §8's *first read of each side*, for every
// job type, on any pair: the first statement preflight sends to the source is the library's
// ReplicationConfig read, then the plugin's state, then (a member only) the view, and no catalog or
// table statement comes before them; the first statement it sends to the destination is its
// ReplicationConfig read (GR C4), then the plugin's state. The command validate runs the same Run.
func TestMySQLTheFirstStatementIsTheConfigurationRead(t *testing.T) {
	for _, typ := range []jobspec.Type{jobspec.Archive, jobspec.Purge, jobspec.CopyOnly} {
		t.Run(string(typ), func(t *testing.T) {
			s, d, spec := orderPair(t, "first"+string(typ), typ)
			var srcLog, dstLog statementLog
			src, dst := recorded(t, "TEST_SOURCE", schemaOf(t, s), &srcLog), recorded(t, "TEST_DEST", schemaOf(t, d), &dstLog)
			if _, err := Run(context.Background(), Config{
				Source: src, SourceDatabase: schemaOf(t, s), Destination: dst, DestinationDatabase: schemaOf(t, d), JobSchema: schemaOf(t, d),
				Spec: spec, Verification: jobspec.VerifyCount, Processing: defaultProcessing,
			}); err != nil {
				t.Fatalf("Run() = %v, want success", err)
			}
			got := srcLog.all()
			at := func(sub string) int {
				return slices.IndexFunc(got, func(q string) bool { return strings.Contains(q, sub) })
			}
			if len(got) < 4 || !strings.Contains(got[0], "@@GLOBAL.read_only, @@GLOBAL.super_read_only") {
				t.Fatalf("the source's first statements were %q, want the configuration read first", got[:min(len(got), 4)])
			}
			if !strings.Contains(got[1], "information_schema.PLUGINS") ||
				!strings.Contains(got[2], "group_replication_group_name") || !strings.Contains(got[3], "group_replication_transaction_size_limit") {
				t.Errorf("the statements after the configuration read were %q, want the plugin's state, the group name and the limit", got[1:4])
			}
			if view := at("replication_group_members"); view >= 0 && view != 4 {
				t.Errorf("the view was read at statement %d, want it right after the plugin's own reads (4)", view)
			}
			for _, tableRead := range []string{"TABLES", "COLUMNS", "STATISTICS", "`orders`", "`order_lines`"} {
				if i := at(tableRead); i >= 0 && i < 4 {
					t.Errorf("a statement naming %s was sent at %d, before the group facts: %q", tableRead, i, got[i])
				}
			}
			gotD := dstLog.all()
			if len(gotD) < 2 || !strings.Contains(gotD[0], "@@GLOBAL.read_only, @@GLOBAL.super_read_only") || !strings.Contains(gotD[1], "information_schema.PLUGINS") {
				t.Fatalf("the destination's first statements were %q, want the configuration read first, then the plugin's state", gotD[:min(len(gotD), 2)])
			}
			for _, tableRead := range []string{"TABLES", "COLUMNS", "STATISTICS", "`orders`", "`order_lines`"} {
				if i := slices.IndexFunc(gotD, func(q string) bool { return strings.Contains(q, tableRead) }); i >= 0 && i < 2 {
					t.Errorf("a destination statement naming %s was sent at %d, before the group facts: %q", tableRead, i, gotD[i])
				}
			}
		})
	}
}

// TestMySQLTheFactsOfAMemberSource: against the group's primary, preflight returns the plugin's
// state with the group name and the limit, the configuration (a primary is writable), the view
// with the source's own ONLINE row, and the group name as the identity. The destination is a
// plain server.
func TestMySQLTheFactsOfAMemberSource(t *testing.T) {
	requireMemberSource(t)
	s, d, spec := orderPair(t, "member", jobspec.Archive)
	id, err := database.ReadServerIdentity(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Run(context.Background(), Config{
		Source: s, SourceDatabase: schemaOf(t, s), SourceServerUUID: id.ServerUUID,
		Destination: d, DestinationDatabase: schemaOf(t, d), JobSchema: schemaOf(t, d),
		Spec: spec, Verification: jobspec.VerifyCount, Processing: defaultProcessing,
	})
	if err != nil {
		t.Fatalf("Run() = %v, want success", err)
	}
	g := res.Group
	if g.Source.PluginStatus != "ACTIVE" || g.Source.GroupName == "" || !g.Source.TransactionSizeLimitKnown || g.Source.TransactionSizeLimit == 0 {
		t.Errorf("source plugin = %+v, want ACTIVE with a name and a known limit", g.Source)
	}
	if g.SourceConfig.ReadOnly || g.SourceConfig.SuperReadOnly {
		t.Errorf("the primary's configuration = %+v, want it writable", g.SourceConfig)
	}
	var own *dbsrepl.GroupMember
	for i := range g.Members {
		if g.Members[i].Self {
			own = &g.Members[i]
		}
	}
	if own == nil || own.State != "ONLINE" || own.MemberID != id.ServerUUID || len(g.Members) < 2 {
		t.Errorf("view = %+v, want the source's own ONLINE row among its members", g.Members)
	}
	if g.Identity != g.Source.GroupName {
		t.Errorf("identity = %q, want the group name %q", g.Identity, g.Source.GroupName)
	}
	if g.Destination.GroupName != "" {
		t.Errorf("destination plugin = %+v, want a server outside any group", g.Destination)
	}
	if block := res.GroupReport(defaultProcessing); !strings.Contains(block, "member of group "+g.Source.GroupName+", ONLINE, ") {
		t.Errorf("report = %q, want the member line", block)
	}
}

// memberAccount creates an account on the member behind admin with exactly the grants given
// ({{A}} is the quoted account), drops it after the test, and returns its name and password.
func memberAccount(t *testing.T, admin *sql.DB, suffix string, grants ...string) (user, pass string) {
	t.Helper()
	user, pass = acctName(suffix), "ga-c2-"+acctName(suffix)
	account := "'" + user + "'@'%'"
	execAll(t, admin, "DROP USER IF EXISTS "+account, "CREATE USER "+account+" IDENTIFIED BY '"+pass+"'")
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account) })
	for _, g := range grants {
		execAll(t, admin, strings.ReplaceAll(g, "{{A}}", account))
	}
	return user, pass
}

// TestMySQLAMissingGrantRefusesAMemberSource is spec §8's *missing grant*, at preflight: an
// account without SELECT on the view is refused with the grant in the message, and the one grant
// is enough. A plain pair needs nothing new (TestMySQLThePlainPairNeedsNoGroupGrant).
func TestMySQLAMissingGrantRefusesAMemberSource(t *testing.T) {
	requireMemberSource(t)
	s, d, spec := orderPair(t, "grant", jobspec.Archive)
	admin := openServer(t, "TEST_SOURCE", "")
	srcSchema, dstSchema := schemaOf(t, s), schemaOf(t, d)
	user, pass := memberAccount(t, admin, "viewgrant",
		"GRANT SELECT, DELETE, TRIGGER ON `"+srcSchema+"`.* TO {{A}}", "GRANT PROCESS ON *.* TO {{A}}", acctSeesGrants)
	as := acctOpen(t, "TEST_SOURCE", srcSchema, user, pass)
	err := acctRun(context.Background(), t, as, d, srcSchema, dstSchema, dstSchema, spec)
	e := refusedWith(t, err, "SOURCE_GROUP_VIEW_PERMISSION_CHECK")
	acctMessage(t, e, "performance_schema.replication_group_members", "GRANT SELECT ON performance_schema.replication_group_members", "denied")

	// A new session sees a grant made after the first one connected (the pool's are open).
	execAll(t, admin, "GRANT SELECT ON performance_schema.replication_group_members TO '"+user+"'@'%'")
	as2 := acctOpen(t, "TEST_SOURCE", srcSchema, user, pass)
	if err := acctRun(context.Background(), t, as2, d, srcSchema, dstSchema, dstSchema, spec); err != nil {
		t.Errorf("Run() with the grant = %v, want success: one grant is enough", err)
	}
}

// TestMySQLThePlainPairNeedsNoGroupGrant: an account with no grant on the view runs preflight on
// a server without the plugin, which the destination server is on every estate: nothing about
// groups is asked of it and nothing is refused.
func TestMySQLThePlainPairNeedsNoGroupGrant(t *testing.T) {
	s, d := scratch(t, "TEST_DEST", "plainsrc"), scratch(t, "TEST_DEST", "plaindst")
	execAll(t, s, acctTables...)
	execAll(t, d, acctTables...)
	admin := openServer(t, "TEST_DEST", "")
	srcSchema := schemaOf(t, s)
	as := acctCreate(t, "TEST_DEST", admin, "plainview", srcSchema,
		"GRANT SELECT, DELETE, TRIGGER ON `"+srcSchema+"`.* TO {{A}}", "GRANT PROCESS ON *.* TO {{A}}", acctSeesGrants)
	res, err := Run(context.Background(), Config{
		Source: as, SourceDatabase: srcSchema, SourceServerUUID: "u",
		Destination: d, DestinationDatabase: schemaOf(t, d), JobSchema: schemaOf(t, d),
		Spec: job(t, jobspec.Archive, acctJob), Verification: jobspec.VerifyCount, Processing: defaultProcessing,
	})
	if err != nil {
		t.Fatalf("Run() = %v, want success on a server without the plugin", err)
	}
	if res.Group.Members != nil || res.Group.Identity != "u" || res.GroupReport(defaultProcessing) != "" {
		t.Errorf("result group = %+v, want no view, the server's uuid as identity and no report", res.Group)
	}
}
