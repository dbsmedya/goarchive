//go:build integration

package engine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dbsmedya/goarchive/v3/internal/config"
	"github.com/dbsmedya/goarchive/v3/internal/database"
	"github.com/dbsmedya/goarchive/v3/internal/jobspec"
	"github.com/dbsmedya/goarchive/v3/internal/logger"
	"github.com/dbsmedya/goarchive/v3/internal/preflight"
)

// These tests run the group facts through a run (GR C2): a source account without the view's
// grant is refused before any lock, and a run on a member logs spec §4.7's block once. They need
// a Group Replication source and skip on the plain estate; the group estate (TOPOLOGY=group) runs
// them.

// requireGroupMemberSource skips the test unless TEST_SOURCE is a Group Replication member: the
// plugin is ACTIVE and a group name is set.
func requireGroupMemberSource(t *testing.T) {
	t.Helper()
	g, err := dbsrepl.NewInspector(openServer(t, "TEST_SOURCE", "", nil)).GroupReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if g.PluginStatus != "ACTIVE" || g.GroupName == "" {
		t.Skip("the source is not a Group Replication member: this test runs on the group estate (TOPOLOGY=group)")
	}
}

// grantGroupView gives a source account SELECT on the group's view when the server is a Group
// Replication member (spec §4.2, GR C2): every source account of these tests is meant to get past
// that read, and to be refused for what its test leaves out. account is quoted, 'user'@'%'.
func grantGroupView(t *testing.T, admin *sql.DB, account string) {
	t.Helper()
	g, err := dbsrepl.NewInspector(admin).GroupReplication(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if g.PluginStatus == "ACTIVE" && g.GroupName != "" {
		execAll(t, admin, "GRANT SELECT ON performance_schema.replication_group_members TO "+account)
	}
}

// openAccount opens a pool on a test server as another account, as goarchive opens its own.
func openAccount(t *testing.T, prefix, schema, user, pass string) *sql.DB {
	t.Helper()
	port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
	cfg := config.DatabaseConfig{Host: os.Getenv(prefix + "_HOST"), Port: port, User: user, Password: pass, Database: schema, TLS: "disable"}
	db, err := database.OpenPool(t.Context(), &cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(0)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestMySQLAMissingGrantIsRefusedBeforeAnyLock is spec §8's *missing grant*: a source account
// without SELECT on the view is refused by preflight with the grant in the message. Nothing of
// the run exists afterwards: no tracking schema, no lock (newEnv's leak guard), no row moved.
func TestMySQLAMissingGrantIsRefusedBeforeAnyLock(t *testing.T) {
	requireGroupMemberSource(t)
	e := newEnv(t, "grpgrant")
	f := keyFamilies[0]
	e.both(t, f.tables()...)
	f.seed(t, e)

	admin := openServer(t, "TEST_SOURCE", "", nil)
	user, pass := "ga3m5_grpgrant_"+strconv.Itoa(os.Getpid()), "ga3m5-grant-pw"
	account := "'" + user + "'@'%'"
	execAll(t, admin, "DROP USER IF EXISTS "+account, "CREATE USER "+account+" IDENTIFIED BY '"+pass+"'",
		"GRANT SELECT, DELETE, TRIGGER ON `"+e.srcSchema+"`.* TO "+account, "GRANT PROCESS ON *.* TO "+account,
		"GRANT SELECT ON `mysql`.* TO "+account)
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account) })

	var before int
	if err := e.src.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM r").Scan(&before); err != nil || before == 0 {
		t.Fatalf("seeded roots = %d (%v), want some", before, err)
	}
	cfg := e.config(t, job(t, jobspec.Archive, keyJob))
	cfg.Source = openAccount(t, "TEST_SOURCE", e.srcSchema, user, pass)
	e.record(cfg)
	_, err := Run(context.Background(), cfg)
	var pe *preflight.Error
	if !errors.As(err, &pe) || pe.Check != "SOURCE_GROUP_VIEW_PERMISSION_CHECK" {
		t.Fatalf("Run() = %v, want SOURCE_GROUP_VIEW_PERMISSION_CHECK", err)
	}
	if !strings.Contains(pe.Message, "GRANT SELECT ON performance_schema.replication_group_members") {
		t.Errorf("the refusal %q does not name the grant", pe.Message)
	}
	var tables int
	if err := e.dst.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'archiver_job'",
		e.dstSchema).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Error("a tracking table exists after a refusal before any lock")
	}
	var roots int
	if err := e.src.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM r").Scan(&roots); err != nil || roots != before {
		t.Errorf("source roots = %d (%v), want the %d seeded rows untouched", roots, err, before)
	}
}

// TestMySQLARunOnAMemberLogsTheGroupReportOnce: the run's log carries the block as one entry, at
// info level, ahead of the first batch's entries, and the run completes as it did before.
func TestMySQLARunOnAMemberLogsTheGroupReportOnce(t *testing.T) {
	requireGroupMemberSource(t)
	e := newEnv(t, "grplog")
	f := keyFamilies[0]
	e.both(t, f.tables()...)
	f.seed(t, e)
	cfg := e.config(t, job(t, jobspec.Archive, keyJob))
	core, logs := observer.New(zapcore.DebugLevel)
	cfg.Logger = &logger.Logger{SugaredLogger: zap.New(core).Sugar()}
	e.mustRun(t, cfg)

	var entries []observer.LoggedEntry
	for _, le := range logs.All() {
		if strings.HasPrefix(le.Message, "Group Replication:") {
			entries = append(entries, le)
		}
	}
	if len(entries) != 1 || entries[0].Level != zapcore.InfoLevel {
		t.Fatalf("the log has %d group reports (%v), want one at info level", len(entries), entries)
	}
	for _, want := range []string{"  source:      member of group ", ", ONLINE, ", "  destination: Group Replication plugin not active",
		"  sizes:       batch_size 2, batch_delete_size 2 ("} {
		if !strings.Contains(entries[0].Message, want) {
			t.Errorf("the entry %q lacks %q", entries[0].Message, want)
		}
	}
}
