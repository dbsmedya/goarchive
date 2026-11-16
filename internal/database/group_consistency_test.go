package database

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// The failover fence, G12 (spec §4.2, §8; GR C8): on every physical source and destination connection, in the pool's
// connector, before the pool hands the connection out. The decision is made from two facts the library reads on the driver
// connection itself, kept apart: the plugin's state (does the rule apply?) and the session's own group_replication_
// consistency (is the session fenced?). Never from the global value, and never one fact from the other. A connection that
// cannot be established is refused and closed: establish or refuse.

const (
	setFence = "SET SESSION group_replication_consistency = 'BEFORE_ON_PRIMARY_FAILOVER'"
	fence    = "BEFORE_ON_PRIMARY_FAILOVER"
)

// libraryReads is what the library's read does on a driver connection, as kinds (kindsOf): three statements for a plugin that
// is ACTIVE (the group name's row exists) and four for one that is not (the plugin's status follows). The statements' own text
// is the library's, and a unit test of goarchive does not carry it.
func libraryReads(active bool) []string {
	reads := []string{"group name", "session value", "global value"}
	if !active {
		reads = append(reads, "plugin status")
	}
	return reads
}

// kindsOf names each recorded statement by what it asks, so a test compares what was asked and not the library's text.
func kindsOf(statements []string) []string {
	out := make([]string, len(statements))
	for i, s := range statements {
		switch {
		case strings.HasPrefix(s, "X "):
			out[i] = "SET"
		case strings.Contains(s, "max_error_count"):
			out[i] = "max_error_count"
		case strings.Contains(s, "character_set_client"):
			out[i] = "charset"
		case strings.Contains(s, "group_replication_group_name"):
			out[i] = "group name"
		case strings.Contains(s, "session_variables"):
			out[i] = "session value"
		case strings.Contains(s, "global_variables"):
			out[i] = "global value"
		case strings.Contains(s, "PLUGIN_STATUS"):
			out[i] = "plugin status"
		default:
			out[i] = s
		}
	}
	return out
}

func refusal(t *testing.T, err error) *GroupConsistencyError {
	t.Helper()
	var e *GroupConsistencyError
	if !errors.As(err, &e) {
		t.Fatalf("error = %v (%T), want a *GroupConsistencyError", err, err)
	}
	return e
}

// TestGroupFenceDecisionTable is spec §4.2's table, a row at a time, from the two facts' values.
func TestGroupFenceDecisionTable(t *testing.T) {
	for _, tc := range []struct {
		name         string
		plugin       string
		session      string // "" is no value returned
		global       string
		globalKnown  bool
		wantSet      bool
		wantRefusal  bool
		wantSession  string // the session's value afterwards
		wantReadsAre []string
	}{
		// the plugin is not ACTIVE: the rule does not apply, whatever the session says
		{"no plugin, EVENTUAL", "", "EVENTUAL", "EVENTUAL", true, false, false, "EVENTUAL", libraryReads(false)},
		{"no plugin, a stronger value (the 8.4 default on a plain server)", "", fence, fence, true, false, false, fence, libraryReads(false)},
		{"no plugin, no value returned", "", "", "", false, false, false, "", libraryReads(false)},
		{"no plugin, an unfamiliar value", "", "SOMETHING_NEW", "EVENTUAL", true, false, false, "SOMETHING_NEW", libraryReads(false)},
		{"plugin DISABLED, EVENTUAL", "DISABLED", "EVENTUAL", "EVENTUAL", true, false, false, "EVENTUAL", libraryReads(false)},
		{"plugin INACTIVE, EVENTUAL", "INACTIVE", "EVENTUAL", "EVENTUAL", true, false, false, "EVENTUAL", libraryReads(false)},
		{"an unfamiliar plugin status, EVENTUAL", "FROZEN", "EVENTUAL", "EVENTUAL", true, false, false, "EVENTUAL", libraryReads(false)},
		{"plugin DISABLED, no value returned", "DISABLED", "", "", false, false, false, "", libraryReads(false)},
		// the plugin is ACTIVE and the session is EVENTUAL: set, read again, require
		{"ACTIVE, EVENTUAL", "ACTIVE", "EVENTUAL", "EVENTUAL", true, true, false, fence, nil},
		{"ACTIVE, EVENTUAL, the global is stronger (the global decides nothing)", "ACTIVE", "EVENTUAL", fence, true, true, false, fence, nil},
		{"ACTIVE, EVENTUAL, the global unknown", "ACTIVE", "EVENTUAL", "", false, true, false, fence, nil},
		// each of the four stronger values includes the fence and is never weakened
		{"ACTIVE, BEFORE_ON_PRIMARY_FAILOVER", "ACTIVE", fence, "EVENTUAL", true, false, false, fence, libraryReads(true)},
		{"ACTIVE, BEFORE", "ACTIVE", "BEFORE", "EVENTUAL", true, false, false, "BEFORE", libraryReads(true)},
		{"ACTIVE, AFTER", "ACTIVE", "AFTER", "EVENTUAL", true, false, false, "AFTER", libraryReads(true)},
		{"ACTIVE, BEFORE_AND_AFTER", "ACTIVE", "BEFORE_AND_AFTER", "EVENTUAL", true, false, false, "BEFORE_AND_AFTER", libraryReads(true)},
		// the stronger session on a server whose global is EVENTUAL: the global decides nothing
		{"ACTIVE, a stronger session, the global EVENTUAL", "ACTIVE", "BEFORE", "EVENTUAL", true, false, false, "BEFORE", libraryReads(true)},
		// anything else: refused
		{"ACTIVE, an unfamiliar value", "ACTIVE", "SOMETHING_NEW", "EVENTUAL", true, false, true, "SOMETHING_NEW", libraryReads(true)},
		{"ACTIVE, a lower-case spelling is not the server's", "ACTIVE", "eventual", "EVENTUAL", true, false, true, "eventual", libraryReads(true)},
		{"ACTIVE, no value returned", "ACTIVE", "", "EVENTUAL", true, false, true, "", libraryReads(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := newFenceServer(tc.plugin, tc.session)
			conn.global, conn.globalKnown = tc.global, tc.globalKnown
			err := ensureGroupConsistency(context.Background(), conn, "gr2:3306")
			if tc.wantRefusal {
				e := refusal(t, err)
				if e.Addr != "gr2:3306" {
					t.Errorf("the refusal names %q, want gr2:3306", e.Addr)
				}
			} else if err != nil {
				t.Fatalf("ensureGroupConsistency = %v, want the connection accepted", err)
			}
			execs := conn.execStatements()
			if tc.wantSet != (len(execs) == 1) || (tc.wantSet && execs[0] != setFence) || (!tc.wantSet && len(execs) != 0) {
				t.Errorf("statements issued = %q, want %v of the one SET", execs, tc.wantSet)
			}
			conn.mu.Lock()
			session := conn.session
			conn.mu.Unlock()
			if session != tc.wantSession {
				t.Errorf("the session's value afterwards = %q, want %q", session, tc.wantSession)
			}
			if tc.wantReadsAre != nil {
				if got := kindsOf(conn.statements()); !slices.Equal(got, tc.wantReadsAre) {
					t.Errorf("statements = %q, want only the library's reads %q", got, tc.wantReadsAre)
				}
			}
		})
	}
}

// TestGroupFenceSetsOnceAndReadsBackWhole: where the table asks for the SET it is one statement, goarchive's, on the same
// driver connection, between the library's whole reads: the read-back is the function again, all of it.
func TestGroupFenceSetsOnceAndReadsBackWhole(t *testing.T) {
	conn := newFenceServer("ACTIVE", "EVENTUAL")
	if err := ensureGroupConsistency(context.Background(), conn, "gr2:3306"); err != nil {
		t.Fatal(err)
	}
	want := append(append(libraryReads(true), "SET"), libraryReads(true)...)
	if got := kindsOf(conn.statements()); !slices.Equal(got, want) {
		t.Errorf("statements:\n%q\nwant\n%q", got, want)
	}
	if conn.wasClosed() {
		t.Error("ensureGroupConsistency closed the connection it accepted")
	}
}

// TestGroupFenceRefusesWhatItCannotEstablish: each row of the table that ends in a refusal, with the reason and what was
// found in the words, and the two ways out; the connection is not the check's to close.
func TestGroupFenceRefusesWhatItCannotEstablish(t *testing.T) {
	denied := &mysql.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation"}
	for _, tc := range []struct {
		name  string
		setup func(c *fenceServer)
		found []string
	}{
		{"an unfamiliar value", func(c *fenceServer) { c.session = "SOMETHING_NEW" }, []string{"SOMETHING_NEW"}},
		{"no value returned", func(c *fenceServer) { c.sessionKnown = false }, []string{"no group_replication_consistency value"}},
		{"the SET is refused", func(c *fenceServer) { c.setErr = denied }, []string{"cannot set", fence, "1227"}},
		{"the read-back is not the value set", func(c *fenceServer) { c.setBecomes = "EVENTUAL" }, []string{"EVENTUAL", "after the SET"}},
		{"the SET changes nothing", func(c *fenceServer) { c.setIgnored = true }, []string{"EVENTUAL", "after the SET"}},
		{"the read-back returns no value", func(c *fenceServer) { c.setForgets = true },
			[]string{"after the SET", "no group_replication_consistency value"}},
		{"the read-back fails", func(c *fenceServer) {
			c.readBackErr = &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"}
		}, []string{"cannot read", "1142"}},
		{"the plugin's state cannot be read", func(c *fenceServer) {
			c.plugin = "" // no name row, so the library reads PLUGINS, which fails
			c.failQuery = map[string]error{"PLUGIN_STATUS": &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"}}
		}, []string{"cannot read", "1142"}},
		{"the session read is refused", func(c *fenceServer) {
			c.failQuery = map[string]error{"performance_schema.session_variables": &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"}}
		}, []string{"cannot read", "1142"}},
		{"the global read is refused (the fact is whole)", func(c *fenceServer) {
			c.failQuery = map[string]error{"performance_schema.global_variables WHERE VARIABLE_NAME = 'group_replication_consistency'": &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"}}
		}, []string{"cannot read", "1142"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := newFenceServer("ACTIVE", "EVENTUAL")
			tc.setup(conn)
			e := refusal(t, ensureGroupConsistency(context.Background(), conn, "gr2:3306"))
			msg := e.Error()
			for _, want := range append([]string{"GROUP_CONSISTENCY_CHECK", "gr2:3306"}, tc.found...) {
				if !strings.Contains(msg, want) {
					t.Errorf("the refusal %q does not contain %q", msg, want)
				}
			}
			// the two ways out, in the operator's terms
			if !strings.Contains(msg, "privilege") || !strings.Contains(msg, "default") {
				t.Errorf("the refusal %q does not name the two ways out: give the account what the SET needs, or make the stronger value its or the server's default", msg)
			}
			if conn.wasClosed() {
				t.Error("the check closed the connection: that is the connector's act")
			}
		})
	}
}

// TestGroupFenceCausesAreKept: a refusal wraps the cause it found, so errors.Is and As see it, and no refusal is a bare
// driver sentinel.
func TestGroupFenceCausesAreKept(t *testing.T) {
	cause := &mysql.MySQLError{Number: 1227, Message: "denied"}
	conn := newFenceServer("ACTIVE", "EVENTUAL")
	conn.setErr = cause
	err := ensureGroupConsistency(context.Background(), conn, "gr2:3306")
	var me *mysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1227 {
		t.Errorf("error = %v, want the server's 1227 in its chain", err)
	}
}

// TestGroupFenceErrorsAreNeverBareDriverSentinels: database/sql takes driver.ErrBadConn and driver.ErrSkip for an
// instruction, so Connect never returns one bare: the library wraps them, and goarchive's refusal wraps the library's error
// in turn. errors.Is still finds the cause, which is what database/sql's retry of a dead connection needs.
func TestGroupFenceErrorsAreNeverBareDriverSentinels(t *testing.T) {
	for _, sentinel := range []error{driver.ErrSkip, driver.ErrBadConn} {
		for _, statement := range []string{"group_replication_group_name", "performance_schema.session_variables", "performance_schema.global_variables"} {
			conn := newFenceServer("ACTIVE", "EVENTUAL")
			conn.failQuery = map[string]error{statement: sentinel}
			err := ensureGroupConsistency(context.Background(), conn, "gr2:3306")
			if err == nil {
				t.Fatalf("%v on %s: accepted", sentinel, statement)
			}
			if err == sentinel { //nolint:errorlint // the point is that it is not the sentinel itself
				t.Errorf("%v on %s came back bare", sentinel, statement)
			}
			if !errors.Is(err, sentinel) {
				t.Errorf("%v on %s: %v, want the cause in the chain", sentinel, statement, err)
			}
		}
	}
}

// TestGroupFenceNeedsTheContextTakingQuery: a driver connection that lacks the context-taking query is refused, with its
// type named, because the library's function takes exactly that and goarchive makes the assertion; and one that lacks the
// context-taking exec is refused when the SET is needed, not before.
func TestGroupFenceNeedsTheContextTakingQuery(t *testing.T) {
	e := refusal(t, ensureGroupConsistency(context.Background(), noQueryConn{}, "gr2:3306"))
	if !strings.Contains(e.Error(), "noQueryConn") {
		t.Errorf("the refusal %q does not name the driver connection's type", e.Error())
	}

	// A connection with the query and no exec passes where no SET is needed and is refused where one is.
	plain := &queryOnlyConn{s: newFenceServer("", "EVENTUAL")}
	if err := ensureGroupConsistency(context.Background(), plain, "db2:3306"); err != nil {
		t.Errorf("a query-only connection on a server without the plugin = %v, want it accepted: nothing is set there", err)
	}
	fenced := &queryOnlyConn{s: newFenceServer("ACTIVE", "EVENTUAL")}
	e = refusal(t, ensureGroupConsistency(context.Background(), fenced, "gr2:3306"))
	if !strings.Contains(e.Error(), "queryOnlyConn") {
		t.Errorf("the refusal %q does not name the type that cannot run the SET", e.Error())
	}
}

type noQueryConn struct{}

func (noQueryConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no") }
func (noQueryConn) Close() error                        { return nil }
func (noQueryConn) Begin() (driver.Tx, error)           { return nil, errors.New("no") }

// queryOnlyConn is the model with QueryContext and without ExecContext.
type queryOnlyConn struct{ s *fenceServer }

func (q *queryOnlyConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no") }
func (q *queryOnlyConn) Close() error                        { return q.s.Close() }
func (q *queryOnlyConn) Begin() (driver.Tx, error)           { return nil, errors.New("no") }
func (q *queryOnlyConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return q.s.QueryContext(ctx, query, args)
}

// The remaining tests drive the connector as the pool does: Connect, on a connector whose connections are models.

func connectorOver(make func() driver.Conn) (warningsConnector, *modelConnector) {
	inner := &modelConnector{make: make}
	return warningsConnector{Connector: inner, addr: "gr2:3306"}, inner
}

func serverOf(t *testing.T, conn driver.Conn) *fenceServer {
	t.Helper()
	switch c := conn.(type) {
	case *fenceServer:
		return c
	case *queryOnlyConn:
		return c.s
	}
	t.Fatalf("%T is not a model", conn)
	return nil
}

// TestConnectorFencesEveryConnectionAndClosesARefusedOne: the check runs on every physical connection the pool opens,
// startup and replacements alike; a refused connection is closed and never reaches the pool; the checks that were there
// run first, so the pins are in force when the library reads.
func TestConnectorFencesEveryConnectionAndClosesARefusedOne(t *testing.T) {
	sessions := []string{"EVENTUAL", "SOMETHING_NEW", "EVENTUAL"}
	made := 0
	c, inner := connectorOver(func() driver.Conn {
		s := newFenceServer("ACTIVE", sessions[made%len(sessions)])
		made++
		return s
	})

	// the first connection is fenced and handed out
	conn, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("the first Connect = %v", err)
	}
	first := serverOf(t, conn)
	if execs := first.execStatements(); !slices.Equal(execs, []string{setFence}) {
		t.Errorf("the handed-out connection ran %q, want the one SET", execs)
	}
	if first.wasClosed() {
		t.Error("the connection handed out was closed")
	}
	// the existing checks come first, then the library's reads
	statements := kindsOf(first.statements())
	if len(statements) < 3 || statements[0] != "max_error_count" || statements[1] != "charset" || statements[2] != "group name" {
		t.Errorf("statements = %q, want the max_error_count and charset checks before the library's reads", statements)
	}

	// a replacement with an unfamiliar value is refused, closed and not handed out
	conn, err = c.Connect(context.Background())
	if conn != nil {
		t.Errorf("a refused connection was handed out: %T", conn)
	}
	_ = refusal(t, err)
	if second := serverOf(t, inner.conns[1]); !second.wasClosed() {
		t.Error("the refused connection was not closed")
	}

	// a later replacement is checked as the first was: nothing is remembered
	conn, err = c.Connect(context.Background())
	if err != nil {
		t.Fatalf("the third Connect = %v", err)
	}
	if execs := serverOf(t, conn).execStatements(); !slices.Equal(execs, []string{setFence}) {
		t.Errorf("the replacement ran %q, want its own SET", execs)
	}
	if got := len(inner.conns); got != 3 {
		t.Errorf("the connector opened %d connections, want 3", got)
	}
}

// TestConnectorLeavesAServerWithoutThePluginAlone: on a server whose plugin is not ACTIVE the connector reads and sets
// nothing of the fence, and the statement count of a new connection there is what the spec's section 5 says: the two
// existing checks and the library's four reads.
func TestConnectorLeavesAServerWithoutThePluginAlone(t *testing.T) {
	for _, plugin := range []string{"", "DISABLED"} {
		c, inner := connectorOver(func() driver.Conn { return newFenceServer(plugin, "BEFORE_ON_PRIMARY_FAILOVER") })
		conn, err := c.Connect(context.Background())
		if err != nil {
			t.Fatalf("plugin %q: Connect = %v", plugin, err)
		}
		s := serverOf(t, conn)
		if execs := s.execStatements(); len(execs) != 0 {
			t.Errorf("plugin %q: the connector ran %q on a server where the rule does not apply", plugin, execs)
		}
		if got := len(s.statements()); got != 2+4 {
			t.Errorf("plugin %q: %d statements on a new connection, want the two existing checks and the library's four reads", plugin, got)
		}
		if len(inner.conns) != 1 {
			t.Errorf("plugin %q: %d connections opened", plugin, len(inner.conns))
		}
	}
}

type ctxKey struct{}

// TestConnectorHandsTheConnectContextToEveryRead: the caller's context reaches the driver's query unwrapped, which is what
// lets a deadline or a cancellation end a read that a fence holds; a read ended by it refuses and closes the connection,
// and is not named a decision of the server, so the pool treats it as the transient it is.
func TestConnectorHandsTheConnectContextToEveryRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "marker"))
	defer cancel()
	conn := newFenceServer("ACTIVE", "EVENTUAL")
	conn.onQuery = func(statement string) {
		if strings.Contains(statement, "session_variables") {
			cancel() // the context ends while this read is under way
		}
	}
	c, _ := connectorOver(func() driver.Conn { return conn })
	got, err := c.Connect(ctx)
	if got != nil || err == nil {
		t.Fatalf("Connect = %v, %v, want a refusal", got, err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want the cancellation in its chain", err)
	}
	var named *GroupConsistencyError
	if errors.As(err, &named) {
		t.Errorf("a cancelled read is named a refusal: %v", err)
	}
	if !conn.wasClosed() {
		t.Error("a connection whose read was cancelled was not closed")
	}
	for i, qctx := range conn.queryContexts() {
		if qctx.Value(ctxKey{}) != "marker" {
			t.Errorf("query %d did not receive the caller's context", i)
		}
	}
	// the library's reads never reach a SET
	if execs := conn.execStatements(); len(execs) != 0 {
		t.Errorf("a cancelled read was followed by %q", execs)
	}
}

// TestConnectorReadFailuresRefuseAndClose: a read that fails closes the connection, and the failure is returned as the
// server's decision (named) when the server made it, and unnamed (so the pool may retry) when the network did.
func TestConnectorReadFailuresRefuseAndClose(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		named bool
	}{
		{"a MySQL error is the server's decision", &mysql.MySQLError{Number: 1142, Message: "denied"}, true},
		{"a network failure is transient", fmt.Errorf("read tcp: %w", driver.ErrBadConn), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := newFenceServer("ACTIVE", "EVENTUAL")
			conn.failQuery = map[string]error{"performance_schema.session_variables": tc.err}
			c, _ := connectorOver(func() driver.Conn { return conn })
			got, err := c.Connect(context.Background())
			if got != nil || err == nil {
				t.Fatalf("Connect = %v, %v, want a refusal", got, err)
			}
			var named *GroupConsistencyError
			if errors.As(err, &named) != tc.named {
				t.Errorf("error = %v, named = %v, want named = %v", err, errors.As(err, &named), tc.named)
			}
			if !conn.wasClosed() {
				t.Error("a connection whose read failed was not closed")
			}
		})
	}
}

// TestOpenPoolDoesNotRetryAFenceRefusal: a refused first connection is a property of the server and the account, as the
// other two checks' refusals are, and the pool's first ping returns it as it is.
func TestOpenPoolDoesNotRetryAFenceRefusal(t *testing.T) {
	c, _ := connectorOver(func() driver.Conn { return newFenceServer("ACTIVE", "SOMETHING_NEW") })
	_, err := openOn(context.Background(), c, &config.DatabaseConfig{})
	if err == nil {
		t.Fatal("openOn accepted a refused connection")
	}
	_ = refusal(t, err)
	if _, retry := errors.AsType[*retryableError](err); retry {
		t.Errorf("%v is marked retryable: a fence refusal is not a transient", err)
	}
}
