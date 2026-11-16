package database

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
)

// fenceServer is a driver connection that answers the statements of the connector's session checks and of the library's
// GroupConsistency read from a model of one server, and records everything done to it: every query with the context it
// received, every Exec, and whether it was closed. It plays the part of dbsgomysql's ScriptedDriverConn (internal/
// testsupport/scriptdriver.go), which the plan had copied into internal/testestate; testestate imports this package, so a
// test of this package cannot use it from there, and the model lives here, beside the one test file that needs it.
//
// The model: plugin is the server's Group Replication plugin ("" for none; "ACTIVE" makes the group-name row come back;
// any other status is listed by information_schema.PLUGINS); session and global are group_replication_consistency, with a
// flag that the server returned the row at all.
type fenceServer struct {
	mu sync.Mutex

	plugin       string
	session      string
	sessionKnown bool
	global       string
	globalKnown  bool

	// failures, by a substring of the statement: the error the statement returns.
	failQuery map[string]error
	// setErr is what the SET returns; setIgnored accepts it and changes nothing; setBecomes accepts it and leaves the
	// session at another value.
	setErr      error
	setIgnored  bool
	setBecomes  string
	setForgets  bool  // the SET is accepted and the server then returns no session value
	readBackErr error // the session read that follows the SET fails
	onQuery     func(statement string)

	// the record: calls holds every statement in arrival order, queries tagged "Q " and execs "X ".
	calls    []string
	queries  []fenceQuery
	execs    []string
	closed   bool
	sessions int // session reads so far
}

type fenceQuery struct {
	statement string
	ctx       context.Context
}

func newFenceServer(plugin, session string) *fenceServer {
	return &fenceServer{plugin: plugin, session: session, sessionKnown: session != "", global: "EVENTUAL", globalKnown: true}
}

var (
	_ driver.Conn           = (*fenceServer)(nil)
	_ driver.QueryerContext = (*fenceServer)(nil)
	_ driver.ExecerContext  = (*fenceServer)(nil)
)

func (s *fenceServer) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no Prepare") }
func (s *fenceServer) Begin() (driver.Tx, error)           { return nil, errors.New("no Begin") }

func (s *fenceServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fenceServer) wasClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// statements are every query and exec in the order they were issued, each prefixed "Q " or "X ".
func (s *fenceServer) statements() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *fenceServer) execStatements() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.execs...)
}

func (s *fenceServer) queryContexts() []context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []context.Context
	for _, q := range s.queries {
		out = append(out, q.ctx)
	}
	return out
}

func (s *fenceServer) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execs = append(s.execs, query)
	s.calls = append(s.calls, "X "+query)
	if s.setErr != nil {
		return nil, s.setErr
	}
	const prefix = "SET SESSION group_replication_consistency = '"
	if strings.HasPrefix(query, prefix) {
		value := strings.TrimSuffix(strings.TrimPrefix(query, prefix), "'")
		switch {
		case s.setIgnored:
		case s.setForgets:
			s.sessionKnown = false
		case s.setBecomes != "":
			s.session, s.sessionKnown = s.setBecomes, true
		default:
			s.session, s.sessionKnown = value, true
		}
	}
	return driver.RowsAffected(0), nil
}

func (s *fenceServer) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	s.queries = append(s.queries, fenceQuery{statement: query, ctx: ctx})
	s.calls = append(s.calls, "Q "+query)
	hook := s.onQuery
	s.mu.Unlock()
	if hook != nil {
		hook(query)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for sub, err := range s.failQuery {
		if strings.Contains(query, sub) {
			return nil, err
		}
	}
	switch {
	case strings.Contains(query, "@@SESSION.max_error_count"):
		return rowsOf([]string{"@@SESSION.max_error_count"}, [][]driver.Value{{uint64(1024)}}), nil
	case strings.Contains(query, "@@character_set_client"):
		return rowsOf([]string{"c", "n", "r", "o"}, [][]driver.Value{{[]byte("utf8mb4"), []byte("utf8mb4"), []byte("utf8mb4"), []byte("utf8mb4_general_ci")}}), nil
	case strings.Contains(query, "group_replication_group_name"):
		if s.plugin == "ACTIVE" {
			return rowsOf([]string{"VARIABLE_VALUE"}, [][]driver.Value{{[]byte("8a94f357-aab4-11df-86ab-c80aa9429563")}}), nil
		}
		return rowsOf([]string{"VARIABLE_VALUE"}, nil), nil
	case strings.Contains(query, "performance_schema.session_variables"):
		s.sessions++
		if s.sessions > 1 && s.readBackErr != nil {
			return nil, s.readBackErr
		}
		if !s.sessionKnown {
			return rowsOf([]string{"VARIABLE_VALUE"}, nil), nil
		}
		return rowsOf([]string{"VARIABLE_VALUE"}, [][]driver.Value{{[]byte(s.session)}}), nil
	case strings.Contains(query, "performance_schema.global_variables"):
		if !s.globalKnown {
			return rowsOf([]string{"VARIABLE_VALUE"}, nil), nil
		}
		return rowsOf([]string{"VARIABLE_VALUE"}, [][]driver.Value{{[]byte(s.global)}}), nil
	case strings.Contains(query, "information_schema.PLUGINS"):
		if s.plugin == "" || s.plugin == "ACTIVE" {
			return rowsOf([]string{"PLUGIN_STATUS"}, nil), nil
		}
		return rowsOf([]string{"PLUGIN_STATUS"}, [][]driver.Value{{[]byte(s.plugin)}}), nil
	}
	return nil, errors.New("fenceServer: an unexpected statement: " + query)
}

type fenceRows struct {
	columns []string
	rows    [][]driver.Value
	next    int
}

func rowsOf(columns []string, rows [][]driver.Value) driver.Rows {
	return &fenceRows{columns: columns, rows: rows}
}

func (r *fenceRows) Columns() []string { return r.columns }
func (r *fenceRows) Close() error      { return nil }
func (r *fenceRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

// modelConnector hands out the connections its make function builds, and keeps them.
type modelConnector struct {
	mu    sync.Mutex
	make  func() driver.Conn
	conns []driver.Conn
}

func (c *modelConnector) Connect(context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn := c.make()
	c.conns = append(c.conns, conn)
	return conn, nil
}

func (c *modelConnector) Driver() driver.Driver { return nil }
