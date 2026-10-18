package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// noRow, as a fakeSession value, answers the probe with a result that has no row.
type noRow struct{}

// fakeSession is a driver connection whose session answers maxErrorCountProbe with values,
// in order (queryErr instead once they run out), and maxErrorCountSet with setErr. It answers
// sessionCharsetProbe with charset, the pinned session when nil (charsetErr instead when set).
// It records every statement it runs.
type fakeSession struct {
	values     []driver.Value
	queryErr   error
	setErr     error
	charset    []driver.Value
	charsetErr error
	ran        []string
	closed     bool
}

func (f *fakeSession) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fakeSession: Prepare")
}
func (f *fakeSession) Begin() (driver.Tx, error) { return nil, errors.New("fakeSession: Begin") }
func (f *fakeSession) Close() error              { f.closed = true; return nil }

func (f *fakeSession) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	f.ran = append(f.ran, q)
	if q == sessionCharsetProbe {
		if f.charsetErr != nil {
			return nil, f.charsetErr
		}
		v := f.charset
		if v == nil {
			v = []driver.Value{[]byte("utf8mb4"), []byte("utf8mb4"), []byte("utf8mb4"), []byte("utf8mb4_general_ci")}
		}
		return &oneRow{v: v}, nil
	}
	if len(f.values) == 0 {
		if f.queryErr != nil {
			return nil, f.queryErr
		}
		return nil, errors.New("fakeSession: no value left")
	}
	v := f.values[0]
	f.values = f.values[1:]
	_, empty := v.(noRow)
	return &oneValue{v: v, done: empty}, nil
}

func (f *fakeSession) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	f.ran = append(f.ran, q)
	return driver.RowsAffected(0), f.setErr
}

type oneValue struct {
	v    driver.Value
	done bool
}

func (r *oneValue) Columns() []string { return []string{"@@SESSION.max_error_count"} }
func (r *oneValue) Close() error      { return nil }
func (r *oneValue) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.v
	return nil
}

// oneRow answers sessionCharsetProbe: one row of its values.
type oneRow struct {
	v    []driver.Value
	done bool
}

func (r *oneRow) Columns() []string {
	return []string{"@@character_set_client", "@@character_set_connection", "@@character_set_results", "@@collation_connection"}[:len(r.v)]
}
func (r *oneRow) Close() error { return nil }
func (r *oneRow) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(dest, r.v)
	return nil
}

var denied = &mysql.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the SYSTEM_VARIABLES_ADMIN or SESSION_VARIABLES_ADMIN privilege(s) for this operation"}

// TestEnsureMaxErrorCount (M7c-F5): a session above 0 is only read; a session at 0 is set to
// 1024 and read back. Every failure the server decides is MAX_ERROR_COUNT_CHECK, and MySQL's
// 1227 names both remedies; a failure of the connection itself stays unnamed and unwraps to
// its cause, so driver.ErrBadConn keeps its retries.
func TestEnsureMaxErrorCount(t *testing.T) {
	read, set := maxErrorCountProbe, maxErrorCountSet
	for _, tc := range []struct {
		name      string
		session   fakeSession
		ran       []string
		denied    bool
		transient bool     // an unnamed error wrapping driver.ErrBadConn
		want      []string // substrings of the error; nil (and not transient) wants no error
	}{
		{name: "above 0: read only, never set", session: fakeSession{values: []driver.Value{uint64(1024)}}, ran: []string{read}},
		{name: "1 is above 0 too", session: fakeSession{values: []driver.Value{uint64(1)}}, ran: []string{read}},
		{name: "int64 is read as well", session: fakeSession{values: []driver.Value{int64(64)}}, ran: []string{read}},
		{name: "0: set to 1024 and read back", session: fakeSession{values: []driver.Value{uint64(0), uint64(1024)}}, ran: []string{read, set, read}},
		{name: "0, the account may not set it", session: fakeSession{values: []driver.Value{uint64(0)}, setErr: denied}, ran: []string{read, set}, denied: true,
			want: []string{"MAX_ERROR_COUNT_CHECK", "127.0.0.1:3307", "max_error_count is 0", "global max_error_count", "SESSION_VARIABLES_ADMIN", "1227"}},
		{name: "0, the server refuses the set otherwise", session: fakeSession{values: []driver.Value{uint64(0)}, setErr: &mysql.MySQLError{Number: 1064, Message: "syntax"}}, ran: []string{read, set},
			want: []string{"MAX_ERROR_COUNT_CHECK", "cannot set the session's max_error_count to 1024", "1064"}},
		{name: "0 after the set", session: fakeSession{values: []driver.Value{uint64(0), uint64(0)}}, ran: []string{read, set, read},
			want: []string{"MAX_ERROR_COUNT_CHECK", "still 0"}},
		{name: "a value of the wrong type", session: fakeSession{values: []driver.Value{[]byte("1024")}}, ran: []string{read},
			want: []string{"MAX_ERROR_COUNT_CHECK", "cannot read", "[]uint8"}},
		{name: "a negative value", session: fakeSession{values: []driver.Value{int64(-1)}}, ran: []string{read},
			want: []string{"MAX_ERROR_COUNT_CHECK", "cannot read"}},
		{name: "no row", session: fakeSession{values: []driver.Value{noRow{}}}, ran: []string{read},
			want: []string{"MAX_ERROR_COUNT_CHECK", "no row"}},
		{name: "the server refuses the probe", session: fakeSession{queryErr: &mysql.MySQLError{Number: 1142, Message: "denied"}}, ran: []string{read},
			want: []string{"MAX_ERROR_COUNT_CHECK", "cannot read", "1142"}},
		{name: "the connection dies at the probe", session: fakeSession{queryErr: driver.ErrBadConn}, ran: []string{read}, transient: true},
		{name: "the connection dies at the set", session: fakeSession{values: []driver.Value{uint64(0)}, setErr: driver.ErrBadConn}, ran: []string{read, set}, transient: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.session
			err := ensureMaxErrorCount(context.Background(), &s, "127.0.0.1:3307")
			if !slices.Equal(s.ran, tc.ran) {
				t.Errorf("statements = %q, want %q", s.ran, tc.ran)
			}
			var mec *MaxErrorCountError
			if tc.transient {
				if errors.As(err, &mec) || !errors.Is(err, driver.ErrBadConn) || !strings.Contains(err.Error(), "127.0.0.1:3307") {
					t.Fatalf("err = %v, want an unnamed error naming the server and wrapping driver.ErrBadConn", err)
				}
				return
			}
			if tc.want == nil {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if !errors.As(err, &mec) {
				t.Fatalf("err = %v, want a *MaxErrorCountError", err)
			}
			if mec.Denied != tc.denied {
				t.Errorf("Denied = %v, want %v", mec.Denied, tc.denied)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("err = %q, want it to contain %q", err, w)
				}
			}
			if tc.denied {
				var me *mysql.MySQLError
				if !errors.As(err, &me) || me.Number != 1227 {
					t.Errorf("err does not unwrap to MySQL's 1227: %v", err)
				}
			}
		})
	}
}

// fakeConnector hands out sessions in order, and an error once they run out.
type fakeConnector struct{ sessions []*fakeSession }

func (f *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	if len(f.sessions) == 0 {
		return nil, errors.New("fakeConnector: no session left")
	}
	s := f.sessions[0]
	f.sessions = f.sessions[1:]
	return s, nil
}
func (f *fakeConnector) Driver() driver.Driver { return mysql.MySQLDriver{} }

// TestWarningsConnectorChecksEveryConnection (M7c-F5): the check runs on each physical
// connection, so a replacement opened after the global fell to 0 is set too; a connection
// that fails it is closed and never returned.
func TestWarningsConnectorChecksEveryConnection(t *testing.T) {
	first := &fakeSession{values: []driver.Value{uint64(1024)}}
	second := &fakeSession{values: []driver.Value{uint64(0), uint64(1024)}}
	third := &fakeSession{values: []driver.Value{uint64(0)}, setErr: denied}
	c := warningsConnector{Connector: &fakeConnector{sessions: []*fakeSession{first, second, third}}, addr: "db:3306"}
	ctx := context.Background()

	for i, s := range []*fakeSession{first, second} {
		conn, err := c.Connect(ctx)
		if err != nil || conn != s {
			t.Fatalf("connection %d: Connect = %v, %v; want the session, nil", i+1, conn, err)
		}
		if s.closed {
			t.Errorf("connection %d was closed although it passed the check", i+1)
		}
	}
	if !slices.Contains(second.ran, maxErrorCountSet) {
		t.Errorf("the second connection ran %q; a session at 0 must be set", second.ran)
	}
	conn, err := c.Connect(ctx)
	var mec *MaxErrorCountError
	if conn != nil || !errors.As(err, &mec) || !mec.Denied {
		t.Fatalf("third connection: Connect = %v, %v; want nil and a denied MAX_ERROR_COUNT_CHECK", conn, err)
	}
	if !third.closed {
		t.Error("a connection that failed the check was left open")
	}
}

// TestOpenDBSurfacesTheRefusal (M7c-F5, Fable plan review P-4): database/sql hands the
// connector's refusal to the caller that asked for a connection, as is and without a retry
// (a retry would meet "no session left"), and counts no open connection. Mid-run, that
// caller is the batch's BeginTx, Conn or statement, which then fails by name.
func TestOpenDBSurfacesTheRefusal(t *testing.T) {
	refused := &fakeSession{values: []driver.Value{uint64(0)}, setErr: denied}
	db := sql.OpenDB(warningsConnector{Connector: &fakeConnector{sessions: []*fakeSession{refused}}, addr: "db:3306"})
	t.Cleanup(func() { _ = db.Close() })

	conn, err := db.Conn(context.Background())
	if conn != nil {
		_ = conn.Close()
	}
	var mec *MaxErrorCountError
	if !errors.As(err, &mec) || !mec.Denied {
		t.Fatalf("db.Conn = %v; want the denied MAX_ERROR_COUNT_CHECK as is", err)
	}
	if n := db.Stats().OpenConnections; n != 0 {
		t.Errorf("OpenConnections = %d after the refusal, want 0", n)
	}
	if !refused.closed {
		t.Error("the refused driver connection was left open")
	}
}

// TestPoolConnectorWrapsOnlyWarningReaders (M7c-F5): source and destination pools get the
// check; a replica pool, which reads replica status only, does not, so a replica account
// is never refused for a setting it never uses.
func TestPoolConnectorWrapsOnlyWarningReaders(t *testing.T) {
	cfg := &config.DatabaseConfig{Host: "127.0.0.1", Port: 3307, User: "u", Password: "p", Database: "d"}
	for _, reads := range []bool{true, false} {
		c, err := poolConnector(cfg, reads)
		if err != nil {
			t.Fatal(err)
		}
		w, wrapped := c.(warningsConnector)
		if wrapped != reads {
			t.Errorf("poolConnector(readsWarnings=%v) wrapped = %v", reads, wrapped)
		}
		if wrapped && w.addr != "127.0.0.1:3307" {
			t.Errorf("addr = %q, want 127.0.0.1:3307", w.addr)
		}
	}
}
