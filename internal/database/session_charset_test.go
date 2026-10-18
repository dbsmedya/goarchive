package database

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	mysql "github.com/go-sql-driver/mysql"
)

// TestEnsureSessionCharset (CHS-1): a session is accepted only as utf8mb4 for client,
// connection and results, collated utf8mb4_general_ci, whether the driver returns the
// values as bytes or strings. Anything else the server reports, NULL included (no conversion
// of results at all), is SESSION_CHARSET_CHECK naming the values; a failure of the connection
// itself stays unnamed, so database/sql and connectWithRetry treat it as transient.
func TestEnsureSessionCharset(t *testing.T) {
	strs := func(vs ...any) []driver.Value {
		out := make([]driver.Value, len(vs))
		for i, v := range vs {
			out[i] = v
		}
		return out
	}
	b := func(s string) []byte { return []byte(s) }
	for _, tc := range []struct {
		name       string
		session    fakeSession
		transient  bool
		want       []string // substrings of the error; nil (and not transient) wants no error
		wantValues [4]string
	}{
		{name: "bytes", session: fakeSession{charset: strs(b("utf8mb4"), b("utf8mb4"), b("utf8mb4"), b("utf8mb4_general_ci"))}},
		{name: "string", session: fakeSession{charset: strs("utf8mb4", "utf8mb4", "utf8mb4", "utf8mb4_general_ci")}},
		{name: "latin1", session: fakeSession{charset: strs(b("latin1"), b("latin1"), b("latin1"), b("latin1_swedish_ci"))},
			want:       []string{"SESSION_CHARSET_CHECK", "127.0.0.1:3307", "client=latin1", "results=latin1", "collation=latin1_swedish_ci", "init_connect"},
			wantValues: [4]string{"latin1", "latin1", "latin1", "latin1_swedish_ci"}},
		{name: "null results", session: fakeSession{charset: strs(b("utf8mb4"), b("utf8mb4"), nil, b("utf8mb4_general_ci"))},
			want:       []string{"SESSION_CHARSET_CHECK", "results=NULL"},
			wantValues: [4]string{"utf8mb4", "utf8mb4", "NULL", "utf8mb4_general_ci"}},
		{name: "collation", session: fakeSession{charset: strs(b("utf8mb4"), b("utf8mb4"), b("utf8mb4"), b("utf8mb4_0900_ai_ci"))},
			want: []string{"SESSION_CHARSET_CHECK", "collation=utf8mb4_0900_ai_ci"}},
		{name: "a result of the wrong shape", session: fakeSession{charset: strs(b("utf8mb4"), b("utf8mb4"), b("utf8mb4"))},
			want: []string{"SESSION_CHARSET_CHECK", "cannot read", "3 columns"}},
		{name: "a value of the wrong type", session: fakeSession{charset: strs(b("utf8mb4"), int64(45), b("utf8mb4"), b("utf8mb4_general_ci"))},
			want: []string{"SESSION_CHARSET_CHECK", "cannot read", "int64"}},
		{name: "server error", session: fakeSession{charsetErr: &mysql.MySQLError{Number: 1105, Message: "unknown"}},
			want: []string{"SESSION_CHARSET_CHECK", "cannot read", "1105"}},
		{name: "network", session: fakeSession{charsetErr: io.ErrUnexpectedEOF}, transient: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.session
			err := ensureSessionCharset(context.Background(), &s, "127.0.0.1:3307")
			var sce *SessionCharsetError
			if tc.transient {
				if errors.As(err, &sce) || !errors.Is(err, io.ErrUnexpectedEOF) || !strings.Contains(err.Error(), "127.0.0.1:3307") {
					t.Fatalf("err = %v, want an unnamed error naming the server and wrapping the cause", err)
				}
				return
			}
			if tc.want == nil {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if !errors.As(err, &sce) {
				t.Fatalf("err = %v, want a *SessionCharsetError", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("err = %q, want it to contain %q", err, w)
				}
			}
			if tc.wantValues != ([4]string{}) && sce.Values != tc.wantValues {
				t.Errorf("Values = %q, want %q", sce.Values, tc.wantValues)
			}
		})
	}
}

// TestWarningsConnectorChecksCharset (CHS-1): every physical connection's session character
// set is checked, after max_error_count; one that fails is closed and never returned.
func TestWarningsConnectorChecksCharset(t *testing.T) {
	narrowed := &fakeSession{values: []driver.Value{uint64(1024)},
		charset: []driver.Value{[]byte("latin1"), []byte("latin1"), []byte("latin1"), []byte("latin1_swedish_ci")}}
	c := warningsConnector{Connector: &fakeConnector{sessions: []*fakeSession{narrowed}}, addr: "db:3306"}
	conn, err := c.Connect(context.Background())
	var sce *SessionCharsetError
	if conn != nil || !errors.As(err, &sce) {
		t.Fatalf("Connect = %v, %v; want nil and SESSION_CHARSET_CHECK", conn, err)
	}
	if !narrowed.closed {
		t.Error("a connection that failed the check was left open")
	}
	if got := narrowed.ran; len(got) != 2 || got[0] != maxErrorCountProbe || got[1] != sessionCharsetProbe {
		t.Errorf("statements = %q, want the max_error_count probe, then the charset probe", got)
	}
}
