package replication

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"testing"

	dbsrepl "github.com/dbsmedya/dbsgomysql/pkg/replication"
	"github.com/go-sql-driver/mysql"
)

// fakeNetError is a net.Error that is not a *net.OpError, so the timeout case
// exercises the interface match rather than the concrete type.
type fakeNetError struct{ timeout bool }

func (e fakeNetError) Error() string   { return "i/o timeout" }
func (e fakeNetError) Timeout() bool   { return e.timeout }
func (e fakeNetError) Temporary() bool { return false }

// classifyCases are the enumerated mappings, reused for the wrapped variants.
func classifyCases() []struct {
	name string
	err  error
	want holdCause
} {
	return []struct {
		name string
		err  error
		want holdCause
	}{
		{
			name: "1 dial failure",
			err:  &net.OpError{Op: "dial", Err: errors.New("connection refused")},
			want: causeUnreachable,
		},
		{
			name: "2 net.Error with Timeout",
			err:  fakeNetError{timeout: true},
			want: causeUnreachable,
		},
		{
			name: "3 driver.ErrBadConn",
			err:  driver.ErrBadConn,
			want: causeUnreachable,
		},
		{
			name: "4 MySQL server error",
			err:  &mysql.MySQLError{Number: 1227, Message: "denied"},
			want: causeUnreadable,
		},
	}
}

func TestClassify(t *testing.T) {
	for _, tc := range classifyCases() {
		t.Run(tc.name, func(t *testing.T) {
			if got := classify(tc.err); got != tc.want {
				t.Errorf("classify(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}

	// Case 5: the fail-closed fallback. A decode or protocol failure is never
	// evidence of health.
	t.Run("5 unknown error falls back to unreadable", func(t *testing.T) {
		if got := classify(errors.New("malformed packet")); got != causeUnreadable {
			t.Errorf("classify(generic) = %s, want %s", got, causeUnreadable)
		}
	})
}

// Case 6: the library wraps every read failure in its own *OpError, so
// classification must unwrap — once, and at any depth.
func TestClassifyUnwrapsLibraryOpError(t *testing.T) {
	for _, tc := range classifyCases() {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := &dbsrepl.OpError{Op: "replica_status", Err: tc.err}
			if got := classify(wrapped); got != tc.want {
				t.Errorf("classify(OpError{%v}) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}

	t.Run("double wrap", func(t *testing.T) {
		inner := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
		opErr := &dbsrepl.OpError{Op: "replica_status", Err: inner}
		wrapped := fmt.Errorf("gate read: %w", opErr)

		if got := classify(wrapped); got != causeUnreachable {
			t.Errorf("classify(double-wrapped dial failure) = %s, want %s", got, causeUnreachable)
		}
	})
}

func TestClassifyHoldCauseString(t *testing.T) {
	cases := map[holdCause]string{
		causeUnreachable: "unreachable",
		causeUnreadable:  "unreadable",
		causeUnhealthy:   "unhealthy",
	}
	for cause, want := range cases {
		if got := cause.String(); got != want {
			t.Errorf("holdCause(%d).String() = %q, want %q", int(cause), got, want)
		}
	}

	// An out-of-range value names itself rather than impersonating a real cause.
	if got := holdCause(9).String(); got != "holdCause(9)" {
		t.Errorf("holdCause(9).String() = %q, want %q", got, "holdCause(9)")
	}
}
