package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbsmedya/goarchive/v3/internal/config"
)

// TestOpenPoolClassifiesFailures (M7c-F6): openOn marks as retryable only what
// connectWithRetry retried before it: a pool that cannot ping, unless the ping carries the
// connector's MAX_ERROR_COUNT_CHECK or SESSION_CHARSET_CHECK. A failure after the ping (the
// UTC probe, the AUTO_INCREMENT zero assertion) is never retried. Every failure closes the
// pool, its connections included, and the error keeps its text.
func TestOpenPoolClassifiesFailures(t *testing.T) {
	mode := []byte("STRICT_TRANS_TABLES,NO_AUTO_VALUE_ON_ZERO")
	narrowed := []driver.Value{[]byte("latin1"), []byte("latin1"), []byte("latin1"), []byte("latin1_swedish_ci")}
	for _, tc := range []struct {
		name      string
		session   *fakeSession // nil: the connector has no session to give
		checked   bool         // the connector runs the per-connection checks (warningsConnector)
		retryable bool
		check     func(t *testing.T, err error)
		closed    bool // the session must be closed after the failure
	}{
		{name: "ok", session: &fakeSession{values: []driver.Value{int64(86400), mode}}},
		{name: "transient", retryable: true, check: func(t *testing.T, err error) {
			if err.Error() != "fakeConnector: no session left" {
				t.Errorf("text = %q, want the ping's own", err)
			}
		}},
		{name: "refused by the connector: max_error_count", checked: true, closed: true,
			session: &fakeSession{values: []driver.Value{uint64(0)}, setErr: denied},
			check: func(t *testing.T, err error) {
				if _, ok := errors.AsType[*MaxErrorCountError](err); !ok {
					t.Errorf("err = %v, want a MaxErrorCountError", err)
				}
			}},
		{name: "refused by the connector: session charset", checked: true, closed: true,
			session: &fakeSession{values: []driver.Value{uint64(1024)}, charset: narrowed},
			check: func(t *testing.T, err error) {
				if _, ok := errors.AsType[*SessionCharsetError](err); !ok {
					t.Errorf("err = %v, want a SessionCharsetError", err)
				}
			}},
		{name: "UTC", closed: true, session: &fakeSession{values: []driver.Value{int64(0)}},
			check: func(t *testing.T, err error) {
				if !strings.HasPrefix(err.Error(), "session time zone is not UTC") {
					t.Errorf("err = %v, want the UTC refusal", err)
				}
			}},
		{name: "auto-zero", closed: true, session: &fakeSession{values: []driver.Value{int64(86400), []byte("STRICT_TRANS_TABLES")}},
			check: func(t *testing.T, err error) {
				if _, ok := errors.AsType[*AutoIncrementZeroModeError](err); !ok {
					t.Errorf("err = %v, want an AutoIncrementZeroModeError", err)
				}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeConnector{}
			if tc.session != nil {
				fc.sessions = []*fakeSession{tc.session}
			}
			var c driver.Connector = fc
			if tc.checked {
				c = warningsConnector{Connector: fc, addr: "db:3306"}
			}
			db, err := openOn(context.Background(), c, &config.DatabaseConfig{})
			if tc.check == nil {
				if err != nil || db == nil {
					t.Fatalf("openOn = %v, %v; want a pool", db, err)
				}
				_ = db.Close()
				return
			}
			if db != nil || err == nil {
				t.Fatalf("openOn = %v, %v; want nil and an error", db, err)
			}
			tc.check(t, err)
			if _, retryable := errors.AsType[*retryableError](err); retryable != tc.retryable {
				t.Errorf("retryable = %v, want %v: %v", retryable, tc.retryable, err)
			}
			if tc.closed && !tc.session.closed {
				t.Error("the failed pool's session was left open")
			}
		})
	}
}

// TestConnectWithRetryClassifiesFailures (M7c-F6): Manager retries, with its backoff, only a
// failure OpenPool marks retryable, and keeps its messages byte for byte: the last attempt's
// error after "failed after 3 retries: ", any other failure at once after "<name>: ".
func TestConnectWithRetryClassifiesFailures(t *testing.T) {
	pool := sql.OpenDB(&fakeConnector{})
	t.Cleanup(func() { _ = pool.Close() })
	mec := &MaxErrorCountError{Addr: "db:3306", Reason: "the session's max_error_count is still 0"}
	utc := errors.New("session time zone is not UTC")
	zero := &AutoIncrementZeroModeError{}
	for _, tc := range []struct {
		name   string
		script []error // one per attempt; nil returns pool
		calls  int
		want   string // the error text; "" wants pool
	}{
		{"retried", []error{&retryableError{errors.New("dial")}, &retryableError{errors.New("dial")}, &retryableError{errors.New("dial")}}, 3, "failed after 3 retries: dial"},
		{"no retry, connector", []error{mec}, 1, "source: " + mec.Error()},
		{"no retry, UTC", []error{utc}, 1, "source: " + utc.Error()},
		{"no retry, auto-zero", []error{zero}, 1, "source: " + zero.Error()},
		{"recovers", []error{&retryableError{errors.New("dial")}, nil}, 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			m := &Manager{backoff: time.Millisecond, open: func(context.Context, *config.DatabaseConfig, bool) (*sql.DB, error) {
				err := tc.script[calls]
				calls++
				if err != nil {
					return nil, err
				}
				return pool, nil
			}}
			db, err := m.connectWithRetry(context.Background(), "source", &config.DatabaseConfig{}, true)
			if calls != tc.calls {
				t.Errorf("%d attempts, want %d", calls, tc.calls)
			}
			if tc.want == "" {
				if err != nil || db != pool {
					t.Fatalf("connectWithRetry = %v, %v; want the scripted pool", db, err)
				}
				return
			}
			if db != nil || err == nil || err.Error() != tc.want {
				t.Fatalf("connectWithRetry = %v, %v; want %q", db, err, tc.want)
			}
			if _, retryable := errors.AsType[*retryableError](err); retryable {
				t.Errorf("Manager's error still carries the retry marker: %v", err)
			}
		})
	}
}

// TestConnectWithRetryRetriesAnUnparsableDSN (M7c-F6, Codex N1): a host that validation
// accepts but the driver cannot parse (root@localhost) failed in the old connect's connector
// build, so Manager tried it three times and said "failed after 3 retries: ". OpenPool marks
// that failure retryable and keeps the rule; a refused Global stays unretried.
func TestConnectWithRetryRetriesAnUnparsableDSN(t *testing.T) {
	cfg := &config.DatabaseConfig{Host: "root@localhost", Port: 3306, User: "test", Password: "test", Database: "sakila", TLS: "disable"}
	if _, err := OpenPool(context.Background(), cfg, true); err == nil {
		t.Fatal("OpenPool parsed a DSN with root@localhost as its host")
	} else if _, retryable := errors.AsType[*retryableError](err); !retryable {
		t.Errorf("OpenPool(root@localhost) = %v, want a retryable failure as the old connect's", err)
	}
	valid := *cfg
	valid.Host = "localhost"
	if _, err := OpenPool(context.Background(), &valid, true, Global{"time_zone", "'+01:00'"}); err == nil {
		t.Fatal("OpenPool accepted a refused global")
	} else if _, retryable := errors.AsType[*retryableError](err); retryable {
		t.Errorf("a refused global is retryable: %v", err)
	}

	calls := 0
	m := &Manager{backoff: time.Millisecond, open: func(ctx context.Context, cfg *config.DatabaseConfig, readsWarnings bool) (*sql.DB, error) {
		calls++
		return OpenPool(ctx, cfg, readsWarnings)
	}}
	_, err := m.connectWithRetry(context.Background(), "source", cfg, true)
	if calls != 3 || err == nil || !strings.HasPrefix(err.Error(), "failed after 3 retries: ") {
		t.Errorf("connectWithRetry(root@localhost) = %d attempts, %v; want 3 and \"failed after 3 retries: \"", calls, err)
	}
}
